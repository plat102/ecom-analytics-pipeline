"""
BigQuery loader utilities.

Provides generic functions for loading data from GCS to BigQuery,
shared between manual load scripts and Cloud Functions.
"""

from typing import Optional
from google.cloud import bigquery


def construct_gcs_uri(
    bucket: str,
    table_name: str,
    date: Optional[str] = None
) -> str:
    """
    Construct GCS URI pattern for the given table and date.

    Args:
        bucket: GCS bucket name
        table_name: Table name (events, ip_locations, products)
        date: Optional date string in YYYYMMDD format

    Returns:
        GCS URI pattern (may include wildcards)

    Examples:
        >>> construct_gcs_uri("raw_glamira", "events", "20260404")
        'gs://raw_glamira/raw/events/events_20260404_part*.jsonl.gz'
    """
    if table_name == "events":
        if date:
            pattern = f"events_{date}_part*.jsonl.gz"
        else:
            pattern = "events_*.jsonl.gz"
        return f"gs://{bucket}/raw/events/{pattern}"

    elif table_name == "ip_locations":
        if date:
            pattern = f"ip_locations_{date}.jsonl.gz"
        else:
            pattern = "ip_locations_*.jsonl.gz"
        return f"gs://{bucket}/raw/ip_locations/{pattern}"

    elif table_name == "products":
        if date:
            pattern = f"products_{date}.jsonl.gz"
        else:
            pattern = "products_*.jsonl.gz"
        return f"gs://{bucket}/raw/products/{pattern}"

    else:
        raise ValueError(f"Unknown table: {table_name}")


def _build_events_typed_query(external_table_id: str, final_table_id: str) -> str:
    """
    Build INSERT query for events table with typed schema (33 columns, 47 field paths).

    Handles:
    - Converting option (both object and array) to REPEATED RECORD
    - Converting cart_products with nested option to RECORD REPEATED structure
    - Null handling: empty arrays for REPEATED fields, NULL for scalar fields

    Args:
        external_table_id: Temporary external table ID
        final_table_id: Target events table ID

    Returns:
        SQL INSERT query string
    """
    return f"""
    INSERT INTO `{final_table_id}` (
        _id, device_id, user_id_db, email_address, ip, store_id, user_agent, resolution,
        utm_source, utm_medium, api_version, collection, time_stamp, local_time,
        collect_id, current_url, referrer_url, show_recommendation,
        product_id, viewing_product_id,
        option,
        price, currency, cat_id, key_search,
        cart_products,
        order_id, is_paypal, recommendation, recommendation_product_id,
        recommendation_product_position, recommendation_clicked_position,
        ingested_at
    )
    SELECT
        JSON_VALUE(doc, '$._id') AS _id,
        JSON_VALUE(doc, '$.device_id') AS device_id,
        IFNULL(JSON_VALUE(doc, '$.user_id_db'), '') AS user_id_db,
        IFNULL(JSON_VALUE(doc, '$.email_address'), '') AS email_address,
        JSON_VALUE(doc, '$.ip') AS ip,
        CAST(JSON_VALUE(doc, '$.store_id') AS STRING) AS store_id,
        JSON_VALUE(doc, '$.user_agent') AS user_agent,
        JSON_VALUE(doc, '$.resolution') AS resolution,
        CAST(JSON_VALUE(doc, '$.utm_source') AS STRING) AS utm_source,
        CAST(JSON_VALUE(doc, '$.utm_medium') AS STRING) AS utm_medium,
        JSON_VALUE(doc, '$.api_version') AS api_version,
        JSON_VALUE(doc, '$.collection') AS collection,
        CAST(JSON_VALUE(doc, '$.time_stamp') AS INT64) AS time_stamp,
        JSON_VALUE(doc, '$.local_time') AS local_time,
        JSON_VALUE(doc, '$.collect_id') AS collect_id,
        JSON_VALUE(doc, '$.current_url') AS current_url,
        JSON_VALUE(doc, '$.referrer_url') AS referrer_url,
        JSON_VALUE(doc, '$.show_recommendation') AS show_recommendation,
        JSON_VALUE(doc, '$.product_id') AS product_id,
        JSON_VALUE(doc, '$.viewing_product_id') AS viewing_product_id,

        -- Convert option to REPEATED RECORD
        -- Note: option field is pre-transformed in exporter (OBJECT -> ARRAY)
        -- This query assumes option is already in ARRAY format from GCS files
        ARRAY(
            SELECT AS STRUCT
                JSON_VALUE(opt, '$.option_id') AS option_id,
                JSON_VALUE(opt, '$.option_label') AS option_label,
                JSON_VALUE(opt, '$.value_id') AS value_id,
                JSON_VALUE(opt, '$.value_label') AS value_label,
                JSON_VALUE(opt, '$.quality') AS quality,
                JSON_VALUE(opt, '$.quality_label') AS quality_label
            FROM UNNEST(
                CASE
                    WHEN JSON_TYPE(JSON_QUERY(doc, '$.option')) = 'array'
                    THEN JSON_QUERY_ARRAY(doc, '$.option')
                    ELSE []
                END
            ) AS opt
        ) AS option,

        JSON_VALUE(doc, '$.price') AS price,
        JSON_VALUE(doc, '$.currency') AS currency,
        JSON_VALUE(doc, '$.cat_id') AS cat_id,
        JSON_VALUE(doc, '$.key_search') AS key_search,

        ARRAY(
            SELECT AS STRUCT
                CAST(JSON_VALUE(cp, '$.product_id') AS INT64) AS product_id,
                CAST(JSON_VALUE(cp, '$.amount') AS INT64) AS amount,
                JSON_VALUE(cp, '$.price') AS price,
                JSON_VALUE(cp, '$.currency') AS currency,
                ARRAY(
                    SELECT AS STRUCT
                        CAST(JSON_VALUE(opt, '$.option_id') AS INT64) AS option_id,
                        JSON_VALUE(opt, '$.option_label') AS option_label,
                        CAST(JSON_VALUE(opt, '$.value_id') AS INT64) AS value_id,
                        JSON_VALUE(opt, '$.value_label') AS value_label
                    FROM UNNEST(
                        CASE
                            WHEN JSON_TYPE(JSON_QUERY(cp, '$.option')) = 'array'
                            THEN JSON_QUERY_ARRAY(cp, '$.option')
                            ELSE []
                        END
                    ) AS opt
                ) AS option
            FROM UNNEST(JSON_QUERY_ARRAY(doc, '$.cart_products')) AS cp
        ) AS cart_products,

        CAST(JSON_VALUE(doc, '$.order_id') AS STRING) AS order_id,
        CAST(JSON_VALUE(doc, '$.is_paypal') AS BOOL) AS is_paypal,
        CAST(JSON_VALUE(doc, '$.recommendation') AS BOOL) AS recommendation,
        JSON_VALUE(doc, '$.recommendation_product_id') AS recommendation_product_id,
        CAST(JSON_VALUE(doc, '$.recommendation_product_position') AS STRING) AS recommendation_product_position,
        CAST(JSON_VALUE(doc, '$.recommendation_clicked_position') AS INT64) AS recommendation_clicked_position,

        CURRENT_TIMESTAMP() AS ingested_at
    FROM (
        SELECT PARSE_JSON(line) AS doc
        FROM `{external_table_id}`
        WHERE line IS NOT NULL AND TRIM(line) != ''
    )
    """


def load_via_external_table(
    client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    table_name: str,
    gcs_uri: str
) -> int:
    """
    Load data from GCS to BigQuery using external table approach.

    Reads JSONL.gz files as raw text lines via external table,
    then parses JSON in SQL query when inserting to final table.

    For events table: Uses typed schema with 33 columns (47 field paths including nested).
    For other tables: Uses simple JSON column schema (raw_doc + ingested_at).

    Args:
        client: BigQuery client
        project_id: GCP project ID
        dataset_id: BigQuery dataset ID
        table_name: Target table name
        gcs_uri: GCS URI pattern (may include wildcards)

    Returns:
        Number of rows inserted
    """
    external_table_id = f"{project_id}.{dataset_id}.{table_name}_external_temp"
    final_table_id = f"{project_id}.{dataset_id}.{table_name}"

    print(f"Creating external table: {external_table_id}")
    print(f"Source URI: {gcs_uri}")

    external_config = bigquery.ExternalConfig("CSV")
    external_config.source_uris = [gcs_uri]
    external_config.options.skip_leading_rows = 0
    external_config.options.field_delimiter = "\u0001"
    external_config.options.quote_character = ""
    external_config.options.allow_quoted_newlines = False
    external_config.options.allow_jagged_rows = True
    external_config.schema = [bigquery.SchemaField("line", "STRING")]

    external_table = bigquery.Table(external_table_id)
    external_table.external_data_configuration = external_config

    try:
        client.delete_table(external_table_id, not_found_ok=True)
        external_table = client.create_table(external_table)
        print(f"External table created")

        if table_name == "events":
            query = _build_events_typed_query(external_table_id, final_table_id)
            print(f"Using typed schema query for events table")
        else:
            query = f"""
            INSERT INTO `{final_table_id}` (raw_doc, ingested_at)
            SELECT
                PARSE_JSON(line) as raw_doc,
                CURRENT_TIMESTAMP() as ingested_at
            FROM `{external_table_id}`
            WHERE line IS NOT NULL AND TRIM(line) != ''
            """
            print(f"Using JSON column schema for {table_name} table")

        print(f"Running INSERT query to final table: {final_table_id}")
        query_job = client.query(query)
        query_job.result()

        rows_inserted = query_job.num_dml_affected_rows
        print(f"Successfully inserted {rows_inserted:,} rows")

        return rows_inserted

    finally:
        client.delete_table(external_table_id, not_found_ok=True)
        print(f"Cleaned up external table")


def validate_table(
    client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    table_name: str
) -> dict:
    """
    Validate table by querying basic statistics.

    Args:
        client: BigQuery client
        project_id: GCP project ID
        dataset_id: BigQuery dataset ID
        table_name: Table to validate

    Returns:
        Dictionary with validation results:
        {
            'total_rows': int,
            'earliest_ingestion': datetime,
            'latest_ingestion': datetime,
            'distinct_ingestion_dates': int
        }
    """
    table_id = f"{project_id}.{dataset_id}.{table_name}"

    query = f"""
    SELECT
        COUNT(*) as total_rows,
        MIN(ingested_at) as earliest_ingestion,
        MAX(ingested_at) as latest_ingestion,
        COUNT(DISTINCT DATE(ingested_at)) as distinct_ingestion_dates
    FROM `{table_id}`
    """

    result = client.query(query).result()
    row = list(result)[0]

    return {
        "total_rows": row.total_rows,
        "earliest_ingestion": row.earliest_ingestion,
        "latest_ingestion": row.latest_ingestion,
        "distinct_ingestion_dates": row.distinct_ingestion_dates
    }


def parse_table_from_gcs_path(file_path: str) -> Optional[str]:
    """
    Parse BigQuery table name from GCS file path.

    Used by Cloud Functions to determine which table to load based on
    the uploaded file path.

    Args:
        file_path: GCS file path (e.g., 'raw/events/events_20260404_part001.jsonl.gz')

    Returns:
        Table name ('events', 'ip_locations', 'products') or None if not recognized

    Examples:
        >>> parse_table_from_gcs_path('raw/events/events_20260404_part001.jsonl.gz')
        'events'
        >>> parse_table_from_gcs_path('raw/ip_locations/ip_locations_20260404.jsonl.gz')
        'ip_locations'
        >>> parse_table_from_gcs_path('other/path/file.txt')
        None
    """
    if file_path.startswith('raw/events/'):
        return 'events'
    elif file_path.startswith('raw/ip_locations/'):
        return 'ip_locations'
    elif file_path.startswith('raw/products/'):
        return 'products'
    return None
