"""
BigQuery data loading utilities.

Shared loading logic for CLI scripts and Cloud Functions.
Provides functions for loading JSONL.gz files from GCS to BigQuery
using external table approach with typed schema queries.
"""

from typing import Optional
from google.cloud import bigquery

from .query_builders import (
    build_events_query,
    build_ip_locations_query,
    build_products_query
)


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
    For ip_locations table: Uses typed schema with 5 columns.
    For products table: Uses typed schema with 42 columns (flat react_data fields).

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
            query = build_events_query(external_table_id, final_table_id)
            print(f"Using typed schema query for events table")
        elif table_name == "ip_locations":
            query = build_ip_locations_query(external_table_id, final_table_id)
            print(f"Using typed schema query for ip_locations table")
        elif table_name == "products":
            query = build_products_query(external_table_id, final_table_id)
            print(f"Using typed schema query for products table")
        else:
            raise ValueError(f"Unknown table: {table_name}")

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
