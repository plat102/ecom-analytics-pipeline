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


def load_via_external_table(
    client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    table_name: str,
    gcs_uri: str
) -> int:
    """
    Load data from GCS to BigQuery using load job with JSON transformation.

    Loads JSONL.gz files and wraps each record in a JSON column with
    ingested_at timestamp.

    Args:
        client: BigQuery client
        project_id: GCP project ID
        dataset_id: BigQuery dataset ID
        table_name: Target table name
        gcs_uri: GCS URI pattern (may include wildcards)

    Returns:
        Number of rows inserted
    """
    temp_table_id = f"{project_id}.{dataset_id}.{table_name}_load_temp"
    final_table_id = f"{project_id}.{dataset_id}.{table_name}"

    print(f"Loading to temp table: {temp_table_id}")
    print(f"Source URI: {gcs_uri}")

    temp_table_schema = [bigquery.SchemaField("raw_doc", "JSON")]

    try:
        client.delete_table(temp_table_id, not_found_ok=True)
        temp_table = bigquery.Table(temp_table_id, schema=temp_table_schema)
        client.create_table(temp_table)
        print(f"Created temp table")

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            json_extension="GEOJSON"
        )

        print(f"Starting load job...")
        load_job = client.load_table_from_uri(
            gcs_uri,
            temp_table_id,
            job_config=job_config
        )

        load_job.result()
        print(f"Load job completed: {load_job.output_rows:,} rows loaded to temp table")

        query = f"""
        INSERT INTO `{final_table_id}` (raw_doc, ingested_at)
        SELECT
            raw_doc,
            CURRENT_TIMESTAMP() as ingested_at
        FROM `{temp_table_id}`
        """

        print(f"Inserting into final table: {final_table_id}")
        query_job = client.query(query)
        query_job.result()

        rows_inserted = query_job.num_dml_affected_rows
        print(f"Successfully inserted {rows_inserted:,} rows")

        return rows_inserted

    finally:
        client.delete_table(temp_table_id, not_found_ok=True)
        print(f"Cleaned up temp table")


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
