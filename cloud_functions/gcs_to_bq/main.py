"""
Cloud Function: Auto-load GCS files to BigQuery on finalize event.

Trigger: GCS object finalize on gs://raw_glamira/raw/**
Behavior: Parse GCS path to determine target table, load to BigQuery

NOTE: This function imports from common/ module (copied during deployment).
See copy_common.sh and deploy.sh for deployment preparation.

NOTE: Raw layer may contain duplicates on Cloud Function retry.
Deduplicate in P07 dbt:
  QUALIFY ROW_NUMBER() OVER (PARTITION BY _id ORDER BY ingested_at DESC) = 1
"""

import json
import functions_framework
from google.cloud import bigquery
from typing import Dict, Any

from common.bigquery import get_client, parse_table_from_gcs_path
from common.bigquery.query_builders import (
    build_events_query,
    build_ip_locations_query,
    build_products_query
)


PROJECT_ID = "ecom-analytics-tp"
DATASET_ID = "glamira_raw"


def load_to_bigquery(gcs_uri: str, table_name: str) -> Dict[str, Any]:
    """
    Load GCS file to BigQuery using external table approach.

    Args:
        gcs_uri: Full GCS URI (gs://bucket/path/file.jsonl.gz)
        table_name: Target table name (events, ip_locations, products)

    Returns:
        Dict with load results (rows_inserted, success, error)
    """
    client = get_client(PROJECT_ID)

    external_table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}_external_temp"
    final_table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"

    try:
        # Create external table
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

        client.delete_table(external_table_id, not_found_ok=True)
        external_table = client.create_table(external_table)

        # Use shared query builders
        if table_name == "events":
            query = build_events_query(external_table_id, final_table_id)
        elif table_name == "ip_locations":
            query = build_ip_locations_query(external_table_id, final_table_id)
        elif table_name == "products":
            query = build_products_query(external_table_id, final_table_id)
        else:
            raise ValueError(f"Unknown table: {table_name}")

        # Execute query
        query_job = client.query(query)
        query_job.result()

        rows_inserted = query_job.num_dml_affected_rows

        return {
            "success": True,
            "rows_inserted": rows_inserted,
            "table": final_table_id,
            "gcs_uri": gcs_uri
        }

    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "table": final_table_id,
            "gcs_uri": gcs_uri
        }

    finally:
        client.delete_table(external_table_id, not_found_ok=True)


@functions_framework.cloud_event
def gcs_to_bigquery(cloud_event):
    """
    Cloud Function entry point.

    Triggered on GCS object finalize event.
    """
    data = cloud_event.data

    bucket_name = data["bucket"]
    file_path = data["name"]
    gcs_uri = f"gs://{bucket_name}/{file_path}"

    # Structured logging
    log_entry = {
        "severity": "INFO",
        "message": "GCS file created",
        "gcs_uri": gcs_uri,
        "bucket": bucket_name,
        "file_path": file_path
    }
    print(json.dumps(log_entry))

    # Parse table name from path
    table_name = parse_table_from_gcs_path(file_path)

    if not table_name:
        log_entry = {
            "severity": "INFO",
            "message": "Skipping file - not in raw/ directory",
            "file_path": file_path
        }
        print(json.dumps(log_entry))
        return

    # Load to BigQuery
    log_entry = {
        "severity": "INFO",
        "message": "Starting BigQuery load",
        "table": table_name,
        "gcs_uri": gcs_uri
    }
    print(json.dumps(log_entry))

    result = load_to_bigquery(gcs_uri, table_name)

    if result["success"]:
        log_entry = {
            "severity": "INFO",
            "message": "BigQuery load successful",
            "table": result["table"],
            "rows_inserted": result["rows_inserted"],
            "gcs_uri": gcs_uri
        }
        print(json.dumps(log_entry))
    else:
        log_entry = {
            "severity": "ERROR",
            "message": "BigQuery load failed",
            "table": result["table"],
            "error": result["error"],
            "gcs_uri": gcs_uri
        }
        print(json.dumps(log_entry))
        raise Exception(f"BigQuery load failed: {result['error']}")
