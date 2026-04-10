"""
GCS format conversion utilities

Convert JSON array to JSONL.gz on GCS for BigQuery ingestion
"""

import gzip
import json
from typing import Optional, Callable, Dict, Any
from google.cloud import storage


def json_array_to_jsonl_gz(
    input_uri: str,
    output_uri: str,
    filter_func: Optional[Callable[[Dict], bool]] = None
) -> int:
    """
    Convert JSON array to JSONL.gz on GCS

    Args:
        input_uri: gs://bucket/path/file.json
        output_uri: gs://bucket/path/file.jsonl.gz
        filter_func: Optional filter, returns True to keep item

    Returns:
        Number of items written
    """
    client = storage.Client()

    # Parse URIs
    input_parts = input_uri.replace('gs://', '').split('/', 1)
    output_parts = output_uri.replace('gs://', '').split('/', 1)

    # Download
    bucket = client.bucket(input_parts[0])
    blob = bucket.blob(input_parts[1])
    data = json.loads(blob.download_as_string())

    if not isinstance(data, list):
        raise ValueError(f"Expected JSON array, got {type(data).__name__}")

    # Filter
    if filter_func:
        data = [item for item in data if filter_func(item)]

    # Convert and compress
    jsonl = '\n'.join(json.dumps(item, ensure_ascii=False) for item in data)
    compressed = gzip.compress(jsonl.encode('utf-8'))

    # Upload
    output_bucket = client.bucket(output_parts[0])
    output_blob = output_bucket.blob(output_parts[1])
    output_blob.upload_from_string(compressed, content_type='application/gzip')

    return len(data)


def filter_by_field(field: str, value: Any) -> Callable[[Dict], bool]:
    """
    Create filter function for item[field] == value

    Example:
        filter_func = filter_by_field('status', 'success')
    """
    return lambda item: item.get(field) == value
