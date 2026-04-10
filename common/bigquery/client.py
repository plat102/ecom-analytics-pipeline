"""
BigQuery client wrapper.

Provides a centralized BigQuery client initialization for use across
CLI scripts and Cloud Functions.
"""

from typing import Optional
from google.cloud import bigquery


def get_client(project_id: Optional[str] = None) -> bigquery.Client:
    """
    Get a BigQuery client instance.

    Args:
        project_id: GCP project ID. If None, uses default from environment.

    Returns:
        bigquery.Client: Initialized BigQuery client

    Examples:
        >>> client = get_client("my-project")
        >>> client = get_client()  # Uses default project from environment
    """
    if project_id:
        return bigquery.Client(project=project_id)
    return bigquery.Client()
