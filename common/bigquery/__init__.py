"""
BigQuery infrastructure utilities.

This module provides BigQuery client initialization, connection management,
and data loading utilities shared across CLI and Cloud Functions.
"""

from .client import get_client
from .loader import (
    construct_gcs_uri,
    load_via_external_table,
    validate_table,
    parse_table_from_gcs_path
)
from .query_builders import (
    build_events_query,
    build_ip_locations_query,
    build_products_query
)

__all__ = [
    "get_client",
    "construct_gcs_uri",
    "load_via_external_table",
    "validate_table",
    "parse_table_from_gcs_path",
    "build_events_query",
    "build_ip_locations_query",
    "build_products_query",
]
