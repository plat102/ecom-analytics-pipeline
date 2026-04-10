"""Data transformations for BigQuery schema compatibility."""

from .bigquery_schema import transform_event_for_bigquery

__all__ = ["transform_event_for_bigquery"]
