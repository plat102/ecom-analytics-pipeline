"""
BigQuery infrastructure utilities.

This module provides BigQuery client initialization and connection management.
"""

from .client import get_client

__all__ = ["get_client"]
