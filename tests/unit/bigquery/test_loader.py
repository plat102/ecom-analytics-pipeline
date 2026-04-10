"""
Tests for BigQuery loader utilities.

Tests ensure that loader functions work correctly for constructing GCS URIs,
parsing table names from paths, and basic validation logic.
"""

import pytest
from common.bigquery.loader import (
    construct_gcs_uri,
    parse_table_from_gcs_path
)


class TestConstructGcsUri:
    """Tests for construct_gcs_uri function."""

    def test_events_with_date(self):
        """Test events URI construction with date."""
        uri = construct_gcs_uri("raw_glamira", "events", "20260404")

        assert uri == "gs://raw_glamira/raw/events/events_20260404_part*.jsonl.gz"

    def test_events_without_date(self):
        """Test events URI construction without date."""
        uri = construct_gcs_uri("raw_glamira", "events")

        assert uri == "gs://raw_glamira/raw/events/events_*.jsonl.gz"

    def test_ip_locations_with_date(self):
        """Test ip_locations URI construction with date."""
        uri = construct_gcs_uri("raw_glamira", "ip_locations", "20260404")

        assert uri == "gs://raw_glamira/raw/ip_locations/ip_locations_20260404.jsonl.gz"

    def test_ip_locations_without_date(self):
        """Test ip_locations URI construction without date."""
        uri = construct_gcs_uri("raw_glamira", "ip_locations")

        assert uri == "gs://raw_glamira/raw/ip_locations/ip_locations_*.jsonl.gz"

    def test_products_with_date(self):
        """Test products URI construction with date."""
        uri = construct_gcs_uri("raw_glamira", "products", "20260329")

        assert uri == "gs://raw_glamira/raw/products/products_20260329.jsonl.gz"

    def test_products_without_date(self):
        """Test products URI construction without date."""
        uri = construct_gcs_uri("raw_glamira", "products")

        assert uri == "gs://raw_glamira/raw/products/products_*.jsonl.gz"

    def test_custom_bucket(self):
        """Test URI construction with custom bucket name."""
        uri = construct_gcs_uri("my-custom-bucket", "events", "20260404")

        assert uri.startswith("gs://my-custom-bucket/")

    def test_unknown_table_raises_error(self):
        """Test that unknown table name raises ValueError."""
        with pytest.raises(ValueError, match="Unknown table"):
            construct_gcs_uri("raw_glamira", "unknown_table")

    def test_all_uris_use_jsonl_gz(self):
        """Test that all URIs use .jsonl.gz extension."""
        tables = ["events", "ip_locations", "products"]
        for table in tables:
            uri = construct_gcs_uri("raw_glamira", table, "20260404")
            assert uri.endswith(".jsonl.gz")

    def test_all_uris_under_raw_directory(self):
        """Test that all URIs are under raw/ directory."""
        tables = ["events", "ip_locations", "products"]
        for table in tables:
            uri = construct_gcs_uri("raw_glamira", table, "20260404")
            assert "/raw/" in uri


class TestParseTableFromGcsPath:
    """Tests for parse_table_from_gcs_path function."""

    def test_events_path(self):
        """Test parsing events table from path."""
        table = parse_table_from_gcs_path("raw/events/events_20260404_part001.jsonl.gz")

        assert table == "events"

    def test_ip_locations_path(self):
        """Test parsing ip_locations table from path."""
        table = parse_table_from_gcs_path("raw/ip_locations/ip_locations_20260404.jsonl.gz")

        assert table == "ip_locations"

    def test_products_path(self):
        """Test parsing products table from path."""
        table = parse_table_from_gcs_path("raw/products/products_20260329.jsonl.gz")

        assert table == "products"

    def test_non_raw_path_returns_none(self):
        """Test that non-raw/ paths return None."""
        table = parse_table_from_gcs_path("other/path/file.jsonl.gz")

        assert table is None

    def test_unrecognized_table_returns_none(self):
        """Test that unrecognized table name returns None."""
        table = parse_table_from_gcs_path("raw/unknown/file.jsonl.gz")

        assert table is None

    def test_empty_path_returns_none(self):
        """Test that empty path returns None."""
        table = parse_table_from_gcs_path("")

        assert table is None

    def test_path_with_subdirectories(self):
        """Test that paths with subdirectories still match (uses startswith)."""
        table = parse_table_from_gcs_path("raw/events/2026/04/events_20260404_part001.jsonl.gz")

        assert table == "events"

    def test_path_case_sensitivity(self):
        """Test that path matching is case-sensitive."""
        table = parse_table_from_gcs_path("RAW/events/events_20260404.jsonl.gz")

        assert table is None

    def test_all_supported_tables(self):
        """Test that all supported tables are recognized."""
        paths_and_tables = [
            ("raw/events/events_20260404_part001.jsonl.gz", "events"),
            ("raw/ip_locations/ip_locations_20260404.jsonl.gz", "ip_locations"),
            ("raw/products/products_20260329.jsonl.gz", "products"),
        ]

        for path, expected_table in paths_and_tables:
            result = parse_table_from_gcs_path(path)
            assert result == expected_table, f"Failed for path: {path}"


class TestLoaderConsistency:
    """Test consistency between construct_gcs_uri and parse_table_from_gcs_path."""

    def test_roundtrip_consistency(self):
        """Test that constructed URIs can be parsed back to table names."""
        tables = ["events", "ip_locations", "products"]

        for table in tables:
            uri = construct_gcs_uri("raw_glamira", table, "20260404")

            file_path = uri.replace("gs://raw_glamira/", "")

            if "*" not in file_path:
                parsed_table = parse_table_from_gcs_path(file_path)
                assert parsed_table == table

    def test_uri_pattern_matches_parser_expectations(self):
        """Test that URI patterns match what parser expects."""
        uri = construct_gcs_uri("raw_glamira", "events", "20260404")

        assert uri.startswith("gs://raw_glamira/raw/events/")

        file_path = "raw/events/events_20260404_part001.jsonl.gz"
        parsed = parse_table_from_gcs_path(file_path)
        assert parsed == "events"
