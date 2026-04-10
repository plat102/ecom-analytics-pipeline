"""
Tests for BigQuery typed schema query builders.

Tests ensure that query builders generate correct SQL INSERT statements
for all three tables: events, ip_locations, products.
"""

import pytest
from common.bigquery.query_builders import (
    build_events_query,
    build_ip_locations_query,
    build_products_query
)


class TestIpLocationsQueryBuilder:
    """Tests for ip_locations query builder."""

    def test_query_structure(self):
        """Test that ip_locations query has correct structure."""
        query = build_ip_locations_query("ext_table", "final_table")

        assert "INSERT INTO `final_table`" in query
        assert "FROM `ext_table`" in query

    def test_all_fields_present(self):
        """Test that all 5 fields are present in query."""
        query = build_ip_locations_query("ext", "final")

        required_fields = ["ip", "country", "region", "city", "ingested_at"]
        for field in required_fields:
            assert field in query

    def test_json_parsing(self):
        """Test that query uses JSON_VALUE for field extraction."""
        query = build_ip_locations_query("ext", "final")

        assert "JSON_VALUE(doc, '$.ip')" in query
        assert "JSON_VALUE(doc, '$.country')" in query
        assert "PARSE_JSON(line)" in query

    def test_timestamp_generation(self):
        """Test that ingested_at uses CURRENT_TIMESTAMP."""
        query = build_ip_locations_query("ext", "final")

        assert "CURRENT_TIMESTAMP() AS ingested_at" in query

    def test_null_filtering(self):
        """Test that query filters out null/empty lines."""
        query = build_ip_locations_query("ext", "final")

        assert "WHERE line IS NOT NULL AND TRIM(line) != ''" in query


class TestProductsQueryBuilder:
    """Tests for products query builder."""

    def test_query_structure(self):
        """Test that products query has correct structure."""
        query = build_products_query("ext_table", "final_table")

        assert "INSERT INTO `final_table`" in query
        assert "FROM `ext_table`" in query

    def test_all_key_fields_present(self):
        """Test that key fields are present in query."""
        query = build_products_query("ext", "final")

        key_fields = [
            "product_id", "product_name", "sku", "price",
            "store_code", "currency_code", "ingested_at"
        ]
        for field in key_fields:
            assert field in query

    def test_repeated_field_handling(self):
        """Test that visible_contents REPEATED field is handled correctly."""
        query = build_products_query("ext", "final")

        assert "visible_contents" in query
        assert "ARRAY(" in query
        assert "JSON_VALUE(value)" in query
        assert "UNNEST(JSON_QUERY_ARRAY(doc, '$.visible_contents'))" in query

    def test_type_casting(self):
        """Test that numeric fields use CAST."""
        query = build_products_query("ext", "final")

        assert "CAST(JSON_VALUE(doc, '$.http_status') AS INT64)" in query
        assert "CAST(JSON_VALUE(doc, '$.fallback_used') AS BOOL)" in query
        assert "CAST(JSON_VALUE(doc, '$.none_metal_weight') AS FLOAT64)" in query

    def test_all_42_fields_present(self):
        """Test that all 42 product fields are in SELECT clause."""
        query = build_products_query("ext", "final")

        all_fields = [
            "product_id", "url", "status", "http_status", "error_message",
            "fallback_used", "product_name", "sku", "attribute_set_id",
            "attribute_set", "type_id", "product_type", "product_type_value",
            "price", "min_price", "max_price", "min_price_format",
            "max_price_format", "gold_weight", "none_metal_weight",
            "fixed_silver_weight", "material_design", "qty", "collection",
            "collection_id", "category", "category_name", "store_code",
            "gender", "platinum_palladium_info_in_alloy",
            "bracelet_without_chain", "show_popup_quantity_eternity",
            "visible_contents", "configure_mode", "included_chain_weight",
            "currency_code", "tax_rate", "primary_image_url",
            "primary_video_url", "gender_label", "is_solid", "ingested_at"
        ]

        for field in all_fields:
            assert field in query, f"Field {field} not found in query"


class TestEventsQueryBuilder:
    """Tests for events query builder."""

    def test_query_structure(self):
        """Test that events query has correct structure."""
        query = build_events_query("ext_table", "final_table")

        assert "INSERT INTO `final_table`" in query
        assert "FROM `ext_table`" in query

    def test_all_key_fields_present(self):
        """Test that key fields are present in query."""
        query = build_events_query("ext", "final")

        key_fields = [
            "_id", "device_id", "ip", "collection", "time_stamp",
            "ingested_at"
        ]
        for field in key_fields:
            assert field in query

    def test_nested_structures(self):
        """Test that nested structures (option, cart_products) are handled."""
        query = build_events_query("ext", "final")

        assert "ARRAY(" in query
        assert "SELECT AS STRUCT" in query
        assert "UNNEST(" in query

    def test_option_field_handling(self):
        """Test that option field is converted to REPEATED RECORD."""
        query = build_events_query("ext", "final")

        assert "option_id" in query
        assert "option_label" in query
        assert "value_id" in query
        assert "value_label" in query
        assert "quality" in query
        assert "quality_label" in query

    def test_cart_products_nested_option(self):
        """Test that cart_products with nested option is handled."""
        query = build_events_query("ext", "final")

        assert "cart_products" in query
        assert "UNNEST(JSON_QUERY_ARRAY(doc, '$.cart_products'))" in query

    def test_json_type_checking(self):
        """Test that query checks JSON type before processing arrays."""
        query = build_events_query("ext", "final")

        assert "JSON_TYPE(JSON_QUERY(doc, '$.option')) = 'array'" in query
        assert "CASE" in query
        assert "THEN JSON_QUERY_ARRAY" in query
        assert "ELSE []" in query

    def test_null_handling(self):
        """Test that query handles null values appropriately."""
        query = build_events_query("ext", "final")

        assert "IFNULL(JSON_VALUE(doc, '$.user_id_db'), '')" in query
        assert "IFNULL(JSON_VALUE(doc, '$.email_address'), '')" in query


class TestQueryBuilderConsistency:
    """Test consistency across all query builders."""

    def test_all_use_parse_json(self):
        """Test that all queries use PARSE_JSON pattern."""
        queries = [
            build_events_query("ext", "final"),
            build_ip_locations_query("ext", "final"),
            build_products_query("ext", "final")
        ]

        for query in queries:
            assert "PARSE_JSON(line) AS doc" in query

    def test_all_filter_null_lines(self):
        """Test that all queries filter null/empty lines."""
        queries = [
            build_events_query("ext", "final"),
            build_ip_locations_query("ext", "final"),
            build_products_query("ext", "final")
        ]

        for query in queries:
            assert "WHERE line IS NOT NULL AND TRIM(line) != ''" in query

    def test_all_add_ingested_at(self):
        """Test that all queries add ingested_at timestamp."""
        queries = [
            build_events_query("ext", "final"),
            build_ip_locations_query("ext", "final"),
            build_products_query("ext", "final")
        ]

        for query in queries:
            assert "CURRENT_TIMESTAMP() AS ingested_at" in query

    def test_table_id_substitution(self):
        """Test that external and final table IDs are properly substituted."""
        ext_table = "project.dataset.external_temp"
        final_table = "project.dataset.final"

        queries = [
            build_events_query(ext_table, final_table),
            build_ip_locations_query(ext_table, final_table),
            build_products_query(ext_table, final_table)
        ]

        for query in queries:
            assert ext_table in query
            assert final_table in query
