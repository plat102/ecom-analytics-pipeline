"""
Tests for event transformation utilities.
"""

import pytest
from common.transformations.bigquery_schema import (
    normalize_option_field,
    transform_event_for_bigquery
)


class TestNormalizeOptionField:
    """Test option field normalization"""

    def test_none_returns_empty_array(self):
        assert normalize_option_field(None) == []

    def test_empty_string_returns_empty_array(self):
        assert normalize_option_field("") == []

    def test_empty_dict_returns_empty_array(self):
        assert normalize_option_field({}) == []

    def test_empty_list_returns_empty_array(self):
        assert normalize_option_field([]) == []

    def test_object_with_empty_values(self):
        """Test OBJECT structure with empty string values"""
        result = normalize_option_field({
            "alloy": "",
            "diamond": "",
            "shapediamond": ""
        })

        assert len(result) == 3
        assert result[0]["option_label"] == "alloy"
        assert result[0]["value_label"] is None
        assert result[0]["option_id"] is None

    def test_object_with_values(self):
        """Test OBJECT structure with actual filter values"""
        result = normalize_option_field({
            "alloy": "gold",
            "diamond": "ruby",
            "shapediamond": "round"
        })

        assert len(result) == 3

        alloy_record = next(r for r in result if r["option_label"] == "alloy")
        assert alloy_record == {
            "option_id": None,
            "option_label": "alloy",
            "value_id": None,
            "value_label": "gold",
            "quality": None,
            "quality_label": None
        }

        diamond_record = next(r for r in result if r["option_label"] == "diamond")
        assert diamond_record["value_label"] == "ruby"

    def test_object_with_mixed_values(self):
        """Test OBJECT with mix of empty and non-empty values"""
        result = normalize_option_field({
            "alloy": "",
            "diamond": "aquamarine",
            "shapediamond": ""
        })

        assert len(result) == 3

        diamond_record = next(r for r in result if r["option_label"] == "diamond")
        assert diamond_record["value_label"] == "aquamarine"

        alloy_record = next(r for r in result if r["option_label"] == "alloy")
        assert alloy_record["value_label"] is None

    def test_array_passthrough_with_full_fields(self):
        """Test ARRAY structure passes through with all fields"""
        input_array = [
            {
                "option_label": "alloy",
                "option_id": "198911",
                "value_label": "red-750",
                "value_id": "1593626"
            }
        ]

        result = normalize_option_field(input_array)

        assert len(result) == 1
        assert result[0]["option_label"] == "alloy"
        assert result[0]["option_id"] == "198911"
        assert result[0]["value_label"] == "red-750"
        assert result[0]["value_id"] == "1593626"
        assert result[0]["quality"] is None
        assert result[0]["quality_label"] is None

    def test_array_with_quality_fields(self):
        """Test ARRAY structure with quality fields"""
        input_array = [
            {
                "option_label": "stone/diamonds",
                "option_id": "57695",
                "value_label": "diamond-Brillant",
                "value_id": "308213",
                "quality": "A",
                "quality_label": "I"
            }
        ]

        result = normalize_option_field(input_array)

        assert len(result) == 1
        assert result[0]["quality"] == "A"
        assert result[0]["quality_label"] == "I"

    def test_array_with_empty_value_labels(self):
        """Test ARRAY structure with empty value_label (common in add_to_cart)"""
        input_array = [
            {
                "option_label": "alloy",
                "option_id": "105683",
                "value_label": "",
                "value_id": "765253"
            }
        ]

        result = normalize_option_field(input_array)

        assert len(result) == 1
        assert result[0]["value_label"] == ""

    def test_array_multiple_items(self):
        """Test ARRAY with multiple option items"""
        input_array = [
            {
                "option_label": "alloy",
                "option_id": "105683",
                "value_label": "gold",
                "value_id": "765253"
            },
            {
                "option_label": "diamond",
                "option_id": "105685",
                "value_label": "ruby",
                "value_id": "765297"
            }
        ]

        result = normalize_option_field(input_array)

        assert len(result) == 2
        assert result[0]["option_label"] == "alloy"
        assert result[1]["option_label"] == "diamond"


class TestTransformEventForBigQuery:
    """Test full document transformation"""

    def test_transform_option_object(self):
        """Test document with option as OBJECT"""
        doc = {
            "_id": "5ed8cb2cfeae1f377811165c",
            "collection": "view_listing_page",
            "option": {
                "alloy": "",
                "diamond": "",
                "shapediamond": ""
            }
        }

        result = transform_event_for_bigquery(doc)

        assert result["_id"] == "5ed8cb2cfeae1f377811165c"
        assert result["collection"] == "view_listing_page"
        assert isinstance(result["option"], list)
        assert len(result["option"]) == 3

    def test_transform_option_array(self):
        """Test document with option as ARRAY"""
        doc = {
            "_id": "5ed8cb2b34103036e28df46c",
            "collection": "select_product_option",
            "option": [
                {
                    "option_label": "alloy",
                    "option_id": "198911",
                    "value_label": "red-750",
                    "value_id": "1593626"
                }
            ]
        }

        result = transform_event_for_bigquery(doc)

        assert result["_id"] == "5ed8cb2b34103036e28df46c"
        assert isinstance(result["option"], list)
        assert len(result["option"]) == 1

    def test_transform_cart_products_with_empty_string(self):
        """Test cart_products with option as empty string"""
        doc = {
            "_id": "123",
            "collection": "checkout",
            "cart_products": [
                {
                    "product_id": 103324,
                    "option": ""  # Empty string case
                }
            ]
        }

        result = transform_event_for_bigquery(doc)

        assert len(result["cart_products"]) == 1
        assert result["cart_products"][0]["option"] == []

    def test_transform_cart_products_with_array(self):
        """Test cart_products with option as ARRAY (normal case)"""
        doc = {
            "_id": "123",
            "collection": "checkout",
            "cart_products": [
                {
                    "product_id": 103324,
                    "option": [
                        {
                            "option_id": 261151,
                            "option_label": "diamond",
                            "value_id": 2166253,
                            "value_label": "White Sapphire"
                        }
                    ]
                }
            ]
        }

        result = transform_event_for_bigquery(doc)

        assert len(result["cart_products"]) == 1
        assert isinstance(result["cart_products"][0]["option"], list)
        assert len(result["cart_products"][0]["option"]) == 1

    def test_transform_document_without_option(self):
        """Test document without option field"""
        doc = {
            "_id": "456",
            "collection": "view_home_page"
        }

        result = transform_event_for_bigquery(doc)

        assert result["_id"] == "456"
        assert result["collection"] == "view_home_page"
        assert "option" not in result

    def test_transform_preserves_other_fields(self):
        """Test that transformation preserves all other fields"""
        doc = {
            "_id": "789",
            "collection": "view_product_detail",
            "product_id": "100034",
            "ip": "217.122.124.139",
            "user_agent": "Mozilla/5.0",
            "option": {"alloy": "gold"}
        }

        result = transform_event_for_bigquery(doc)

        assert result["_id"] == "789"
        assert result["collection"] == "view_product_detail"
        assert result["product_id"] == "100034"
        assert result["ip"] == "217.122.124.139"
        assert result["user_agent"] == "Mozilla/5.0"
        assert isinstance(result["option"], list)
