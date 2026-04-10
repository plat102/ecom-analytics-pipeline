"""
Event data transformations for BigQuery compatibility.

Transforms MongoDB event documents to match BigQuery typed schema requirements.
"""

from typing import Any, Dict, List, Optional


def normalize_option_field(option_value: Any) -> List[Dict[str, Optional[str]]]:
    """
    Normalize option field to REPEATED RECORD format for BigQuery.

    Handles two MongoDB structures:
    - OBJECT (listing/filter pages): {"alloy": "gold", "diamond": "ruby"}
    - ARRAY (product config): [{"option_id": "123", "option_label": "alloy", ...}]

    Converts OBJECT to ARRAY by transforming key-value pairs into records.

    Args:
        option_value: The option field from MongoDB document

    Returns:
        List of option records. Empty list if None/empty.

    Examples:
        OBJECT to ARRAY transformation:
        >>> normalize_option_field({"alloy": "gold", "diamond": "ruby"})
        [
            {"option_id": None, "option_label": "alloy", "value_id": None,
             "value_label": "gold", "quality": None, "quality_label": None},
            {"option_id": None, "option_label": "diamond", "value_id": None,
             "value_label": "ruby", "quality": None, "quality_label": None}
        ]

        ARRAY passthrough:
        >>> normalize_option_field([{"option_id": "123", "option_label": "alloy"}])
        [{"option_id": "123", "option_label": "alloy", "value_id": None, ...}]

        Empty cases:
        >>> normalize_option_field(None)
        []
        >>> normalize_option_field("")
        []
        >>> normalize_option_field({})
        []
    """
    # Handle null/empty
    if option_value is None or option_value == "":
        return []

    # ARRAY structure - ensure all fields exist
    if isinstance(option_value, list):
        normalized = []
        for item in option_value:
            if isinstance(item, dict):
                normalized.append({
                    "option_id": item.get("option_id"),
                    "option_label": item.get("option_label"),
                    "value_id": item.get("value_id"),
                    "value_label": item.get("value_label"),
                    "quality": item.get("quality"),
                    "quality_label": item.get("quality_label")
                })
        return normalized

    # OBJECT structure - transform to ARRAY
    if isinstance(option_value, dict):
        if not option_value:
            return []

        normalized = []
        for key, value in option_value.items():
            normalized.append({
                "option_id": None,
                "option_label": key,
                "value_id": None,
                "value_label": str(value) if value not in (None, "") else None,
                "quality": None,
                "quality_label": None
            })
        return normalized

    # Unknown type - return empty
    return []


def transform_event_for_bigquery(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transform MongoDB event document for BigQuery typed schema.

    Transformations applied:
    - Top-level option field: OBJECT to ARRAY
    - cart_products[].option field: empty string to empty array

    Args:
        event: MongoDB event document (ObjectId already converted to string)

    Returns:
        Transformed event ready for BigQuery load

    Examples:
        >>> event = {"_id": "123", "option": {"alloy": "gold"}}
        >>> transform_event_for_bigquery(event)
        {"_id": "123", "option": [{"option_id": None, "option_label": "alloy", ...}]}
    """
    transformed = dict(event)

    # Transform top-level option field
    if "option" in transformed:
        transformed["option"] = normalize_option_field(transformed["option"])

    # Transform cart_products - handle empty string in nested option
    if "cart_products" in transformed and isinstance(transformed["cart_products"], list):
        for product in transformed["cart_products"]:
            if isinstance(product, dict) and "option" in product:
                product["option"] = normalize_option_field(product["option"])

    return transformed
