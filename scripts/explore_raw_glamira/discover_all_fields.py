#!/usr/bin/env python3
"""
Comprehensive field discovery for BigQuery typed schema definition

Purpose:
- Discover ALL fields including nested structures
- Sample from multiple event types to ensure complete coverage
- Identify mixed data types (e.g., store_id as int vs string)
- Generate field list with BQ type recommendations

Strategy:
- Stratified sampling: 100 docs per event type (2700 total for 27 event types)
- Recursive field extraction for nested objects and arrays
- Type tracking with frequency counts
"""

from common.database.mongodb_client import get_mongodb_client
from common.utils.logger import get_logger
from collections import defaultdict
from typing import Any, Dict, List, Set
import json

logger = get_logger(__name__)


def get_type_name(value: Any) -> str:
    """Get a descriptive type name for a value"""
    if value is None:
        return "null"
    elif isinstance(value, bool):
        return "bool"
    elif isinstance(value, int):
        return "int"
    elif isinstance(value, float):
        return "float"
    elif isinstance(value, str):
        return "string"
    elif isinstance(value, list):
        return "array"
    elif isinstance(value, dict):
        return "object"
    else:
        return type(value).__name__


def extract_fields_recursive(
    doc: Dict,
    prefix: str = "",
    field_stats: Dict = None
) -> None:
    """
    Recursively extract all fields including nested ones

    Args:
        doc: Document or subdocument to extract from
        prefix: Field path prefix (e.g., 'cart_products.option')
        field_stats: Dictionary to accumulate field statistics
    """
    if field_stats is None:
        field_stats = defaultdict(lambda: {"types": defaultdict(int), "samples": []})

    for key, value in doc.items():
        # Build field path
        field_path = f"{prefix}.{key}" if prefix else key

        # Get type
        type_name = get_type_name(value)
        field_stats[field_path]["types"][type_name] += 1

        # Store sample values (limit to 5)
        if len(field_stats[field_path]["samples"]) < 5:
            if isinstance(value, (list, dict)):
                sample_repr = f"<{type_name}>"
            else:
                sample_repr = str(value)[:100]

            if sample_repr not in field_stats[field_path]["samples"]:
                field_stats[field_path]["samples"].append(sample_repr)

        # Recurse into nested structures
        if isinstance(value, dict) and value:
            # Explore object fields
            extract_fields_recursive(value, field_path, field_stats)

        elif isinstance(value, list) and value:
            # Explore array element structures
            for item in value[:3]:  # Sample first 3 items
                if isinstance(item, dict):
                    # Array of objects - explore object structure
                    extract_fields_recursive(item, f"{field_path}[]", field_stats)
                else:
                    # Array of primitives - just track type
                    item_type = get_type_name(item)
                    field_stats[f"{field_path}[]"]["types"][item_type] += 1

    return field_stats


def get_bq_type_recommendation(type_counts: Dict[str, int]) -> str:
    """
    Recommend BigQuery type based on observed types

    Priority:
    1. If mixed types exist, use STRING (safe default)
    2. If single type, map to appropriate BQ type
    """
    types = list(type_counts.keys())

    # Remove null from consideration
    non_null_types = [t for t in types if t != "null"]

    if not non_null_types:
        return "STRING"  # All nulls, use STRING as safe default

    # Mixed types detected
    if len(non_null_types) > 1:
        # Special case: int + float -> FLOAT64
        if set(non_null_types) == {"int", "float"}:
            return "FLOAT64"
        # Otherwise: mixed types -> STRING (safe)
        return "STRING (mixed types)"

    # Single type mapping
    single_type = non_null_types[0]
    type_mapping = {
        "bool": "BOOL",
        "int": "INT64",
        "float": "FLOAT64",
        "string": "STRING",
        "array": "ARRAY/REPEATED",
        "object": "RECORD/STRUCT"
    }

    return type_mapping.get(single_type, "STRING")


def main():
    logger.info("=" * 80)
    logger.info("Comprehensive Field Discovery for BigQuery Typed Schema")
    logger.info("=" * 80)

    mongo = get_mongodb_client()
    collection = mongo.get_collection()

    try:
        # Step 1: Get all event types
        logger.info("\nStep 1: Getting event types...")
        event_types = collection.distinct("collection")
        logger.info(f"Found {len(event_types)} event types")

        # Step 2: Stratified sampling - 100 docs per event type
        logger.info("\nStep 2: Sampling documents (100 per event type)...")
        all_field_stats = defaultdict(lambda: {"types": defaultdict(int), "samples": []})
        total_docs = 0

        for event_type in sorted(event_types):
            # Sample 100 docs for this event type
            samples = list(collection.find(
                {"collection": event_type}
            ).limit(100))

            logger.info(f"  {event_type:40} - {len(samples)} docs sampled")

            # Extract fields from each sample
            for doc in samples:
                extract_fields_recursive(doc, "", all_field_stats)
                total_docs += 1

        logger.info(f"\nTotal documents analyzed: {total_docs}")
        logger.info(f"Total unique field paths discovered: {len(all_field_stats)}")

        # Step 3: Analyze and report
        logger.info("\n" + "=" * 120)
        logger.info(f"{'Field Path':60} | {'BQ Type':20} | {'Types Observed':30} | Samples")
        logger.info("=" * 120)

        field_report = []

        for field_path in sorted(all_field_stats.keys()):
            stats = all_field_stats[field_path]

            # Get type counts
            type_counts = stats["types"]
            type_summary = ", ".join([f"{t}({c})" for t, c in sorted(type_counts.items())])

            # Get BQ recommendation
            bq_type = get_bq_type_recommendation(type_counts)

            # Sample values
            samples_str = " | ".join(stats["samples"][:3])

            # Print
            logger.info(f"{field_path:60} | {bq_type:20} | {type_summary:30} | {samples_str[:40]}")

            # Collect for summary
            field_report.append({
                "field_path": field_path,
                "bq_type": bq_type,
                "type_counts": dict(type_counts),
                "samples": stats["samples"][:3]
            })

        logger.info("=" * 120)

        # Step 4: Export to JSON
        output_file = "data/field_discovery_report.json"
        with open(output_file, "w") as f:
            json.dump({
                "total_documents_analyzed": total_docs,
                "total_event_types": len(event_types),
                "total_unique_fields": len(field_report),
                "fields": field_report
            }, f, indent=2)

        logger.info(f"\nField discovery report saved to: {output_file}")

        # Step 5: Summary stats
        logger.info("\n" + "=" * 80)
        logger.info("SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Total fields discovered: {len(field_report)}")

        # Count by category
        top_level = [f for f in field_report if "." not in f["field_path"]]
        nested = [f for f in field_report if "." in f["field_path"] and "[]" not in f["field_path"]]
        array_fields = [f for f in field_report if "[]" in f["field_path"]]

        logger.info(f"  - Top-level fields: {len(top_level)}")
        logger.info(f"  - Nested fields: {len(nested)}")
        logger.info(f"  - Array element fields: {len(array_fields)}")

        # Count mixed types
        mixed_type_fields = [f for f in field_report if "mixed types" in f["bq_type"]]
        logger.info(f"  - Mixed type fields (require STRING): {len(mixed_type_fields)}")

        if mixed_type_fields:
            logger.info("\nMixed type fields:")
            for f in mixed_type_fields:
                types = ", ".join([f"{k}({v})" for k, v in f["type_counts"].items()])
                logger.info(f"  - {f['field_path']:50} | {types}")

        logger.info("\n" + "=" * 80)
        logger.info("Next step: Use this report to create bigquery/schemas/events_typed.json")
        logger.info("=" * 80)

    finally:
        mongo.close()


if __name__ == "__main__":
    main()
