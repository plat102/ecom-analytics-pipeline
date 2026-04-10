#!/usr/bin/env python3
"""
Export sample documents for each event type

Purpose:
- Export 1 sample document per event type (27 files)
- Save as pretty-printed JSON for easy inspection
- Remove _id field (not needed for schema definition)
"""

from common.database.mongodb.client import get_mongodb_client
from common.utils.logger import get_logger
import json
from pathlib import Path

logger = get_logger(__name__)


def main():
    logger.info("=" * 80)
    logger.info("Exporting Sample Events by Type")
    logger.info("=" * 80)

    mongo = get_mongodb_client()
    collection = mongo.get_collection()

    try:
        # Create output directory
        output_dir = Path("data/sample_events")
        output_dir.mkdir(exist_ok=True, parents=True)

        # Use known event types (from data dictionary) instead of slow distinct() query
        event_types = [
            "add_to_cart_action", "back_to_product_action", "checkout", "checkout_success",
            "landing_page_recommendation_clicked", "landing_page_recommendation_noticed",
            "landing_page_recommendation_visible", "listing_page_recommendation_clicked",
            "listing_page_recommendation_noticed", "listing_page_recommendation_visible",
            "product_detail_recommendation_clicked", "product_detail_recommendation_noticed",
            "product_detail_recommendation_visible", "product_view_all_recommend_clicked",
            "search_box_action", "select_product_option", "select_product_option_quality",
            "sorting_relevance_click_action", "view_all_recommend", "view_home_page",
            "view_landing_page", "view_listing_page", "view_my_account", "view_product_detail",
            "view_shopping_cart", "view_sorting_relevance", "view_static_page"
        ]
        logger.info(f"Exporting samples for {len(event_types)} event types\n")

        # Export one sample per event type
        for event_type in sorted(event_types):
            # Get one document
            doc = collection.find_one({"collection": event_type})

            if doc:
                # Remove _id (ObjectId not JSON serializable, not needed)
                if "_id" in doc:
                    doc["_id"] = str(doc["_id"])

                # Save to file
                filename = f"{event_type}.json"
                filepath = output_dir / filename

                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(doc, f, indent=2, ensure_ascii=False)

                logger.info(f"  {event_type:45} -> {filename}")

        logger.info(f"\n{len(event_types)} sample event files saved to: {output_dir}")
        logger.info("=" * 80)

    finally:
        mongo.close()


if __name__ == "__main__":
    main()
