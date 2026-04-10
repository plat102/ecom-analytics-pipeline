"""
Export small test dataset for Cloud Function testing.

Exports 1000 events with transformation to verify E2E flow.
"""

import json
from datetime import datetime
from bson import ObjectId

from common.database.mongodb.client import get_mongodb_client
from common.utils.logger import get_logger
from common.transformations.bigquery_schema import transform_event_for_bigquery
from common.storage.gcs.writer import write_and_upload_jsonl_gz
from common.database.mongodb.utils import MongoJSONEncoder
from config import settings

logger = get_logger(__name__)

LIMIT = 1000
DATE_STR = "20260410_test"
GCS_BUCKET = settings.GCS_BUCKET
GCS_PATH = f"raw/events/events_{DATE_STR}_part001.jsonl.gz"

# Collections with diverse option field structures
TEST_COLLECTIONS = [
    "view_product_detail",       # option ARRAY with full metadata
    "select_product_option",      # option ARRAY
    "view_listing_page",          # option OBJECT (key-value pairs)
    "view_filter_page",           # option OBJECT
    "checkout_success",           # cart_products with nested option
    "view_shopping_cart"          # cart_products
]


def main():
    """Export test sample to GCS."""
    logger.info("=" * 70)
    logger.info("EXPORT TEST SAMPLE FOR CLOUD FUNCTION")
    logger.info("=" * 70)
    logger.info(f"Limit: {LIMIT} events")
    logger.info(f"Collections: {', '.join(TEST_COLLECTIONS)}")
    logger.info(f"Date: {DATE_STR}")
    logger.info(f"Destination: gs://{GCS_BUCKET}/{GCS_PATH}")
    logger.info("=" * 70)

    mongo_client = None
    try:
        # Connect to MongoDB
        mongo_client = get_mongodb_client()
        collection = mongo_client.get_collection()

        # Query: Filter by test collections
        query = {"collection": {"$in": TEST_COLLECTIONS}}

        # Skip count (too slow with 41M docs), just fetch directly
        logger.info(f"Fetching first {LIMIT} documents from test collections...")

        # Fetch documents with limit
        cursor = collection.find(query).limit(LIMIT)

        # Transform and collect
        records = []
        for doc in cursor:
            # Convert ObjectId to string
            doc_json = json.loads(json.dumps(doc, cls=MongoJSONEncoder))

            # Transform for BigQuery (OBJECT to ARRAY)
            doc_transformed = transform_event_for_bigquery(doc_json)

            records.append(doc_transformed)

        logger.info(f"Fetched and transformed {len(records)} events")

        # Upload to GCS
        logger.info(f"Uploading to GCS: {GCS_PATH}")
        stats = write_and_upload_jsonl_gz(
            records=iter(records),
            gcs_bucket=GCS_BUCKET,
            gcs_path=GCS_PATH,
            cleanup=True
        )

        if stats['success']:
            logger.info("=" * 70)
            logger.info("EXPORT SUCCESSFUL!")
            logger.info(f"Records: {stats['records']:,}")
            logger.info(f"GCS URI: {stats['gcs_uri']}")
            logger.info(f"Uncompressed: {stats['uncompressed_bytes'] / 1024:.2f} KB")
            logger.info(f"Compressed: {stats['compressed_bytes'] / 1024:.2f} KB")
            logger.info(f"Compression ratio: {stats['compression_ratio']:.1f}%")
            logger.info("=" * 70)
            logger.info("")
            logger.info("Next steps:")
            logger.info("1. Wait for Cloud Function deployment to complete")
            logger.info("2. Check Cloud Function logs:")
            logger.info("   gcloud functions logs read gcs-to-bq-loader --region=us-central1 --limit=50")
            logger.info("3. Verify data in BigQuery:")
            logger.info(f"   bq query --use_legacy_sql=false \"SELECT COUNT(*) FROM glamira_raw.events WHERE DATE(ingested_at) = '{datetime.now().strftime('%Y-%m-%d')}'\"")
            logger.info("4. Test option transformation:")
            logger.info("   bq query --use_legacy_sql=false \"SELECT collection, option FROM glamira_raw.events WHERE collection = 'view_listing_page' AND DATE(ingested_at) = '{datetime.now().strftime('%Y-%m-%d')}' LIMIT 3\"")
            return True
        else:
            logger.error("Upload failed!")
            return False

    except Exception as e:
        logger.error(f"Export failed: {e}")
        logger.exception(e)
        return False

    finally:
        if mongo_client:
            mongo_client.close()


if __name__ == "__main__":
    import sys
    success = main()
    sys.exit(0 if success else 1)
