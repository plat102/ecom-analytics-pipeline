"""
MongoDB Events Exporter

Export events from MongoDB to GCS in JSONL.gz format with batching and resume support.
"""

import json
from datetime import datetime
from typing import Optional, List

from bson import ObjectId

from common.database.mongodb_client import get_mongodb_client
from common.utils.logger import get_logger
from ingestion.shared.gcs_writer import write_and_upload_jsonl_gz
from ingestion.shared.mongodb_utils import MongoJSONEncoder
from ingestion.shared.checkpoint import save_checkpoint, load_checkpoint, clear_checkpoint
from ingestion.sources.mongodb.events import config

logger = get_logger(__name__)


def export_events(
    mode: str = "full",
    collections: Optional[List[str]] = None,
    date_str: Optional[str] = None,
    resume: bool = False
) -> bool:
    """
    Export MongoDB events to GCS in batched JSONL.gz files.

    Args:
        mode: Export mode - "full" (all collections) or "filter" (specific collections)
        collections: List of collection names to export (required if mode="filter")
        date_str: Date string YYYYMMDD (default: today)
        resume: Whether to resume from checkpoint

    Returns:
        bool: True if successful

    Example:
        >>> export_events(mode="full")
        >>> export_events(mode="filter", collections=["view_product_detail", "checkout_success"])
    """
    if date_str is None:
        date_str = datetime.now().strftime("%Y%m%d")

    if mode == "filter" and not collections:
        logger.error("mode='filter' requires --collections argument")
        return False

    logger.info("=" * 70)
    logger.info("MONGODB EVENTS EXPORT TO GCS")
    logger.info("=" * 70)
    logger.info(f"Mode: {mode}")
    if mode == "filter":
        logger.info(f"Collections: {', '.join(collections)}")
    logger.info(f"Date: {date_str}")
    logger.info(f"Resume: {resume}")
    logger.info("=" * 70)

    mongo_client = None
    try:
        # Load checkpoint if resuming
        start_part = 1
        last_id = None

        if resume:
            checkpoint = load_checkpoint(config.CHECKPOINT_FILE)
            if checkpoint:
                start_part = checkpoint['part_number']
                last_id = checkpoint.get('last_id')
                logger.info(f"Resuming from part {start_part}")

        # Connect to MongoDB
        mongo_client = get_mongodb_client()
        collection = mongo_client.get_collection()

        # Build query
        query = {}
        if mode == "filter":
            query["collection"] = {"$in": collections}

        if last_id:
            query["_id"] = {"$gt": ObjectId(last_id)}

        # Count total documents
        total_count = collection.count_documents(query)
        logger.info(f"Total documents to export: {total_count:,}")

        # Stream and batch export
        cursor = collection.find(query).batch_size(config.MONGODB_BATCH_SIZE)

        batch = []
        part_number = start_part
        total_exported = 0
        total_uploaded = 0

        for doc in cursor:
            # Convert ObjectId to string for JSON serialization
            doc_json = json.loads(json.dumps(doc, cls=MongoJSONEncoder))
            batch.append(doc_json)

            if len(batch) >= config.BATCH_SIZE:
                # Upload batch
                gcs_path = f"{config.GCS_DESTINATION_PREFIX}/events_{date_str}_part{part_number:03d}.jsonl.gz"

                logger.info(f"Uploading part {part_number} ({len(batch):,} records)...")

                stats = write_and_upload_jsonl_gz(
                    records=iter(batch),
                    gcs_bucket=config.GCS_BUCKET,
                    gcs_path=gcs_path,
                    cleanup=True
                )

                if stats['success']:
                    total_uploaded += stats['records']
                    logger.info(f"Part {part_number} uploaded: {stats['gcs_uri']}")
                    logger.info(f"Compression: {stats['uncompressed_bytes'] / (1024**2):.2f} MB -> "
                                f"{stats['compressed_bytes'] / (1024**2):.2f} MB "
                                f"({stats['compression_ratio']:.1f}%)")

                    # Save checkpoint
                    save_checkpoint(
                        config.CHECKPOINT_FILE,
                        {"part_number": part_number + 1, "last_id": str(doc['_id'])}
                    )

                    part_number += 1
                    total_exported += len(batch)
                    batch = []

                    logger.info(f"Progress: {total_exported:,} / {total_count:,} "
                                f"({total_exported/total_count*100:.1f}%)")
                else:
                    logger.error(f"Failed to upload part {part_number}")
                    return False

        # Upload remaining batch
        if batch:
            gcs_path = f"{config.GCS_DESTINATION_PREFIX}/events_{date_str}_part{part_number:03d}.jsonl.gz"

            logger.info(f"Uploading final part {part_number} ({len(batch):,} records)...")

            stats = write_and_upload_jsonl_gz(
                records=iter(batch),
                gcs_bucket=config.GCS_BUCKET,
                gcs_path=gcs_path,
                cleanup=True
            )

            if stats['success']:
                total_uploaded += stats['records']
                total_exported += len(batch)
                logger.info(f"Final part {part_number} uploaded: {stats['gcs_uri']}")
            else:
                logger.error(f"Failed to upload final part {part_number}")
                return False

        # Clear checkpoint on success
        clear_checkpoint(config.CHECKPOINT_FILE)

        logger.info("=" * 70)
        logger.info("EXPORT COMPLETE!")
        logger.info(f"Total records exported: {total_exported:,}")
        logger.info(f"Total records uploaded: {total_uploaded:,}")
        logger.info(f"Total parts: {part_number}")
        logger.info("=" * 70)

        return True

    except Exception as e:
        logger.error(f"Export failed: {e}")
        logger.exception(e)
        return False
    finally:
        if mongo_client:
            mongo_client.close()
