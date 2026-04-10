"""
Google Cloud Storage (GCS) client for Data Lake operations.

Authentication:
- On GCP VM: Uses default credentials automatically
- Local dev: Set GOOGLE_APPLICATION_CREDENTIALS environment variable
"""

from pathlib import Path
from typing import Optional

from common.utils.logger import get_logger

logger = get_logger(__name__)


def upload_to_gcs(
    local_file_path: Path,
    bucket_name: str,
    destination_blob_name: str,
    overwrite: bool = True
) -> bool:
    """
    Upload file from local filesystem to Google Cloud Storage.

    Args:
        local_file_path: Path to local file
        bucket_name: GCS bucket name (e.g., "ecom-analytics-data-lake")
        destination_blob_name: Destination path in GCS (e.g., "raw/glamira/products/file.json")
        overwrite: If False, skip upload if blob already exists (default: True)

    Returns:
        bool: True if upload successful, False otherwise
    """
    try:
        from google.cloud import storage
    except ImportError:
        logger.error(
            "google-cloud-storage not installed! "
            "Install with: poetry add google-cloud-storage"
        )
        return False

    # Validate local file
    if not local_file_path.exists():
        logger.error(f"Local file not found: {local_file_path}")
        return False

    if not local_file_path.is_file():
        logger.error(f"Path is not a file: {local_file_path}")
        return False

    try:
        # Initialize GCS client (auto-detects credentials)
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        # Check if blob exists (optional)
        if not overwrite and blob.exists():
            logger.info(
                f"Blob already exists (skipping): gs://{bucket_name}/{destination_blob_name}"
            )
            return True

        # Upload file
        file_size_mb = local_file_path.stat().st_size / (1024 * 1024)
        logger.info(
            f"Uploading {local_file_path.name} ({file_size_mb:.2f} MB) "
            f"to gs://{bucket_name}/{destination_blob_name}..."
        )

        blob.upload_from_filename(str(local_file_path))

        logger.info(f"Upload successful: gs://{bucket_name}/{destination_blob_name}")
        return True

    except Exception as e:
        logger.error(f"Upload failed: {e}")
        return False


def download_from_gcs(
    bucket_name: str,
    source_blob_name: str,
    destination_file_path: Path,
    overwrite: bool = True
) -> bool:
    """
    Download file from Google Cloud Storage to local filesystem.

    Args:
        bucket_name: GCS bucket name
        source_blob_name: Source path in GCS (e.g., "raw/glamira/products/file.json")
        destination_file_path: Local destination path
        overwrite: If False, skip download if file already exists (default: True)

    Returns:
        bool: True if download successful, False otherwise
    """
    try:
        from google.cloud import storage
    except ImportError:
        logger.error(
            "google-cloud-storage not installed! "
            "Install with: poetry add google-cloud-storage"
        )
        return False

    # Check if file exists (optional)
    if not overwrite and destination_file_path.exists():
        logger.info(f"File already exists (skipping): {destination_file_path}")
        return True

    try:
        # Initialize GCS client
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(source_blob_name)

        # Check if blob exists
        if not blob.exists():
            logger.error(f"Blob not found: gs://{bucket_name}/{source_blob_name}")
            return False

        # Create parent directory if needed
        destination_file_path.parent.mkdir(parents=True, exist_ok=True)

        # Download file
        logger.info(
            f"Downloading gs://{bucket_name}/{source_blob_name} "
            f"to {destination_file_path}..."
        )

        blob.download_to_filename(str(destination_file_path))

        file_size_mb = destination_file_path.stat().st_size / (1024 * 1024)
        logger.info(f"Download successful: {destination_file_path.name} ({file_size_mb:.2f} MB)")
        return True

    except Exception as e:
        logger.error(f"Download failed: {e}")
        return False


def list_blobs(
    bucket_name: str,
    prefix: Optional[str] = None,
    max_results: Optional[int] = None
) -> list:
    """
    List blobs in GCS bucket with optional prefix filter.

    Args:
        bucket_name: GCS bucket name
        prefix: Filter by prefix (e.g., "raw/glamira/products/")
        max_results: Maximum number of results to return

    Returns:
        list: List of blob names
    """
    try:
        from google.cloud import storage
    except ImportError:
        logger.error(
            "google-cloud-storage not installed! "
            "Install with: poetry add google-cloud-storage"
        )
        return []

    try:
        storage_client = storage.Client()
        blobs = storage_client.list_blobs(bucket_name, prefix=prefix, max_results=max_results)

        blob_names = [blob.name for blob in blobs]
        logger.info(f"Found {len(blob_names)} blobs in gs://{bucket_name}/{prefix or ''}")
        return blob_names

    except Exception as e:
        logger.error(f"Failed to list blobs: {e}")
        return []
