"""
Generic Checkpoint Management

Provides reusable checkpoint save/load functionality for all ingestion sources.
"""

import json
from datetime import datetime, UTC
from pathlib import Path
from typing import Optional, Dict, Any

from common.utils.logger import get_logger

logger = get_logger(__name__)


def save_checkpoint(checkpoint_file: Path, data: Dict[str, Any]) -> None:
    """
    Save checkpoint data to file.

    Generic checkpoint save that works for any data structure.
    Automatically adds version and timestamp metadata.

    Args:
        checkpoint_file: Path to checkpoint file
        data: Checkpoint data (any JSON-serializable dict)

    Example:
        >>> save_checkpoint(
        ...     checkpoint_file=Path("checkpoint.json"),
        ...     data={"part_number": 5, "last_id": "abc123"}
        ... )
    """
    checkpoint_file.parent.mkdir(parents=True, exist_ok=True)

    checkpoint = {
        "version": "1.0",
        "timestamp": datetime.now(UTC).isoformat(),
        **data
    }

    with open(checkpoint_file, 'w', encoding='utf-8') as f:
        json.dump(checkpoint, f, indent=2, ensure_ascii=False)

    logger.info(f"Checkpoint saved: {checkpoint_file.name}")


def load_checkpoint(checkpoint_file: Path) -> Optional[Dict[str, Any]]:
    """
    Load checkpoint data from file.

    Args:
        checkpoint_file: Path to checkpoint file

    Returns:
        dict: Checkpoint data (without metadata) or None if file doesn't exist

    Example:
        >>> data = load_checkpoint(Path("checkpoint.json"))
        >>> if data:
        ...     print(f"Resuming from part {data['part_number']}")
    """
    if not checkpoint_file.exists():
        return None

    try:
        with open(checkpoint_file, 'r', encoding='utf-8') as f:
            checkpoint = json.load(f)

        # Remove metadata before returning
        checkpoint.pop('version', None)
        checkpoint.pop('timestamp', None)

        logger.info(f"Checkpoint loaded: {checkpoint_file.name}")
        return checkpoint

    except (json.JSONDecodeError, Exception) as e:
        logger.warning(f"Failed to load checkpoint: {e}")
        return None


def clear_checkpoint(checkpoint_file: Path) -> None:
    """
    Delete checkpoint file.

    Args:
        checkpoint_file: Path to checkpoint file
    """
    if checkpoint_file.exists():
        checkpoint_file.unlink()
        logger.info(f"Checkpoint cleared: {checkpoint_file.name}")
