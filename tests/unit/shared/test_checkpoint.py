"""
Unit tests for ingestion.shared.checkpoint module

Tests save/load/clear checkpoint functionality for resume capability.
"""

import json
from pathlib import Path
from datetime import datetime

import pytest

from ingestion.shared.checkpoint import (
    save_checkpoint,
    load_checkpoint,
    clear_checkpoint
)


class TestSaveCheckpoint:
    """Test save_checkpoint function"""

    def test_save_checkpoint_creates_file(self, tmp_path):
        """Should create checkpoint file with correct structure"""
        checkpoint_file = tmp_path / "test_checkpoint.json"
        data = {"part_number": 5, "last_id": "abc123"}

        save_checkpoint(checkpoint_file, data)

        assert checkpoint_file.exists()

    def test_save_checkpoint_contains_version_and_timestamp(self, tmp_path):
        """Should include version and timestamp metadata"""
        checkpoint_file = tmp_path / "test_checkpoint.json"
        data = {"part_number": 5}

        save_checkpoint(checkpoint_file, data)

        with open(checkpoint_file, 'r') as f:
            checkpoint = json.load(f)

        assert "version" in checkpoint
        assert checkpoint["version"] == "1.0"
        assert "timestamp" in checkpoint
        # Verify timestamp is valid ISO format
        datetime.fromisoformat(checkpoint["timestamp"])

    def test_save_checkpoint_preserves_data(self, tmp_path):
        """Should preserve all data fields in checkpoint"""
        checkpoint_file = tmp_path / "test_checkpoint.json"
        data = {
            "part_number": 10,
            "last_id": "xyz789",
            "total_records": 5000
        }

        save_checkpoint(checkpoint_file, data)

        with open(checkpoint_file, 'r') as f:
            checkpoint = json.load(f)

        assert checkpoint["part_number"] == 10
        assert checkpoint["last_id"] == "xyz789"
        assert checkpoint["total_records"] == 5000

    def test_save_checkpoint_creates_parent_directory(self, tmp_path):
        """Should create parent directories if they don't exist"""
        nested_path = tmp_path / "nested" / "dir" / "checkpoint.json"
        data = {"test": "data"}

        save_checkpoint(nested_path, data)

        assert nested_path.exists()
        assert nested_path.parent.exists()

    def test_save_checkpoint_overwrites_existing(self, tmp_path):
        """Should overwrite existing checkpoint file"""
        checkpoint_file = tmp_path / "checkpoint.json"

        # Save first checkpoint
        save_checkpoint(checkpoint_file, {"part_number": 1})

        # Save second checkpoint
        save_checkpoint(checkpoint_file, {"part_number": 2})

        with open(checkpoint_file, 'r') as f:
            checkpoint = json.load(f)

        assert checkpoint["part_number"] == 2


class TestLoadCheckpoint:
    """Test load_checkpoint function"""

    def test_load_checkpoint_returns_none_when_file_not_exists(self, tmp_path):
        """Should return None if checkpoint file doesn't exist"""
        checkpoint_file = tmp_path / "nonexistent.json"

        result = load_checkpoint(checkpoint_file)

        assert result is None

    def test_load_checkpoint_returns_data_without_metadata(self, tmp_path):
        """Should return data fields only, excluding version and timestamp"""
        checkpoint_file = tmp_path / "checkpoint.json"
        data = {"part_number": 5, "last_id": "abc"}

        save_checkpoint(checkpoint_file, data)
        result = load_checkpoint(checkpoint_file)

        # Should have data fields
        assert result["part_number"] == 5
        assert result["last_id"] == "abc"

        # Should NOT have metadata fields
        assert "version" not in result
        assert "timestamp" not in result

    def test_load_checkpoint_with_complex_data(self, tmp_path):
        """Should correctly load complex nested data structures"""
        checkpoint_file = tmp_path / "checkpoint.json"
        data = {
            "total_products": 100,
            "results": [
                {"product_id": "1", "status": "success"},
                {"product_id": "2", "status": "error"}
            ]
        }

        save_checkpoint(checkpoint_file, data)
        result = load_checkpoint(checkpoint_file)

        assert result["total_products"] == 100
        assert len(result["results"]) == 2
        assert result["results"][0]["product_id"] == "1"

class TestClearCheckpoint:
    """Test clear_checkpoint function"""

    def test_clear_checkpoint_deletes_file(self, tmp_path):
        """Should delete checkpoint file if it exists"""
        checkpoint_file = tmp_path / "checkpoint.json"

        # Create checkpoint
        save_checkpoint(checkpoint_file, {"test": "data"})
        assert checkpoint_file.exists()

        # Clear checkpoint
        clear_checkpoint(checkpoint_file)

        assert not checkpoint_file.exists()

    def test_clear_checkpoint_handles_nonexistent_file(self, tmp_path):
        """Should handle clearing nonexistent file gracefully"""
        checkpoint_file = tmp_path / "nonexistent.json"

        # Should not raise error
        clear_checkpoint(checkpoint_file)

        assert not checkpoint_file.exists()


class TestCheckpointRoundTrip:
    """Test save/load/clear workflow"""

    def test_full_checkpoint_workflow(self, tmp_path):
        """Test complete checkpoint lifecycle: save -> load -> clear"""
        checkpoint_file = tmp_path / "workflow.json"
        original_data = {
            "part_number": 15,
            "last_id": "test_id",
            "total_exported": 75000
        }

        # Save
        save_checkpoint(checkpoint_file, original_data)
        assert checkpoint_file.exists()

        # Load
        loaded_data = load_checkpoint(checkpoint_file)
        assert loaded_data == original_data

        # Clear
        clear_checkpoint(checkpoint_file)
        assert not checkpoint_file.exists()

        # Load after clear
        assert load_checkpoint(checkpoint_file) is None
