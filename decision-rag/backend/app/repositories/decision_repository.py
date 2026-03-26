"""
Repository for managing local storage of decision documents.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from ..core.config import settings
from ..schemas.decision import DecisionDocument
from ..utils.validators import validate_decision_document

logger = logging.getLogger(__name__)


class DecisionRepository:
    """Repository for storing and retrieving decision documents."""

    def __init__(self, data_dir: Optional[str] = None):
        """
        Initialize the repository.

        Args:
            data_dir: Directory for storing data (defaults to settings.DATA_DIR)
        """
        self.data_dir = Path(data_dir or settings.DATA_DIR)
        self.decisions_dir = self.data_dir / "decisions"
        self.checkpoint_file = self.data_dir / "checkpoint.json"

        # Create directories
        self.decisions_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"Repository initialized at {self.data_dir}")

    def save_decision(self, decision: DecisionDocument) -> bool:
        """
        Save a decision document to local storage.

        Args:
            decision: DecisionDocument to save

        Returns:
            True if saved successfully
        """
        # Validate document
        is_valid, error = validate_decision_document(decision)
        if not is_valid:
            logger.error(f"Invalid decision document: {error}")
            return False

        try:
            # Create filename from NativeId (sanitize for filesystem)
            safe_id = self._sanitize_filename(decision.NativeId)
            file_path = self.decisions_dir / f"{safe_id}.json"

            # Save as JSON
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(decision.model_dump(), f, ensure_ascii=False, indent=2)

            logger.debug(f"Saved decision: {decision.NativeId}")
            return True

        except Exception as e:
            logger.error(f"Error saving decision {decision.NativeId}: {e}")
            return False

    def get_decision(self, native_id: str) -> Optional[DecisionDocument]:
        """
        Retrieve a decision document by NativeId.

        Args:
            native_id: NativeId of the decision

        Returns:
            DecisionDocument or None if not found
        """
        try:
            safe_id = self._sanitize_filename(native_id)
            file_path = self.decisions_dir / f"{safe_id}.json"

            if not file_path.exists():
                return None

            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            return DecisionDocument(**data)

        except Exception as e:
            logger.error(f"Error loading decision {native_id}: {e}")
            return None

    def decision_exists(self, native_id: str) -> bool:
        """
        Check if a decision already exists in storage.

        Args:
            native_id: NativeId of the decision

        Returns:
            True if decision exists
        """
        safe_id = self._sanitize_filename(native_id)
        file_path = self.decisions_dir / f"{safe_id}.json"
        return file_path.exists()

    def get_all_native_ids(self) -> List[str]:
        """
        Get list of all stored decision NativeIds.

        Returns:
            List of NativeId strings
        """
        ids = []
        for file_path in self.decisions_dir.glob("*.json"):
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if "NativeId" in data:
                        ids.append(data["NativeId"])
            except Exception as e:
                logger.warning(f"Error reading {file_path}: {e}")

        return ids

    def save_checkpoint(self, checkpoint_data: Dict[str, Any]) -> bool:
        """
        Save progress checkpoint.

        Args:
            checkpoint_data: Checkpoint data to save

        Returns:
            True if saved successfully
        """
        try:
            checkpoint_data["timestamp"] = datetime.now().isoformat()

            with open(self.checkpoint_file, "w", encoding="utf-8") as f:
                json.dump(checkpoint_data, f, ensure_ascii=False, indent=2)

            logger.info("Checkpoint saved")
            return True

        except Exception as e:
            logger.error(f"Error saving checkpoint: {e}")
            return False

    def load_checkpoint(self) -> Optional[Dict[str, Any]]:
        """
        Load last checkpoint.

        Returns:
            Checkpoint data or None if no checkpoint exists
        """
        if not self.checkpoint_file.exists():
            return None

        try:
            with open(self.checkpoint_file, "r", encoding="utf-8") as f:
                data = json.load(f)

            logger.info(f"Loaded checkpoint from {data.get('timestamp')}")
            return data

        except Exception as e:
            logger.error(f"Error loading checkpoint: {e}")
            return None

    def get_statistics(self) -> Dict[str, Any]:
        """
        Get repository statistics.

        Returns:
            Dictionary with statistics
        """
        total_files = len(list(self.decisions_dir.glob("*.json")))

        # Calculate total size
        total_size = sum(f.stat().st_size for f in self.decisions_dir.glob("*.json"))

        return {
            "total_documents": total_files,
            "storage_size_mb": total_size / (1024 * 1024),
            "storage_path": str(self.decisions_dir),
        }

    def _sanitize_filename(self, native_id: str) -> str:
        """
        Sanitize NativeId for use as filename.

        Args:
            native_id: NativeId to sanitize

        Returns:
            Safe filename string
        """
        # Replace problematic characters
        safe = native_id.replace("/", "_").replace("\\", "_").replace(":", "_")
        return safe

    def delete_decision(self, native_id: str) -> bool:
        """
        Delete a single decision file by native_id.

        Args:
            native_id: NativeId of decision to delete

        Returns:
            True if deleted successfully, False otherwise
        """
        try:
            safe_id = self._sanitize_filename(native_id)
            file_path = self.decisions_dir / f"{safe_id}.json"

            if not file_path.exists():
                logger.debug(f"File not found (not an error): {native_id}")
                return True  # Not found is considered success

            file_path.unlink()
            logger.debug(f"Deleted decision file: {native_id}")
            return True

        except Exception as e:
            logger.error(f"Error deleting decision {native_id}: {e}")
            return False

    def delete_decisions(self, native_ids: List[str]) -> Dict[str, Any]:
        """
        Delete multiple decision files in batch.

        Args:
            native_ids: List of NativeIds to delete

        Returns:
            Dictionary with stats (total, deleted, failed, errors)
        """
        stats = {
            "total": len(native_ids),
            "deleted": 0,
            "failed": 0,
            "errors": [],
        }

        for native_id in native_ids:
            if self.delete_decision(native_id):
                stats["deleted"] += 1
            else:
                stats["failed"] += 1
                stats["errors"].append(native_id)

        logger.info(
            f"Batch deletion complete: {stats['deleted']} deleted, {stats['failed']} failed"
        )
        return stats

    def clear_repository(self) -> None:
        """
        Clear all stored decision documents and checkpoint.
        """
        for file_path in self.decisions_dir.glob("*.json"):
            try:
                file_path.unlink()
                logger.debug(f"Deleted file: {file_path}")
            except Exception as e:
                logger.error(f"Error deleting file {file_path}: {e}")

        if self.checkpoint_file.exists():
            try:
                self.checkpoint_file.unlink()
                logger.debug(f"Deleted checkpoint file: {self.checkpoint_file}")
            except Exception as e:
                logger.error(f"Error deleting checkpoint file: {e}")
