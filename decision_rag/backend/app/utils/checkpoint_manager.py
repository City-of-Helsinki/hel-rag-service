"""
Checkpoint management utilities for pipeline operations.

Provides robust checkpoint management with automatic saving at intervals
and structured data for different job types.
"""

from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, Optional

if TYPE_CHECKING:
    from ..repositories import DecisionRepository


class CheckpointManager:
    """
    Manages checkpoint operations with structured data and periodic saving.

    Handles checkpoint creation, loading, and automatic saving at configured
    intervals to prevent data loss during pipeline interruptions.
    """

    def __init__(self, repository: "DecisionRepository", job_type: str):
        """
        Initialize checkpoint manager.

        Args:
            repository: Repository instance for checkpoint storage
            job_type: Type of job (fetch, ingest, full_pipeline)
        """
        self.repository = repository
        self.job_type = job_type
        self._checkpoint_data: Dict[str, Any] = {}
        self._last_checkpoint_time: Optional[datetime] = None

    def load_checkpoint(self) -> Optional[Dict[str, Any]]:
        """
        Load existing checkpoint for this job type.

        Returns:
            Checkpoint data for this job type, or None if not found
        """
        checkpoint = self.repository.load_checkpoint()
        if checkpoint and self.job_type in checkpoint:
            self._checkpoint_data = checkpoint[self.job_type]
            return self._checkpoint_data
        return None

    def initialize_checkpoint(self, **kwargs) -> None:
        """
        Initialize a new checkpoint with base data.

        Args:
            **kwargs: Initial checkpoint data fields
        """
        self._checkpoint_data = {
            "timestamp": datetime.now().isoformat(),
            "completed": False,
            **kwargs,
        }

    def update_field(self, field: str, value: Any) -> None:
        """
        Update a single checkpoint field.

        Args:
            field: Field name to update
            value: New value for the field
        """
        self._checkpoint_data[field] = value

    def update_fields(self, **kwargs) -> None:
        """
        Update multiple checkpoint fields at once.

        Args:
            **kwargs: Fields to update with their new values
        """
        self._checkpoint_data.update(kwargs)

    def get_field(self, field: str, default: Any = None) -> Any:
        """
        Get a checkpoint field value.

        Args:
            field: Field name to retrieve
            default: Default value if field doesn't exist

        Returns:
            Field value or default
        """
        return self._checkpoint_data.get(field, default)

    def save(self) -> bool:
        """
        Save current checkpoint state.

        Returns:
            True if checkpoint was saved successfully
        """
        # Update timestamp
        self._checkpoint_data["timestamp"] = datetime.now().isoformat()

        # Wrap in job type structure
        full_checkpoint = {self.job_type: self._checkpoint_data}

        # Save to repository
        success = self.repository.save_checkpoint(full_checkpoint)

        if success:
            self._last_checkpoint_time = datetime.now()

        return success

    def mark_completed(self, **final_stats) -> bool:
        """
        Mark checkpoint as completed with final statistics.

        Args:
            **final_stats: Final statistics to include in checkpoint

        Returns:
            True if checkpoint was saved successfully
        """
        self._checkpoint_data["completed"] = True
        self._checkpoint_data.update(final_stats)
        return self.save()

    def should_save(self, counter: int, interval: int) -> bool:
        """
        Check if checkpoint should be saved based on counter and interval.

        Args:
            counter: Current counter value (e.g., documents processed)
            interval: Save interval (e.g., save every N documents)

        Returns:
            True if checkpoint should be saved now
        """
        return counter > 0 and counter % interval == 0

    def get_resume_point(self, field: str, default: Any = None) -> Any:
        """
        Get the resume point for a specific field.

        Useful for determining where to resume from after loading checkpoint.

        Args:
            field: Field name containing resume point
            default: Default value if field doesn't exist

        Returns:
            Resume point value or default
        """
        checkpoint = self.load_checkpoint()
        if checkpoint and not checkpoint.get("completed", False):
            return checkpoint.get(field, default)
        return default


class FetchCheckpoint(CheckpointManager):
    """Checkpoint manager specialized for fetch operations."""

    def __init__(self, repository: "DecisionRepository"):
        """Initialize fetch checkpoint manager."""
        super().__init__(repository, "fetch")

    def initialize(self, start_date: str, end_date: str) -> None:
        """
        Initialize fetch checkpoint.

        Args:
            start_date: Start date for fetch operation
            end_date: End date for fetch operation
        """
        self.initialize_checkpoint(
            start_date=start_date,
            end_date=end_date,
            last_date=start_date,
            documents_saved=0,
            documents_skipped=0,
            documents_failed=0,
        )

    def update_progress(
        self,
        last_date: str,
        documents_saved: int,
        documents_skipped: int,
        documents_failed: int = 0,
    ) -> None:
        """
        Update fetch progress.

        Args:
            last_date: Last processed date
            documents_saved: Total documents saved
            documents_skipped: Total documents skipped
            documents_failed: Total documents failed
        """
        self.update_fields(
            last_date=last_date,
            documents_saved=documents_saved,
            documents_skipped=documents_skipped,
            documents_failed=documents_failed,
        )


class IngestCheckpoint(CheckpointManager):
    """Checkpoint manager specialized for ingest operations."""

    def __init__(self, repository: "DecisionRepository"):
        """Initialize ingest checkpoint manager."""
        super().__init__(repository, "ingest")

    def initialize(
        self,
        total_documents: int,
        start_date: Optional[str] = None,
        batch_size: int = 100,
    ) -> None:
        """
        Initialize ingest checkpoint.

        Args:
            total_documents: Total number of documents to process
            start_date: Optional start date filter
            batch_size: Batch size for processing
        """
        self.initialize_checkpoint(
            total_documents=total_documents,
            start_date=start_date,
            batch_size=batch_size,
            processed=0,
            successful=0,
            failed=0,
            skipped=0,
            total_chunks=0,
            total_attachments=0,
            total_attachment_chunks=0,
            last_processed_index=0,
        )

    def update_progress(
        self,
        processed: int,
        successful: int,
        failed: int,
        skipped: int,
        total_chunks: int,
        total_attachments: int,
        total_attachment_chunks: int,
        last_processed_index: int,
    ) -> None:
        """
        Update ingest progress.

        Args:
            processed: Total documents processed
            successful: Successfully ingested documents
            failed: Failed documents
            skipped: Skipped documents
            total_chunks: Total chunks created
            total_attachments: Total attachments processed
            total_attachment_chunks: Total attachment chunks created
            last_processed_index: Index of last processed document
        """
        self.update_fields(
            processed=processed,
            successful=successful,
            failed=failed,
            skipped=skipped,
            total_chunks=total_chunks,
            total_attachments=total_attachments,
            total_attachment_chunks=total_attachment_chunks,
            last_processed_index=last_processed_index,
        )


class FullPipelineCheckpoint(CheckpointManager):
    """Checkpoint manager specialized for full pipeline operations."""

    def __init__(self, repository: "DecisionRepository"):
        """Initialize full pipeline checkpoint manager."""
        super().__init__(repository, "full_pipeline")

    def initialize(self, start_date: str, end_date: str, batch_size: int = 100) -> None:
        """
        Initialize full pipeline checkpoint.

        Args:
            start_date: Start date for pipeline
            end_date: End date for pipeline
            batch_size: Batch size for processing
        """
        self.initialize_checkpoint(
            start_date=start_date,
            end_date=end_date,
            last_date=start_date,
            batch_size=batch_size,
            # Fetch stats
            native_ids_fetched=0,
            native_ids_skipped_existing=0,
            documents_fetched=0,
            fetch_errors=0,
            fetch_retry_attempts=0,
            fetch_documents_recovered=0,
            fetch_permanently_failed=0,
            # Process stats
            documents_processed=0,
            documents_successful=0,
            documents_failed=0,
            documents_skipped=0,
            total_chunks=0,
            total_attachments=0,
            total_attachment_chunks=0,
            total_deleted=0,
            batches_processed=0,
        )

    def update_progress(
        self,
        last_date: str,
        # Fetch stats
        native_ids_fetched: int,
        native_ids_skipped_existing: int,
        documents_fetched: int,
        fetch_errors: int,
        fetch_retry_attempts: int,
        fetch_documents_recovered: int,
        fetch_permanently_failed: int,
        # Process stats
        documents_processed: int,
        documents_successful: int,
        documents_failed: int,
        documents_skipped: int,
        total_chunks: int,
        total_attachments: int,
        total_attachment_chunks: int,
        total_deleted: int,
        batches_processed: int,
    ) -> None:
        """
        Update full pipeline progress.

        Args:
            last_date: Last processed date
            native_ids_fetched: Total native IDs fetched
            native_ids_skipped_existing: Native IDs skipped (already exist)
            documents_fetched: Total documents fetched
            fetch_errors: Total fetch errors
            fetch_retry_attempts: Total retry attempts
            fetch_documents_recovered: Documents recovered after retries
            fetch_permanently_failed: Documents permanently failed
            documents_processed: Total documents processed
            documents_successful: Successfully processed documents
            documents_failed: Failed documents
            documents_skipped: Skipped documents
            total_chunks: Total chunks created
            total_attachments: Total attachments processed
            total_attachment_chunks: Total attachment chunks created
            total_deleted: Total files deleted
            batches_processed: Total batches processed
        """
        self.update_fields(
            last_date=last_date,
            native_ids_fetched=native_ids_fetched,
            native_ids_skipped_existing=native_ids_skipped_existing,
            documents_fetched=documents_fetched,
            fetch_errors=fetch_errors,
            fetch_retry_attempts=fetch_retry_attempts,
            fetch_documents_recovered=fetch_documents_recovered,
            fetch_permanently_failed=fetch_permanently_failed,
            documents_processed=documents_processed,
            documents_successful=documents_successful,
            documents_failed=documents_failed,
            documents_skipped=documents_skipped,
            total_chunks=total_chunks,
            total_attachments=total_attachments,
            total_attachment_chunks=total_attachment_chunks,
            total_deleted=total_deleted,
            batches_processed=batches_processed,
        )
