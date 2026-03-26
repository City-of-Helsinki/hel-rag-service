"""Scheduler service for automated pipeline execution.

Schedules the full pipeline to run at specified intervals or times, with state management and job tracking integration. Scheduler can be paused, resumed, or triggered manually via API endpoints."""

import threading
from datetime import datetime
from typing import Optional

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from app.core import get_logger, settings
from app.services.data_fetcher import DecisionDataFetcher
from app.services.ingestion_pipeline import IngestionPipeline
from app.services.job_manager import JobManager
from app.services.scheduler_state import SchedulerStateManager
from app.utils.checkpoint_manager import FullPipelineCheckpoint
from app.utils.date_utils import parse_date

logger = get_logger(__name__)


class SchedulerService:
    """
    Service for scheduling and managing automated pipeline executions.

    Integrates APScheduler for robust job scheduling with state persistence
    and job management integration.
    """

    def __init__(
        self,
        job_manager: JobManager,
        fetcher: DecisionDataFetcher,
        pipeline: IngestionPipeline,
    ):
        """
        Initialize scheduler service.

        Args:
            job_manager: Job manager for tracking pipeline executions
            fetcher: Data fetcher instance
            pipeline: Ingestion pipeline instance
        """
        self.job_manager = job_manager
        self.fetcher = fetcher
        self.pipeline = pipeline

        # State management
        self.state_manager = SchedulerStateManager(settings.SCHEDULER_STATE_FILE)

        # APScheduler instance
        self.scheduler: Optional[BackgroundScheduler] = None
        self._lock = threading.Lock()
        self._running_job_id: Optional[str] = None

        logger.info("Scheduler service initialized")

    def start(self) -> None:
        """Start the scheduler."""
        if not settings.SCHEDULER_ENABLED:
            logger.info("Scheduler is disabled in configuration")
            return

        with self._lock:
            if self.scheduler is not None and self.scheduler.running:
                logger.warning("Scheduler is already running")
                return

            try:
                # Load state
                state = self.state_manager.load_state()

                # Create scheduler
                timezone = pytz.timezone(settings.SCHEDULER_TIMEZONE)
                self.scheduler = BackgroundScheduler(timezone=timezone)

                # Configure job based on settings
                if settings.SCHEDULER_START_TIME:
                    # Use cron trigger for specific time
                    hour, minute = settings.SCHEDULER_START_TIME.split(":")
                    trigger = CronTrigger(
                        hour=int(hour),
                        minute=int(minute),
                        timezone=timezone,
                    )
                    logger.info(
                        f"Scheduler configured for daily execution at {settings.SCHEDULER_START_TIME}"
                    )
                else:
                    # Use interval trigger
                    trigger = IntervalTrigger(
                        hours=settings.SCHEDULER_INTERVAL_HOURS,
                        timezone=timezone,
                    )
                    logger.info(
                        f"Scheduler configured for execution every {settings.SCHEDULER_INTERVAL_HOURS} hours"
                    )

                # Add job
                self.scheduler.add_job(
                    self._execute_pipeline,
                    trigger=trigger,
                    id="pipeline_job",
                    name="Decision Pipeline Execution",
                    max_instances=settings.SCHEDULER_MAX_INSTANCES,
                    replace_existing=True,
                )

                # Start scheduler if not paused
                if not state.paused:
                    self.scheduler.start()
                    logger.info("Scheduler started successfully")

                    # Update state with next execution time
                    next_run = self.get_next_run_time()
                    if next_run:
                        self.state_manager.update_execution_times(next_execution=next_run)
                else:
                    logger.info("Scheduler configured but paused")

                # Update state
                self.state_manager.update_enabled(True)
                self.state_manager.update_schedule(
                    settings.SCHEDULER_INTERVAL_HOURS,
                    settings.SCHEDULER_TIMEZONE,
                )

            except Exception as e:
                logger.error(f"Failed to start scheduler: {e}", exc_info=True)
                raise

    def shutdown(self, wait: bool = True) -> None:
        """
        Shutdown the scheduler gracefully.

        Args:
            wait: Whether to wait for running jobs to complete
        """
        with self._lock:
            if self.scheduler is None:
                logger.info("Scheduler is not running")
                return

            try:
                logger.info("Shutting down scheduler...")
                self.scheduler.shutdown(wait=wait)
                self.scheduler = None
                logger.info("Scheduler shutdown complete")

            except Exception as e:
                logger.error(f"Error during scheduler shutdown: {e}", exc_info=True)

    def pause(self) -> bool:
        """
        Pause scheduled execution.

        Returns:
            True if paused successfully, False otherwise
        """
        with self._lock:
            if self.scheduler is None or not self.scheduler.running:
                logger.warning("Cannot pause: scheduler is not running")
                return False

            try:
                self.scheduler.pause()
                self.state_manager.set_paused(True)
                logger.info("Scheduler paused")
                return True

            except Exception as e:
                logger.error(f"Failed to pause scheduler: {e}", exc_info=True)
                return False

    def resume(self) -> bool:
        """
        Resume scheduled execution.

        Returns:
            True if resumed successfully, False otherwise
        """
        with self._lock:
            if self.scheduler is None:
                logger.warning("Cannot resume: scheduler not initialized")
                return False

            try:
                if not self.scheduler.running:
                    self.scheduler.start()

                self.scheduler.resume()
                self.state_manager.set_paused(False)

                # Update next execution time
                next_run = self.get_next_run_time()
                if next_run:
                    self.state_manager.update_execution_times(next_execution=next_run)

                logger.info("Scheduler resumed")
                return True

            except Exception as e:
                logger.error(f"Failed to resume scheduler: {e}", exc_info=True)
                return False

    def trigger_now(self) -> Optional[str]:
        """
        Trigger immediate pipeline execution (bypasses schedule).

        Returns:
            Job ID if triggered successfully, None otherwise
        """
        # Check if another job is running
        if self._running_job_id is not None:
            running_job = self.job_manager.get_job(self._running_job_id)
            if running_job and running_job.status == "running":
                logger.warning(
                    f"Cannot trigger: job {self._running_job_id} is still running"
                )
                return None

        try:
            # Execute in separate thread to not block
            thread = threading.Thread(target=self._execute_pipeline, daemon=True)
            thread.start()
            logger.info("Manual pipeline trigger initiated")
            return self._running_job_id

        except Exception as e:
            logger.error(f"Failed to trigger pipeline: {e}", exc_info=True)
            return None

    def get_status(self) -> dict:
        """
        Get current scheduler status.

        Returns:
            Dictionary with status information
        """
        state = self.state_manager.get_state()

        status = {
            "enabled": settings.SCHEDULER_ENABLED,
            "running": self.scheduler is not None and self.scheduler.running,
            "paused": state.paused,
            "interval_hours": settings.SCHEDULER_INTERVAL_HOURS,
            "start_time": settings.SCHEDULER_START_TIME or None,
            "timezone": settings.SCHEDULER_TIMEZONE,
            "last_execution": state.last_execution_time,
            "next_execution": state.next_execution_time,
            "failure_count": state.failure_count,
            "current_job_id": self._running_job_id,
        }

        # Add next run time from scheduler
        next_run = self.get_next_run_time()
        if next_run:
            status["next_scheduled_run"] = next_run.isoformat()

        return status

    def get_next_run_time(self) -> Optional[datetime]:
        """
        Get next scheduled run time.

        Returns:
            Next run time or None if not scheduled
        """
        if self.scheduler is None:
            return None

        job = self.scheduler.get_job("pipeline_job")
        if job and job.next_run_time:
            return job.next_run_time

        return None

    def is_healthy(self) -> bool:
        """
        Check if scheduler is healthy.

        Returns:
            True if healthy, False otherwise
        """
        if not settings.SCHEDULER_ENABLED:
            return True  # Healthy if disabled (expected state)

        if self.scheduler is None:
            return False

        # Check if scheduler is running when it should be
        state = self.state_manager.get_state()
        if state.paused:
            return True  # Healthy if paused (expected state)

        return self.scheduler.running

    def _execute_pipeline(self) -> None:
        """
        Execute the full pipeline (internal method called by scheduler).

        This method is called by APScheduler and should not be called directly.
        Implements the same streaming batch processing as the API endpoint.
        """

        start_time = datetime.now()

        # Create job
        job_id = self.job_manager.create_job("scheduled_full_pipeline")
        self._running_job_id = job_id

        try:
            logger.info(f"Starting scheduled pipeline execution (job: {job_id})")
            self.job_manager.start_job(job_id)

            # Initialize checkpoint manager
            checkpoint_mgr = FullPipelineCheckpoint(self.pipeline.repository)

            # Update state
            self.state_manager.update_execution_times(last_execution=start_time)

            # Determine date range
            start_date = settings.SCHEDULER_START_DATE or settings.START_DATE
            end_date = settings.SCHEDULER_END_DATE or settings.END_DATE

            start = parse_date(start_date)
            end = parse_date(end_date)

            # Initialize checkpoint
            checkpoint_mgr.initialize(start.isoformat(), end.isoformat(), settings.SCHEDULER_BATCH_SIZE)

            logger.info(f"Processing date range: {start.date()} to {end.date()}")
            logger.info(
                f"Batch size: {settings.SCHEDULER_BATCH_SIZE}, "
                f"Skip existing: {settings.SCHEDULER_SKIP_EXISTING}, "
                f"Keep files: {settings.SCHEDULER_KEEP_FILES}"
            )

            # Initialize statistics
            stats = {
                "total_fetched": 0,
                "total_processed": 0,
                "total_successful": 0,
                "total_failed": 0,
                "total_skipped": 0,
                "total_chunks": 0,
                "total_attachments": 0,
                "total_attachment_chunks": 0,
                "total_deleted": 0,
                "batches_processed": 0,
            }

            # Batch accumulator
            batch_buffer = []
            batch_native_ids = []

            self.job_manager.update_progress(job_id, 5, "Fetching documents...")

            # Stream documents from fetcher (same as API endpoint)
            for document in self.fetcher.fetch_all_decisions(
                api_key=settings.API_KEY,
                start_date=start,
                end_date=end,
            ):
                stats["total_fetched"] += 1

                # Check if already in vector store (same as API endpoint)
                if settings.SCHEDULER_SKIP_EXISTING and self.pipeline.vector_store.document_exists(
                    document.NativeId
                ):
                    stats["total_skipped"] += 1
                    logger.debug(f"Skipping existing document: {document.NativeId}")
                    continue

                # Save document temporarily
                if self.pipeline.repository.save_decision(document):
                    batch_buffer.append(document)
                    batch_native_ids.append(document.NativeId)

                # Process batch when size reached (same as API endpoint)
                if len(batch_buffer) >= settings.SCHEDULER_BATCH_SIZE:
                    logger.info(f"Processing batch of {len(batch_buffer)} documents")
                    self.job_manager.update_progress(
                        job_id,
                        min(
                            90,
                            10
                            + (stats["total_fetched"] / max(stats["total_fetched"] + 100, 1))
                            * 80,
                        ),
                        f"Processing batch {stats['batches_processed'] + 1}...",
                    )

                    # Process the batch
                    batch_stats = self.pipeline.process_batch(
                        batch_native_ids, reindex=not settings.SCHEDULER_SKIP_EXISTING
                    )

                    stats["total_processed"] += batch_stats["processed"]
                    stats["total_successful"] += batch_stats["successful"]
                    stats["total_failed"] += batch_stats["failed"]
                    stats["total_skipped"] += batch_stats["skipped"]
                    stats["total_chunks"] += batch_stats["total_chunks"]
                    stats["total_attachments"] += batch_stats.get("total_attachments", 0)
                    stats["total_attachment_chunks"] += batch_stats.get(
                        "total_attachment_chunks", 0
                    )
                    stats["batches_processed"] += 1

                    # Delete processed files if not keeping them (same as API endpoint)
                    if not settings.SCHEDULER_KEEP_FILES:
                        for native_id in batch_native_ids:
                            try:
                                if self.pipeline.repository.delete_decision(native_id):
                                    stats["total_deleted"] += 1
                            except Exception as e:
                                logger.warning(f"Failed to delete file {native_id}: {e}")

                    # Clear batch
                    batch_buffer = []
                    batch_native_ids = []

                    # Save checkpoint periodically
                    if checkpoint_mgr.should_save(
                        stats["batches_processed"],
                        settings.CHECKPOINT_INTERVAL_FULL_PIPELINE_BATCHES,
                    ):
                        fetcher_stats = self.fetcher.stats
                        checkpoint_mgr.update_progress(
                            document.DateDecision or end.isoformat(),
                            fetcher_stats.get("ids_fetched", 0),
                            fetcher_stats.get("ids_skipped_existing", 0),
                            stats["total_fetched"],
                            fetcher_stats.get("errors", 0),
                            fetcher_stats.get("retry_attempts", 0),
                            fetcher_stats.get("documents_recovered", 0),
                            fetcher_stats.get("permanently_failed", 0),
                            stats["total_processed"],
                            stats["total_successful"],
                            stats["total_failed"],
                            stats["total_skipped"],
                            stats["total_chunks"],
                            stats["total_attachments"],
                            stats["total_attachment_chunks"],
                            stats["total_deleted"],
                            stats["batches_processed"],
                        )
                        checkpoint_mgr.save()

            # Process remaining documents in final batch (same as API endpoint)
            if batch_buffer:
                logger.info(f"Processing final batch of {len(batch_buffer)} documents")
                self.job_manager.update_progress(job_id, 95, "Processing final batch...")

                batch_stats = self.pipeline.process_batch(
                    batch_native_ids, reindex=not settings.SCHEDULER_SKIP_EXISTING
                )

                stats["total_processed"] += batch_stats["processed"]
                stats["total_successful"] += batch_stats["successful"]
                stats["total_failed"] += batch_stats["failed"]
                stats["total_skipped"] += batch_stats["skipped"]
                stats["total_chunks"] += batch_stats["total_chunks"]
                stats["total_attachments"] += batch_stats.get("total_attachments", 0)
                stats["total_attachment_chunks"] += batch_stats.get("total_attachment_chunks", 0)
                stats["batches_processed"] += 1

                # Delete processed files
                if not settings.SCHEDULER_KEEP_FILES:
                    for native_id in batch_native_ids:
                        try:
                            if self.pipeline.repository.delete_decision(native_id):
                                stats["total_deleted"] += 1
                        except Exception as e:
                            logger.warning(f"Failed to delete file {native_id}: {e}")

            # Mark checkpoint as completed
            fetcher_stats = self.fetcher.stats
            checkpoint_mgr.mark_completed(
                last_date=end.isoformat(),
                native_ids_fetched=fetcher_stats.get("ids_fetched", 0),
                native_ids_skipped_existing=fetcher_stats.get("ids_skipped_existing", 0),
                documents_fetched=stats["total_fetched"],
                fetch_errors=fetcher_stats.get("errors", 0),
                fetch_retry_attempts=fetcher_stats.get("retry_attempts", 0),
                fetch_documents_recovered=fetcher_stats.get("documents_recovered", 0),
                fetch_permanently_failed=fetcher_stats.get("permanently_failed", 0),
                documents_processed=stats["total_processed"],
                documents_successful=stats["total_successful"],
                documents_failed=stats["total_failed"],
                documents_skipped=stats["total_skipped"],
                total_chunks=stats["total_chunks"],
                total_attachments=stats["total_attachments"],
                total_attachment_chunks=stats["total_attachment_chunks"],
                files_deleted=stats["total_deleted"],
                batches_processed=stats["batches_processed"],
            )

            # Calculate duration
            duration = (datetime.now() - start_time).total_seconds()

            # Complete job (same statistics structure as API endpoint)
            combined_stats = {
                "native_ids_fetched": fetcher_stats.get("ids_fetched", 0),
                "documents_fetched": stats["total_fetched"],
                "fetch_errors": fetcher_stats.get("errors", 0),
                "fetch_retry_attempts": fetcher_stats.get("retry_attempts", 0),
                "fetch_documents_recovered": fetcher_stats.get("documents_recovered", 0),
                "fetch_permanently_failed": fetcher_stats.get("permanently_failed", 0),
                "documents_processed": stats["total_processed"],
                "successful": stats["total_successful"],
                "failed": stats["total_failed"],
                "skipped": stats["total_skipped"],
                "total_chunks": stats["total_chunks"],
                "attachments_processed": stats["total_attachments"],
                "attachment_chunks": stats["total_attachment_chunks"],
                "files_deleted": stats["total_deleted"],
                "batches_processed": stats["batches_processed"],
                "duration_seconds": duration,
            }

            self.job_manager.complete_job(
                job_id,
                statistics=combined_stats,
                message=f"Full pipeline completed: {stats['total_successful']} documents processed successfully",
            )

            # Record execution
            self.state_manager.add_execution_record(
                status="success",
                duration_seconds=duration,
                documents_processed=stats["total_successful"],
                statistics=combined_stats,
            )

            logger.info(
                f"Scheduled pipeline execution completed successfully in {duration:.2f}s"
            )
            logger.info(
                f"Statistics: Native IDs={fetcher_stats.get('ids_fetched', 0)}, "
                f"Fetched={stats['total_fetched']}, "
                f"Fetch Errors={fetcher_stats.get('errors', 0)}, "
                f"Retry Attempts={fetcher_stats.get('retry_attempts', 0)}, "
                f"Recovered={fetcher_stats.get('documents_recovered', 0)}, "
                f"Permanently Failed={fetcher_stats.get('permanently_failed', 0)}, "
                f"Processed={stats['total_processed']}, "
                f"Successful={stats['total_successful']}, "
                f"Failed={stats['total_failed']}, "
                f"Skipped={stats['total_skipped']}, "
                f"Batches={stats['batches_processed']}"
            )

        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            error_msg = str(e)

            logger.error(f"Scheduled pipeline execution failed: {e}", exc_info=True)

            # Fail job
            self.job_manager.fail_job(job_id, error=error_msg)

            # Record execution
            self.state_manager.add_execution_record(
                status="failed",
                duration_seconds=duration,
                error=error_msg,
            )

        finally:
            self._running_job_id = None

            # Update next execution time
            next_run = self.get_next_run_time()
            if next_run:
                self.state_manager.update_execution_times(next_execution=next_run)
