"""Job manager for tracking background pipeline operations, which includes fetching, ingesting, and full pipeline runs.

Provides thread-safe management of job status, progress, and statistics."""

import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional


@dataclass
class Job:
    """Represents a background job."""

    job_id: str
    type: str  # "fetch", "ingest", "full_pipeline"
    status: str = "created"  # created, running, completed, failed, cancelled
    progress: Optional[float] = None  # 0-100
    message: Optional[str] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    statistics: Dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None


class JobManager:
    """
    Thread-safe manager for background jobs.

    Tracks job status, progress, and statistics for long-running pipeline operations.
    """

    def __init__(self):
        """Initialize job manager."""
        self._jobs: Dict[str, Job] = {}
        self._lock = threading.Lock()
        self._shutdown_requested = False

    def create_job(self, job_type: str) -> str:
        """
        Create a new job.

        Args:
            job_type: Type of job (fetch, ingest, full_pipeline)

        Returns:
            Job ID
        """
        job_id = str(uuid.uuid4())
        job = Job(job_id=job_id, type=job_type)

        with self._lock:
            self._jobs[job_id] = job

        return job_id

    def start_job(self, job_id: str) -> None:
        """
        Mark job as started.

        Args:
            job_id: Job identifier
        """
        with self._lock:
            if job_id in self._jobs:
                self._jobs[job_id].status = "running"
                self._jobs[job_id].start_time = datetime.now()

    def update_progress(
        self,
        job_id: str,
        progress: float,
        message: Optional[str] = None,
    ) -> None:
        """
        Update job progress.

        Args:
            job_id: Job identifier
            progress: Progress percentage (0-100)
            message: Optional status message
        """
        with self._lock:
            if job_id in self._jobs:
                self._jobs[job_id].progress = progress
                if message:
                    self._jobs[job_id].message = message

    def complete_job(
        self,
        job_id: str,
        statistics: Optional[Dict[str, Any]] = None,
        message: Optional[str] = None,
    ) -> None:
        """
        Mark job as completed.

        Args:
            job_id: Job identifier
            statistics: Job execution statistics
            message: Optional completion message
        """
        with self._lock:
            if job_id in self._jobs:
                self._jobs[job_id].status = "completed"
                self._jobs[job_id].end_time = datetime.now()
                self._jobs[job_id].progress = 100.0
                if statistics:
                    self._jobs[job_id].statistics = statistics
                if message:
                    self._jobs[job_id].message = message

    def fail_job(
        self,
        job_id: str,
        error: str,
        statistics: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Mark job as failed.

        Args:
            job_id: Job identifier
            error: Error message
            statistics: Partial statistics if available
        """
        with self._lock:
            if job_id in self._jobs:
                self._jobs[job_id].status = "failed"
                self._jobs[job_id].end_time = datetime.now()
                self._jobs[job_id].error = error
                self._jobs[job_id].message = f"Job failed: {error}"
                if statistics:
                    self._jobs[job_id].statistics = statistics

    def cancel_job(self, job_id: str) -> bool:
        """
        Cancel a running job.

        Args:
            job_id: Job identifier

        Returns:
            True if job was cancelled, False if job not found or already finished
        """
        with self._lock:
            if job_id in self._jobs:
                job = self._jobs[job_id]
                if job.status in ("created", "running"):
                    job.status = "cancelled"
                    job.end_time = datetime.now()
                    job.message = "Job cancelled by user"
                    return True
        return False

    def get_job(self, job_id: str) -> Optional[Job]:
        """
        Get job by ID.

        Args:
            job_id: Job identifier

        Returns:
            Job object or None if not found
        """
        with self._lock:
            return self._jobs.get(job_id)

    def list_jobs(
        self,
        job_type: Optional[str] = None,
        status: Optional[str] = None,
    ) -> List[Job]:
        """
        List all jobs with optional filters.

        Args:
            job_type: Filter by job type
            status: Filter by job status

        Returns:
            List of jobs matching filters
        """
        with self._lock:
            jobs = list(self._jobs.values())

        # Apply filters
        if job_type:
            jobs = [j for j in jobs if j.type == job_type]
        if status:
            jobs = [j for j in jobs if j.status == status]

        # Sort by start time (most recent first)
        jobs.sort(key=lambda j: j.start_time or datetime.min, reverse=True)

        return jobs

    def cleanup_old_jobs(self, max_jobs: int = 100) -> None:
        """
        Remove old completed/failed jobs to prevent memory buildup.

        Keeps only the most recent jobs.

        Args:
            max_jobs: Maximum number of jobs to keep
        """
        with self._lock:
            if len(self._jobs) <= max_jobs:
                return

            # Sort jobs by end time (most recent first)
            sorted_jobs = sorted(
                self._jobs.values(),
                key=lambda j: j.end_time or datetime.max,
                reverse=True,
            )

            # Keep only completed/failed jobs for cleanup
            finished_jobs = [
                j for j in sorted_jobs if j.status in ("completed", "failed", "cancelled")
            ]

            # Remove oldest finished jobs
            if len(finished_jobs) > max_jobs // 2:
                jobs_to_remove = finished_jobs[max_jobs // 2 :]
                for job in jobs_to_remove:
                    del self._jobs[job.job_id]

    def request_shutdown(self) -> None:
        """Request graceful shutdown of all pipeline operations."""
        with self._lock:
            self._shutdown_requested = True

    def is_shutdown_requested(self) -> bool:
        """
        Check if shutdown has been requested.

        Returns:
            True if shutdown was requested, False otherwise
        """
        with self._lock:
            return self._shutdown_requested

    def reset_shutdown_flag(self) -> None:
        """Reset the shutdown flag."""
        with self._lock:
            self._shutdown_requested = False


# Global job manager instance
job_manager = JobManager()
