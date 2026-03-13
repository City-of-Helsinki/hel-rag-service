"""State management for scheduler persistence and recovery."""

import json
import threading
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from app.core import get_logger

logger = get_logger(__name__)


@dataclass
class ExecutionRecord:
    """Record of a single pipeline execution."""

    timestamp: str
    status: str  # "success", "failed", "cancelled"
    duration_seconds: Optional[float] = None
    documents_processed: Optional[int] = None
    error: Optional[str] = None
    statistics: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SchedulerState:
    """Persistent state for scheduler."""

    enabled: bool = False
    interval_hours: int = 24
    timezone: str = "Europe/Helsinki"
    last_execution_time: Optional[str] = None
    next_execution_time: Optional[str] = None
    execution_history: List[ExecutionRecord] = field(default_factory=list)
    failure_count: int = 0
    paused: bool = False
    paused_at: Optional[str] = None


class SchedulerStateManager:
    """
    Thread-safe manager for scheduler state persistence.

    Handles reading, writing, and managing scheduler state with atomic operations
    to prevent corruption.
    """

    MAX_HISTORY_SIZE = 100  # Keep last 100 execution records

    def __init__(self, state_file_path: str):
        """
        Initialize state manager.

        Args:
            state_file_path: Path to state file
        """
        self.state_file_path = Path(state_file_path)
        self._lock = threading.RLock()
        self._state: Optional[SchedulerState] = None

        # Ensure parent directory exists
        self.state_file_path.parent.mkdir(parents=True, exist_ok=True)

    def load_state(self) -> SchedulerState:
        """
        Load state from file.

        Returns:
            SchedulerState instance

        If file doesn't exist or is corrupted, returns default state.
        """
        with self._lock:
            if self._state is not None:
                return self._state

            if not self.state_file_path.exists():
                logger.info("No state file found, creating default state")
                self._state = SchedulerState()
                return self._state

            try:
                with open(self.state_file_path, "r", encoding="utf-8") as f:
                    data = json.load(f)

                # Convert execution history
                execution_history = [
                    ExecutionRecord(**record) for record in data.get("execution_history", [])
                ]

                self._state = SchedulerState(
                    enabled=data.get("enabled", False),
                    interval_hours=data.get("interval_hours", 24),
                    timezone=data.get("timezone", "Europe/Helsinki"),
                    last_execution_time=data.get("last_execution_time"),
                    next_execution_time=data.get("next_execution_time"),
                    execution_history=execution_history,
                    failure_count=data.get("failure_count", 0),
                    paused=data.get("paused", False),
                    paused_at=data.get("paused_at"),
                )

                logger.info(f"Loaded scheduler state from {self.state_file_path}")
                return self._state

            except Exception as e:
                logger.error(f"Failed to load scheduler state: {e}", exc_info=True)
                logger.warning("Using default state")
                self._state = SchedulerState()
                return self._state

    def save_state(self) -> None:
        """
        Save current state to file atomically.

        Uses atomic write (write to temp file, then rename) to prevent corruption.
        """
        if self._state is None:
            logger.warning("No state to save")
            return

        with self._lock:
            try:
                # Prepare data
                data = {
                    "enabled": self._state.enabled,
                    "interval_hours": self._state.interval_hours,
                    "timezone": self._state.timezone,
                    "last_execution_time": self._state.last_execution_time,
                    "next_execution_time": self._state.next_execution_time,
                    "execution_history": [
                        asdict(record) for record in self._state.execution_history
                    ],
                    "failure_count": self._state.failure_count,
                    "paused": self._state.paused,
                    "paused_at": self._state.paused_at,
                }

                # Write to temporary file
                temp_file = self.state_file_path.with_suffix(".tmp")
                with open(temp_file, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)

                # Atomic rename
                temp_file.replace(self.state_file_path)

                logger.debug(f"Saved scheduler state to {self.state_file_path}")

            except Exception as e:
                logger.error(f"Failed to save scheduler state: {e}", exc_info=True)

    def get_state(self) -> SchedulerState:
        """
        Get current state.

        Returns:
            Current SchedulerState
        """
        if self._state is None:
            return self.load_state()
        return self._state

    def update_enabled(self, enabled: bool) -> None:
        """
        Update enabled status.

        Args:
            enabled: Whether scheduler is enabled
        """
        state = self.get_state()
        state.enabled = enabled
        self.save_state()

    def update_schedule(self, interval_hours: int, timezone: str) -> None:
        """
        Update schedule configuration.

        Args:
            interval_hours: Interval between executions
            timezone: Timezone for scheduling
        """
        state = self.get_state()
        state.interval_hours = interval_hours
        state.timezone = timezone
        self.save_state()

    def update_execution_times(
        self,
        last_execution: Optional[datetime] = None,
        next_execution: Optional[datetime] = None,
    ) -> None:
        """
        Update execution times.

        Args:
            last_execution: Last execution time
            next_execution: Next scheduled execution time
        """
        state = self.get_state()

        if last_execution is not None:
            state.last_execution_time = last_execution.isoformat()

        if next_execution is not None:
            state.next_execution_time = next_execution.isoformat()

        self.save_state()

    def add_execution_record(
        self,
        status: str,
        duration_seconds: Optional[float] = None,
        documents_processed: Optional[int] = None,
        error: Optional[str] = None,
        statistics: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Add execution record to history.

        Args:
            status: Execution status (success, failed, cancelled)
            duration_seconds: Execution duration
            documents_processed: Number of documents processed
            error: Error message if failed
            statistics: Additional execution statistics
        """
        state = self.get_state()

        record = ExecutionRecord(
            timestamp=datetime.now().isoformat(),
            status=status,
            duration_seconds=duration_seconds,
            documents_processed=documents_processed,
            error=error,
            statistics=statistics or {},
        )

        state.execution_history.append(record)

        # Trim history if too large
        if len(state.execution_history) > self.MAX_HISTORY_SIZE:
            state.execution_history = state.execution_history[-self.MAX_HISTORY_SIZE :]

        # Update failure count
        if status == "failed":
            state.failure_count += 1
        elif status == "success":
            state.failure_count = 0  # Reset on success

        self.save_state()

    def set_paused(self, paused: bool) -> None:
        """
        Set pause state.

        Args:
            paused: Whether scheduler is paused
        """
        state = self.get_state()
        state.paused = paused

        if paused:
            state.paused_at = datetime.now().isoformat()
        else:
            state.paused_at = None

        self.save_state()

    def get_recent_history(self, count: int = 10) -> List[ExecutionRecord]:
        """
        Get recent execution history.

        Args:
            count: Number of recent records to return

        Returns:
            List of execution records
        """
        state = self.get_state()
        return state.execution_history[-count:]

    def reset_state(self) -> None:
        """Reset state to defaults (for testing or maintenance)."""
        with self._lock:
            self._state = SchedulerState()
            self.save_state()
            logger.info("Scheduler state reset to defaults")
