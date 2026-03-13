"""Tests for scheduler service."""

import time
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from app.services import SchedulerService
from app.services.scheduler_state import SchedulerStateManager


@pytest.fixture
def temp_state_file(tmp_path):
    """Create temporary state file."""
    return str(tmp_path / "test_scheduler_state.json")


@pytest.fixture
def state_manager(temp_state_file):
    """Create state manager with temp file."""
    return SchedulerStateManager(temp_state_file)


@pytest.fixture
def mock_job_manager():
    """Create mock job manager."""
    manager = MagicMock()
    manager.create_job.return_value = "test-job-123"
    return manager


@pytest.fixture
def mock_fetcher():
    """Create mock data fetcher."""
    fetcher = MagicMock()
    fetcher.fetch_all.return_value = {"documents_fetched": 10}
    return fetcher


@pytest.fixture
def mock_pipeline():
    """Create mock ingestion pipeline."""
    pipeline = MagicMock()
    pipeline.ingest_all.return_value = {"documents_ingested": 10}
    return pipeline


@pytest.fixture
def scheduler(mock_job_manager, mock_fetcher, mock_pipeline, temp_state_file):
    """Create scheduler service with mocks."""
    with patch("app.services.scheduler.settings") as mock_settings:
        mock_settings.SCHEDULER_ENABLED = True
        mock_settings.SCHEDULER_INTERVAL_HOURS = 1
        mock_settings.SCHEDULER_START_TIME = ""
        mock_settings.SCHEDULER_TIMEZONE = "Europe/Helsinki"
        mock_settings.SCHEDULER_MAX_INSTANCES = 1
        mock_settings.SCHEDULER_STATE_FILE = temp_state_file
        mock_settings.SCHEDULER_START_DATE = "2025-01-01T00:00:00"
        mock_settings.SCHEDULER_END_DATE = "2025-12-31T23:59:59"
        mock_settings.SCHEDULER_BATCH_SIZE = 100
        mock_settings.SCHEDULER_SKIP_EXISTING = True
        mock_settings.SCHEDULER_KEEP_FILES = False
        mock_settings.START_DATE = "2025-01-01T00:00:00"
        mock_settings.END_DATE = "2025-12-31T23:59:59"

        service = SchedulerService(
            job_manager=mock_job_manager,
            fetcher=mock_fetcher,
            pipeline=mock_pipeline,
        )

        yield service

        # Cleanup
        if service.scheduler is not None:
            service.shutdown(wait=False)


class TestSchedulerStateManager:
    """Tests for SchedulerStateManager."""

    def test_load_state_creates_default(self, state_manager):
        """Test loading state creates default when file doesn't exist."""
        state = state_manager.load_state()

        assert state.enabled is False
        assert state.interval_hours == 24
        assert state.timezone == "Europe/Helsinki"
        assert state.last_execution_time is None
        assert state.execution_history == []

    def test_save_and_load_state(self, state_manager):
        """Test saving and loading state."""
        state = state_manager.load_state()
        state.enabled = True
        state.interval_hours = 12

        state_manager.save_state()

        # Load again
        state_manager._state = None
        loaded_state = state_manager.load_state()

        assert loaded_state.enabled is True
        assert loaded_state.interval_hours == 12

    def test_update_enabled(self, state_manager):
        """Test updating enabled status."""
        state_manager.load_state()
        state_manager.update_enabled(True)

        state = state_manager.get_state()
        assert state.enabled is True

    def test_update_schedule(self, state_manager):
        """Test updating schedule configuration."""
        state_manager.load_state()
        state_manager.update_schedule(6, "UTC")

        state = state_manager.get_state()
        assert state.interval_hours == 6
        assert state.timezone == "UTC"

    def test_update_execution_times(self, state_manager):
        """Test updating execution times."""
        state_manager.load_state()
        now = datetime.now()
        state_manager.update_execution_times(last_execution=now)

        state = state_manager.get_state()
        assert state.last_execution_time == now.isoformat()

    def test_add_execution_record(self, state_manager):
        """Test adding execution record."""
        state_manager.load_state()
        state_manager.add_execution_record(
            status="success",
            duration_seconds=120.5,
            documents_processed=50,
        )

        history = state_manager.get_recent_history(1)
        assert len(history) == 1
        assert history[0].status == "success"
        assert history[0].duration_seconds == 120.5
        assert history[0].documents_processed == 50

    def test_add_execution_record_trims_history(self, state_manager):
        """Test that history is trimmed to max size."""
        state_manager.load_state()

        # Add more than MAX_HISTORY_SIZE records
        for i in range(150):
            state_manager.add_execution_record(status="success")

        state = state_manager.get_state()
        assert len(state.execution_history) == state_manager.MAX_HISTORY_SIZE

    def test_failure_count_increments(self, state_manager):
        """Test failure count increments on failures."""
        state_manager.load_state()

        state_manager.add_execution_record(status="failed", error="Test error")
        state = state_manager.get_state()
        assert state.failure_count == 1

        state_manager.add_execution_record(status="failed", error="Test error 2")
        state = state_manager.get_state()
        assert state.failure_count == 2

    def test_failure_count_resets_on_success(self, state_manager):
        """Test failure count resets on success."""
        state_manager.load_state()

        state_manager.add_execution_record(status="failed")
        state_manager.add_execution_record(status="failed")
        state = state_manager.get_state()
        assert state.failure_count == 2

        state_manager.add_execution_record(status="success")
        state = state_manager.get_state()
        assert state.failure_count == 0

    def test_set_paused(self, state_manager):
        """Test setting paused state."""
        state_manager.load_state()
        state_manager.set_paused(True)

        state = state_manager.get_state()
        assert state.paused is True
        assert state.paused_at is not None

        state_manager.set_paused(False)
        state = state_manager.get_state()
        assert state.paused is False
        assert state.paused_at is None

    def test_reset_state(self, state_manager):
        """Test resetting state."""
        state_manager.load_state()
        state_manager.update_enabled(True)
        state_manager.add_execution_record(status="success")

        state_manager.reset_state()

        state = state_manager.get_state()
        assert state.enabled is False
        assert len(state.execution_history) == 0


class TestSchedulerService:
    """Tests for SchedulerService."""

    def test_initialization(self, scheduler):
        """Test scheduler initialization."""
        assert scheduler.scheduler is None
        assert scheduler._running_job_id is None

    @patch("app.services.scheduler.settings")
    def test_start_with_interval_trigger(self, mock_settings, scheduler):
        """Test starting scheduler with interval trigger."""
        mock_settings.SCHEDULER_ENABLED = True
        mock_settings.SCHEDULER_INTERVAL_HOURS = 2
        mock_settings.SCHEDULER_START_TIME = ""
        mock_settings.SCHEDULER_TIMEZONE = "Europe/Helsinki"
        mock_settings.SCHEDULER_MAX_INSTANCES = 1
        mock_settings.SCHEDULER_STATE_FILE = scheduler.state_manager.state_file_path

        scheduler.start()

        assert scheduler.scheduler is not None
        assert scheduler.scheduler.running

        scheduler.shutdown(wait=False)

    @patch("app.services.scheduler.settings")
    def test_start_with_cron_trigger(self, mock_settings, scheduler):
        """Test starting scheduler with cron trigger."""
        mock_settings.SCHEDULER_ENABLED = True
        mock_settings.SCHEDULER_INTERVAL_HOURS = 24
        mock_settings.SCHEDULER_START_TIME = "02:00"
        mock_settings.SCHEDULER_TIMEZONE = "Europe/Helsinki"
        mock_settings.SCHEDULER_MAX_INSTANCES = 1
        mock_settings.SCHEDULER_STATE_FILE = scheduler.state_manager.state_file_path

        scheduler.start()

        assert scheduler.scheduler is not None
        assert scheduler.scheduler.running

        scheduler.shutdown(wait=False)

    def test_start_when_disabled(self, scheduler):
        """Test that start does nothing when disabled."""
        with patch("app.services.scheduler.settings") as mock_settings:
            mock_settings.SCHEDULER_ENABLED = False

            scheduler.start()

            assert scheduler.scheduler is None

    def test_pause_and_resume(self, scheduler):
        """Test pausing and resuming scheduler."""
        scheduler.start()

        # Pause
        result = scheduler.pause()
        assert result is True

        state = scheduler.state_manager.get_state()
        assert state.paused is True

        # Resume
        result = scheduler.resume()
        assert result is True

        state = scheduler.state_manager.get_state()
        assert state.paused is False

        scheduler.shutdown(wait=False)

    def test_pause_when_not_running(self, scheduler):
        """Test pause when scheduler is not running."""
        result = scheduler.pause()
        assert result is False

    def test_get_status(self, scheduler):
        """Test getting scheduler status."""
        status = scheduler.get_status()

        assert "enabled" in status
        assert "running" in status
        assert "paused" in status
        assert "interval_hours" in status
        assert "timezone" in status

    def test_is_healthy_when_disabled(self, scheduler):
        """Test health check when disabled."""
        with patch("app.services.scheduler.settings") as mock_settings:
            mock_settings.SCHEDULER_ENABLED = False

            result = scheduler.is_healthy()
            assert result is True

    def test_is_healthy_when_enabled_and_running(self, scheduler):
        """Test health check when enabled and running."""
        scheduler.start()

        result = scheduler.is_healthy()
        assert result is True

        scheduler.shutdown(wait=False)

    def test_is_healthy_when_enabled_but_not_running(self, scheduler):
        """Test health check when enabled but not running."""
        with patch("app.services.scheduler.settings") as mock_settings:
            mock_settings.SCHEDULER_ENABLED = True

            result = scheduler.is_healthy()
            assert result is False

    def test_trigger_now(self, scheduler):
        """Test manual trigger."""
        job_id = scheduler.trigger_now()

        # Give thread time to start
        time.sleep(0.1)

        assert job_id is not None

    def test_trigger_now_when_job_running(self, scheduler, mock_job_manager):
        """Test manual trigger when job already running."""
        # Simulate running job
        scheduler._running_job_id = "existing-job"
        mock_job_manager.get_job.return_value = MagicMock(status="running")

        job_id = scheduler.trigger_now()

        assert job_id is None

    def test_get_next_run_time(self, scheduler):
        """Test getting next run time."""
        scheduler.start()

        next_run = scheduler.get_next_run_time()

        assert next_run is not None
        assert isinstance(next_run, datetime)

        scheduler.shutdown(wait=False)

    def test_get_next_run_time_when_not_started(self, scheduler):
        """Test getting next run time when not started."""
        result = scheduler.get_next_run_time()
        assert result is None


@pytest.mark.asyncio
class TestSchedulerEndpoints:
    """Test scheduler API endpoints."""

    async def test_get_scheduler_health(self, scheduler):
        """Test health endpoint."""
        from app.api.v1.endpoints.scheduler import get_scheduler_health

        with patch("app.api.v1.endpoints.scheduler.get_scheduler", return_value=scheduler):
            response = await get_scheduler_health(scheduler)

            assert response.healthy in [True, False]
            assert response.status is not None

    async def test_get_scheduler_status(self, scheduler):
        """Test status endpoint."""
        from app.api.v1.endpoints.scheduler import get_scheduler_status

        with patch("app.api.v1.endpoints.scheduler.get_scheduler", return_value=scheduler):
            response = await get_scheduler_status(scheduler)

            assert hasattr(response, "enabled")
            assert hasattr(response, "running")
            assert hasattr(response, "paused")

    async def test_trigger_manual_execution(self, scheduler):
        """Test trigger endpoint."""
        from app.api.v1.endpoints.scheduler import trigger_manual_execution

        with patch("app.api.v1.endpoints.scheduler.get_scheduler", return_value=scheduler):
            response = await trigger_manual_execution(scheduler)

            assert response.success in [True, False]
            assert response.message is not None

    async def test_pause_scheduler_endpoint(self, scheduler):
        """Test pause endpoint."""
        from app.api.v1.endpoints.scheduler import pause_scheduler

        scheduler.start()

        with patch("app.api.v1.endpoints.scheduler.get_scheduler", return_value=scheduler):
            response = await pause_scheduler(scheduler)

            assert response.success is True
            assert "paused" in response.message.lower()

        scheduler.shutdown(wait=False)

    async def test_resume_scheduler_endpoint(self, scheduler):
        """Test resume endpoint."""
        from app.api.v1.endpoints.scheduler import resume_scheduler

        scheduler.start()
        scheduler.pause()

        with patch("app.api.v1.endpoints.scheduler.get_scheduler", return_value=scheduler):
            response = await resume_scheduler(scheduler)

            assert response.success is True
            assert "resumed" in response.message.lower()

        scheduler.shutdown(wait=False)

    async def test_get_execution_history(self, scheduler):
        """Test history endpoint."""
        from app.api.v1.endpoints.scheduler import get_execution_history

        # Add some history
        scheduler.state_manager.load_state()
        scheduler.state_manager.add_execution_record(
            status="success", duration_seconds=100, documents_processed=10
        )

        with patch("app.api.v1.endpoints.scheduler.get_scheduler", return_value=scheduler):
            response = await get_execution_history(count=10, scheduler=scheduler)

            assert isinstance(response, list)
            assert len(response) == 1
            assert response[0].status == "success"
