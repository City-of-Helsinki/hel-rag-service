"""
Tests for shutdown mechanism and Elasticsearch retry logic.
"""

from unittest.mock import MagicMock, patch

import pytest

from app.services.elasticsearch_store import ElasticsearchVectorStore
from app.services.job_manager import JobManager
from app.services.vector_store import MaxRetriesExceededError


class TestJobManagerShutdown:
    """Test shutdown functionality in JobManager."""

    def test_shutdown_request(self):
        """Test that shutdown can be requested and checked."""
        manager = JobManager()

        # Initially no shutdown requested
        assert not manager.is_shutdown_requested()

        # Request shutdown
        manager.request_shutdown()
        assert manager.is_shutdown_requested()

        # Reset shutdown
        manager.reset_shutdown_flag()
        assert not manager.is_shutdown_requested()

    def test_shutdown_prevents_new_jobs(self):
        """Test that shutdown flag can be used to prevent new jobs."""
        manager = JobManager()

        # Create job normally
        job_id = manager.create_job("test")
        assert job_id is not None

        # Request shutdown
        manager.request_shutdown()

        # In actual implementation, the API endpoints check this flag
        # before creating jobs
        assert manager.is_shutdown_requested()


class TestElasticsearchRetryTracking:
    """Test Elasticsearch retry tracking functionality."""

    @patch("app.services.elasticsearch_store.Elasticsearch")
    def test_retry_count_initialization(self, mock_es_class):
        """Test that retry counter is initialized correctly."""
        mock_es_instance = MagicMock()
        mock_es_instance.ping.return_value = True
        mock_es_instance.indices.exists.return_value = True
        mock_es_class.return_value = mock_es_instance

        store = ElasticsearchVectorStore()

        assert store._retry_count == 0
        assert store._max_total_retries > 0

    @patch("app.services.elasticsearch_store.Elasticsearch")
    def test_increment_retry_count(self, mock_es_class):
        """Test retry counter increment."""
        mock_es_instance = MagicMock()
        mock_es_instance.ping.return_value = True
        mock_es_instance.indices.exists.return_value = True
        mock_es_class.return_value = mock_es_instance

        store = ElasticsearchVectorStore()
        store._max_total_retries = 5

        # Increment retry count
        store._increment_retry_count()
        assert store._retry_count == 1

        store._increment_retry_count()
        assert store._retry_count == 2

    @patch("app.services.elasticsearch_store.Elasticsearch")
    def test_max_retries_exceeded_error(self, mock_es_class):
        """Test that MaxRetriesExceededError is raised when limit reached."""
        mock_es_instance = MagicMock()
        mock_es_instance.ping.return_value = True
        mock_es_instance.indices.exists.return_value = True
        mock_es_class.return_value = mock_es_instance

        store = ElasticsearchVectorStore()
        store._max_total_retries = 3
        store._retry_count = 2

        # This should raise the error
        with pytest.raises(MaxRetriesExceededError) as exc_info:
            store._increment_retry_count()

        assert "Maximum total retry attempts" in str(exc_info.value)

    @patch("app.services.elasticsearch_store.Elasticsearch")
    def test_reset_retry_count(self, mock_es_class):
        """Test that retry counter resets after successful operation."""
        mock_es_instance = MagicMock()
        mock_es_instance.ping.return_value = True
        mock_es_instance.indices.exists.return_value = True
        mock_es_class.return_value = mock_es_instance

        store = ElasticsearchVectorStore()
        store._retry_count = 5

        # Reset should clear counter
        store._reset_retry_count()
        assert store._retry_count == 0


class TestIntegration:
    """Integration tests for shutdown and retry mechanisms."""

    def test_job_manager_with_multiple_jobs(self):
        """Test job manager with multiple jobs and shutdown."""
        manager = JobManager()

        # Create multiple jobs
        job1_id = manager.create_job("fetch")
        job2_id = manager.create_job("ingest")

        manager.start_job(job1_id)
        manager.start_job(job2_id)

        # Both jobs should be running
        running_jobs = manager.list_jobs(status="running")
        assert len(running_jobs) == 2

        # Request shutdown
        manager.request_shutdown()
        assert manager.is_shutdown_requested()

        # In actual implementation, running jobs would check this flag
        # and stop at the next checkpoint

    @patch("app.services.elasticsearch_store.Elasticsearch")
    def test_retry_tracking_across_operations(self, mock_es_class):
        """Test that retry tracking persists across multiple operations."""
        mock_es_instance = MagicMock()
        mock_es_instance.ping.return_value = True
        mock_es_instance.indices.exists.return_value = True
        mock_es_class.return_value = mock_es_instance

        store = ElasticsearchVectorStore()
        store._max_total_retries = 10

        # Simulate multiple failed operations
        for _i in range(3):
            store._increment_retry_count()

        assert store._retry_count == 3

        # Successful operation resets counter
        store._reset_retry_count()
        assert store._retry_count == 0
