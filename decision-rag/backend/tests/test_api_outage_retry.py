"""
Tests for API outage detection and batch-level retry logic.
"""

import time
from datetime import datetime
from unittest.mock import MagicMock, patch

import httpx
import pytest

from app.core.config import settings
from app.schemas.decision import DecisionDocument, DecisionId, DecisionIdResponse
from app.services.api_client import APIOutageError, DecisionAPIClient
from app.services.data_fetcher import DecisionDataFetcher


class TestAPIOutageDetection:
    """Test API outage detection in api_client."""

    @patch("app.services.api_client.httpx.Client")
    def test_404_on_decision_ids_raises_api_outage_error(self, mock_client_class):
        """Test that 404 from decision IDs endpoint raises APIOutageError."""
        # Setup mock client
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        # Create mock response with 404
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.text = "Not Found"
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "404 Not Found", request=MagicMock(), response=mock_response
        )

        mock_client.get.return_value = mock_response

        # Create client and attempt to fetch decision IDs
        client = DecisionAPIClient()

        with pytest.raises(APIOutageError) as exc_info:
            client.fetch_decision_ids("test-key", "2024-01-01", "2024-01-07")

        assert "API returned 404" in str(exc_info.value)

    @patch("app.services.api_client.httpx.Client")
    def test_504_on_decision_ids_raises_api_outage_error(self, mock_client_class):
        """Test that 504 from decision IDs endpoint raises APIOutageError."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        mock_response = MagicMock()
        mock_response.status_code = 504
        mock_response.text = "Gateway Timeout"
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "504 Gateway Timeout", request=MagicMock(), response=mock_response
        )

        mock_client.get.return_value = mock_response

        client = DecisionAPIClient()

        with pytest.raises(APIOutageError) as exc_info:
            client.fetch_decision_ids("test-key", "2024-01-01", "2024-01-07")

        assert "API returned 504" in str(exc_info.value)

    @patch("app.services.api_client.httpx.Client")
    def test_non_404_or_500_http_errors_not_treated_as_outage(self, mock_client_class):
        """Test that non-404 or 500 HTTP errors are not treated as API outage."""
        # Setup mock client
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        # Create mock response with 401 error
        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_response.text = "Unauthorized"
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "401 Unauthorized", request=MagicMock(), response=mock_response
        )

        mock_client.get.return_value = mock_response

        # Create client and attempt to fetch decision IDs
        client = DecisionAPIClient()

        # Should raise HTTPStatusError, not APIOutageError
        with pytest.raises(httpx.HTTPStatusError):
            client.fetch_decision_ids("test-key", "2024-01-01", "2024-01-07")

    @patch("app.services.api_client.httpx.Client")
    def test_successful_decision_ids_fetch_no_error(self, mock_client_class):
        """Test successful decision IDs fetch does not raise any error."""
        # Setup mock client
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        # Create successful response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "count": 2,
            "decisions": [
                {"NativeId": "TEST-001"},
                {"NativeId": "TEST-002"},
            ],
        }
        mock_client.get.return_value = mock_response

        # Create client and fetch decision IDs
        client = DecisionAPIClient()
        result = client.fetch_decision_ids("test-key", "2024-01-01", "2024-01-07")

        assert result.count == 2
        assert len(result.decisions) == 2


class TestBatchRetryLogic:
    """Test batch-level retry logic in data_fetcher."""

    @patch("app.services.data_fetcher.DecisionAPIClient")
    def test_successful_batch_retry_after_outage(self, mock_api_client_class):
        """Test successful batch retry after API outage."""
        # Create mock API client
        mock_client = MagicMock()
        mock_api_client_class.return_value = mock_client

        # First call raises APIOutageError, second call succeeds
        mock_client.fetch_decision_ids.side_effect = [
            APIOutageError("API outage"),
            DecisionIdResponse(
                count=1, decisions=[DecisionId(NativeId="TEST-001")]
            ),
        ]

        mock_client.fetch_decision_document.return_value = DecisionDocument(
            NativeId="TEST-001",
            Title="Test",
            Content="Test content",
            DateDecision="2024-01-15",
            CaseIDLabel="TEST",
        )

        # Create fetcher with mocked client
        fetcher = DecisionDataFetcher(api_client=mock_client)

        # Patch settings to use shorter wait times for testing
        with patch.object(settings, "API_OUTAGE_INITIAL_WAIT", 0.1):
            with patch.object(settings, "API_OUTAGE_MAX_RETRIES", 3):
                start_date = datetime(2024, 1, 1)
                end_date = datetime(2024, 1, 7)

                # Fetch batch - should retry and succeed
                documents = fetcher._fetch_batch(start_date, end_date, "test-key")

                # Verify stats
                assert fetcher.stats["api_outages_detected"] == 1
                assert fetcher.stats["batch_retries_attempted"] == 1
                assert fetcher.stats["batches_recovered_from_outage"] == 1
                assert fetcher.stats["batches_permanently_failed"] == 0
                assert len(documents) == 1

    @patch("app.services.data_fetcher.DecisionAPIClient")
    def test_batch_permanent_failure_after_max_retries(self, mock_api_client_class):
        """Test batch permanently fails after max retry attempts."""
        # Create mock API client
        mock_client = MagicMock()
        mock_api_client_class.return_value = mock_client

        # All calls raise APIOutageError
        mock_client.fetch_decision_ids.side_effect = APIOutageError("API outage")

        # Create fetcher with mocked client
        fetcher = DecisionDataFetcher(api_client=mock_client)

        # Patch settings to use shorter wait times and fewer retries
        with patch.object(settings, "API_OUTAGE_INITIAL_WAIT", 0.05):
            with patch.object(settings, "API_OUTAGE_MAX_RETRIES", 2):
                start_date = datetime(2024, 1, 1)
                end_date = datetime(2024, 1, 7)

                # Fetch batch - should fail permanently
                documents = fetcher._fetch_batch(start_date, end_date, "test-key")

                # Verify stats
                assert fetcher.stats["api_outages_detected"] == 1
                assert fetcher.stats["batch_retries_attempted"] == 2
                assert fetcher.stats["batches_recovered_from_outage"] == 0
                assert fetcher.stats["batches_permanently_failed"] == 1
                assert len(documents) == 0

    @patch("app.services.data_fetcher.DecisionAPIClient")
    def test_exponential_backoff_timing(self, mock_api_client_class):
        """Test that exponential backoff is applied correctly."""
        # Create mock API client
        mock_client = MagicMock()
        mock_api_client_class.return_value = mock_client

        # Fail twice, then succeed
        mock_client.fetch_decision_ids.side_effect = [
            APIOutageError("API outage"),
            APIOutageError("API outage"),
            APIOutageError("API outage"),
            DecisionIdResponse(
                count=1, decisions=[DecisionId(NativeId="TEST-001")]
            ),
        ]

        mock_client.fetch_decision_document.return_value = DecisionDocument(
            NativeId="TEST-001",
            Title="Test",
            Content="Test content",
            DateDecision="2024-01-15",
            CaseIDLabel="TEST",
        )

        # Create fetcher with mocked client
        fetcher = DecisionDataFetcher(api_client=mock_client)

        # Patch settings
        initial_wait = 0.1
        backoff_multiplier = 2.0
        with patch.object(settings, "API_OUTAGE_INITIAL_WAIT", initial_wait):
            with patch.object(settings, "API_OUTAGE_BACKOFF_MULTIPLIER", backoff_multiplier):
                with patch.object(settings, "API_OUTAGE_MAX_RETRIES", 5):
                    start_date = datetime(2024, 1, 1)
                    end_date = datetime(2024, 1, 7)

                    # Measure time
                    start_time = time.time()
                    _ = fetcher._fetch_batch(start_date, end_date, "test-key")
                    elapsed = time.time() - start_time

                    # Calculate expected wait times:
                    # Attempt 1: 0.1s
                    # Attempt 2: 0.2s
                    # Attempt 3: 0.4s
                    expected_wait = initial_wait * (1 + backoff_multiplier + backoff_multiplier ** 2)

                    # Allow some tolerance
                    assert elapsed >= expected_wait * 0.8
                    assert fetcher.stats["batch_retries_attempted"] == 3

    @patch("app.services.data_fetcher.DecisionAPIClient")
    def test_max_wait_time_respected(self, mock_api_client_class):
        """Test that maximum wait time is not exceeded."""
        # Create mock API client
        mock_client = MagicMock()
        mock_api_client_class.return_value = mock_client

        # Fail several times
        mock_client.fetch_decision_ids.side_effect = [
            APIOutageError("API outage"),
        ] * 5

        # Create fetcher with mocked client
        fetcher = DecisionDataFetcher(api_client=mock_client)

        # Patch settings - high backoff but low max wait
        with patch.object(settings, "API_OUTAGE_INITIAL_WAIT", 0.1):
            with patch.object(settings, "API_OUTAGE_BACKOFF_MULTIPLIER", 10.0):
                with patch.object(settings, "API_OUTAGE_MAX_WAIT", 0.15):
                    with patch.object(settings, "API_OUTAGE_MAX_RETRIES", 3):
                        start_date = datetime(2024, 1, 1)
                        end_date = datetime(2024, 1, 7)

                        start_time = time.time()
                        _ = fetcher._fetch_batch(start_date, end_date, "test-key")
                        elapsed = time.time() - start_time

                        # Expected: max_wait * retries = 0.15 * 3 = 0.45s
                        # With tolerances for execution overhead
                        assert elapsed < 1.0  # Should not take long due to max wait
                        assert fetcher.stats["batch_retries_attempted"] == 3


class TestBatchRetryIntegration:
    """Integration tests for batch retry with full pipeline."""

    @patch("app.services.data_fetcher.DecisionAPIClient")
    def test_full_batch_processing_with_mid_pipeline_outage(self, mock_api_client_class):
        """Test full batch processing when outage occurs mid-pipeline."""
        # Create mock API client
        mock_client = MagicMock()
        mock_api_client_class.return_value = mock_client

        # Simulate: batch 1 succeeds, batch 2 has outage, batch 3 succeeds
        call_count = [0]

        def fetch_side_effect(api_key, start_date, end_date):
            call_count[0] += 1
            if call_count[0] == 2:
                # Second batch - initial call fails
                raise APIOutageError("API outage")
            # Return successful response
            return DecisionIdResponse(
                count=1, decisions=[DecisionId(NativeId=f"TEST-{call_count[0]:03d}")]
            )

        mock_client.fetch_decision_ids.side_effect = fetch_side_effect

        mock_client.fetch_decision_document.return_value = DecisionDocument(
            NativeId="TEST-001",
            Title="Test",
            Content="Test content",
            DateDecision="2024-01-15",
            CaseIDLabel="TEST",
        )

        # Create fetcher
        fetcher = DecisionDataFetcher(api_client=mock_client)

        # Patch settings
        with patch.object(settings, "API_OUTAGE_INITIAL_WAIT", 0.05):
            with patch.object(settings, "API_OUTAGE_MAX_RETRIES", 2):
                with patch.object(settings, "BATCH_SIZE_DAYS", 7):
                    # Fetch documents across 3 weeks
                    start_date = datetime(2024, 1, 1)
                    end_date = datetime(2024, 1, 21)

                    _ = list(
                        fetcher.fetch_all_decisions("test-key", start_date, end_date)
                    )

                    # Should successfully process all batches
                    assert fetcher.stats["total_batches"] == 3
                    assert fetcher.stats["batches_completed"] == 3
                    assert fetcher.stats["api_outages_detected"] == 1
                    assert fetcher.stats["batches_recovered_from_outage"] == 1

    @patch("app.services.data_fetcher.DecisionAPIClient")
    def test_continuation_after_api_recovery(self, mock_api_client_class):
        """Test pipeline continues after API recovery."""
        # Create mock API client
        mock_client = MagicMock()
        mock_api_client_class.return_value = mock_client

        # First call fails, second succeeds
        mock_client.fetch_decision_ids.side_effect = [
            APIOutageError("API outage"),
            DecisionIdResponse(
                count=2,
                decisions=[
                    DecisionId(NativeId="TEST-001"),
                    DecisionId(NativeId="TEST-002"),
                ],
            ),
        ]

        mock_client.fetch_decision_document.side_effect = [
            DecisionDocument(
                NativeId="TEST-001",
                Title="Test 1",
                Content="Content 1",
                DateDecision="2024-01-15",
                CaseIDLabel="TEST",
            ),
            DecisionDocument(
                NativeId="TEST-002",
                Title="Test 2",
                Content="Content 2",
                DateDecision="2024-01-16",
                CaseIDLabel="TEST",
            ),
        ]

        # Create fetcher
        fetcher = DecisionDataFetcher(api_client=mock_client)

        # Patch settings
        with patch.object(settings, "API_OUTAGE_INITIAL_WAIT", 0.05):
            with patch.object(settings, "API_OUTAGE_MAX_RETRIES", 3):
                start_date = datetime(2024, 1, 1)
                end_date = datetime(2024, 1, 7)

                documents = list(
                    fetcher.fetch_all_decisions("test-key", start_date, end_date)
                )

                # Should recover and fetch both documents
                assert len(documents) == 2
                assert fetcher.stats["documents_fetched"] == 2
                assert fetcher.stats["batches_recovered_from_outage"] == 1


class TestStatisticsTracking:
    """Test statistics tracking for API outage retry."""

    @patch("app.services.data_fetcher.DecisionAPIClient")
    def test_statistics_tracking_accuracy(self, mock_api_client_class):
        """Test that all statistics are tracked accurately."""
        # Create mock API client
        mock_client = MagicMock()
        mock_api_client_class.return_value = mock_client

        # Simulate multiple outages and retries
        responses = [
            # Batch 1: Outage, then recovery after 2 retries
            APIOutageError("Outage 1"),
            APIOutageError("Outage 1 retry 1"),
            DecisionIdResponse(count=1, decisions=[DecisionId(NativeId="TEST-001")]),
            # Batch 2: Success immediately
            DecisionIdResponse(count=1, decisions=[DecisionId(NativeId="TEST-002")]),
            # Batch 3: Permanent failure
            APIOutageError("Outage 3"),
        ] + [APIOutageError("Outage 3")] * 5

        mock_client.fetch_decision_ids.side_effect = responses

        mock_client.fetch_decision_document.return_value = DecisionDocument(
            NativeId="TEST-001",
            Title="Test",
            Content="Test content",
            DateDecision="2024-01-15",
            CaseIDLabel="TEST",
        )

        # Create fetcher
        fetcher = DecisionDataFetcher(api_client=mock_client)

        # Patch settings
        with patch.object(settings, "API_OUTAGE_INITIAL_WAIT", 0.01):
            with patch.object(settings, "API_OUTAGE_MAX_RETRIES", 2):
                with patch.object(settings, "BATCH_SIZE_DAYS", 7):
                    start_date = datetime(2024, 1, 1)
                    end_date = datetime(2024, 1, 21)

                    _ = list(
                        fetcher.fetch_all_decisions("test-key", start_date, end_date)
                    )

                    # Verify statistics
                    assert fetcher.stats["total_batches"] == 3
                    assert fetcher.stats["api_outages_detected"] == 2  # Batch 1 and 3
                    assert fetcher.stats["batch_retries_attempted"] == 4  # 2 for batch 1, 2 for batch 3
                    assert fetcher.stats["batches_recovered_from_outage"] == 1  # Batch 1
                    assert fetcher.stats["batches_permanently_failed"] == 1  # Batch 3


class TestEdgeCases:
    """Test edge cases and error scenarios."""

    @patch("app.services.data_fetcher.DecisionAPIClient")
    def test_batch_with_404_on_ids_but_eventual_recovery(self, mock_api_client_class):
        """Test batch that fails initially but recovers."""
        mock_client = MagicMock()
        mock_api_client_class.return_value = mock_client

        # Fail once, then recover
        mock_client.fetch_decision_ids.side_effect = [
            APIOutageError("API outage"),
            DecisionIdResponse(count=0, decisions=[]),  # Empty but valid
        ]

        fetcher = DecisionDataFetcher(api_client=mock_client)

        with patch.object(settings, "API_OUTAGE_INITIAL_WAIT", 0.01):
            with patch.object(settings, "API_OUTAGE_MAX_RETRIES", 3):
                start_date = datetime(2024, 1, 1)
                end_date = datetime(2024, 1, 7)

                documents = fetcher._fetch_batch(start_date, end_date, "test-key")

                assert len(documents) == 0
                assert fetcher.stats["batches_recovered_from_outage"] == 1

    @patch("app.services.data_fetcher.DecisionAPIClient")
    def test_document_failures_after_successful_ids_fetch(self, mock_api_client_class):
        """Test that document-level failures are handled separately from API outages."""
        mock_client = MagicMock()
        mock_api_client_class.return_value = mock_client

        # IDs fetch succeeds
        mock_client.fetch_decision_ids.return_value = DecisionIdResponse(
            count=2,
            decisions=[
                DecisionId(NativeId="TEST-001"),
                DecisionId(NativeId="TEST-002"),
            ],
        )

        # Document fetch fails for one document
        mock_client.fetch_decision_document.side_effect = [
            DecisionDocument(
                NativeId="TEST-001",
                Title="Test",
                Content="Test content",
                DateDecision="2024-01-15",
                CaseIDLabel="TEST",
            ),
            None,  # Simulate document not found
        ]

        fetcher = DecisionDataFetcher(api_client=mock_client)

        start_date = datetime(2024, 1, 1)
        end_date = datetime(2024, 1, 7)

        with patch.object(settings, "DOCUMENT_FETCH_MAX_RETRIES", 0):
            _ = fetcher._fetch_batch(start_date, end_date, "test-key")
            # No API outage detected
            assert fetcher.stats["api_outages_detected"] == 0
            assert fetcher.stats["batch_retries_attempted"] == 0
            # But document errors tracked
            assert fetcher.stats["errors"] >= 1

    @patch("app.services.data_fetcher.DecisionAPIClient")
    def test_unexpected_error_during_retry(self, mock_api_client_class):
        """Test handling of unexpected errors during batch retry."""
        mock_client = MagicMock()
        mock_api_client_class.return_value = mock_client

        # Initial call raises APIOutageError, retry raises different error
        mock_client.fetch_decision_ids.side_effect = [
            APIOutageError("API outage"),
            ValueError("Unexpected error"),
        ]

        fetcher = DecisionDataFetcher(api_client=mock_client)

        with patch.object(settings, "API_OUTAGE_INITIAL_WAIT", 0.01):
            with patch.object(settings, "API_OUTAGE_MAX_RETRIES", 3):
                start_date = datetime(2024, 1, 1)
                end_date = datetime(2024, 1, 7)

                documents = fetcher._fetch_batch(start_date, end_date, "test-key")

                # Should handle gracefully and return empty
                assert len(documents) == 0
                assert fetcher.stats["api_outages_detected"] == 1
                assert fetcher.stats["batch_retries_attempted"] == 1
                assert fetcher.stats["batches_permanently_failed"] == 1

    @patch("app.services.data_fetcher.DecisionAPIClient")
    def test_no_outage_normal_operation(self, mock_api_client_class):
        """Test normal operation without any outages."""
        mock_client = MagicMock()
        mock_api_client_class.return_value = mock_client

        # All calls succeed
        mock_client.fetch_decision_ids.return_value = DecisionIdResponse(
            count=2,
            decisions=[
                DecisionId(NativeId="TEST-001"),
                DecisionId(NativeId="TEST-002"),
            ],
        )

        mock_client.fetch_decision_document.side_effect = [
            DecisionDocument(
                NativeId="TEST-001",
                Title="Test 1",
                Content="Content 1",
                DateDecision="2024-01-15",
                CaseIDLabel="TEST",
            ),
            DecisionDocument(
                NativeId="TEST-002",
                Title="Test 2",
                Content="Content 2",
                DateDecision="2024-01-16",
                CaseIDLabel="TEST",
            ),
        ]

        fetcher = DecisionDataFetcher(api_client=mock_client)

        # Patch BATCH_SIZE_DAYS to ensure only 1 batch is created
        # This matches the mock setup which provides exactly 2 document responses
        with patch.object(settings, "BATCH_SIZE_DAYS", 7):
            start_date = datetime(2024, 1, 1)
            end_date = datetime(2024, 1, 7)

            documents = list(
                fetcher.fetch_all_decisions("test-key", start_date, end_date)
            )

            # No outage statistics should be recorded
            assert fetcher.stats["api_outages_detected"] == 0
            assert fetcher.stats["batch_retries_attempted"] == 0
            assert fetcher.stats["batches_recovered_from_outage"] == 0
            assert fetcher.stats["batches_permanently_failed"] == 0
            # But documents should be fetched successfully
            assert len(documents) == 2
