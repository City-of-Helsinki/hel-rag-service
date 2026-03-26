"""
Tests for full data ingestion pipeline.
"""

from datetime import datetime

import pytest

from app.repositories.decision_repository import DecisionRepository
from app.schemas.decision import DecisionDocument


class TestDeleteMethods:
    """Tests for delete_decision and delete_decisions methods."""

    def test_delete_decision_success(self, tmp_path):
        """Test successful deletion of a decision file."""
        # Create repository
        repo = DecisionRepository(str(tmp_path))

        # Create a test decision
        decision = DecisionDocument(
            NativeId="HEL:2024-001",
            Title="Test Decision",
            DateDecision="2024-01-01",
            Content="Test content",
        )

        # Save decision
        assert repo.save_decision(decision)

        # Verify file exists
        safe_id = repo._sanitize_filename(decision.NativeId)
        file_path = repo.decisions_dir / f"{safe_id}.json"
        assert file_path.exists()

        # Delete decision
        assert repo.delete_decision(decision.NativeId)

        # Verify file is deleted
        assert not file_path.exists()

    def test_delete_decision_not_found(self, tmp_path):
        """Test deletion of non-existent file (should succeed)."""
        repo = DecisionRepository(str(tmp_path))

        # Try to delete non-existent decision
        result = repo.delete_decision("HEL:2024-999")

        # Should return True (not found is considered success)
        assert result is True

    def test_delete_decisions_batch(self, tmp_path):
        """Test batch deletion of multiple decisions."""
        repo = DecisionRepository(str(tmp_path))

        # Create multiple decisions
        decisions = [
            DecisionDocument(
                NativeId=f"HEL:2024-{i:03d}",
                Title=f"Decision {i}",
                DateDecision="2024-01-01",
                Content=f"Content {i}",
            )
            for i in range(5)
        ]

        # Save all decisions
        for decision in decisions:
            assert repo.save_decision(decision)

        # Verify all files exist
        for decision in decisions:
            safe_id = repo._sanitize_filename(decision.NativeId)
            file_path = repo.decisions_dir / f"{safe_id}.json"
            assert file_path.exists()

        # Delete all decisions
        native_ids = [d.NativeId for d in decisions]
        stats = repo.delete_decisions(native_ids)

        # Verify statistics
        assert stats["total"] == 5
        assert stats["deleted"] == 5
        assert stats["failed"] == 0
        assert len(stats["errors"]) == 0

        # Verify all files are deleted
        for decision in decisions:
            safe_id = repo._sanitize_filename(decision.NativeId)
            file_path = repo.decisions_dir / f"{safe_id}.json"
            assert not file_path.exists()

    def test_delete_decisions_partial_failure(self, tmp_path):
        """Test batch deletion with some failures."""
        repo = DecisionRepository(str(tmp_path))

        # Create some decisions
        decisions = [
            DecisionDocument(
                NativeId=f"HEL:2024-{i:03d}",
                Title=f"Decision {i}",
                DateDecision="2024-01-01",
                Content=f"Content {i}",
            )
            for i in range(3)
        ]

        # Save only first 2 decisions
        for decision in decisions[:2]:
            assert repo.save_decision(decision)

        # Try to delete all 3 (one doesn't exist)
        native_ids = [d.NativeId for d in decisions]
        stats = repo.delete_decisions(native_ids)

        # Should successfully delete the 3 (non-existent is considered success)
        assert stats["total"] == 3
        assert stats["deleted"] == 3
        assert stats["failed"] == 0


class TestCheckpointEnhancement:
    """Tests for enhanced checkpoint functionality."""

    def test_checkpoint_full_pipeline_structure(self, tmp_path):
        """Test checkpoint saves full pipeline data structure."""
        repo = DecisionRepository(str(tmp_path))

        checkpoint_data = {
            "full_pipeline": {
                "timestamp": datetime.now().isoformat(),
                "last_date": "2024-03-15",
                "documents_fetched": 150,
                "documents_processed": 145,
                "documents_successful": 140,
                "documents_failed": 5,
                "documents_skipped": 5,
                "last_native_id": "HEL:2024-123456",
                "completed": False,
            }
        }

        # Save checkpoint
        assert repo.save_checkpoint(checkpoint_data)

        # Load checkpoint
        loaded = repo.load_checkpoint()

        # Verify structure
        assert "full_pipeline" in loaded
        assert loaded["full_pipeline"]["documents_fetched"] == 150
        assert loaded["full_pipeline"]["documents_processed"] == 145
        assert loaded["full_pipeline"]["completed"] is False

    def test_checkpoint_resume(self, tmp_path):
        """Test resuming from checkpoint."""
        repo = DecisionRepository(str(tmp_path))

        # Save checkpoint
        checkpoint_data = {
            "full_pipeline": {
                "timestamp": datetime.now().isoformat(),
                "last_date": "2024-02-01",
                "documents_fetched": 50,
                "documents_processed": 50,
                "documents_successful": 48,
                "documents_failed": 2,
                "documents_skipped": 0,
                "completed": False,
            }
        }
        repo.save_checkpoint(checkpoint_data)

        # Load checkpoint
        checkpoint = repo.load_checkpoint()

        # Should be able to resume from this date
        assert checkpoint["full_pipeline"]["last_date"] == "2024-02-01"
        assert checkpoint["full_pipeline"]["completed"] is False


class TestBatchProcessing:
    """Tests for batch processing logic."""

    @pytest.fixture
    def mock_decision_documents(self):
        """Generate mock decision documents."""
        return [
            DecisionDocument(
                NativeId=f"HEL:2024-{i:03d}",
                Title=f"Decision {i}",
                DateDecision="2024-01-15",
                Content=f"Test content for decision {i}",
            )
            for i in range(1, 151)  # 150 documents
        ]

    def test_batch_accumulation(self, tmp_path, mock_decision_documents):
        """Test batch accumulator logic."""
        repo = DecisionRepository(str(tmp_path))
        batch_size = 50
        batch_buffer = []
        batches_processed = 0

        # Simulate streaming and batching
        for doc in mock_decision_documents:
            repo.save_decision(doc)
            batch_buffer.append(doc.NativeId)

            if len(batch_buffer) >= batch_size:
                # Process batch
                batches_processed += 1
                # Delete files
                stats = repo.delete_decisions(batch_buffer)
                assert stats["deleted"] == batch_size
                batch_buffer = []

        # Process remaining
        if batch_buffer:
            batches_processed += 1
            stats = repo.delete_decisions(batch_buffer)
            assert stats["deleted"] == len(batch_buffer)

        # Verify 3 batches processed (50, 50, 50)
        assert batches_processed == 3

        # Verify all files deleted
        assert len(list(repo.decisions_dir.glob("*.json"))) == 0


class TestMemoryManagement:
    """Tests for memory management and file cleanup."""

    def test_keep_files_option(self, tmp_path):
        """Test that keep_files option preserves files."""
        repo = DecisionRepository(str(tmp_path))

        decisions = [
            DecisionDocument(
                NativeId=f"HEL:2024-{i:03d}",
                Title=f"Decision {i}",
                DateDecision="2024-01-01",
                Content=f"Content {i}",
            )
            for i in range(10)
        ]

        # Save all decisions
        for decision in decisions:
            repo.save_decision(decision)

        # Simulate keep_files=True (don't delete)
        keep_files = True
        if not keep_files:
            native_ids = [d.NativeId for d in decisions]
            repo.delete_decisions(native_ids)

        # Verify all files still exist
        assert len(list(repo.decisions_dir.glob("*.json"))) == 10

    def test_delete_after_processing(self, tmp_path):
        """Test deletion after successful processing."""
        repo = DecisionRepository(str(tmp_path))

        decisions = [
            DecisionDocument(
                NativeId=f"HEL:2024-{i:03d}",
                Title=f"Decision {i}",
                DateDecision="2024-01-01",
                Content=f"Content {i}",
            )
            for i in range(10)
        ]

        # Save all decisions
        for decision in decisions:
            repo.save_decision(decision)

        # Verify files exist
        assert len(list(repo.decisions_dir.glob("*.json"))) == 10

        # Delete after processing
        native_ids = [d.NativeId for d in decisions]
        stats = repo.delete_decisions(native_ids)

        # Verify deletion
        assert stats["deleted"] == 10
        assert len(list(repo.decisions_dir.glob("*.json"))) == 0


class TestErrorHandling:
    """Tests for error handling in full pipeline."""

    def test_continue_on_document_error(self, tmp_path):
        """Test pipeline continues when individual document fails."""
        repo = DecisionRepository(str(tmp_path))

        # Create valid and invalid documents
        decisions = [
            DecisionDocument(
                NativeId=f"HEL:2024-{i:03d}",
                Title=f"Decision {i}",
                DateDecision="2024-01-01",
                Content=f"Content {i}" if i % 3 != 0 else None,  # Every 3rd has no content
            )
            for i in range(10)
        ]

        # Save all decisions
        for decision in decisions:
            repo.save_decision(decision)

        # Simulate processing with errors
        successful = 0
        failed = 0

        for decision in decisions:
            try:
                if decision.Content:
                    # Would be processed successfully
                    successful += 1
                else:
                    # Would fail
                    failed += 1
            except Exception:
                failed += 1

        # Pipeline should have processed all, with some failures
        assert successful + failed == 10
        assert failed > 0  # Some should have failed

    def test_dont_delete_failed_documents(self, tmp_path):
        """Test that failed documents are not deleted."""
        repo = DecisionRepository(str(tmp_path))

        decisions = [
            DecisionDocument(
                NativeId=f"HEL:2024-{i:03d}",
                Title=f"Decision {i}",
                DateDecision="2024-01-01",
                Content=f"Content {i}",
            )
            for i in range(10)
        ]

        # Save all decisions
        for decision in decisions:
            repo.save_decision(decision)

        # Simulate partial batch with failures
        successful_ids = [d.NativeId for d in decisions[:7]]
        failed_ids = [d.NativeId for d in decisions[7:]]

        # Only delete successful ones
        stats = repo.delete_decisions(successful_ids)
        assert stats["deleted"] == 7

        # Failed documents should still exist
        for failed_id in failed_ids:
            assert repo.decision_exists(failed_id)


@pytest.fixture
def tmp_path(tmp_path_factory):
    """Create a temporary directory for tests."""
    return tmp_path_factory.mktemp("test_data")
