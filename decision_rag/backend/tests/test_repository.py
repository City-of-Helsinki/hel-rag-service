"""
Tests for the DecisionRepository.
"""

from app.repositories import DecisionRepository
from app.schemas import DecisionDocument


def test_repository_initialization(temp_data_dir):
    """Test repository initialization."""
    repo = DecisionRepository(temp_data_dir)

    assert repo.data_dir.exists()
    assert repo.decisions_dir.exists()


def test_save_and_get_decision(temp_data_dir, sample_decision_data):
    """Test saving and retrieving a decision."""
    repo = DecisionRepository(temp_data_dir)

    # Create decision document
    decision = DecisionDocument(**sample_decision_data)

    # Save decision
    assert repo.save_decision(decision) is True

    # Retrieve decision
    retrieved = repo.get_decision(decision.NativeId)
    assert retrieved is not None
    assert retrieved.NativeId == decision.NativeId
    assert retrieved.Title == decision.Title


def test_decision_exists(temp_data_dir, sample_decision_data):
    """Test checking if decision exists."""
    repo = DecisionRepository(temp_data_dir)

    decision = DecisionDocument(**sample_decision_data)

    # Should not exist initially
    assert repo.decision_exists(decision.NativeId) is False

    # Save and check again
    repo.save_decision(decision)
    assert repo.decision_exists(decision.NativeId) is True


def test_checkpoint_save_and_load(temp_data_dir):
    """Test checkpoint functionality."""
    repo = DecisionRepository(temp_data_dir)

    checkpoint_data = {
        "last_date": "2024-01-15",
        "documents_saved": 42,
    }

    # Save checkpoint
    assert repo.save_checkpoint(checkpoint_data) is True

    # Load checkpoint
    loaded = repo.load_checkpoint()
    assert loaded is not None
    assert loaded["last_date"] == checkpoint_data["last_date"]
    assert loaded["documents_saved"] == checkpoint_data["documents_saved"]
    assert "timestamp" in loaded


def test_get_statistics(temp_data_dir, sample_decision_data):
    """Test getting repository statistics."""
    repo = DecisionRepository(temp_data_dir)

    # Save some decisions
    for i in range(3):
        data = sample_decision_data.copy()
        data["NativeId"] = f"TEST-2024-{i:03d}"
        decision = DecisionDocument(**data)
        repo.save_decision(decision)

    # Get statistics
    stats = repo.get_statistics()
    assert stats["total_documents"] == 3
    assert stats["storage_size_mb"] > 0
    assert "storage_path" in stats
