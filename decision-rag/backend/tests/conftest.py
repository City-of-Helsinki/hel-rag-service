"""
Test configuration and fixtures.
"""

import shutil
import tempfile

import pytest

from app.core.config import Settings


@pytest.fixture
def temp_data_dir():
    """Create a temporary directory for test data."""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir)


@pytest.fixture
def test_settings(temp_data_dir):
    """Create test settings with temporary directory."""
    settings = Settings()
    settings.DATA_DIR = temp_data_dir
    settings.DECISIONS_DIR = f"{temp_data_dir}/decisions"
    settings.CHECKPOINT_FILE = f"{temp_data_dir}/checkpoint.json"
    return settings


@pytest.fixture
def sample_decision_data():
    """Sample decision data for testing."""
    return {
        "NativeId": "TEST-2024-001",
        "Title": "Test Decision",
        "Content": "<p>Test content</p>",
        "DateDecision": "2024-01-15",
        "CaseIDLabel": "TEST-001",
    }
