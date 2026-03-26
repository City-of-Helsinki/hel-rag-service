"""
Tests for utility functions.
"""

from datetime import datetime

from app.utils import (
    format_date_for_api,
    generate_date_range,
    parse_date,
    validate_date_range,
    validate_native_id,
    weeks_between,
)


def test_parse_date():
    """Test date parsing."""
    date = parse_date("2024-01-15")
    assert date.year == 2024
    assert date.month == 1
    assert date.day == 15


def test_format_date_for_api():
    """Test date formatting."""
    date = datetime(2024, 1, 15)
    formatted = format_date_for_api(date)
    assert formatted == "2024-01-15T00:00:00"


def test_generate_date_range():
    """Test date range generation."""
    start = datetime(2024, 1, 1)
    end = datetime(2024, 1, 15)

    ranges = generate_date_range(start, end, step_days=7)

    assert len(ranges) == 2  # 2 weeks
    assert ranges[0][0] == start


def test_weeks_between():
    """Test weeks calculation."""
    start = datetime(2024, 1, 1)
    end = datetime(2024, 1, 15)

    weeks = weeks_between(start, end)
    assert weeks == 2  # 14 days = 2 weeks


def test_validate_native_id():
    """Test NativeId validation."""
    assert validate_native_id("TEST-2024-001") is True
    assert validate_native_id("") is False
    assert validate_native_id(None) is False


def test_validate_date_range():
    """Test date range validation."""
    start = datetime(2024, 1, 1)
    end = datetime(2024, 12, 31)

    is_valid, error = validate_date_range(start, end)
    assert is_valid is True
    assert error is None

    # Invalid range (start after end)
    is_valid, error = validate_date_range(end, start)
    assert is_valid is False
    assert error is not None
