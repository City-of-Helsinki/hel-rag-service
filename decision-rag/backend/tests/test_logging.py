"""Tests for logging configuration and log management."""

import json
import logging
from datetime import datetime, timedelta
from pathlib import Path

from app.core.logging import JsonFormatter, cleanup_old_logs, setup_logging


class TestLogCleanup:
    """Tests for log cleanup functionality."""

    def test_cleanup_old_logs_removes_old_files(self, tmp_path):
        """Test that old log files are deleted based on retention period."""
        # Create test log files with different ages
        old_log = tmp_path / "old_pipeline_2026-01-20.log"
        recent_log = tmp_path / "recent_pipeline_2026-01-27.log"
        current_log = tmp_path / "pipeline.log"

        # Write some content
        old_log.write_text("old log content")
        recent_log.write_text("recent log content")
        current_log.write_text("current log content")

        # Modify timestamps (old log = 10 days ago)
        ten_days_ago = datetime.now() - timedelta(days=10)
        old_time = ten_days_ago.timestamp()
        Path(old_log).touch()
        import os

        os.utime(old_log, (old_time, old_time))

        # Run cleanup with 7 day retention
        cleanup_old_logs(str(tmp_path), retention_days=7)

        # Old log should be deleted
        assert not old_log.exists()
        # Recent logs should still exist
        assert recent_log.exists()
        assert current_log.exists()

    def test_cleanup_old_logs_disabled_with_zero_retention(self, tmp_path):
        """Test that cleanup is disabled when retention_days is 0 or negative."""
        old_log = tmp_path / "old.log"
        old_log.write_text("content")

        # Set to very old
        old_time = (datetime.now() - timedelta(days=100)).timestamp()
        import os

        os.utime(old_log, (old_time, old_time))

        # Cleanup with 0 retention should not delete
        cleanup_old_logs(str(tmp_path), retention_days=0)
        assert old_log.exists()

        # Cleanup with negative retention should not delete
        cleanup_old_logs(str(tmp_path), retention_days=-1)
        assert old_log.exists()

    def test_cleanup_old_logs_handles_nonexistent_directory(self):
        """Test that cleanup handles non-existent directory gracefully."""
        # Should not raise exception
        cleanup_old_logs("/nonexistent/directory", retention_days=7)

    def test_cleanup_old_logs_handles_permission_error(self, tmp_path, monkeypatch):
        """Test that cleanup handles permission errors gracefully."""
        log_file = tmp_path / "test.log"
        log_file.write_text("content")

        # Make file very old
        old_time = (datetime.now() - timedelta(days=10)).timestamp()
        import os

        os.utime(log_file, (old_time, old_time))

        # Mock unlink to raise permission error
        original_unlink = Path.unlink

        def mock_unlink(self, *args, **kwargs):
            if self == log_file:
                raise PermissionError("No permission")
            return original_unlink(self, *args, **kwargs)

        monkeypatch.setattr(Path, "unlink", mock_unlink)

        # Should not crash
        cleanup_old_logs(str(tmp_path), retention_days=7)

        # File should still exist (couldn't be deleted)
        assert log_file.exists()


class TestSetupLogging:
    """Tests for logging setup."""

    def test_setup_logging_creates_log_directory(self, tmp_path):
        """Test that setup_logging creates the log directory if it doesn't exist."""
        log_dir = tmp_path / "test_logs"
        assert not log_dir.exists()

        setup_logging(
            log_level="INFO",
            log_dir=str(log_dir),
            log_file="test_pipeline.log",
            api_log_file="test_api.log",
            error_log_file="test_errors.log",
            retention_days=0,  # Disable cleanup for test
        )

        assert log_dir.exists()

    def test_setup_logging_creates_handlers_with_rotation(self, tmp_path):
        """Test that setup_logging creates rotating file handlers with JSON formatters."""
        log_dir = tmp_path / "test_logs"

        setup_logging(
            log_level="DEBUG",
            log_dir=str(log_dir),
            log_file="test_pipeline.log",
            api_log_file="test_api.log",
            error_log_file="test_errors.log",
            retention_days=0,
            rotation_when="midnight",
            rotation_interval=1,
        )

        # Check that log files are created
        assert (log_dir / "test_pipeline.log").exists()
        assert (log_dir / "test_api.log").exists()
        assert (log_dir / "test_errors.log").exists()

        # All root-logger handlers must use JsonFormatter
        root_logger = logging.getLogger()
        for handler in root_logger.handlers:
            assert isinstance(handler.formatter, JsonFormatter), (
                f"Handler {handler} uses {type(handler.formatter)} instead of JsonFormatter"
            )

        # Emit a record and verify the file output is valid JSON with required keys
        test_logger = logging.getLogger("test")
        test_logger.info("Test message")

        pipeline_content = (log_dir / "test_pipeline.log").read_text().strip()
        for line in pipeline_content.splitlines():
            if line.strip():
                record = json.loads(line)
                assert "timestamp" in record
                assert "logger" in record
                assert "level" in record
                assert "message" in record

    def test_setup_logging_with_different_log_levels(self, tmp_path):
        """Test that different log levels work correctly."""
        log_dir = tmp_path / "test_logs"

        # Test with WARNING level
        setup_logging(
            log_level="WARNING",
            log_dir=str(log_dir),
            log_file="test.log",
            api_log_file="api.log",
            error_log_file="errors.log",
            retention_days=0,
        )

        logger = logging.getLogger("test_warning")
        logger.debug("Debug message - should not appear")
        logger.info("Info message - should not appear")
        logger.warning("Warning message - should appear")

        content = (log_dir / "test.log").read_text()
        assert "Debug message" not in content
        assert "Info message" not in content
        assert "Warning message" in content


class TestJsonFormatter:
    """Tests for the JsonFormatter class."""

    def _make_record(
        self,
        msg: str = "hello",
        level: int = logging.INFO,
    ) -> logging.LogRecord:
        """Create a minimal LogRecord for testing."""
        return logging.LogRecord(
            name="test.logger",
            level=level,
            pathname="test_file.py",
            lineno=42,
            msg=msg,
            args=(),
            exc_info=None,
        )

    def test_console_format_produces_valid_json_with_required_keys(self):
        """Console formatter emits valid JSON containing the four required keys."""
        formatter = JsonFormatter(include_location=False, include_exc_info=False)
        record = self._make_record("hello world")
        output = formatter.format(record)

        data = json.loads(output)
        assert data["message"] == "hello world"
        assert data["logger"] == "test.logger"
        assert data["level"] == "INFO"
        assert "timestamp" in data
        assert "function" not in data
        assert "line" not in data

    def test_file_format_includes_location_fields(self):
        """File formatter includes ``function`` and ``line`` fields."""
        formatter = JsonFormatter(include_location=True, include_exc_info=False)
        record = self._make_record("file message")
        output = formatter.format(record)

        data = json.loads(output)
        assert "function" in data
        assert isinstance(data["line"], int)

    def test_error_format_includes_exc_info_when_present(self):
        """Error formatter serialises attached exception traceback."""
        import sys

        formatter = JsonFormatter(include_location=True, include_exc_info=True)
        try:
            raise ValueError("test error")
        except ValueError:
            exc_info = sys.exc_info()

        record = self._make_record("error occurred", level=logging.ERROR)
        record.exc_info = exc_info
        output = formatter.format(record)

        data = json.loads(output)
        assert "exc_info" in data
        assert any("ValueError" in line for line in data["exc_info"])

    def test_error_format_omits_exc_info_when_absent(self):
        """Error formatter omits the ``exc_info`` key when no exception is attached."""
        formatter = JsonFormatter(include_location=True, include_exc_info=True)
        record = self._make_record("clean error", level=logging.ERROR)
        output = formatter.format(record)

        data = json.loads(output)
        assert "exc_info" not in data

    def test_fallback_on_serialization_error(self, monkeypatch):
        """Formatter returns minimal valid JSON when ``json.dumps`` fails."""
        import json as _json

        import app.core.logging as log_module

        formatter = JsonFormatter()
        record = self._make_record("crash")

        _original_dumps = _json.dumps
        first_call = [True]

        def _flaky_dumps(obj, **kwargs):
            if first_call[0]:
                first_call[0] = False
                raise TypeError("not serializable")
            return _original_dumps(obj, **kwargs)

        monkeypatch.setattr(log_module.json, "dumps", _flaky_dumps)
        output = formatter.format(record)

        data = _json.loads(output)
        assert data["level"] == "ERROR"
        assert "Failed to serialize log record" in data["message"]
