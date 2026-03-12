"""
Logging configuration for the pipeline.
"""

import logging
import sys
from datetime import datetime, timedelta
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path


def cleanup_old_logs(log_dir: str, retention_days: int) -> None:
    """
    Remove log files older than the retention period.

    Args:
        log_dir: Directory containing log files
        retention_days: Number of days to keep logs
    """
    if retention_days <= 0:
        logging.info("Log cleanup disabled (retention_days <= 0)")
        return

    log_path = Path(log_dir)
    if not log_path.exists():
        return

    cutoff_date = datetime.now() - timedelta(days=retention_days)
    deleted_count = 0
    total_size = 0

    try:
        # Find all log files
        for log_file in log_path.glob("*.log*"):
            try:
                # Skip if it's not a file
                if not log_file.is_file():
                    continue

                # Get file modification time
                file_mtime = datetime.fromtimestamp(log_file.stat().st_mtime)

                # Delete if older than cutoff date
                if file_mtime < cutoff_date:
                    file_size = log_file.stat().st_size
                    log_file.unlink()
                    deleted_count += 1
                    total_size += file_size
                    logging.debug(f"Deleted old log file: {log_file.name}")

            except Exception as e:
                logging.warning(f"Failed to process log file {log_file}: {e}")
                continue

        if deleted_count > 0:
            size_mb = total_size / (1024 * 1024)
            logging.info(
                f"Log cleanup completed: Deleted {deleted_count} old log file(s), "
                f"freed {size_mb:.2f} MB"
            )
        else:
            logging.debug("Log cleanup completed: No old log files to delete")

    except Exception as e:
        logging.error(f"Error during log cleanup: {e}")


def setup_logging(
    log_level: str = "INFO",
    log_dir: str = "data/logs",
    log_file: str = "pipeline.log",
    api_log_file: str = "api.log",
    error_log_file: str = "errors.log",
    retention_days: int = 7,
    rotation_when: str = "midnight",
    rotation_interval: int = 1,
) -> None:
    """
    Set up logging configuration for the application.

    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_dir: Directory for log files
        log_file: Main application log file
        api_log_file: API request/response log file
        error_log_file: Error log file
        retention_days: Number of days to keep old logs (0 = disable cleanup)
        rotation_when: When to rotate logs (midnight, W0-W6 for weekdays, etc.)
        rotation_interval: Interval for rotation
        backup_count: Number of backup files to keep (0 = use retention_days instead)
    """
    # Create log directory if it doesn't exist
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level.upper()))

    # Remove existing handlers
    root_logger.handlers.clear()

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
    console_handler.setFormatter(console_formatter)
    root_logger.addHandler(console_handler)

    # File handler for main logs with rotation
    file_handler = TimedRotatingFileHandler(
        log_path / log_file,
        when=rotation_when,
        interval=rotation_interval,
    )
    file_handler.suffix = "_%Y-%m-%d"
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    file_handler.setFormatter(file_formatter)
    root_logger.addHandler(file_handler)

    # API logger with rotation
    api_logger = logging.getLogger("api")
    api_logger.setLevel(logging.DEBUG)  # Set logger level explicitly
    api_handler = TimedRotatingFileHandler(
        log_path / api_log_file,
        when=rotation_when,
        interval=rotation_interval,
    )
    api_handler.suffix = "_%Y-%m-%d"
    api_handler.setLevel(logging.DEBUG)
    api_handler.setFormatter(file_formatter)
    api_logger.addHandler(api_handler)
    api_logger.propagate = False  # Don't propagate to root logger

    # Error logger with rotation
    error_logger = logging.getLogger("errors")
    error_handler = TimedRotatingFileHandler(
        log_path / error_log_file,
        when=rotation_when,
        interval=rotation_interval,
    )
    error_handler.suffix = "_%Y-%m-%d"
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(file_formatter)
    error_logger.addHandler(error_handler)

    logging.info("Logging initialized")

    # Cleanup old logs
    cleanup_old_logs(log_dir, retention_days)


def get_logger(name: str) -> logging.Logger:
    """Get a logger instance with the given name."""
    return logging.getLogger(name)
