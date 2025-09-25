"""Comprehensive logging configuration for eGon validation framework."""

import logging
import logging.handlers
import os
import sys
from pathlib import Path
from typing import Optional
import json
from datetime import datetime


class JsonFormatter(logging.Formatter):
    """JSON formatter for structured logging in production."""

    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "timestamp": datetime.fromtimestamp(record.created).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        # Add exception info if present
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)

        # Add extra fields from record
        for key, value in record.__dict__.items():
            if key not in (
                "name",
                "msg",
                "args",
                "levelname",
                "levelno",
                "pathname",
                "filename",
                "module",
                "lineno",
                "funcName",
                "created",
                "msecs",
                "relativeCreated",
                "thread",
                "threadName",
                "processName",
                "process",
                "getMessage",
                "exc_info",
                "exc_text",
                "stack_info",
            ):
                log_entry[key] = value

        return json.dumps(log_entry, separators=(",", ":"))


def setup_logging(
    level: str = None,
    log_dir: Optional[str] = None,
    enable_console: bool = True,
    enable_file: bool = True,
    json_format: bool = False,
    max_file_size_mb: int = 10,
    backup_count: int = 5,
) -> None:
    """
    Setup comprehensive logging for eGon validation framework.

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_dir: Directory for log files (defaults to logs/)
        enable_console: Enable console logging
        enable_file: Enable file logging
        json_format: Use JSON format for logs (production)
        max_file_size_mb: Maximum size per log file in MB
        backup_count: Number of backup files to keep
    """
    # Determine log level from environment or parameter
    if level is None:
        level = os.getenv("EGON_LOG_LEVEL", "INFO").upper()

    # Determine if we're in production (use JSON logs)
    if json_format is None:
        json_format = (
            os.getenv("EGON_ENVIRONMENT", "development").lower() == "production"
        )

    # Setup log directory
    if log_dir is None:
        log_dir = os.getenv("EGON_LOG_DIR", "logs")

    log_path = Path(log_dir)
    log_path.mkdir(exist_ok=True)

    # Root logger setup
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, level))

    # Clear existing handlers
    root_logger.handlers.clear()

    # Choose formatter
    if json_format:
        formatter = JsonFormatter()
    else:
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s "
            "[%(filename)s:%(lineno)d]"
        )

    # Console handler
    if enable_console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(getattr(logging, level))
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)

    # File handlers
    if enable_file:
        # Main application log
        main_log_file = log_path / "egon_validation.log"
        main_handler = logging.handlers.RotatingFileHandler(
            main_log_file,
            maxBytes=max_file_size_mb * 1024 * 1024,
            backupCount=backup_count,
            encoding="utf-8",
        )
        main_handler.setLevel(getattr(logging, level))
        main_handler.setFormatter(formatter)
        root_logger.addHandler(main_handler)

        # Error-only log
        error_log_file = log_path / "egon_validation_errors.log"
        error_handler = logging.handlers.RotatingFileHandler(
            error_log_file,
            maxBytes=max_file_size_mb * 1024 * 1024,
            backupCount=backup_count,
            encoding="utf-8",
        )
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(formatter)
        root_logger.addHandler(error_handler)

    # Setup module-specific loggers
    _setup_module_loggers()


def _setup_module_loggers() -> None:
    """Setup specialized loggers for different modules."""
    # Initialize loggers (they will be used via get_logger)
    logging.getLogger("egon_validation.db")
    logging.getLogger("egon_validation.rules")
    logging.getLogger("egon_validation.runner")
    logging.getLogger("egon_validation.ssh")

    # Set levels (can be overridden by environment)
    for logger_name in ["db", "rules", "runner", "ssh"]:
        env_var = f"EGON_LOG_LEVEL_{logger_name.upper()}"
        level = os.getenv(env_var, None)
        if level:
            logger = logging.getLogger(f"egon_validation.{logger_name}")
            logger.setLevel(getattr(logging, level.upper()))


def get_logger(name: str) -> logging.Logger:
    """Get logger for specific module."""
    if not name.startswith("egon_validation."):
        name = f"egon_validation.{name}"
    return logging.getLogger(name)


# Airflow-compatible logging setup
def setup_airflow_logging() -> None:
    """Setup logging compatible with Airflow < 2.0."""
    # Airflow 1.x uses different logging configuration
    # Keep it simple and compatible
    setup_logging(
        level=os.getenv("AIRFLOW_LOG_LEVEL", "INFO"),
        log_dir="/opt/airflow/logs/egon_validation",
        enable_console=True,
        enable_file=True,
        json_format=True,  # Structured logs for Airflow
        max_file_size_mb=50,
        backup_count=10,
    )
