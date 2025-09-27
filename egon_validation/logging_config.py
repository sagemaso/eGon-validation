"""Comprehensive logging configuration for eGon validation framework."""

import logging
import sys
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
    level: str = "INFO", format_style: str = "pipeline"
) -> logging.Logger:
    """
    Setup structured logging for egon-validation.

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR)
        format_style: "pipeline" for structured logs, "dev" for human-readable

    Returns:
        Configured logger
    """
    logger = logging.getLogger("egon_validation")

    # Avoid duplicate handlers
    if logger.handlers:
        return logger

    # Set level
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    logger.setLevel(numeric_level)

    # Create handler
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(numeric_level)

    # Set format based on style
    if format_style == "pipeline":
        # Structured format for pipeline logs
        formatter = logging.Formatter(
            '{"timestamp":"%(asctime)s","level":"%(levelname)s",'
            '"component":"%(name)s","message":"%(message)s",'
            '"module":"%(module)s","function":"%(funcName)s"}'
        )
    else:
        # Human-readable format for development
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s "
            "[%(filename)s:%(lineno)d]"
        )

    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """Get logger for egon-validation component."""
    if name:
        return logging.getLogger(f"egon_validation.{name}")
    return logging.getLogger("egon_validation")
