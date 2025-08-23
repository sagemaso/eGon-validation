import logging
import sys
from typing import Optional

def setup_logging(level: str = "INFO", format_style: str = "pipeline") -> logging.Logger:
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
            '{"timestamp":"%(asctime)s","level":"%(levelname)s","component":"%(name)s",'
            '"message":"%(message)s","module":"%(module)s","function":"%(funcName)s"}'
        )
    else:
        # Human-readable format for development
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
    
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    # Don't propagate to root logger
    logger.propagate = False
    
    return logger

def get_logger(name: Optional[str] = None) -> logging.Logger:
    """Get logger for egon-validation component."""
    if name:
        return logging.getLogger(f"egon_validation.{name}")
    return logging.getLogger("egon_validation")