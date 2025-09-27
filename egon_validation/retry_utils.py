import time
import functools
from typing import Callable, Type, Tuple, Any
from egon_validation.logging_config import get_logger

logger = get_logger("retry_utils")


def retry_on_exception(
    exceptions: Tuple[Type[Exception], ...] = (Exception,),
    max_attempts: int = 3,
    backoff_seconds: float = 1.0,
    backoff_multiplier: float = 2.0,
    max_backoff_seconds: float = 30.0,
):
    """
    Decorator to retry function calls on specific exceptions.

    Args:
        exceptions: Tuple of exception types to retry on
        max_attempts: Maximum number of retry attempts
        backoff_seconds: Initial backoff time in seconds
        backoff_multiplier: Multiplier for exponential backoff
        max_backoff_seconds: Maximum backoff time in seconds
    """

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            last_exception = None
            current_backoff = backoff_seconds

            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e

                    if attempt == max_attempts:
                        logger.error(
                            f"Function {func.__name__} failed after {max_attempts} attempts",
                            extra={
                                "function": func.__name__,
                                "max_attempts": max_attempts,
                                "final_error": str(e),
                                "attempt": attempt,
                            },
                        )
                        raise

                    logger.warning(
                        f"Function {func.__name__} failed on attempt {attempt}, retrying",
                        extra={
                            "function": func.__name__,
                            "attempt": attempt,
                            "max_attempts": max_attempts,
                            "error": str(e),
                            "retry_after_seconds": current_backoff,
                        },
                    )

                    time.sleep(current_backoff)
                    current_backoff = min(
                        current_backoff * backoff_multiplier,
                        max_backoff_seconds,
                    )
                except Exception as e:
                    # Don't retry on unexpected exceptions
                    logger.error(
                        f"Function {func.__name__} failed with unexpected exception",
                        extra={
                            "function": func.__name__,
                            "attempt": attempt,
                            "error": str(e),
                            "error_type": type(e).__name__,
                        },
                    )
                    raise

            # This should never be reached, but just in case
            if last_exception:
                raise last_exception

        return wrapper

    return decorator


def retry_database_operation(func: Callable) -> Callable:
    """
    Convenient decorator for database operations with common retry settings.
    Retries on connection errors, timeouts, and operational errors.
    """
    import sqlalchemy.exc as sql_exc
    import psycopg2

    database_exceptions = (
        sql_exc.DisconnectionError,
        sql_exc.TimeoutError,
        sql_exc.OperationalError,
        psycopg2.OperationalError,
        psycopg2.DatabaseError,
        ConnectionError,
        OSError,  # Network-related errors
    )

    return retry_on_exception(
        exceptions=database_exceptions,
        max_attempts=3,
        backoff_seconds=2.0,
        backoff_multiplier=2.0,
        max_backoff_seconds=16.0,
    )(func)
