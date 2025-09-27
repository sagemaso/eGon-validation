"""Retry mechanisms for database operations and rule execution."""

import time
import functools
import random
from typing import Type, Tuple, Callable, Any, Union
from egon_validation.exceptions import (
    DatabaseConnectionError,
    ValidationTimeoutError,
)
from egon_validation.logging_config import get_logger
from sqlalchemy.exc import OperationalError, DisconnectionError

logger = get_logger("retry")


def exponential_backoff(
    max_attempts: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    backoff_factor: float = 2.0,
    jitter: bool = True,
    retry_on: Union[Type[Exception], Tuple[Type[Exception], ...]] = (
        OperationalError,
        DisconnectionError,
        DatabaseConnectionError,
        ConnectionError,
        TimeoutError,
    ),
):
    """
    Decorator for exponential backoff retry with configurable parameters.

    Args:
        max_attempts: Maximum number of retry attempts
        base_delay: Initial delay in seconds
        max_delay: Maximum delay between retries
        backoff_factor: Multiplier for delay on each retry
        jitter: Add randomization to delay to prevent thundering herd
        retry_on: Exception types to retry on

    Example:
        @exponential_backoff(max_attempts=5, base_delay=0.5)
        def fetch_data(engine, query):
            return db.fetch_one(engine, query)
    """

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            last_exception = None

            for attempt in range(max_attempts):
                try:
                    result = func(*args, **kwargs)
                    if attempt > 0:
                        logger.info(
                            f"Function {func.__name__} succeeded after "
                            f"{attempt + 1} attempts"
                        )
                    return result

                except retry_on as e:
                    last_exception = e

                    if attempt == max_attempts - 1:
                        logger.error(
                            f"Function {func.__name__} failed after "
                            f"{max_attempts} attempts: {str(e)}",
                            extra={"attempts": max_attempts, "final_error": str(e)},
                            exc_info=True,
                        )
                        raise e

                    # Calculate delay with exponential backoff
                    delay = min(base_delay * (backoff_factor**attempt), max_delay)

                    # Add jitter to prevent thundering herd
                    if jitter:
                        delay += random.uniform(0, delay * 0.1)

                    logger.warning(
                        f"Function {func.__name__} failed (attempt "
                        f"{attempt + 1}/{max_attempts}), retrying in "
                        f"{delay:.2f}s: {str(e)}",
                        extra={
                            "attempt": attempt + 1,
                            "max_attempts": max_attempts,
                            "delay": delay,
                            "error": str(e),
                        },
                    )

                    time.sleep(delay)

                except Exception as e:
                    # Don't retry on non-retryable exceptions
                    logger.error(
                        f"Function {func.__name__} failed with "
                        f"non-retryable error: {str(e)}",
                        extra={"error": str(e), "attempt": attempt + 1},
                        exc_info=True,
                    )
                    raise e

            # Should never reach here, but just in case
            if last_exception:
                raise last_exception

        return wrapper

    return decorator


def circuit_breaker(
    failure_threshold: int = 5,
    recovery_timeout: int = 60,
    expected_exception: Union[Type[Exception], Tuple[Type[Exception], ...]] = Exception,
):
    """
    Circuit breaker pattern implementation.

    Args:
        failure_threshold: Number of failures before opening circuit
        recovery_timeout: Time in seconds before attempting recovery
        expected_exception: Exception types that trigger circuit opening
    """

    def decorator(func: Callable) -> Callable:
        func._circuit_failures = 0
        func._circuit_last_failure_time = None
        func._circuit_state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN

        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            current_time = time.time()

            # Check if circuit should be half-open (recovery attempt)
            if (
                func._circuit_state == "OPEN"
                and func._circuit_last_failure_time
                and current_time - func._circuit_last_failure_time > recovery_timeout
            ):
                func._circuit_state = "HALF_OPEN"
                logger.info(
                    f"Circuit breaker for {func.__name__} entering HALF_OPEN state"
                )

            # Fail fast if circuit is open
            if func._circuit_state == "OPEN":
                raise DatabaseConnectionError(
                    f"Circuit breaker is OPEN for {func.__name__}. "
                    f"Too many failures ({func._circuit_failures}). "
                    f"Will retry after {recovery_timeout}s."
                )

            try:
                result = func(*args, **kwargs)

                # Reset on success
                if func._circuit_failures > 0:
                    logger.info(
                        f"Circuit breaker for {func.__name__} reset after success"
                    )
                    func._circuit_failures = 0
                    func._circuit_state = "CLOSED"

                return result

            except expected_exception as e:
                func._circuit_failures += 1
                func._circuit_last_failure_time = current_time

                if func._circuit_failures >= failure_threshold:
                    func._circuit_state = "OPEN"
                    logger.error(
                        f"Circuit breaker for {func.__name__} opened after "
                        f"{failure_threshold} failures",
                        extra={
                            "failures": func._circuit_failures,
                            "state": "OPEN",
                            "error": str(e),
                        },
                    )

                raise e

        return wrapper

    return decorator


class RetryableOperation:
    """Context manager for retryable operations with custom configuration."""

    def __init__(
        self,
        operation_name: str,
        max_attempts: int = 3,
        base_delay: float = 1.0,
        backoff_factor: float = 2.0,
        retry_on: Tuple[Type[Exception], ...] = (
            OperationalError,
            DatabaseConnectionError,
        ),
    ):
        self.operation_name = operation_name
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.backoff_factor = backoff_factor
        self.retry_on = retry_on
        self.attempt = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type and issubclass(exc_type, self.retry_on):
            self.attempt += 1

            if self.attempt < self.max_attempts:
                delay = self.base_delay * (self.backoff_factor ** (self.attempt - 1))
                logger.warning(
                    f"Operation {self.operation_name} failed (attempt "
                    f"{self.attempt}/{self.max_attempts}), retrying in "
                    f"{delay:.2f}s: {str(exc_val)}",
                    extra={
                        "attempt": self.attempt,
                        "delay": delay,
                        "error": str(exc_val),
                    },
                )
                time.sleep(delay)
                return True  # Suppress the exception to retry
            else:
                logger.error(
                    f"Operation {self.operation_name} failed after "
                    f"{self.max_attempts} attempts: {str(exc_val)}",
                    extra={"attempts": self.max_attempts, "final_error": str(exc_val)},
                )

        return False  # Don't suppress other exceptions


# Pre-configured decorators for common scenarios
database_retry = exponential_backoff(
    max_attempts=3,
    base_delay=1.0,
    backoff_factor=2.0,
    retry_on=(
        OperationalError,
        DisconnectionError,
        DatabaseConnectionError,
        ConnectionError,
    ),
)

rule_execution_retry = exponential_backoff(
    max_attempts=2,
    base_delay=0.5,
    backoff_factor=1.5,
    retry_on=(OperationalError, ValidationTimeoutError),
)

connection_circuit_breaker = circuit_breaker(
    failure_threshold=5,
    recovery_timeout=30,
    expected_exception=(DatabaseConnectionError, OperationalError),
)
