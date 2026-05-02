# Common transient I/O error substrings across S3, GCS, and Azure storage drivers.
import time
from collections.abc import Callable
from functools import wraps
from typing import ParamSpec, TypeVar

from src.utils.logger import get_logger

P = ParamSpec("P")
R = TypeVar("R")

_TRANSIENT_STORAGE_ERRORS = (
    "Connection reset",
    "SocketException",
    "SocketTimeoutException",
    "RequestTimeout",
    "ServiceUnavailable",
    "SlowDown",
)


def retry_on_transient_storage_error(
    max_retries: int = 3,
    delay: float = 1.0,
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Retry decorator for transient object storage I/O errors."""

    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        @wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            last_exception: Exception | None = None

            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    is_transient = any(pattern in str(e) for pattern in _TRANSIENT_STORAGE_ERRORS)
                    if is_transient and attempt < max_retries - 1:
                        logger = get_logger(getattr(func, "__name__", "storage_operation"))
                        logger.warning(
                            f"Storage operation failed (attempt {attempt + 1}/{max_retries}). "
                            f"Retrying in {delay} seconds..."
                        )
                        time.sleep(delay)
                        continue
                    raise

            if last_exception is not None:
                raise last_exception

            raise RuntimeError("Storage operation retry loop exited without running")

        return wrapper

    return decorator
