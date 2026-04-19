"""Local filesystem checks for loading destinations (no Spark required)."""

import os
from pathlib import Path

from src.utils.exceptions import HandlerError
from src.utils.logger import get_logger

_logger = get_logger(__name__)


def check_local_storage_root(path: Path) -> None:
    """
    Ensure ``path`` exists as a directory and is writable (POSIX ``W_OK``).

    Raises:
        HandlerError: If the path is not a writable directory
    """
    p = path.expanduser().resolve()
    if not p.is_dir():
        raise HandlerError(f"Local loading storage_root is not a directory: {p}")
    if not os.access(p, os.W_OK):
        raise HandlerError(f"Local loading storage_root is not writable: {p}")
    _logger.debug("Successfully validated local storage access", extra_fields={"path": str(p)})
