"""
Canonical filesystem root for the Spine tree (directory that contains ``src/``).

This is derived only from the location of this package on disk, not from
``CONFIG_PATH``, the process working directory, or where operator YAML lives.
Operator config may live elsewhere (absolute ``CONFIG_PATH``); local loading
paths that are relative still resolve under this root unless you use an
absolute ``storage_root``.
"""

from pathlib import Path


def repository_root() -> Path:
    """Return the repository root: parent of ``src/`` (contains ``src/``, ``config/``, etc.)."""
    # This file lives at ``<root>/src/config/repository_root.py``.
    return Path(__file__).resolve().parents[2]
