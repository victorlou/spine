"""HTTP URL composition helpers (runtime joining, not config canonicalization)."""

from __future__ import annotations


def join_http_base_and_path(base_url: str, path: str) -> str:
    """
    Join a base URL and a path or endpoint segment with exactly one slash between them.

    ``base_url`` may include or omit a trailing slash; ``path`` may include or omit a
    leading slash.
    """
    base = str(base_url).rstrip("/")
    rel = str(path).lstrip("/")
    return f"{base}/{rel}" if rel else base
