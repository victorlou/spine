"""
Canonical loading destination identifiers and alias normalization.

``LoadingConfig`` validates and stores canonical names. Callers that accept raw
strings (for example URI builders) should use :func:`normalize_loading_destination`
so aliases stay consistent with config validation.

Object-store-style loading (shared prefix semantics) is the quartet
``s3``, ``local``, ``gcs``, and ``azure_blob`` — see :data:`OBJECT_STORE_DESTINATIONS`.
"""

from __future__ import annotations

from typing import Final, FrozenSet

_LOADING_DESTINATION_ALIASES: Final[dict[str, str]] = {
    "azure": "azure_blob",
    "blob": "azure_blob",
}

# Destinations that use LoadingConfig.prefix and the object-store loader path
# (including local filesystem via Spark file://).
OBJECT_STORE_DESTINATIONS: Final[FrozenSet[str]] = frozenset({"s3", "local", "gcs", "azure_blob"})


def normalize_loading_destination(value: str) -> str:
    """Map user-facing aliases to canonical destination identifiers."""
    key = str(value).strip().lower()
    return _LOADING_DESTINATION_ALIASES.get(key, key)


def is_object_store_destination(destination: str) -> bool:
    """True when *destination* is (or aliases to) an object-store loading destination."""
    return normalize_loading_destination(destination) in OBJECT_STORE_DESTINATIONS


__all__ = [
    "OBJECT_STORE_DESTINATIONS",
    "is_object_store_destination",
    "normalize_loading_destination",
]
