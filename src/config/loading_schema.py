"""
Loading destination identity, aliases, and canonical string forms for config validation.

**Destinations** — ``OBJECT_STORE_DESTINATIONS``, :func:`normalize_loading_destination`, and
:func:`is_object_store_destination` define how YAML and code refer to storage backends
(``LoadingConfig`` stores canonical names).

**Field normalizers** — :func:`normalize_object_store_bucket_label` and Azure helpers run
at Pydantic validation time (see ``LoadingConfig``); they are idempotent on values that
are already canonical.
"""

from __future__ import annotations

from typing import Final, FrozenSet, Optional

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


def normalize_object_store_bucket_label(value: Optional[str]) -> Optional[str]:
    """Strip whitespace and leading/trailing slashes from S3/GCS bucket-style names."""
    if value is None:
        return None
    s = str(value).strip().strip("/")
    return s if s else None


def normalize_azure_container_label(value: Optional[str]) -> Optional[str]:
    """Canonical Azure Blob container name (strip, trim slashes, lowercase per DNS rules)."""
    if value is None:
        return None
    s = str(value).strip().strip("/").lower()
    return s if s else None


def normalize_azure_account_label(value: Optional[str]) -> Optional[str]:
    """Canonical Azure storage account name (strip whitespace, lowercase)."""
    if value is None:
        return None
    s = str(value).strip().lower()
    return s if s else None


__all__ = [
    "OBJECT_STORE_DESTINATIONS",
    "is_object_store_destination",
    "normalize_azure_account_label",
    "normalize_azure_container_label",
    "normalize_loading_destination",
    "normalize_object_store_bucket_label",
]
