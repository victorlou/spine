"""
Canonical string forms for loading-related configuration.

Normalization runs at Pydantic validation time (see ``LoadingConfig``). Runtime code
that builds URIs may call these helpers again for call sites that bypass the pipeline
config model (tests, utilities); they are idempotent on already-canonical values.
"""

from __future__ import annotations

from typing import Optional

__all__ = [
    "normalize_azure_account_label",
    "normalize_azure_container_label",
    "normalize_object_store_bucket_label",
]


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
