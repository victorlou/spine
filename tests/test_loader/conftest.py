"""Loader-test fixtures and helpers.

Keeps loader/object-store/preflight tests free of repeated ``MagicMock``
SparkSession setups and LoadingConfig boilerplate.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from src.config.config_models import LoadingConfig
from tests.conftest import _build_spark_session_fake


@pytest.fixture
def loader_spark() -> MagicMock:
    """Domain alias for ``spark_session_fake`` for loader tests."""
    return _build_spark_session_fake()


def make_loading_config(destination: str, **overrides: Any) -> LoadingConfig:
    """Build a validated ``LoadingConfig`` for a destination with sensible defaults.

    The inline call sites in loader tests repeat the same shape:
    ``LoadingConfig(destination=..., <bucket-or-root>, prefix=..., format="delta")``.
    Centralizing the defaults removes drift when ``LoadingConfig``'s required
    fields per destination evolve.
    """
    base: dict[str, Any] = {"format": "delta", "prefix": "a/b"}
    if destination == "s3":
        base.setdefault("s3_bucket", "my-bucket")
    elif destination == "gcs":
        base.setdefault("gcs_bucket", "my-gcs-bucket")
    elif destination == "azure_blob":
        base.setdefault("azure_container", "mycontainer")
        base.setdefault("azure_account", "myaccount")
    elif destination == "local":
        base.setdefault("storage_root", ".spine/local-output")
    base.update(overrides)
    return LoadingConfig(destination=destination, **base)
