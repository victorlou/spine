"""Shared pytest hooks and utilities for the Spine test suite."""

import pytest

# Cloud / managed Spark platform signals — cleared by default so local-dev branches stay deterministic.
MANAGED_PLATFORM_ENV_KEYS = (
    "DATABRICKS_RUNTIME_VERSION",
    "EMR_STEP_ID",
    "EMR_CLUSTER_ID",
    "ECS_CONTAINER_METADATA_URI",
    "KUBERNETES_SERVICE_HOST",
)


def clear_managed_platform_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Remove managed-platform env vars so tests do not inherit CI/host signals."""
    for key in MANAGED_PLATFORM_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)


@pytest.fixture(autouse=True)
def _clear_managed_platform_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Baseline isolation: each test starts without managed Spark platform hints."""
    clear_managed_platform_env(monkeypatch)
