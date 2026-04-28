"""Shared pytest fixtures and isolation helpers for the Spine test suite."""

from pathlib import Path

import pytest

from src.config import settings as settings_module

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


@pytest.fixture(autouse=True)
def clear_settings_cache() -> None:
    """Avoid cross-test leakage from module-level settings memoization."""
    settings_module._settings_cache.clear()


@pytest.fixture
def minimal_pipeline_config_dir(tmp_path: Path) -> Path:
    """Create a minimal valid config tree (defaults.yml + sources/) for CLI tests."""
    config_dir = tmp_path / "config"
    sources_dir = config_dir / "sources"
    sources_dir.mkdir(parents=True)

    (config_dir / "defaults.yml").write_text(
        "\n".join(
            [
                'version: "1.0"',
                "defaults:",
                "  loading:",
                '    destination: "local"',
                '    format: "delta"',
                '    write_mode: "overwrite"',
                '    compression: "snappy"',
                '    storage_root: ".spine/local-output"',
                "  context:",
                '    type: "redis"',
                "    ttl: 3600",
                '    prefix: "pipeline:"',
                "    redis:",
                '      host: "${REDIS_HOST:-localhost}"',
                '      port: "${REDIS_PORT:-6379}"',
                '      db: "${REDIS_DB:-0}"',
            ]
        ),
        encoding="utf-8",
    )
    (sources_dir / "sample.yml").write_text(
        "\n".join(
            [
                "enabled: true",
                'type: "rest_api"',
                'base_url: "https://example.com"',
                "resources:",
                "  users:",
                '    path: "/users"',
                '    method: "GET"',
                '    response_type: "json"',
            ]
        ),
        encoding="utf-8",
    )

    return config_dir
