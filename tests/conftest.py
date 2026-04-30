"""Shared pytest fixtures and isolation helpers for the Spine test suite.

Also exposes small factories for tests that need valid Pydantic config or Spark/HTTP fakes
without copy-pasting ``SimpleNamespace`` trees (see ``fake_spark_session``,
``fake_redis_client``, ``make_minimal_pipeline_config``).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional
from unittest.mock import MagicMock

import pytest

from src.config import settings as settings_module
from src.config.config_models import (
    DefaultsConfig,
    PipelineConfig,
    RequestInputConfig,
    ResourceConfig,
    SourceConfig,
    SourceType,
)
from src.utils.dynamic_values import ComplexDynamicValue, DynamicSourceReference, DynamicValueType


def fake_spark_session() -> MagicMock:
    """A MagicMock SparkSession with JVM and Hadoop hooks used by loader/object-store tests."""
    spark = MagicMock()
    spark.sparkContext._jsc.hadoopConfiguration.return_value = MagicMock(name="hadoop_conf")
    spark.sparkContext._jvm = MagicMock(name="jvm")
    return spark


def fake_redis_client() -> MagicMock:
    """In-memory-style Redis fake: dict-backed get/set/delete/exists/scan_iter."""
    store: Dict[str, Any] = {}

    def _get(key: str) -> Any:
        return store.get(key)

    def _set(key: str, value: Any, ex: Optional[int] = None, **_: Any) -> bool:
        store[key] = value
        return True

    def _delete(*keys: str) -> int:
        n = 0
        for k in keys:
            if k in store:
                del store[k]
                n += 1
        return n

    def _exists(key: str) -> bool:
        return key in store

    def _scan_iter(match: str = "*", **__: Any) -> Any:
        pat = match.replace("*", "")
        for k in list(store.keys()):
            if match == "*" or k.startswith(pat) or pat in k:
                yield k

    client = MagicMock()
    client.get.side_effect = _get
    client.set.side_effect = _set
    client.delete.side_effect = _delete
    client.exists.side_effect = _exists
    client.scan_iter.side_effect = _scan_iter
    return client


def make_minimal_pipeline_config(
    tmp_path: Path,
    *,
    sources: Dict[str, SourceConfig],
    queries: Optional[list] = None,
) -> PipelineConfig:
    """Build a validated ``PipelineConfig`` with ``config_root`` under ``tmp_path``."""
    (tmp_path / "queries").mkdir(parents=True, exist_ok=True)
    return PipelineConfig(
        config_root=tmp_path,
        version="1.0",
        defaults=DefaultsConfig(),
        queries=queries or [],
        sources=sources,
    )


def make_rest_chain_resources(
    *,
    child_depends_on_parent: bool,
    parent_enabled: bool = True,
    child_enabled: bool = True,
) -> Dict[str, SourceConfig]:
    """Two-resource REST source: optional SOURCE dependency from child -> parent."""
    parent_inputs: Dict[str, RequestInputConfig] = {}
    child_inputs: Dict[str, RequestInputConfig] = {}
    if child_depends_on_parent:
        child_inputs["post_id"] = RequestInputConfig(
            value=ComplexDynamicValue(
                type=DynamicValueType.SOURCE,
                source_config=DynamicSourceReference(source="parent", field="id"),
            ),
            location="query",
            batch_size=1,
        )
    return {
        "api": SourceConfig(
            type=SourceType.REST_API,
            base_url="https://example.com",
            enabled=True,
            resources={
                "parent": ResourceConfig(
                    enabled=parent_enabled,
                    method="GET",
                    path="/parent",
                    response_type="json",
                    request_inputs=parent_inputs,
                ),
                "child": ResourceConfig(
                    enabled=child_enabled,
                    method="GET",
                    path="/child",
                    response_type="json",
                    request_inputs=child_inputs,
                ),
            },
        ),
    }


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
