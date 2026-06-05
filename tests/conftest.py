"""Shared pytest fixtures and isolation helpers for the Spine test suite.

This module is the single source of truth for cross-test isolation. It clears
module-level caches and singletons (``_settings_cache``, ``SparkManager``,
``AWSCredentialManager``, ``get_logger`` LRU), removes host-leaked environment
variables that production code reads, and exposes shared fakes/factories so
individual test modules do not copy ``MagicMock`` trees or YAML strings.

Importable helpers:
    * ``MANAGED_PLATFORM_ENV_KEYS`` / ``clear_managed_platform_env`` — managed Spark
      platform signals (Databricks/EMR/ECS/K8s).
    * ``SPINE_RUNTIME_ENV_KEYS`` / ``CLOUD_AUTH_ENV_KEYS`` — Spine runtime tunables
      and cloud-auth env vars cleared on every test.
    * ``make_minimal_pipeline_config`` / ``make_rest_chain_resources`` —
      Pydantic-validated config factories used by planner tests.
    * ``write_minimal_pipeline_config_dir`` — on-disk minimal config tree writer
      (``defaults.yml`` + ``sources/*.yml``); the ``minimal_pipeline_config_dir``
      fixture is a thin wrapper for the default shape.

Fixtures:
    * ``spark_session_fake`` — ``MagicMock`` SparkSession with JVM/Hadoop hooks
      used by loader/object-store/parser tests.
    * ``redis_client_fake`` — dict-backed Redis fake (``get``/``set``/``delete``/
      ``exists``/``scan_iter``).
    * ``redis_context_mock`` — ``MagicMock(spec=RedisContextManager)`` for
      handler/planner/parser/collector tests that pass a context through.
    * ``minimal_pipeline_config_dir`` — minimal valid on-disk config tree.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, Optional
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
from src.utils.aws_credentials import AWSCredentialManager
from src.utils.dynamic_values import ComplexDynamicValue, DynamicSourceReference, DynamicValueType
from src.utils.logger import get_logger
from src.utils.redis_context import RedisContextManager
from src.utils.spark_manager import SparkManager

# ---------------------------------------------------------------------------
# Environment isolation
# ---------------------------------------------------------------------------

MANAGED_PLATFORM_ENV_KEYS = (
    "DATABRICKS_RUNTIME_VERSION",
    "EMR_STEP_ID",
    "EMR_CLUSTER_ID",
    "ECS_CONTAINER_METADATA_URI",
    "KUBERNETES_SERVICE_HOST",
)

SPINE_RUNTIME_ENV_KEYS = (
    "CONFIG_PATH",
    "LOG_LEVEL",
    "SPINE_REDACT_LOGS",
    "SPINE_SENSITIVE_KEYS",
    "SPINE_GCS_AUTH_TYPE",
    "SPINE_DESTINATION_PREFLIGHT_FILESYSTEM_TIMEOUT_SECONDS",
    "SPINE_SPARK_DRIVER_BIND_ADDRESS",
    "SPINE_SPARK_DRIVER_HOST",
    "SPARK_LOCAL_IP",
    "SPARK_GCS_CONNECTOR_JAR_URL",
    "SPARK_GCS_CONNECTOR_MODE",
    "SPARK_S3_CONNECTOR_MODE",
    "SPARK_AZURE_CONNECTOR_MODE",
    "PYSPARK_SUBMIT_ARGS",
    "SPARK_SUBMIT_OPTS",
)

OTEL_ENV_KEYS = (
    "OTEL_SDK_DISABLED",
    "OTEL_SERVICE_NAME",
    "OTEL_RESOURCE_ATTRIBUTES",
    "OTEL_EXPORTER_OTLP_ENDPOINT",
    "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
    "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
    "OTEL_EXPORTER_OTLP_PROTOCOL",
    "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL",
    "OTEL_EXPORTER_OTLP_HEADERS",
)

CLOUD_AUTH_ENV_KEYS = (
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "AWS_PROFILE",
    "AWS_REGION",
    "AWS_DEFAULT_REGION",
    "GOOGLE_APPLICATION_CREDENTIALS",
    "GCS_CONTROL_BUCKET",
    "S3_CONTROL_BUCKET",
    "AZURE_CONTROL_CONTAINER",
    "AZURE_CONTROL_ACCOUNT",
    "K_SERVICE",
    "FUNCTION_TARGET",
    "FUNCTION_NAME",
    "GAE_ENV",
    "GKE_METADATA_HOST",
)


def _delenv_all(monkeypatch: pytest.MonkeyPatch, keys: Iterable[str]) -> None:
    for key in keys:
        monkeypatch.delenv(key, raising=False)


def clear_managed_platform_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Remove managed-platform env vars so tests do not inherit CI/host signals."""
    _delenv_all(monkeypatch, MANAGED_PLATFORM_ENV_KEYS)


@pytest.fixture(autouse=True)
def _clear_external_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Baseline env isolation: managed platform + Spine runtime + cloud auth.

    A host-leaked ``AWS_PROFILE``, ``CONFIG_PATH``, or ``LOG_LEVEL`` would
    otherwise change the behavior under test. Tests that need a value set it
    explicitly via ``monkeypatch.setenv``.
    """
    _delenv_all(monkeypatch, MANAGED_PLATFORM_ENV_KEYS)
    _delenv_all(monkeypatch, SPINE_RUNTIME_ENV_KEYS)
    _delenv_all(monkeypatch, CLOUD_AUTH_ENV_KEYS)
    _delenv_all(monkeypatch, OTEL_ENV_KEYS)


def reset_otel_global_providers() -> None:
    """Clear the OTEL global tracer/meter providers and their set-once guards.

    The OpenTelemetry API installs the global provider exactly once; tests that
    install a provider via ``TelemetryManager`` must reset this guard so a later
    test is not stuck with the first test's provider.
    """
    import opentelemetry.metrics._internal as metrics_internal
    import opentelemetry.trace as trace_api
    from opentelemetry.util._once import Once

    trace_api._TRACER_PROVIDER = None
    trace_api._TRACER_PROVIDER_SET_ONCE = Once()
    metrics_internal._METER_PROVIDER = None
    metrics_internal._METER_PROVIDER_SET_ONCE = Once()


@pytest.fixture(autouse=True)
def _reset_telemetry() -> None:
    """Reset the telemetry singleton and OTEL global providers around each test."""
    from src.utils.telemetry_manager import reset_for_tests

    reset_for_tests()
    reset_otel_global_providers()
    yield
    reset_for_tests()
    reset_otel_global_providers()


# ---------------------------------------------------------------------------
# Module-level cache and singleton resets
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _clear_settings_cache() -> None:
    """Avoid cross-test leakage from ``get_settings`` memoization."""
    settings_module._settings_cache.clear()
    yield
    settings_module._settings_cache.clear()


@pytest.fixture(autouse=True)
def _reset_singletons() -> None:
    """Reset ``SparkManager`` and ``AWSCredentialManager`` singletons.

    Both classes cache a ``_instance`` on the class itself; ``SparkManager``
    additionally caches ``_spark``. Without resetting between tests, state from
    one test (mocked credentials, fake Spark session) silently leaks into the
    next.
    """
    SparkManager._instance = None
    SparkManager._spark = None
    AWSCredentialManager._instance = None
    yield
    SparkManager._instance = None
    SparkManager._spark = None
    AWSCredentialManager._instance = None


@pytest.fixture(autouse=True)
def _clear_logger_cache() -> None:
    """Reset the ``get_logger`` LRU so env-driven log level changes take effect."""
    get_logger.cache_clear()
    yield
    get_logger.cache_clear()


# ---------------------------------------------------------------------------
# Shared fakes
# ---------------------------------------------------------------------------


def _build_spark_session_fake() -> MagicMock:
    spark = MagicMock(name="SparkSession")
    spark.sparkContext._jsc.hadoopConfiguration.return_value = MagicMock(name="hadoop_conf")
    spark.sparkContext._jvm = MagicMock(name="jvm")
    return spark


@pytest.fixture
def spark_session_fake() -> MagicMock:
    """A ``MagicMock`` SparkSession with JVM and Hadoop hooks.

    Used by loader/object-store/parser/handler tests instead of bare
    ``MagicMock()`` so the JVM/Hadoop call shape stays consistent.
    """
    return _build_spark_session_fake()


@pytest.fixture
def redis_client_fake() -> MagicMock:
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

    client = MagicMock(name="RedisClient")
    client.get.side_effect = _get
    client.set.side_effect = _set
    client.delete.side_effect = _delete
    client.exists.side_effect = _exists
    client.scan_iter.side_effect = _scan_iter
    return client


@pytest.fixture
def redis_context_mock() -> MagicMock:
    """``MagicMock`` with the ``RedisContextManager`` spec.

    Replaces inline ``MagicMock()`` for ``redis_context`` arguments in planner,
    handler, parser, and collector tests. Using ``spec=`` means typos in
    method names fail the test instead of silently returning a ``MagicMock``.
    """
    return MagicMock(spec=RedisContextManager)


# ---------------------------------------------------------------------------
# Pydantic config factories
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# On-disk config tree
# ---------------------------------------------------------------------------


_DEFAULT_DEFAULTS_BLOCK = (
    'version: "1.0"\n'
    "defaults:\n"
    "  loading:\n"
    '    destination: "local"\n'
    '    format: "delta"\n'
    '    write_mode: "overwrite"\n'
    '    compression: "snappy"\n'
    '    storage_root: ".spine/local-output"\n'
    "  context:\n"
    '    type: "redis"\n'
    "    ttl: 3600\n"
    '    prefix: "pipeline:"\n'
    "    redis:\n"
    '      host: "${REDIS_HOST:-localhost}"\n'
    '      port: "${REDIS_PORT:-6379}"\n'
    '      db: "${REDIS_DB:-0}"\n'
)

_DEFAULT_SOURCES = {
    "sample.yml": (
        "enabled: true\n"
        'type: "rest_api"\n'
        'base_url: "https://example.com"\n'
        "resources:\n"
        "  users:\n"
        '    path: "/users"\n'
        '    method: "GET"\n'
        '    response_type: "json"\n'
    ),
}


def write_minimal_pipeline_config_dir(
    cfg_dir: Path,
    *,
    defaults_yaml: Optional[str] = None,
    sources: Optional[Dict[str, str]] = None,
) -> Path:
    """Write a minimal valid config tree to ``cfg_dir`` and return it.

    Used by both the ``minimal_pipeline_config_dir`` autouse fixture (default
    shape) and tests that need a different shape (e.g. ``test_settings`` needs
    an explicit ``defaults.retry`` block).
    """
    sources_dir = cfg_dir / "sources"
    sources_dir.mkdir(parents=True, exist_ok=True)
    (cfg_dir / "defaults.yml").write_text(
        defaults_yaml if defaults_yaml is not None else _DEFAULT_DEFAULTS_BLOCK,
        encoding="utf-8",
    )
    for name, body in (sources or _DEFAULT_SOURCES).items():
        (sources_dir / name).write_text(body, encoding="utf-8")
    return cfg_dir


@pytest.fixture
def minimal_pipeline_config_dir(tmp_path: Path) -> Path:
    """Create a minimal valid config tree (defaults.yml + sources/) for CLI tests."""
    return write_minimal_pipeline_config_dir(tmp_path / "config")
