"""Tests for SparkManager AWS startup decoupling."""

from types import SimpleNamespace
from unittest.mock import MagicMock, Mock

import pytest

from src.config.config_models import SparkRuntimeConfig
from src.config.spark_runtime import resolve_spark_runtime
from src.utils.exceptions import AWSError, SparkError
from src.utils.spark_manager import SparkManager


def test_spark_manager_construction_does_not_eagerly_load_aws_credentials(monkeypatch) -> None:
    # Ensure singleton state is clean for this test
    SparkManager._instance = None
    SparkManager._spark = None

    def _raise_if_called(self):
        raise AssertionError("_load_credentials should not be called during __new__")

    monkeypatch.setattr(SparkManager, "_load_credentials", _raise_if_called)

    manager = SparkManager()
    assert manager is not None


def test_spark_manager_init_session_fails_fast_when_aws_credentials_fail(monkeypatch) -> None:
    """Missing AWS credentials with an S3 destination must stop init_session immediately.

    The previous behavior (warn + fall back to Spark's default credential chain) silently
    deferred the failure to actual data write time. The unified destination preflight in
    src.loader.destination_preflight is the single source of truth for "can we reach the
    bucket"; the credential loader is no longer optional when S3 is in scope.
    """
    SparkManager._instance = None
    SparkManager._spark = None

    manager = SparkManager()

    def _raise_spark_error():
        raise SparkError(message="mock aws auth failure", operation="_load_credentials")

    monkeypatch.setattr(manager, "_load_credentials", _raise_spark_error)
    monkeypatch.setattr(
        "src.utils.spark_manager.SparkSessionConf.get_java_options", lambda *_: None
    )
    monkeypatch.setattr("src.utils.spark_manager.atexit.register", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        "src.utils.spark_manager.SparkSessionConf.get_configs_for_destinations", lambda **_: {}
    )
    monkeypatch.setattr(
        "src.utils.spark_manager.SparkSessionConf.startup_summary", lambda **_: "ok"
    )

    class _FakeBuilder:
        def config(self, key, value):
            return self

        def getOrCreate(self):
            return SimpleNamespace(stop=lambda: None)

    monkeypatch.setattr(
        "src.utils.spark_manager.SparkSession", SimpleNamespace(builder=_FakeBuilder())
    )

    rt = resolve_spark_runtime(SparkRuntimeConfig())
    with pytest.raises(SparkError, match="mock aws auth failure"):
        manager.init_session(destinations={"s3"}, spark_runtime=rt)

    assert manager._spark is None

    SparkManager._instance = None
    SparkManager._spark = None


def test_spark_manager_skips_aws_credentials_when_s3_not_requested(monkeypatch) -> None:
    SparkManager._instance = None
    SparkManager._spark = None

    manager = SparkManager()

    def _raise_if_called():
        raise AssertionError("_load_credentials should not run when destination set excludes s3")

    monkeypatch.setattr(manager, "_load_credentials", _raise_if_called)
    monkeypatch.setattr(
        "src.utils.spark_manager.SparkSessionConf.get_java_options", lambda *_: None
    )
    monkeypatch.setattr("src.utils.spark_manager.atexit.register", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        "src.utils.spark_manager.SparkSessionConf.get_configs_for_destinations", lambda **_: {}
    )
    monkeypatch.setattr(
        "src.utils.spark_manager.SparkSessionConf.startup_summary", lambda **_: "ok"
    )

    class _FakeBuilder:
        def config(self, key, value):
            return self

        def getOrCreate(self):
            return SimpleNamespace(stop=lambda: None)

    monkeypatch.setattr(
        "src.utils.spark_manager.SparkSession", SimpleNamespace(builder=_FakeBuilder())
    )

    rt = resolve_spark_runtime(SparkRuntimeConfig())
    assert manager.init_session(destinations={"local"}, spark_runtime=rt) is not None

    manager._spark = None
    SparkManager._instance = None


def test_load_credentials_maps_manager_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    SparkManager._instance = None
    mgr = SparkManager()

    fake_cm = MagicMock()
    fake_cm.get_credentials.return_value = {
        "aws_access_key": "AKIA",
        "aws_secret_key": "secret",
        "aws_region": "us-east-1",
        "use_explicit_credentials": False,
        "aws_session_token": "tok",
    }
    monkeypatch.setattr(
        "src.utils.spark_manager.AWSCredentialManager",
        lambda: fake_cm,
    )
    mgr._load_credentials()
    assert mgr.aws_access_key == "AKIA"
    assert mgr.aws_session_token == "tok"


def test_load_credentials_wraps_aws_error(monkeypatch: pytest.MonkeyPatch) -> None:
    SparkManager._instance = None
    mgr = SparkManager()

    fake_cm = MagicMock()
    fake_cm.get_credentials.side_effect = AWSError("no keys")
    monkeypatch.setattr(
        "src.utils.spark_manager.AWSCredentialManager",
        lambda: fake_cm,
    )
    with pytest.raises(SparkError, match="Failed to load AWS credentials"):
        mgr._load_credentials()


def test_get_s3_path_helper() -> None:
    SparkManager._instance = None
    mgr = SparkManager()
    assert mgr.get_s3_path("mybucket", "path/to/o") == "s3a://mybucket/path/to/o"


def test_resolve_spark_runtime_falls_back_to_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    SparkManager._instance = None
    mgr = SparkManager()
    resolved = resolve_spark_runtime(SparkRuntimeConfig())
    monkeypatch.setattr(
        "src.utils.spark_manager.get_settings",
        lambda: SimpleNamespace(
            pipeline_config=SimpleNamespace(
                defaults=SimpleNamespace(spark_runtime=SparkRuntimeConfig()),
            )
        ),
    )
    monkeypatch.setattr(
        "src.utils.spark_manager.resolve_spark_runtime",
        lambda _cfg: resolved,
    )
    assert mgr._resolve_spark_runtime(None) is resolved


def test_init_session_applies_multiple_builder_configs(monkeypatch: pytest.MonkeyPatch) -> None:
    SparkManager._instance = None
    SparkManager._spark = None

    manager = SparkManager()
    monkeypatch.setattr(manager, "_load_credentials", lambda: None)
    monkeypatch.setattr(
        "src.utils.spark_manager.SparkSessionConf.get_java_options", lambda *_: None
    )
    monkeypatch.setattr("src.utils.spark_manager.atexit.register", lambda *a, **k: None)
    monkeypatch.setattr(
        "src.utils.spark_manager.SparkSessionConf.get_configs_for_destinations",
        lambda **_: {"spark.master": "local[1]", "spark.ui.enabled": "false"},
    )
    monkeypatch.setattr(
        "src.utils.spark_manager.SparkSessionConf.startup_summary", lambda **_: "sum"
    )

    cfg_calls = []

    class _FakeBuilder:
        def config(self, key, value):
            cfg_calls.append((key, value))
            return self

        def getOrCreate(self):
            return SimpleNamespace(stop=lambda: None)

    monkeypatch.setattr(
        "src.utils.spark_manager.SparkSession", SimpleNamespace(builder=_FakeBuilder())
    )

    rt = resolve_spark_runtime(SparkRuntimeConfig())
    manager.init_session(destinations={"local"}, spark_runtime=rt)
    assert len(cfg_calls) == 2


def test_get_session_and_stop_session() -> None:
    SparkManager._instance = None
    mgr = SparkManager()
    assert mgr.get_session() is None
    fake_spark = Mock()
    mgr._spark = fake_spark
    assert mgr.get_session() is fake_spark
    mgr.stop_session()
    fake_spark.stop.assert_called_once()
    assert mgr._spark is None
