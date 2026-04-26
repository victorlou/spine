"""Tests for SparkManager AWS startup decoupling."""

from types import SimpleNamespace

from src.config.config_models import SparkRuntimeConfig
from src.config.spark_runtime import resolve_spark_runtime
from src.utils.exceptions import SparkError
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


def test_spark_manager_init_session_falls_back_when_aws_credentials_fail(monkeypatch) -> None:
    # Ensure singleton state is clean for this test
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

    observed = {}

    def _mock_get_configs(**kwargs):
        observed.update(kwargs)
        return {}

    monkeypatch.setattr(
        "src.utils.spark_manager.SparkSessionConf.get_configs_for_destinations", _mock_get_configs
    )
    monkeypatch.setattr(
        "src.utils.spark_manager.SparkSessionConf.get_runtime_readiness_errors", lambda *_: []
    )
    monkeypatch.setattr(
        "src.utils.spark_manager.SparkSessionConf.get_runtime_readiness_notes", lambda *a, **k: []
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
    session = manager.init_session(destinations={"s3"}, spark_runtime=rt)

    assert session is not None
    assert observed["use_explicit_credentials"] is False
    assert observed["aws_access_key"] == ""
    assert observed["aws_secret_key"] == ""
    assert observed["destinations"] == {"s3"}

    # Avoid shutdown-time logging noise from singleton teardown in test process.
    manager._spark = None
    SparkManager._instance = None


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
        "src.utils.spark_manager.SparkSessionConf.get_runtime_readiness_errors", lambda *_: []
    )
    monkeypatch.setattr(
        "src.utils.spark_manager.SparkSessionConf.get_runtime_readiness_notes", lambda *a, **k: []
    )
    monkeypatch.setattr(
        "src.utils.spark_manager.SparkSessionConf.get_configs_for_destinations", lambda **_: {}
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
