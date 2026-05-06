"""Tests for ``BaseHandler`` retries, Spark bootstrap, and data helpers (no full pipeline)."""

import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.handler.base_handler import BaseHandler
from src.utils.exceptions import HandlerError, SparkError


class _StubHandler(BaseHandler):
    def handle(self) -> dict:
        return {"ok": True}

    def validate(self) -> None:
        return None


def test_with_retry_succeeds_first_try() -> None:
    h = _StubHandler(parser=None, loader=None, destination=None, max_retries=2)
    assert h.with_retry(lambda: 7, "op") == 7


def test_with_retry_exhausts_then_handler_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(time, "sleep", lambda s: None)
    h = _StubHandler(parser=None, loader=None, destination=None, max_retries=1, retry_delay=0.01)
    n = {"c": 0}

    def boom() -> int:
        n["c"] += 1
        raise ValueError("x")

    with pytest.raises(HandlerError, match="after 2 attempts"):
        h.with_retry(boom, "op", retryable_exceptions=(ValueError,))


def test_with_retry_non_retryable_wraps() -> None:
    h = _StubHandler(parser=None, loader=None, destination=None)

    def boom() -> None:
        raise RuntimeError("no")

    with pytest.raises(HandlerError, match="op"):
        h.with_retry(boom, "op", retryable_exceptions=(ValueError,))


def test_track_error_appends() -> None:
    h = _StubHandler(parser=None, loader=None, destination=None)
    h.track_error(ValueError("e"), {"k": 1})
    assert len(h.errors) == 1
    assert h.errors[0]["k"] == 1


def test_cleanup_stops_spark_when_present() -> None:
    h = _StubHandler(parser=None, loader=None, destination=None)
    h.spark_manager = MagicMock()
    h.cleanup()
    h.spark_manager.stop_session.assert_called_once()


def test_setup_spark_with_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    h = _StubHandler(parser=None, loader=None, destination=None)
    h.settings = SimpleNamespace(
        loading_destinations={"local"},
        pipeline_config=SimpleNamespace(defaults=SimpleNamespace(spark_runtime=MagicMock())),
    )
    fake_spark = MagicMock(name="spark")
    sm = MagicMock()
    sm.init_session.return_value = fake_spark
    monkeypatch.setattr("src.handler.base_handler.SparkManager", lambda: sm)
    monkeypatch.setattr("src.handler.base_handler.resolve_spark_runtime", lambda _c: None)
    h._setup_spark()
    assert h.spark is fake_spark


def test_setup_spark_spark_error_wrapped(monkeypatch: pytest.MonkeyPatch) -> None:
    h = _StubHandler(parser=None, loader=None, destination=None)
    h.settings = SimpleNamespace(
        loading_destinations={"local"},
        pipeline_config=SimpleNamespace(defaults=SimpleNamespace(spark_runtime=MagicMock())),
    )
    sm = MagicMock()
    sm.init_session.side_effect = SparkError("bad")
    monkeypatch.setattr("src.handler.base_handler.SparkManager", lambda: sm)
    monkeypatch.setattr("src.handler.base_handler.resolve_spark_runtime", lambda _c: None)
    with pytest.raises(HandlerError, match="Spark"):
        h._setup_spark()


def test_test_local_storage_writable(tmp_path: Path) -> None:
    h = _StubHandler(parser=None, loader=None, destination=None)
    root = tmp_path / "out"
    root.mkdir()
    h._test_local_storage_writable(str(root))


def test_load_data_delegates_to_with_retry(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(time, "sleep", lambda s: None)
    loader = MagicMock()
    loader.load.return_value = "s3://done"
    h = _StubHandler(parser=None, loader=loader, destination="here", max_retries=0, retry_delay=0)
    assert h._load_data([{"a": 1}], "pfx") == "s3://done"


def test_parse_data_success_and_failure() -> None:
    parser = MagicMock()
    parser.parse.return_value = [{"x": 1}]
    h = _StubHandler(parser=parser, loader=None, destination=None)
    assert h._parse_data({"raw": 1}) == [{"x": 1}]
    parser.parse.side_effect = ValueError("parse")
    with pytest.raises(HandlerError, match="Failed to parse"):
        h._parse_data({})
