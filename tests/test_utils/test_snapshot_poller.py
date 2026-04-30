"""Tests for snapshot polling utility behavior and edge branches."""

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.utils.exceptions import PipelineError
from src.utils.snapshot_poller import SnapshotError, SnapshotPoller, SnapshotTimeoutError


def _cfg(**overrides):
    base = {
        "max_time": 10,
        "interval": 1,
        "backoff_factor": 2,
        "max_interval": 4,
        "ready_condition": "response.get('status') == 'ready'",
        "error_condition": None,
    }
    base.update(overrides)
    return SimpleNamespace(**base)


def test_wait_for_completion_ready_path_returns_response() -> None:
    poller = SnapshotPoller(_cfg(), MagicMock(), get_snapshot=lambda _p: {"status": "ready"})
    out = poller.wait_for_completion({})
    assert out["status"] == "ready"


def test_wait_for_completion_error_condition_raises_snapshot_error() -> None:
    poller = SnapshotPoller(
        _cfg(error_condition="response.get('status') == 'error'"),
        MagicMock(),
        get_snapshot=lambda _p: {"status": "error"},
    )
    with pytest.raises(SnapshotError, match="error state"):
        poller.wait_for_completion({})


def test_wait_for_completion_timeout_preserves_last_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    logger = MagicMock()
    poller = SnapshotPoller(
        _cfg(max_time=1, interval=0), logger, get_snapshot=lambda _p: {"status": "pending"}
    )
    times = [
        datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC),
        datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC),
        datetime(2026, 1, 1, 0, 0, 2, tzinfo=UTC),
    ]
    idx = {"i": -1}

    def _now():
        idx["i"] += 1
        if idx["i"] < len(times):
            return times[idx["i"]]
        return times[-1]

    monkeypatch.setattr(
        "src.utils.snapshot_poller.datetime",
        SimpleNamespace(now=_now),
    )
    monkeypatch.setattr("src.utils.snapshot_poller.time.sleep", lambda _s: None)
    with pytest.raises(SnapshotTimeoutError, match="did not complete") as excinfo:
        poller.wait_for_completion({})
    assert excinfo.value.last_response == {"status": "pending"}


def test_wait_for_completion_non_retryable_pipeline_error_raises() -> None:
    def _boom(_p):
        raise PipelineError("no retry", operation="poll", is_retryable=False, component="snapshot")

    poller = SnapshotPoller(_cfg(), MagicMock(), get_snapshot=_boom)
    with pytest.raises(PipelineError, match="no retry"):
        poller.wait_for_completion({})


def test_wait_for_completion_retryable_error_retries_then_succeeds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = {"n": 0}

    def _fetch(_p):
        state["n"] += 1
        if state["n"] == 1:
            raise RuntimeError("temporary")
        return {"status": "ready"}

    poller = SnapshotPoller(_cfg(interval=0), MagicMock(), get_snapshot=_fetch)
    monkeypatch.setattr("src.utils.snapshot_poller.time.sleep", lambda _s: None)
    out = poller.wait_for_completion({})
    assert out["status"] == "ready"
    assert state["n"] == 2


def test_wait_for_completion_backoff_uses_max_interval(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(max_time=5, interval=1, backoff_factor=3, max_interval=2)
    poller = SnapshotPoller(cfg, MagicMock(), get_snapshot=lambda _p: {"status": "pending"})
    now = datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)
    seq = [now, now, now, now, now + timedelta(seconds=6)]
    monkeypatch.setattr(
        "src.utils.snapshot_poller.datetime", SimpleNamespace(now=lambda: seq.pop(0))
    )
    sleeps: list[float] = []
    monkeypatch.setattr("src.utils.snapshot_poller.time.sleep", lambda s: sleeps.append(s))
    with pytest.raises(SnapshotTimeoutError, match="did not complete"):
        poller.wait_for_completion({})
    assert sleeps and all(s <= 2 for s in sleeps)


def test_evaluate_condition_failure_returns_false() -> None:
    poller = SnapshotPoller(
        _cfg(ready_condition="response["), MagicMock(), get_snapshot=lambda _p: {}
    )
    assert poller._evaluate_condition("response[", {"status": "ready"}) is False
