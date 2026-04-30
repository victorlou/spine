"""Unit tests for small, deterministic ``DynamicHandler`` orchestration helpers."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.config.config_models import LoadingConfig, ResourceConfig
from src.handler.dynamic_handler import DynamicHandler
from src.utils.dynamic_values import FilterType
from src.utils.exceptions import HandlerError
from src.utils.logger import get_logger
from src.utils.snapshot_poller import SnapshotError, SnapshotTimeoutError


def _bare_handler() -> DynamicHandler:
    h = DynamicHandler.__new__(DynamicHandler)
    h.logger = get_logger("test_dynamic_handler_core")
    h.config = SimpleNamespace(
        defaults=SimpleNamespace(context=SimpleNamespace(prefix="pipeline:")),
        sources={},
    )
    h.redis_context = MagicMock()
    return h


def test_split_into_batches_and_apply_request_limit() -> None:
    h = _bare_handler()
    assert h._split_into_batches([], 3) == []
    assert h._split_into_batches([1, 2, 3, 4], 2) == [[1, 2], [3, 4]]
    assert h._apply_request_limit([1, 2, 3], None, "items", "s", "r") == [1, 2, 3]
    assert h._apply_request_limit([1, 2, 3], 2, "items", "s", "r") == [1, 2]


def test_parse_parent_resource_ref() -> None:
    h = _bare_handler()
    assert h._parse_parent_resource_ref("parent.resource", "other") == ("parent", "resource")
    assert h._parse_parent_resource_ref("only_res", "current") == ("current", "only_res")


def test_get_redis_key() -> None:
    h = _bare_handler()
    assert h._get_redis_key("src", "r1") == "pipeline:src:r1"


def test_collect_effective_loading_configs_skips_disabled_loading() -> None:
    h = _bare_handler()
    res_on = ResourceConfig(loading=LoadingConfig(destination="s3", s3_bucket="b", prefix="a/b"))
    res_off = ResourceConfig(loading=LoadingConfig(enabled=False, destination="s3", s3_bucket="b"))
    source = SimpleNamespace(resources={"a": res_on, "b": res_off})
    h.execution_plan = SimpleNamespace(
        stages=[
            SimpleNamespace(
                resources=[
                    SimpleNamespace(source_name="src", resource_name="a"),
                    SimpleNamespace(source_name="src", resource_name="b"),
                ]
            )
        ],
        get_source_config=lambda n, sc=source: sc if n == "src" else None,
    )
    h._resolve_resource_loading = MagicMock(
        side_effect=lambda s, r, rc: (
            rc.loading if rc and rc.loading and rc.loading.enabled else None
        )
    )  # type: ignore[method-assign]
    out = h._collect_effective_loading_configs()
    assert len(out) == 1
    assert out[0].enabled is True


def test_apply_parameter_filter_params_requires_params_column() -> None:
    h = _bare_handler()
    df = MagicMock()
    df.columns = ["id"]
    filter_config = SimpleNamespace(
        type=FilterType.PARAMS,
        field="_params",
        params_key="snapshotId",
    )
    with pytest.raises(HandlerError, match="Failed to apply filter"):
        h._apply_parameter_filter(df, "input1", filter_config, filter_value="123")


def test_apply_parameter_filter_params_requires_params_key() -> None:
    h = _bare_handler()
    df = MagicMock()
    df.columns = ["_params"]
    filter_config = SimpleNamespace(
        type=FilterType.PARAMS,
        field="_params",
        params_key=None,
    )
    with pytest.raises(HandlerError, match="Failed to apply filter"):
        h._apply_parameter_filter(df, "input1", filter_config, filter_value="123")


def test_generate_all_request_contexts_backfill_failure_falls_back(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h = _bare_handler()
    h.record_limit = None
    h._current_value_resolver = None
    monkeypatch.setattr(
        "src.handler.dynamic_handler.generate_backfill_date_pairs",
        lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("broken")),
    )
    resource_meta = SimpleNamespace(
        resource_name="r",
        backfill_config=object(),
        batch_inputs={},
        config=SimpleNamespace(request_inputs={}),
    )
    service = SimpleNamespace(source_name="s")
    out = h._generate_all_request_contexts(
        resource_meta=resource_meta, service=service, use_backfill=True
    )
    assert out == [{}]


def test_generate_all_request_contexts_empty_backfill_pairs_falls_back(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h = _bare_handler()
    h.record_limit = None
    h._current_value_resolver = None
    monkeypatch.setattr(
        "src.handler.dynamic_handler.generate_backfill_date_pairs", lambda *_a, **_k: []
    )
    resource_meta = SimpleNamespace(
        resource_name="r",
        backfill_config=object(),
        batch_inputs={},
        config=SimpleNamespace(request_inputs={}),
    )
    service = SimpleNamespace(source_name="s")
    out = h._generate_all_request_contexts(
        resource_meta=resource_meta, service=service, use_backfill=True
    )
    assert out == [{}]


def test_handle_snapshot_maps_timeout_and_snapshot_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    h = _bare_handler()
    resource_meta = SimpleNamespace(
        source_name="src",
        resource_name="r",
        config=SimpleNamespace(
            snapshot=SimpleNamespace(
                max_time=30,
                interval=1,
                backoff_factor=2,
                max_interval=8,
                ready_condition={},
                error_condition={},
            )
        ),
    )
    service = SimpleNamespace(poll_snapshot=lambda *_a, **_k: {})

    class _PollerTimeout:
        def __init__(self, **_kwargs) -> None:
            pass

        def wait_for_completion(self, _params):
            raise SnapshotTimeoutError("timeout", {"state": "pending"})

    monkeypatch.setattr("src.handler.dynamic_handler.SnapshotPoller", _PollerTimeout)
    with pytest.raises(HandlerError, match="Snapshot timed out"):
        h._handle_snapshot(resource_meta, service, {})

    class _PollerFailed:
        def __init__(self, **_kwargs) -> None:
            pass

        def wait_for_completion(self, _params):
            raise SnapshotError("failed", {"state": "error"})

    monkeypatch.setattr("src.handler.dynamic_handler.SnapshotPoller", _PollerFailed)
    with pytest.raises(HandlerError, match="Snapshot failed"):
        h._handle_snapshot(resource_meta, service, {})
