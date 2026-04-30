"""Unit tests for small, deterministic ``DynamicHandler`` orchestration helpers."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.config.config_models import (
    LoadingConfig,
    PaginationConfig,
    RequestInputConfig,
    ResourceConfig,
    SourceType,
)
from src.handler.dynamic_handler import DynamicHandler, RawDataBatch
from src.utils.dynamic_values import FilterType, FilterValueSource
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


def test_collect_effective_loading_configs_skips_missing_source_and_resource() -> None:
    h = _bare_handler()
    good_res = ResourceConfig(loading=LoadingConfig(destination="s3", s3_bucket="b", prefix="a/b"))
    source_ok = SimpleNamespace(resources={"good": good_res})
    h.execution_plan = SimpleNamespace(
        stages=[
            SimpleNamespace(
                resources=[
                    SimpleNamespace(source_name="missing_src", resource_name="x"),
                    SimpleNamespace(source_name="src_ok", resource_name="missing_resource"),
                    SimpleNamespace(source_name="src_ok", resource_name="good"),
                ]
            )
        ],
        get_source_config=lambda n: source_ok if n == "src_ok" else None,
    )
    h._resolve_resource_loading = MagicMock(
        side_effect=lambda s, r, rc: rc.loading if rc and rc.loading else None
    )  # type: ignore[method-assign]

    out = h._collect_effective_loading_configs()
    assert len(out) == 1
    assert out[0].prefix == "a/b"


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


def test_handle_snapshot_maps_generic_exception(monkeypatch: pytest.MonkeyPatch) -> None:
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

    class _PollerBoom:
        def __init__(self, **_kwargs) -> None:
            pass

        def wait_for_completion(self, _params):
            raise RuntimeError("boom")

    monkeypatch.setattr("src.handler.dynamic_handler.SnapshotPoller", _PollerBoom)
    with pytest.raises(HandlerError, match="Failed to process snapshot resource"):
        h._handle_snapshot(resource_meta, service, {})


def test_make_single_request_pagination_continues_after_page_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h = _bare_handler()
    h._resolve_resource_header_values = MagicMock(return_value={})
    h._resolve_parameter_values_list = MagicMock(return_value=[])
    h._build_parent_context = MagicMock(return_value={})
    h._resolve_request_body_context = MagicMock(return_value={})

    rc = ResourceConfig(
        response_key="data",
        request_inputs={
            "page": RequestInputConfig(
                value=1,
                location="query",
                pagination=PaginationConfig(page_info_path="page_info"),
            )
        },
    )
    service = SimpleNamespace(
        source_name="src",
        fetch_data=MagicMock(
            side_effect=[
                {"data": [{"id": 1}], "page_info": {"total_page": 3, "page": 1}},
                RuntimeError("page 2 failed"),
                [{"id": 3}],
            ]
        ),
    )
    collector = MagicMock()
    resource_meta = SimpleNamespace(source_name="src", resource_name="users", config=rc)
    source_config = SimpleNamespace(headers={})

    h._make_single_request(
        resource_config=rc,
        context={"page": 1},
        service=service,
        collector=collector,
        resource_meta=resource_meta,
        source_config=source_config,
    )

    collector.add_batch.assert_called_once()
    batch = collector.add_batch.call_args.args[0]
    assert isinstance(batch, RawDataBatch)
    assert len(batch.raw_data) == 2


def test_process_resource_partial_failure_when_some_contexts_fail(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h = _bare_handler()
    h.spark = MagicMock()
    h.backfill_mode = False
    h.record_limit = None
    h.config.defaults = SimpleNamespace(
        log_full_row_count=False,
        streaming=SimpleNamespace(
            enable_streaming=False,
            mode="redis",
            flush_threshold=2,
            disk_config=SimpleNamespace(path="/tmp", file_size_threshold=10),
        ),
    )
    h.execution_plan = SimpleNamespace(
        has_batch_inputs=lambda *_a, **_k: False,
        get_dependent_resources=lambda *_a, **_k: [],
        get_source_config=lambda *_a, **_k: None,
        get_dependent_loading_configs=lambda *_a, **_k: [],
    )
    h._resolve_resource_loading = MagicMock(return_value=None)
    h._generate_all_request_contexts = MagicMock(return_value=[{"k": 1}, {"k": 2}])
    df = MagicMock()
    df.cache.return_value = df
    df.take.return_value = [{"x": 1}]
    h._parse_data = MagicMock(return_value=df)

    def _req(**kwargs):
        ctx = kwargs["context"]
        if ctx["k"] == 2:
            raise RuntimeError("ctx failed")
        kwargs["collector"].add_batch(RawDataBatch(raw_data=[{"id": 1}], request_context={}))

    h._make_single_request = MagicMock(side_effect=lambda **kwargs: _req(**kwargs))

    resource_meta = SimpleNamespace(
        source_name="src",
        resource_name="users",
        config=ResourceConfig(method="GET"),
        backfill_config=None,
        batch_inputs={},
    )
    out = h._process_resource(
        resource_meta=resource_meta,
        service=SimpleNamespace(source_name="src"),
        source_config=SimpleNamespace(type=SourceType.REST_API),
    )
    assert out["status"] == "partial_failure"


def test_process_resource_failed_when_all_contexts_fail(monkeypatch: pytest.MonkeyPatch) -> None:
    h = _bare_handler()
    h.spark = MagicMock()
    h.backfill_mode = False
    h.record_limit = None
    h.config.defaults = SimpleNamespace(
        log_full_row_count=False,
        streaming=SimpleNamespace(
            enable_streaming=False,
            mode="redis",
            flush_threshold=2,
            disk_config=SimpleNamespace(path="/tmp", file_size_threshold=10),
        ),
    )
    h.execution_plan = SimpleNamespace(
        has_batch_inputs=lambda *_a, **_k: False,
        get_dependent_resources=lambda *_a, **_k: [],
        get_source_config=lambda *_a, **_k: None,
        get_dependent_loading_configs=lambda *_a, **_k: [],
    )
    h._resolve_resource_loading = MagicMock(return_value=None)
    h._generate_all_request_contexts = MagicMock(return_value=[{"k": 1}, {"k": 2}])
    h._make_single_request = MagicMock(side_effect=RuntimeError("all failed"))

    resource_meta = SimpleNamespace(
        source_name="src",
        resource_name="users",
        config=ResourceConfig(method="GET"),
        backfill_config=None,
        batch_inputs={},
    )
    out = h._process_resource(
        resource_meta=resource_meta,
        service=SimpleNamespace(source_name="src"),
        source_config=SimpleNamespace(type=SourceType.REST_API),
    )
    assert out["status"] == "failed"
    assert out["warning"] == "No data returned from source"


def test_extract_filter_value_handles_list_scalar_and_missing() -> None:
    h = _bare_handler()
    assert h._extract_filter_value("k", {"k": ["a", "b"]}) == "a"
    assert h._extract_filter_value("k", {"k": "v"}) == "v"
    assert h._extract_filter_value("k", {}) is None


def test_get_filter_value_from_context_parameter_source_match() -> None:
    h = _bare_handler()
    source_value = FilterValueSource(source="parent", field="snapshotId")
    filter_cfg = SimpleNamespace(value_type="parameter", value_source=source_value)
    source_cfg = SimpleNamespace(source="child", field="id", filter=filter_cfg)
    matched_cfg = SimpleNamespace(
        get_source_config=lambda: SimpleNamespace(source="parent", field="snapshotId")
    )
    target_cfg = SimpleNamespace(get_source_config=lambda: source_cfg)
    resource_cfg = SimpleNamespace(request_inputs={"target": target_cfg, "snapshot": matched_cfg})
    out = h._get_filter_value_from_context(
        resource_config=resource_cfg,
        input_name="target",
        context={"snapshot": ["abc"]},
    )
    assert out == "abc"


def test_get_filter_value_from_context_returns_none_for_non_parameter_filter() -> None:
    h = _bare_handler()
    filter_cfg = SimpleNamespace(value_type="static", value_source="x")
    target_cfg = SimpleNamespace(
        get_source_config=lambda: SimpleNamespace(source="child", field="id", filter=filter_cfg)
    )
    resource_cfg = SimpleNamespace(request_inputs={"target": target_cfg})
    out = h._get_filter_value_from_context(resource_cfg, "target", {"target": "x"})
    assert out is None


def test_extract_pagination_info_happy_path_and_missing_required_field() -> None:
    h = _bare_handler()
    cfg = PaginationConfig(page_info_path="meta.page_info")
    ok = h._extract_pagination_info(
        {"meta": {"page_info": {"page": 1, "total_page": 4, "page_size": 10, "total_number": 40}}},
        cfg,
    )
    assert ok is not None
    assert ok["total_page"] == 4
    missing_total = h._extract_pagination_info({"meta": {"page_info": {"page": 1}}}, cfg)
    assert missing_total is None


def test_extract_pagination_info_returns_none_for_invalid_shape() -> None:
    h = _bare_handler()
    cfg = PaginationConfig(page_info_path="meta.page_info")
    assert h._extract_pagination_info({"meta": {"page_info": "oops"}}, cfg) is None


def test_find_pagination_in_nested_value_detects_config_and_path() -> None:
    h = _bare_handler()
    nested = {
        "Paging": {
            "PageNo": {
                "value": {
                    "type": "PAGINATION",
                    "pagination_config": {"page_info_path": "meta.page_info"},
                }
            }
        }
    }
    out = h._find_pagination_in_nested_value(nested)
    assert out is not None
    pagination_cfg, field_path = out
    assert pagination_cfg["page_info_path"] == "meta.page_info"
    assert field_path == "Paging.PageNo"
