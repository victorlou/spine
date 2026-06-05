"""Unit tests for small, deterministic ``DynamicHandler`` orchestration helpers."""

import builtins
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from pyspark.sql import DataFrame as SparkDataFrame

from src.config.config_models import (
    LoadingConfig,
    LoadingFormat,
    PaginationConfig,
    RequestInputConfig,
    ResourceConfig,
    SourceType,
)
from src.handler.dynamic_handler import DynamicHandler, RawDataBatch
from src.planner.execution_plan import ResourceMetadata
from src.utils.dynamic_values import (
    ComplexDynamicValue,
    DynamicSourceReference,
    DynamicValueType,
    FilterConfig,
    FilterOperator,
    FilterType,
    FilterValueSource,
)
from src.utils.exceptions import HandlerError
from src.utils.logger import get_logger
from src.utils.snapshot_poller import SnapshotError, SnapshotTimeoutError
from src.utils.telemetry_manager import TelemetryManager


def _bare_handler() -> DynamicHandler:
    h = DynamicHandler.__new__(DynamicHandler)
    h.logger = get_logger("test_dynamic_handler_core")
    h.config = SimpleNamespace(
        defaults=SimpleNamespace(context=SimpleNamespace(prefix="pipeline:")),
        sources={},
    )
    h.redis_context = MagicMock()
    # Telemetry is a no-op singleton in tests; instrumented methods touch these attributes.
    h._telemetry = TelemetryManager()
    h._tracer = h._telemetry.get_tracer("test")
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


def test_process_resource_database_source_does_not_cache_dataframe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Database path does not call DataFrame.cache() before probe/write."""
    h = _bare_handler()
    h.spark = MagicMock()
    h.backfill_mode = False
    h.record_limit = None
    h.config.defaults = SimpleNamespace(
        streaming=SimpleNamespace(
            enable_streaming=False,
            mode="redis",
            flush_threshold=2,
            disk_config=SimpleNamespace(path="/tmp", file_size_threshold=10),
        ),
    )
    loading = LoadingConfig(
        destination="s3",
        s3_bucket="bucket",
        prefix="sap/vbrp",
        format=LoadingFormat.DELTA,
    )
    h.execution_plan = SimpleNamespace(
        has_batch_inputs=lambda *_a, **_k: False,
        get_dependent_resources=lambda *_a, **_k: [],
        get_source_config=lambda *_a, **_k: SimpleNamespace(type=SourceType.HANA),
        get_dependent_loading_configs=lambda *_a, **_k: [],
    )
    h._resolve_resource_loading = MagicMock(return_value=loading)
    h._generate_all_request_contexts = MagicMock(return_value=[{}])
    df = MagicMock()
    df.take = MagicMock(return_value=[MagicMock()])
    h._build_database_dataframe = MagicMock(return_value=df)
    mock_loader = MagicMock()
    mock_loader.load = MagicMock(return_value="s3a://bucket/sap/vbrp")
    mock_factory = MagicMock()
    mock_factory.create_loader.return_value = mock_loader
    monkeypatch.setattr("src.handler.dynamic_handler.LoaderFactory", mock_factory)

    resource_meta = ResourceMetadata(
        source_name="sap",
        resource_name="vbrp",
        dependencies=set(),
        batch_inputs={},
        config=ResourceConfig(
            method="GET",
            database_schema="SAPHANADB",
            database_table="/BIC/AZ",
            loading=loading,
        ),
    )
    out = h._process_resource(
        resource_meta=resource_meta,
        service=MagicMock(source_name="sap"),
        source_config=SimpleNamespace(type=SourceType.HANA),
    )
    assert out["status"] == "success"
    df.cache.assert_not_called()
    df.unpersist.assert_not_called()
    mock_loader.load.assert_called_once()


def test_process_resource_failed_when_all_contexts_fail(monkeypatch: pytest.MonkeyPatch) -> None:
    h = _bare_handler()
    h.spark = MagicMock()
    h.backfill_mode = False
    h.record_limit = None
    h.config.defaults = SimpleNamespace(
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


# --- _resolve_parameter_values_list ---


def test_resolve_parameter_values_list_context_scalar() -> None:
    h = _bare_handler()
    ic = RequestInputConfig(value="ignored", location="query")
    assert h._resolve_parameter_values_list("x", ic, "api", "res", context={"x": 7}) == [7]


def test_resolve_parameter_values_list_context_list() -> None:
    h = _bare_handler()
    ic = RequestInputConfig(value="ignored", location="query")
    lst = [1, 2]
    out = h._resolve_parameter_values_list("x", ic, "api", "res", context={"x": lst})
    assert out == lst
    assert out is lst


def test_resolve_parameter_values_list_static_list_primitives() -> None:
    h = _bare_handler()
    ic = RequestInputConfig(value=[1, 2], location="query")
    assert h._resolve_parameter_values_list("p", ic, "api", "res") == [1, 2]


def test_resolve_parameter_values_list_static_list_dict_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h = _bare_handler()
    mock_nested = MagicMock(side_effect=lambda name, d: {"resolved": name, **d})
    monkeypatch.setattr(h, "_resolve_nested_value_in_dict", mock_nested)
    ic = RequestInputConfig(value=[{"a": 1}, {"b": 2}], location="query")
    out = h._resolve_parameter_values_list("p", ic, "api", "res")
    assert mock_nested.call_count == 2
    assert out == [{"resolved": "p", "a": 1}, {"resolved": "p", "b": 2}]


def test_resolve_parameter_values_list_parent_delegation() -> None:
    h = _bare_handler()
    ref = DynamicSourceReference(source="parent", field="id")
    ic = RequestInputConfig(
        value=ComplexDynamicValue(type=DynamicValueType.SOURCE, source_config=ref),
        location="query",
    )
    h._resolve_from_parent_resource = MagicMock(return_value=[42])
    out = h._resolve_parameter_values_list("pid", ic, "api", "child", filter_value="fv")
    assert out == [42]
    h._resolve_from_parent_resource.assert_called_once_with(
        input_name="pid",
        input_config=ic,
        source_config=ref,
        source_name="api",
        resource_name="child",
        filter_value="fv",
    )


def test_resolve_parameter_values_list_resolver_plain() -> None:
    h = _bare_handler()
    res = MagicMock()
    res.resolve.return_value = "out"
    h._current_value_resolver = res
    ic = RequestInputConfig(value="static", location="query")
    assert h._resolve_parameter_values_list("p", ic, "api", "res") == ["out"]
    res.resolve.assert_called_once_with("static")


def test_resolve_parameter_values_list_resolver_none_empty() -> None:
    h = _bare_handler()
    res = MagicMock()
    res.resolve.return_value = None
    h._current_value_resolver = res
    ic = RequestInputConfig(value="x", location="query")
    assert h._resolve_parameter_values_list("p", ic, "api", "res") == []


def test_resolve_parameter_values_list_resolver_list_passthrough() -> None:
    h = _bare_handler()
    res = MagicMock()
    res.resolve.return_value = [1, 2]
    h._current_value_resolver = res
    ic = RequestInputConfig(value="x", location="query")
    assert h._resolve_parameter_values_list("p", ic, "api", "res") == [1, 2]


def test_resolve_parameter_values_list_resolver_dict_nested(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h = _bare_handler()
    res = MagicMock()
    res.resolve.return_value = {"a": 1}
    h._current_value_resolver = res
    nested = MagicMock(return_value={"nested": True})
    monkeypatch.setattr(h, "_resolve_nested_value_in_dict", nested)
    ic = RequestInputConfig(value="x", location="query")
    out = h._resolve_parameter_values_list("p", ic, "api", "res")
    nested.assert_called_once_with("p", {"a": 1})
    assert out == [{"nested": True}]


def test_resolve_parameter_values_list_body_jinja_deferred() -> None:
    h = _bare_handler()
    res = MagicMock()
    h._current_value_resolver = res
    ic = RequestInputConfig(value="{{ ts }}", location="body")
    out = h._resolve_parameter_values_list("p", ic, "api", "res", for_batch_expansion=False)
    assert out == ["{{ ts }}"]
    res.resolve.assert_not_called()


def test_resolve_parameter_values_list_batch_expansion_calls_resolve() -> None:
    h = _bare_handler()
    res = MagicMock()
    res.resolve.return_value = [1, 2, 3]
    h._current_value_resolver = res
    ic = RequestInputConfig(value="{{ ts }}", location="body")
    out = h._resolve_parameter_values_list("p", ic, "api", "res", for_batch_expansion=True)
    assert out == [1, 2, 3]
    res.resolve.assert_called_once_with("{{ ts }}")


def test_resolve_parameter_values_list_source_miswired() -> None:
    h = _bare_handler()
    ic = RequestInputConfig(
        value=ComplexDynamicValue(type=DynamicValueType.SOURCE, source_config=None),
        location="query",
    )
    h._current_value_resolver = MagicMock()
    with pytest.raises(
        HandlerError,
        match="SOURCE type dynamic value should be handled via source_config",
    ):
        h._resolve_parameter_values_list("p", ic, "api", "res")


def test_resolve_parameter_values_list_no_value_no_parent() -> None:
    h = _bare_handler()
    ic = SimpleNamespace(value=None, is_static_list=lambda: False, get_source_config=lambda: None)
    with pytest.raises(HandlerError, match="No source or value defined"):
        h._resolve_parameter_values_list("p", ic, "api", "res")


# --- _resolve_resource_header_values ---


def _handler_for_header_resolution() -> DynamicHandler:
    h = _bare_handler()
    h.spark = MagicMock()
    return h


def _source_header_token(
    field: str = "tok", source: str = "parent", filt=None
) -> ComplexDynamicValue:
    return ComplexDynamicValue(
        type=DynamicValueType.SOURCE,
        source_config=DynamicSourceReference(source=source, field=field, filter=filt),
    )


@pytest.fixture
def _restore_isinstance():
    real = builtins.isinstance
    yield
    builtins.isinstance = real


def test_get_value_resolver_prefers_current_cycle_resolver() -> None:
    h = _handler_for_header_resolution()
    custom = MagicMock(name="cycle_resolver")
    h._current_value_resolver = custom
    assert h._get_value_resolver() is custom


def test_get_value_resolver_falls_back_to_module_get_resolver(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h = _handler_for_header_resolution()
    assert getattr(h, "_current_value_resolver", None) is None
    fallback = MagicMock(name="fallback_resolver")
    monkeypatch.setattr(
        "src.handler.dynamic_handler.get_resolver",
        lambda rc: fallback if rc is h.redis_context else None,
    )
    assert h._get_value_resolver() is fallback


def test_resolve_resource_header_values_non_source_uses_shared_resolver() -> None:
    h = _handler_for_header_resolution()
    res = MagicMock()
    res.resolve.return_value = "resolved-plain"
    h._current_value_resolver = res
    out = h._resolve_resource_header_values(
        {"X-Plain": "application/json"},
        source_name="api",
        resource_name="child",
    )
    assert out == {"X-Plain": "resolved-plain"}
    res.resolve.assert_called_once_with("application/json")


@pytest.mark.parametrize(
    ("redis_payload", "match"),
    [
        ([], "Invalid list data format"),
        ([123], "Invalid list data format"),
    ],
)
def test_resolve_resource_header_values_list_invalid_format(
    redis_payload: list, match: str
) -> None:
    h = _handler_for_header_resolution()
    h.redis_context.get.return_value = redis_payload
    headers = {"X-Auth": _source_header_token()}
    with pytest.raises(HandlerError, match=match):
        h._resolve_resource_header_values(headers, source_name="api", resource_name="child")


def test_resolve_resource_header_values_redis_miss_raises() -> None:
    h = _handler_for_header_resolution()
    h.redis_context.get.return_value = None
    with pytest.raises(HandlerError, match="Required data from parent ref"):
        h._resolve_resource_header_values(
            {"X-Auth": _source_header_token()},
            source_name="api",
            resource_name="child",
        )


def test_resolve_resource_header_values_list_success() -> None:
    h = _handler_for_header_resolution()
    h.redis_context.get.return_value = [{"tok": "secret-token"}]
    out = h._resolve_resource_header_values(
        {"Authorization": _source_header_token(field="tok")},
        source_name="api",
        resource_name="child",
    )
    assert out == {"Authorization": "secret-token"}
    h.redis_context.get.assert_called_once_with("pipeline:api:parent", spark=h.spark)


def test_resolve_resource_header_values_list_missing_field_wraps_handler_error() -> None:
    h = _handler_for_header_resolution()
    h.redis_context.get.return_value = [{"other": 1}]
    with pytest.raises(HandlerError, match="Failed to resolve header"):
        h._resolve_resource_header_values(
            {"X-Auth": _source_header_token(field="tok")},
            source_name="api",
            resource_name="child",
        )


def test_resolve_resource_header_values_unsupported_parent_type_raises() -> None:
    h = _handler_for_header_resolution()
    h.redis_context.get.return_value = "not-list-or-dataframe"
    with pytest.raises(HandlerError, match="Unsupported data type"):
        h._resolve_resource_header_values(
            {"X-Auth": _source_header_token()},
            source_name="api",
            resource_name="child",
        )


def test_resolve_resource_header_values_dataframe_branch(
    monkeypatch: pytest.MonkeyPatch, _restore_isinstance
) -> None:
    h = _handler_for_header_resolution()
    sentinel = MagicMock(name="fake_df")
    sentinel.columns = ["tok"]
    post_null = MagicMock()
    post_null.select.return_value.limit.return_value.collect.return_value = [{"tok": "from-df"}]
    sentinel.filter.return_value = post_null

    real_isinstance = builtins.isinstance

    def isinstance_shim(obj, cls):
        if obj is sentinel and cls is SparkDataFrame:
            return True
        return real_isinstance(obj, cls)

    monkeypatch.setattr(builtins, "isinstance", isinstance_shim)
    monkeypatch.setattr(
        "src.handler.dynamic_handler.col", lambda *_a, **_k: MagicMock(name="col_expr")
    )
    h.redis_context.get.return_value = sentinel

    out = h._resolve_resource_header_values(
        {"X-Tok": _source_header_token(field="tok")},
        source_name="api",
        resource_name="child",
    )
    assert out == {"X-Tok": "from-df"}


def test_resolve_resource_header_values_dataframe_missing_column_wraps(
    monkeypatch: pytest.MonkeyPatch, _restore_isinstance
) -> None:
    h = _handler_for_header_resolution()
    sentinel = MagicMock(name="fake_df")
    sentinel.columns = ["other"]

    real_isinstance = builtins.isinstance

    def isinstance_shim(obj, cls):
        if obj is sentinel and cls is SparkDataFrame:
            return True
        return real_isinstance(obj, cls)

    monkeypatch.setattr(builtins, "isinstance", isinstance_shim)
    monkeypatch.setattr(
        "src.handler.dynamic_handler.col", lambda *_a, **_k: MagicMock(name="col_expr")
    )
    h.redis_context.get.return_value = sentinel

    with pytest.raises(HandlerError, match="Failed to resolve header"):
        h._resolve_resource_header_values(
            {"X-Tok": _source_header_token(field="tok")},
            source_name="api",
            resource_name="child",
        )


def test_resolve_resource_header_values_dataframe_empty_after_null_filter_raises(
    monkeypatch: pytest.MonkeyPatch, _restore_isinstance
) -> None:
    h = _handler_for_header_resolution()
    sentinel = MagicMock(name="fake_df")
    sentinel.columns = ["tok"]
    post_null = MagicMock()
    post_null.select.return_value.limit.return_value.collect.return_value = []
    sentinel.filter.return_value = post_null

    real_isinstance = builtins.isinstance

    def isinstance_shim(obj, cls):
        if obj is sentinel and cls is SparkDataFrame:
            return True
        return real_isinstance(obj, cls)

    monkeypatch.setattr(builtins, "isinstance", isinstance_shim)
    monkeypatch.setattr(
        "src.handler.dynamic_handler.col", lambda *_a, **_k: MagicMock(name="col_expr")
    )
    h.redis_context.get.return_value = sentinel

    with pytest.raises(HandlerError, match="No values found in field"):
        h._resolve_resource_header_values(
            {"X-Tok": _source_header_token(field="tok")},
            source_name="api",
            resource_name="child",
        )


def test_resolve_resource_header_values_applies_filter_before_extracting_from_dataframe(
    monkeypatch: pytest.MonkeyPatch, _restore_isinstance
) -> None:
    h = _handler_for_header_resolution()
    filt = FilterConfig(
        type=FilterType.COLUMN,
        field="status",
        operator=FilterOperator.EQUALS,
        value_source="active",
        value_type="static",
    )
    sentinel = MagicMock(name="fake_df_before_filter")
    filtered = MagicMock(name="after_parameter_filter")
    filtered.columns = ["tok"]
    post_null = MagicMock()
    post_null.select.return_value.limit.return_value.collect.return_value = [
        {"tok": "filtered-val"}
    ]
    filtered.filter.return_value = post_null

    real_isinstance = builtins.isinstance

    def isinstance_shim(obj, cls):
        if obj is sentinel and cls is SparkDataFrame:
            return True
        return real_isinstance(obj, cls)

    monkeypatch.setattr(builtins, "isinstance", isinstance_shim)
    monkeypatch.setattr(
        "src.handler.dynamic_handler.col", lambda *_a, **_k: MagicMock(name="col_expr")
    )
    h.redis_context.get.return_value = sentinel
    h._apply_parameter_filter = MagicMock(return_value=filtered)  # type: ignore[method-assign]

    out = h._resolve_resource_header_values(
        {"X-Tok": _source_header_token(field="tok", filt=filt)},
        source_name="api",
        resource_name="child",
    )
    assert out == {"X-Tok": "filtered-val"}
    h._apply_parameter_filter.assert_called_once()
    call_kw = h._apply_parameter_filter.call_args.kwargs
    assert call_kw["df"] is sentinel
    assert call_kw["filter_config"] is filt
