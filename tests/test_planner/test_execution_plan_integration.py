"""Integration-style tests for ``ExecutionPlan`` using validated ``PipelineConfig``."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.config.config_models import (
    QueriesConfig,
    RequestInputConfig,
    ResourceConfig,
    SourceConfig,
    SourceType,
)
from src.planner.execution_plan import ExecutionPlan, ResourceMetadata
from src.utils.dynamic_values import (
    ComplexDynamicValue,
    DynamicSourceReference,
    DynamicValueType,
    FilterConfig,
    FilterOperator,
    FilterValueSource,
)
from src.utils.exceptions import PlanningError
from src.utils.query_utils import format_query_ref_key
from tests.conftest import make_minimal_pipeline_config, make_rest_chain_resources


def test_execution_plan_disabled_parent_included_when_child_selected(tmp_path) -> None:
    """Explicit selection of child keeps disabled parent in the plan as a dependency."""
    cfg = make_minimal_pipeline_config(
        tmp_path,
        sources=make_rest_chain_resources(
            child_depends_on_parent=True,
            parent_enabled=False,
            child_enabled=True,
        ),
    )
    plan = ExecutionPlan(cfg, redis_context=MagicMock(), selection={"api": {"child"}})

    ids = {f"{m.source_name}.{m.resource_name}" for m in plan.get_execution_order()}
    assert "api.parent" in ids
    assert "api.child" in ids


def test_execution_plan_summarize_includes_estimates(tmp_path) -> None:
    cfg = make_minimal_pipeline_config(
        tmp_path,
        sources=make_rest_chain_resources(child_depends_on_parent=False),
    )
    plan = ExecutionPlan(cfg, redis_context=MagicMock(), selection=None)
    summary = plan.summarize()
    assert summary["total_resources"] >= 2
    assert summary["total_stages"] >= 1
    assert any(s["stage_number"] for s in summary["stages"])


# ---------------------------------------------------------------------------
# Three-resource chain: grandparent → parent → child
# ---------------------------------------------------------------------------


def _three_resource_chain_config(tmp_path):
    """Build a PipelineConfig with a three-stage SOURCE dependency chain."""
    parent_input: RequestInputConfig = RequestInputConfig(
        value=ComplexDynamicValue(
            type=DynamicValueType.SOURCE,
            source_config=DynamicSourceReference(source="grandparent", field="id"),
        ),
        location="query",
        batch_size=1,
    )
    child_input: RequestInputConfig = RequestInputConfig(
        value=ComplexDynamicValue(
            type=DynamicValueType.SOURCE,
            source_config=DynamicSourceReference(source="parent", field="ref"),
        ),
        location="query",
        batch_size=1,
    )
    return make_minimal_pipeline_config(
        tmp_path,
        sources={
            "api": SourceConfig(
                type=SourceType.REST_API,
                base_url="https://example.com",
                enabled=True,
                resources={
                    "grandparent": ResourceConfig(
                        enabled=True, method="GET", path="/gp", response_type="json"
                    ),
                    "parent": ResourceConfig(
                        enabled=True,
                        method="GET",
                        path="/parent",
                        response_type="json",
                        request_inputs={"gp_id": parent_input},
                    ),
                    "child": ResourceConfig(
                        enabled=True,
                        method="GET",
                        path="/child",
                        response_type="json",
                        request_inputs={"parent_ref": child_input},
                    ),
                },
            )
        },
    )


@pytest.mark.parametrize("chain_kind", ["two_resource", "three_resource"])
def test_execution_plan_orders_dependency_chain_by_stage(tmp_path, chain_kind: str) -> None:
    """Parent/grandparent runs before dependents; stages strictly increase along SOURCE edges."""
    if chain_kind == "two_resource":
        cfg = make_minimal_pipeline_config(
            tmp_path,
            sources=make_rest_chain_resources(child_depends_on_parent=True),
        )
        plan = ExecutionPlan(cfg, redis_context=MagicMock(), selection=None)
        order = plan.get_execution_order()
        names = [(m.source_name, m.resource_name) for m in order]
        assert ("api", "parent") in names
        assert ("api", "child") in names
        idx_parent = names.index(("api", "parent"))
        idx_child = names.index(("api", "child"))
        assert idx_parent < idx_child
        assert plan.get_stage_for_resource("api", "parent") == 1
        assert plan.get_stage_for_resource("api", "child") >= 2
        return

    cfg = _three_resource_chain_config(tmp_path)
    plan = ExecutionPlan(cfg, redis_context=MagicMock(), selection=None)
    gp_stage = plan.get_stage_for_resource("api", "grandparent")
    parent_stage = plan.get_stage_for_resource("api", "parent")
    child_stage = plan.get_stage_for_resource("api", "child")
    assert gp_stage < parent_stage < child_stage


# ---------------------------------------------------------------------------
# get_enabled_sources
# ---------------------------------------------------------------------------


def test_get_enabled_sources_no_selection_returns_enabled(tmp_path) -> None:
    cfg = make_minimal_pipeline_config(
        tmp_path, sources=make_rest_chain_resources(child_depends_on_parent=False)
    )
    plan = ExecutionPlan(cfg, redis_context=MagicMock(), selection=None)
    assert "api" in plan.get_enabled_sources()


def test_get_enabled_sources_with_selection_limits_to_selection(tmp_path) -> None:
    cfg = make_minimal_pipeline_config(
        tmp_path, sources=make_rest_chain_resources(child_depends_on_parent=False)
    )
    plan = ExecutionPlan(cfg, redis_context=MagicMock(), selection={"api": None})
    assert plan.get_enabled_sources() == ["api"]


# ---------------------------------------------------------------------------
# get_sources_from_plan
# ---------------------------------------------------------------------------


def test_get_sources_from_plan_matches_actual_plan(tmp_path) -> None:
    cfg = make_minimal_pipeline_config(
        tmp_path, sources=make_rest_chain_resources(child_depends_on_parent=False)
    )
    plan = ExecutionPlan(cfg, redis_context=MagicMock(), selection=None)
    assert "api" in plan.get_sources_from_plan()


# ---------------------------------------------------------------------------
# get_enabled_resources
# ---------------------------------------------------------------------------


def test_get_enabled_resources_filters_disabled_resources(tmp_path) -> None:
    cfg = make_minimal_pipeline_config(
        tmp_path,
        sources=make_rest_chain_resources(child_depends_on_parent=False, parent_enabled=False),
    )
    plan = ExecutionPlan(cfg, redis_context=MagicMock(), selection=None)
    resources = plan.get_enabled_resources("api")
    assert "child" in resources
    assert "parent" not in resources


def test_get_enabled_resources_unknown_source_returns_empty(tmp_path) -> None:
    cfg = make_minimal_pipeline_config(
        tmp_path, sources=make_rest_chain_resources(child_depends_on_parent=False)
    )
    plan = ExecutionPlan(cfg, redis_context=MagicMock(), selection=None)
    assert plan.get_enabled_resources("nonexistent_source") == []


# ---------------------------------------------------------------------------
# Circular dependency detection
# ---------------------------------------------------------------------------


def test_execution_plan_circular_dependency_raises_planning_error() -> None:
    """_organize_stages must detect and raise on circular dependencies."""
    plan = ExecutionPlan.__new__(ExecutionPlan)
    plan.stages = []
    plan.logger = SimpleNamespace(
        debug=lambda *a, **k: None,
        warning=lambda *a, **k: None,
        trace=lambda *a, **k: None,
        info=lambda *a, **k: None,
    )

    meta_a = ResourceMetadata(
        source_name="s",
        resource_name="a",
        dependencies={"s.b"},
        batch_inputs={},
    )
    meta_b = ResourceMetadata(
        source_name="s",
        resource_name="b",
        dependencies={"s.a"},
        batch_inputs={},
    )
    plan._resource_metadata = {"s.a": meta_a, "s.b": meta_b}
    plan._dependency_graph = {"s.a": {"s.b"}, "s.b": {"s.a"}}

    with pytest.raises(PlanningError, match="Circular dependency"):
        plan._organize_stages()


# ---------------------------------------------------------------------------
# Selection with no matching resources produces empty plan
# ---------------------------------------------------------------------------


def test_execution_plan_empty_selection_produces_no_stages(tmp_path) -> None:
    cfg = make_minimal_pipeline_config(
        tmp_path, sources=make_rest_chain_resources(child_depends_on_parent=False)
    )
    plan = ExecutionPlan(cfg, redis_context=MagicMock(), selection={"api": {"nonexistent"}})
    assert plan.stages == []
    assert plan.summarize()["total_resources"] == 0


# ---------------------------------------------------------------------------
# Plan helper methods: has_parent_inputs, has_batch_inputs, get_input_filter,
# get_dependent_loading_configs, get_resource_inputs, get_resource_config
# ---------------------------------------------------------------------------


def test_has_parent_inputs_and_has_batch_inputs(tmp_path) -> None:
    """child has SOURCE dependency (parent input) and batch_size set."""
    cfg = make_minimal_pipeline_config(
        tmp_path, sources=make_rest_chain_resources(child_depends_on_parent=True)
    )
    plan = ExecutionPlan(cfg, redis_context=MagicMock(), selection=None)

    assert plan.has_parent_inputs("api", "child") is True
    assert plan.has_parent_inputs("api", "parent") is False
    assert plan.has_batch_inputs("api", "child") is True
    assert plan.has_batch_inputs("api", "parent") is False
    assert plan.has_parent_inputs("api", "nonexistent") is False
    assert plan.has_batch_inputs("api", "nonexistent") is False


def test_get_input_filter_returns_none_when_no_filter(tmp_path) -> None:
    cfg = make_minimal_pipeline_config(
        tmp_path, sources=make_rest_chain_resources(child_depends_on_parent=True)
    )
    plan = ExecutionPlan(cfg, redis_context=MagicMock(), selection=None)
    result = plan.get_input_filter("api", "child", "post_id")
    assert result is None


def test_get_resource_config_and_inputs(tmp_path) -> None:
    cfg = make_minimal_pipeline_config(
        tmp_path, sources=make_rest_chain_resources(child_depends_on_parent=True)
    )
    plan = ExecutionPlan(cfg, redis_context=MagicMock(), selection=None)

    res_cfg = plan.get_resource_config("api", "parent")
    assert res_cfg is not None
    assert res_cfg.method == "GET"

    assert plan.get_resource_config("api", "nonexistent") is None
    assert plan.get_resource_config("nonexistent_source", "r") is None

    _regular, batch = plan.get_resource_inputs("api", "child")
    assert "post_id" in batch
    regular2, batch2 = plan.get_resource_inputs("api", "nonexistent")
    assert regular2 == {}
    assert batch2 == {}


def test_get_dependent_loading_configs_with_child_loading(tmp_path) -> None:
    """Child with loading configured should appear in parent's dependent configs."""
    cfg = make_minimal_pipeline_config(
        tmp_path,
        sources={
            "api": SourceConfig(
                type=SourceType.REST_API,
                base_url="https://example.com",
                enabled=True,
                resources={
                    "parent": ResourceConfig(
                        enabled=True, method="GET", path="/parent", response_type="json"
                    ),
                    "child": ResourceConfig(
                        enabled=True,
                        method="GET",
                        path="/child",
                        response_type="json",
                        loading={
                            "destination": "local",
                            "storage_root": "/tmp",
                            "prefix": "api/child",
                        },
                        request_inputs={
                            "pid": RequestInputConfig(
                                value=ComplexDynamicValue(
                                    type=DynamicValueType.SOURCE,
                                    source_config=DynamicSourceReference(
                                        source="parent", field="id"
                                    ),
                                ),
                                location="query",
                                batch_size=1,
                            )
                        },
                    ),
                },
            )
        },
    )
    plan = ExecutionPlan(cfg, redis_context=MagicMock(), selection=None)
    configs = plan.get_dependent_loading_configs("api", "parent")
    assert len(configs) == 1
    loading_cfg, _source_type = configs[0]
    assert loading_cfg.destination == "local"


# ---------------------------------------------------------------------------
# ResourceMetadata.estimate_request_count branches
# ---------------------------------------------------------------------------


def test_resource_metadata_estimate_count_no_batch_inputs_returns_one() -> None:
    meta = ResourceMetadata(source_name="s", resource_name="r", dependencies=set(), batch_inputs={})
    assert meta.estimate_request_count() == 1


def test_resource_metadata_estimate_count_no_config_returns_none() -> None:
    meta = ResourceMetadata(
        source_name="s",
        resource_name="r",
        dependencies=set(),
        batch_inputs={"key": 10},
        config=None,
    )
    assert meta.estimate_request_count() is None


def test_resource_metadata_estimate_count_with_dependencies_returns_none(tmp_path) -> None:
    cfg = make_minimal_pipeline_config(
        tmp_path, sources=make_rest_chain_resources(child_depends_on_parent=True)
    )
    plan = ExecutionPlan(cfg, redis_context=MagicMock(), selection=None)
    child_meta = plan.get_resource_metadata("api", "child")
    assert child_meta is not None
    assert child_meta.estimate_request_count() is None


# ---------------------------------------------------------------------------
# get_dependent_resources
# ---------------------------------------------------------------------------


def test_get_dependent_resources_returns_child_metadata(tmp_path) -> None:
    cfg = make_minimal_pipeline_config(
        tmp_path, sources=make_rest_chain_resources(child_depends_on_parent=True)
    )
    plan = ExecutionPlan(cfg, redis_context=MagicMock(), selection=None)
    dependents = plan.get_dependent_resources("api", "parent")
    dep_names = {m.resource_name for m in dependents}
    assert "child" in dep_names


def test_get_dependent_resources_no_dependents_returns_empty(tmp_path) -> None:
    cfg = make_minimal_pipeline_config(
        tmp_path, sources=make_rest_chain_resources(child_depends_on_parent=False)
    )
    plan = ExecutionPlan(cfg, redis_context=MagicMock(), selection=None)
    assert plan.get_dependent_resources("api", "parent") == []


# ---------------------------------------------------------------------------
# summarize — unknown-estimate branches
# ---------------------------------------------------------------------------


def test_summarize_with_dynamic_dependency_reports_unknown_estimate(tmp_path) -> None:
    cfg = make_minimal_pipeline_config(
        tmp_path, sources=make_rest_chain_resources(child_depends_on_parent=True)
    )
    plan = ExecutionPlan(cfg, redis_context=MagicMock(), selection=None)
    summary = plan.summarize()
    estimates = [r["estimated_requests"] for s in summary["stages"] for r in s["resources"]]
    assert "unknown" in estimates


# ---------------------------------------------------------------------------
# get_resource_metadata — returns None for missing resource
# ---------------------------------------------------------------------------


def test_get_resource_metadata_unknown_returns_none(tmp_path) -> None:
    cfg = make_minimal_pipeline_config(
        tmp_path, sources=make_rest_chain_resources(child_depends_on_parent=False)
    )
    plan = ExecutionPlan(cfg, redis_context=MagicMock(), selection=None)
    assert plan.get_resource_metadata("api", "nonexistent") is None


# ---------------------------------------------------------------------------
# ResourceMetadata.estimate_request_count — batch input key not in config
# and scalar (non-list) input value
# ---------------------------------------------------------------------------


def test_resource_metadata_estimate_count_key_missing_from_request_inputs(tmp_path) -> None:
    """batch_inputs key not present in config.request_inputs → returns None."""
    cfg = make_minimal_pipeline_config(
        tmp_path, sources=make_rest_chain_resources(child_depends_on_parent=False)
    )
    resource_cfg = cfg.sources["api"].resources["parent"]
    meta = ResourceMetadata(
        source_name="api",
        resource_name="parent",
        dependencies=set(),
        batch_inputs={"nonexistent_key": 1},
        config=resource_cfg,
    )
    assert meta.estimate_request_count() is None


@pytest.mark.parametrize(
    "input_value,expected_count",
    [
        pytest.param([1, 2, 3], 3, id="static_list"),
        pytest.param("static_scalar", 1, id="scalar_non_list"),
    ],
)
def test_resource_metadata_estimate_request_count_static_inputs(
    tmp_path, input_value, expected_count
) -> None:
    """List vs scalar static inputs resolve to deterministic request estimates."""
    cfg = make_minimal_pipeline_config(
        tmp_path,
        sources={
            "api": SourceConfig(
                type=SourceType.REST_API,
                base_url="https://example.com",
                enabled=True,
                resources={
                    "r": ResourceConfig(
                        enabled=True,
                        method="GET",
                        path="/r",
                        response_type="json",
                        request_inputs={
                            "q": RequestInputConfig(value=input_value, batch_size=1),
                        },
                    )
                },
            )
        },
    )
    plan = ExecutionPlan(cfg, redis_context=MagicMock(), selection=None)
    meta = plan.get_resource_metadata("api", "r")
    assert meta is not None
    assert meta.estimate_request_count() == expected_count


# ---------------------------------------------------------------------------
# Disabled parent with child dependency (no selection) — covers
# _get_enabled_dependents loop body and _should_include_resource warning path
# ---------------------------------------------------------------------------


def test_disabled_parent_with_child_dependency_included_without_selection(tmp_path) -> None:
    """Disabled parent is pulled into plan when enabled child depends on it."""
    cfg = make_minimal_pipeline_config(
        tmp_path,
        sources=make_rest_chain_resources(
            child_depends_on_parent=True,
            parent_enabled=False,
            child_enabled=True,
        ),
    )
    plan = ExecutionPlan(cfg, redis_context=MagicMock(), selection=None)
    order = plan.get_execution_order()
    names = {(m.source_name, m.resource_name) for m in order}
    assert ("api", "parent") in names
    assert ("api", "child") in names


# ---------------------------------------------------------------------------
# _get_enabled_dependents — called directly after plan build (when
# _source_configs is populated) to cover the loop body lines 175-187
# ---------------------------------------------------------------------------


def test_get_enabled_dependents_loop_body_covered(tmp_path) -> None:
    """After plan build, _get_enabled_dependents finds child as dependent of parent."""
    cfg = make_minimal_pipeline_config(
        tmp_path, sources=make_rest_chain_resources(child_depends_on_parent=True)
    )
    plan = ExecutionPlan(cfg, redis_context=MagicMock(), selection=None)
    result = plan._get_enabled_dependents("api", "parent")
    assert ("api", "child") in result


def test_get_enabled_dependents_with_selection_marks_selected(tmp_path) -> None:
    """With selection that includes the dependent, is_selected becomes True."""
    cfg = make_minimal_pipeline_config(
        tmp_path, sources=make_rest_chain_resources(child_depends_on_parent=True)
    )
    plan = ExecutionPlan(cfg, redis_context=MagicMock(), selection={"api": {"child"}})
    result = plan._get_enabled_dependents("api", "parent")
    assert ("api", "child") in result


def test_get_enabled_dependents_with_selection_all_endpoints(tmp_path) -> None:
    """With selection source=None (all endpoints), is_selected=True for any dependent."""
    cfg = make_minimal_pipeline_config(
        tmp_path, sources=make_rest_chain_resources(child_depends_on_parent=True)
    )
    plan = ExecutionPlan(cfg, redis_context=MagicMock(), selection={"api": None})
    result = plan._get_enabled_dependents("api", "parent")
    assert ("api", "child") in result


# ---------------------------------------------------------------------------
# _should_include_resource — selection excludes source (line 208)
# and disabled resource with dependents (lines 223-233)
# ---------------------------------------------------------------------------


def test_should_include_resource_source_not_in_selection_returns_false(tmp_path) -> None:
    """When selection doesn't include the source, the resource is excluded."""
    cfg = make_minimal_pipeline_config(
        tmp_path, sources=make_rest_chain_resources(child_depends_on_parent=False)
    )
    plan = ExecutionPlan(cfg, redis_context=MagicMock(), selection={"other": None})
    parent_cfg = cfg.sources["api"].resources["parent"]
    assert plan._should_include_resource("parent", parent_cfg, "api") is False


def test_should_include_resource_disabled_with_dependents_includes_and_warns(tmp_path) -> None:
    """A disabled resource that has enabled dependents is included."""
    cfg = make_minimal_pipeline_config(
        tmp_path,
        sources=make_rest_chain_resources(child_depends_on_parent=True, parent_enabled=False),
    )
    plan = ExecutionPlan(cfg, redis_context=MagicMock(), selection=None)
    parent_cfg = cfg.sources["api"].resources["parent"]
    result = plan._should_include_resource("parent", parent_cfg, "api")
    assert result is True


# ---------------------------------------------------------------------------
# _build_plan warning path — selection targets a nonexistent source (line 253)
# ---------------------------------------------------------------------------


def test_build_plan_selection_nonexistent_source_produces_empty_plan(tmp_path) -> None:
    """Selecting a source not in config results in empty plan and warning."""
    cfg = make_minimal_pipeline_config(
        tmp_path, sources=make_rest_chain_resources(child_depends_on_parent=False)
    )
    plan = ExecutionPlan(cfg, redis_context=MagicMock(), selection={"nonexistent": None})
    assert plan.stages == []
    assert plan.summarize()["total_resources"] == 0


# ---------------------------------------------------------------------------
# get_input_filter — returns filter when filter_relationships is set (line 981)
# ---------------------------------------------------------------------------


def test_build_plan_selection_explicitly_selected_disabled_included(tmp_path) -> None:
    """Explicitly selected disabled resources are still included in the plan."""
    cfg = make_minimal_pipeline_config(
        tmp_path,
        sources=make_rest_chain_resources(child_depends_on_parent=False, parent_enabled=False),
    )
    plan = ExecutionPlan(cfg, redis_context=MagicMock(), selection={"api": {"parent"}})
    order = {m.resource_name for m in plan.get_execution_order()}
    assert "parent" in order


def test_build_plan_empty_resource_set_selection_produces_no_stages(tmp_path) -> None:
    """Empty set selection produces no stages and triggers else-branch (line 297)."""
    cfg = make_minimal_pipeline_config(
        tmp_path, sources=make_rest_chain_resources(child_depends_on_parent=False)
    )
    plan = ExecutionPlan(cfg, redis_context=MagicMock(), selection={"api": set()})
    assert plan.stages == []


def test_get_resource_inputs_non_batch_goes_to_regular(tmp_path) -> None:
    """Inputs without batch_size go into regular_inputs (line 633)."""
    from src.config.config_models import RequestInputConfig

    cfg = make_minimal_pipeline_config(
        tmp_path,
        sources={
            "api": SourceConfig(
                type=SourceType.REST_API,
                base_url="https://example.com",
                enabled=True,
                resources={
                    "r": ResourceConfig(
                        enabled=True,
                        method="GET",
                        path="/r",
                        response_type="json",
                        request_inputs={"q": RequestInputConfig(value="static", location="query")},
                    )
                },
            )
        },
    )
    plan = ExecutionPlan(cfg, redis_context=MagicMock(), selection=None)
    regular, batch = plan.get_resource_inputs("api", "r")
    assert "q" in regular
    assert batch == {}


def test_is_snapshot_trigger_returns_false_without_snapshot_config(tmp_path) -> None:
    """_is_snapshot_trigger returns False when no snapshot triggers configured (lines 639-640)."""
    cfg = make_minimal_pipeline_config(
        tmp_path, sources=make_rest_chain_resources(child_depends_on_parent=False)
    )
    plan = ExecutionPlan(cfg, redis_context=MagicMock(), selection=None)
    meta = plan.get_resource_metadata("api", "parent")
    assert meta is not None
    assert plan._is_snapshot_trigger(meta) is False
    assert plan._is_snapshot_dependent(meta) is False


def test_build_dependency_graph_header_source_dependency_tracked(tmp_path) -> None:
    """ResourceConfig with a SOURCE ComplexDynamicValue in headers adds header dep (lines 455-464)."""
    header_value = ComplexDynamicValue(
        type=DynamicValueType.SOURCE,
        source_config=DynamicSourceReference(source="parent", field="token"),
    )
    cfg = make_minimal_pipeline_config(
        tmp_path,
        sources={
            "api": SourceConfig(
                type=SourceType.REST_API,
                base_url="https://example.com",
                enabled=True,
                resources={
                    "parent": ResourceConfig(
                        enabled=True, method="GET", path="/parent", response_type="json"
                    ),
                    "child": ResourceConfig(
                        enabled=True,
                        method="GET",
                        path="/child",
                        response_type="json",
                        headers={"X-Token": header_value},
                    ),
                },
            )
        },
    )
    plan = ExecutionPlan(cfg, redis_context=MagicMock(), selection=None)
    order = plan.get_execution_order()
    names = [(m.source_name, m.resource_name) for m in order]
    assert ("api", "parent") in names
    assert ("api", "child") in names
    idx_parent = names.index(("api", "parent"))
    idx_child = names.index(("api", "child"))
    assert idx_parent < idx_child


def test_get_input_filter_returns_filter_when_set(tmp_path) -> None:
    from src.utils.dynamic_values import FilterConfig

    filter_cfg = FilterConfig(field="status", value_source="active")
    child_input = RequestInputConfig(
        value=ComplexDynamicValue(
            type=DynamicValueType.SOURCE,
            source_config=DynamicSourceReference(source="parent", field="id", filter=filter_cfg),
        ),
        location="query",
        batch_size=1,
    )
    cfg = make_minimal_pipeline_config(
        tmp_path,
        sources={
            "api": SourceConfig(
                type=SourceType.REST_API,
                base_url="https://example.com",
                enabled=True,
                resources={
                    "parent": ResourceConfig(
                        enabled=True, method="GET", path="/parent", response_type="json"
                    ),
                    "child": ResourceConfig(
                        enabled=True,
                        method="GET",
                        path="/child",
                        response_type="json",
                        request_inputs={"pid": child_input},
                    ),
                },
            )
        },
    )
    plan = ExecutionPlan(cfg, redis_context=MagicMock(), selection=None)
    result = plan.get_input_filter("api", "child", "pid")
    assert result is not None
    assert result.field == "status"


def _write_query_file(tmp_path, name: str, sql: str) -> None:
    qdir = tmp_path / "queries"
    qdir.mkdir(parents=True, exist_ok=True)
    (qdir / f"{name}.sql").write_text(sql, encoding="utf-8")


def _rest_resource_with_databricks_input(query_token: str) -> dict:
    """REST resource whose query input resolves to a Databricks query ref via string marker."""
    return {
        "api": SourceConfig(
            type=SourceType.REST_API,
            base_url="https://example.com",
            enabled=True,
            resources={
                "r": ResourceConfig(
                    enabled=True,
                    method="GET",
                    path="/data",
                    response_type="json",
                    request_inputs={
                        "ids": RequestInputConfig(
                            value=f"databricks('{query_token}')",
                            location="query",
                        )
                    },
                )
            },
        )
    }


def test_execution_plan_databricks_query_resolves_and_stores_redis(tmp_path, monkeypatch) -> None:
    """Plans with databricks('…') inputs load SQL, resolve via DatabricksUtils, and store in Redis."""
    _write_query_file(tmp_path, "q1", "SELECT 1 AS id")
    cfg = make_minimal_pipeline_config(
        tmp_path,
        queries=[QueriesConfig(name="q1", file="q1.sql")],
        sources=_rest_resource_with_databricks_input("q1"),
    )
    mock_du_cls = MagicMock()
    mock_du_cls.return_value.resolve_databricks_query.return_value = [{"id": 1}]
    monkeypatch.setattr("src.planner.execution_plan.DatabricksUtils", mock_du_cls)

    redis = MagicMock()
    ExecutionPlan(cfg, redis_context=redis, selection=None)

    mock_du_cls.assert_called()
    redis.store.assert_called()
    kwargs = redis.store.call_args.kwargs
    assert kwargs["key"] == format_query_ref_key("q1")
    assert kwargs["data"] == [{"id": 1}]
    assert kwargs.get("ttl") == 3600


def test_execution_plan_databricks_missing_query_ref_raises(tmp_path) -> None:
    cfg = make_minimal_pipeline_config(
        tmp_path,
        queries=[],
        sources=_rest_resource_with_databricks_input("unknown_query"),
    )
    with pytest.raises(PlanningError, match="Missing dependency query reference"):
        ExecutionPlan(cfg, redis_context=MagicMock(), selection=None)


def test_execution_plan_databricks_invalid_sql_raises(tmp_path) -> None:
    _write_query_file(tmp_path, "q1", "DELETE FROM some_table")
    cfg = make_minimal_pipeline_config(
        tmp_path,
        queries=[QueriesConfig(name="q1", file="q1.sql")],
        sources=_rest_resource_with_databricks_input("q1"),
    )
    with pytest.raises(PlanningError, match="Invalid query content loaded"):
        ExecutionPlan(cfg, redis_context=MagicMock(), selection=None)


def _rest_resource_with_databricks_source(query_token: str) -> dict:
    """REST resource whose SOURCE input looks up a databricks table (no parent resource)."""
    return {
        "api": SourceConfig(
            type=SourceType.REST_API,
            base_url="https://example.com",
            enabled=True,
            resources={
                "r": ResourceConfig(
                    enabled=True,
                    method="GET",
                    path="/data",
                    response_type="json",
                    request_inputs={
                        "store": RequestInputConfig(value="a", location="query", batch_size=1),
                        "gtin": RequestInputConfig(
                            value=ComplexDynamicValue(
                                type=DynamicValueType.SOURCE,
                                source_config=DynamicSourceReference(
                                    source=f"databricks:{query_token}",
                                    field="gtin",
                                    filter=FilterConfig(
                                        field="store",
                                        operator=FilterOperator.EQUALS,
                                        value_source=FilterValueSource(input="store"),
                                        value_type="parameter",
                                    ),
                                ),
                            ),
                            location="query",
                        ),
                    },
                )
            },
        )
    }


def test_execution_plan_databricks_source_resolves_without_dependency(
    tmp_path, monkeypatch
) -> None:
    """A databricks SOURCE lookup runs the query at plan build and adds no resource dependency."""
    _write_query_file(tmp_path, "q1", "SELECT store, gtin FROM t")
    cfg = make_minimal_pipeline_config(
        tmp_path,
        queries=[QueriesConfig(name="q1", file="q1.sql")],
        sources=_rest_resource_with_databricks_source("q1"),
    )
    mock_du_cls = MagicMock()
    mock_du_cls.return_value.resolve_databricks_query.return_value = [{"store": "a", "gtin": 1}]
    monkeypatch.setattr("src.planner.execution_plan.DatabricksUtils", mock_du_cls)

    redis = MagicMock()
    plan = ExecutionPlan(cfg, redis_context=redis, selection=None)

    mock_du_cls.assert_called()
    assert redis.store.call_args.kwargs["key"] == format_query_ref_key("q1")
    assert plan._resource_metadata["api.r"].dependencies == set()
