"""Integration-style tests for ``ExecutionPlan`` using validated ``PipelineConfig``."""

from unittest.mock import MagicMock

from src.planner.execution_plan import ExecutionPlan
from tests.conftest import make_minimal_pipeline_config, make_rest_chain_resources


def test_execution_plan_orders_dependent_resource_after_parent(tmp_path) -> None:
    """Child that reads from parent via SOURCE dependency runs in a later stage."""
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
