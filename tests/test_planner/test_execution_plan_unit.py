"""Fast unit tests for ExecutionPlan helper methods and summaries."""

from types import SimpleNamespace

from src.planner.execution_plan import ExecutionPlan, ExecutionStage, ResourceMetadata


def _plan() -> ExecutionPlan:
    plan = ExecutionPlan.__new__(ExecutionPlan)
    plan.selection = None
    plan._reverse_graph = {}
    plan._resource_metadata = {}
    plan._source_configs = {}
    plan.stages = []
    plan.logger = SimpleNamespace(warning=lambda *args, **kwargs: None)
    return plan


def test_resource_metadata_estimate_request_count() -> None:
    config = SimpleNamespace(request_inputs={"ids": SimpleNamespace(value=[1, 2, 3])})
    meta = ResourceMetadata(
        source_name="s",
        resource_name="r",
        dependencies=set(),
        batch_inputs={"ids": 2},
        config=config,
    )
    assert meta.estimate_request_count() == 2

    meta.dependencies = {"s.other"}
    assert meta.estimate_request_count() is None


def test_execution_plan_selection_helpers() -> None:
    plan = _plan()
    source_cfg = SimpleNamespace(enabled=True, resources={"a": SimpleNamespace(enabled=True)})

    assert plan._should_include_source("s", source_cfg) is True
    assert plan._should_include_resource("a", source_cfg.resources["a"], "s") is True

    plan.selection = {"s": {"a"}}
    assert plan._should_include_source("s", source_cfg) is True
    assert plan._should_include_resource("a", source_cfg.resources["a"], "s") is True
    assert plan._should_include_resource("b", SimpleNamespace(enabled=True), "s") is False


def test_execution_plan_summarize_and_lookup_helpers() -> None:
    plan = _plan()
    meta = ResourceMetadata(
        source_name="s",
        resource_name="a",
        dependencies=set(),
        batch_inputs={},
        config=SimpleNamespace(request_inputs={}),
    )
    plan._resource_metadata = {"s.a": meta}
    plan._reverse_graph = {"s.a": set()}
    plan.stages = [ExecutionStage(resources=[meta], stage_number=1)]
    plan._source_configs = {
        "s": SimpleNamespace(resources={"a": SimpleNamespace(request_inputs={})})
    }

    summary = plan.summarize()
    assert summary["total_stages"] == 1
    assert summary["total_resources"] == 1
    assert plan.get_execution_order() == [meta]
    assert plan.get_stage_for_resource("s", "a") == 1
    assert plan.get_stage_for_resource("s", "missing") is None
    assert plan.get_resource_config("s", "a") is not None
    assert plan.get_resource_inputs("s", "a") == ({}, {})
