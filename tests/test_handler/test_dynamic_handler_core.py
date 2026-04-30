"""Unit tests for small, deterministic ``DynamicHandler`` orchestration helpers."""

from types import SimpleNamespace
from unittest.mock import MagicMock

from src.config.config_models import LoadingConfig, ResourceConfig
from src.handler.dynamic_handler import DynamicHandler
from src.utils.logger import get_logger


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
