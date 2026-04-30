"""Handler-test fixtures.

Centralizes the ``DynamicHandler.__new__`` shortcut used by
``test_dynamic_handler_core`` and a thin ``ResourceConfig`` factory so handler
unit tests do not repeat the same ``SimpleNamespace`` shape.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, Callable
from unittest.mock import MagicMock

import pytest

from src.config.config_models import ResourceConfig
from src.handler.dynamic_handler import DynamicHandler
from src.utils.logger import get_logger


@pytest.fixture
def bare_handler() -> DynamicHandler:
    """Construct a ``DynamicHandler`` instance bypassing ``__init__``.

    Mirrors the locally-defined ``_bare_handler`` used in handler unit tests
    that exercise small orchestration helpers without the full pipeline graph.
    Tests should override ``execution_plan`` and ``redis_context`` as needed.
    """
    h = DynamicHandler.__new__(DynamicHandler)
    h.logger = get_logger("test_dynamic_handler_core")
    h.config = SimpleNamespace(
        defaults=SimpleNamespace(context=SimpleNamespace(prefix="pipeline:")),
        sources={},
    )
    h.redis_context = MagicMock()
    return h


@pytest.fixture
def make_resource_config() -> Callable[..., ResourceConfig]:
    """Return a ``ResourceConfig`` builder with REST-style defaults.

    Usage::

        rc = make_resource_config(path="/users", request_inputs={...})
    """

    def _factory(**overrides: Any) -> ResourceConfig:
        defaults: dict[str, Any] = {
            "enabled": True,
            "method": "GET",
            "path": "/items",
            "response_type": "json",
        }
        defaults.update(overrides)
        return ResourceConfig(**defaults)

    return _factory
