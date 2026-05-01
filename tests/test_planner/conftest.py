"""Planner-test fixtures.

Wraps the cross-suite ``make_minimal_pipeline_config`` /
``make_rest_chain_resources`` factories from ``tests.conftest`` as fixtures so
planner tests can take a callable rather than importing module functions.
"""

from __future__ import annotations

from pathlib import Path
from typing import Callable, Dict, Optional

import pytest

from src.config.config_models import PipelineConfig, SourceConfig
from tests.conftest import make_minimal_pipeline_config, make_rest_chain_resources


@pytest.fixture
def make_pipeline_config(tmp_path: Path) -> Callable[..., PipelineConfig]:
    """Return a builder bound to the test's ``tmp_path``.

    Usage::

        cfg = make_pipeline_config(sources=make_rest_chain(child_depends_on_parent=True))
    """

    def _factory(
        *,
        sources: Dict[str, SourceConfig],
        queries: Optional[list] = None,
    ) -> PipelineConfig:
        return make_minimal_pipeline_config(tmp_path, sources=sources, queries=queries)

    return _factory


@pytest.fixture
def make_rest_chain() -> Callable[..., Dict[str, SourceConfig]]:
    """Expose ``make_rest_chain_resources`` as a fixture for parameter clarity."""
    return make_rest_chain_resources
