"""Tests for config model helpers."""

from pathlib import Path

from src.config.config_models import DefaultsConfig, PipelineConfig, ResourceConfig, SourceConfig


def test_pipeline_config_get_effective_loading_destinations_respects_enabled_flags() -> None:
    cfg = PipelineConfig(
        config_root=Path("."),
        version="1.0",
        defaults=DefaultsConfig(),
        queries=[],
        sources={
            "enabled_source": SourceConfig(
                enabled=True,
                type="rest_api",
                base_url="https://example.com",
                resources={
                    "posts": ResourceConfig(
                        enabled=True,
                        path="/posts",
                        method="GET",
                        loading={"destination": "gcs", "gcs_bucket": "bucket-a", "prefix": "a/b"},
                    ),
                    "skip_resource": ResourceConfig(
                        enabled=False,
                        path="/skip",
                        method="GET",
                    ),
                },
            ),
            "disabled_source": SourceConfig(
                enabled=False,
                type="rest_api",
                base_url="https://example.com",
                resources={
                    "posts": ResourceConfig(
                        enabled=True,
                        path="/posts",
                        method="GET",
                    )
                },
            ),
        },
    )

    assert cfg.get_effective_loading_destinations() == {"gcs"}
