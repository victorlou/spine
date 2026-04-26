"""Tests for settings behavior."""

from types import SimpleNamespace

from src.config.settings import Settings


def test_settings_model_has_no_embedded_aws_field() -> None:
    """AWS for Spark is resolved in SparkManager / AWSCredentialManager, not on Settings."""
    assert "aws" not in Settings.model_fields


def test_settings_loading_destinations_property_uses_pipeline_config() -> None:
    settings_like = SimpleNamespace(
        _pipeline_config=SimpleNamespace(get_effective_loading_destinations=lambda: {"s3", "gcs"})
    )
    assert Settings.loading_destinations.fget(settings_like) == {"s3", "gcs"}
