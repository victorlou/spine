"""Tests for settings behavior."""

from types import SimpleNamespace

from src.config.settings import AWSSettings
from src.utils.exceptions import AWSError


def test_aws_settings_uses_default_region_when_credentials_unavailable(monkeypatch) -> None:
    def _raise_aws_error():
        raise AWSError(message="mock aws credentials failure", operation="_load_credentials")

    monkeypatch.setattr("src.config.settings.AWSCredentialManager", _raise_aws_error)

    aws = AWSSettings()
    assert aws.REGION == "us-east-1"


def test_settings_loading_destinations_property_uses_pipeline_config() -> None:
    settings_like = SimpleNamespace(
        _pipeline_config=SimpleNamespace(get_effective_loading_destinations=lambda: {"s3", "gcs"})
    )
    from src.config.settings import Settings

    assert Settings.loading_destinations.fget(settings_like) == {"s3", "gcs"}
