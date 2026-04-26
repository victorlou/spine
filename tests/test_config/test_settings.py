"""Tests for settings behavior."""

from src.config.settings import AWSSettings
from src.utils.exceptions import AWSError


def test_aws_settings_uses_default_region_when_credentials_unavailable(monkeypatch) -> None:
    def _raise_aws_error():
        raise AWSError(message="mock aws credentials failure", operation="_load_credentials")

    monkeypatch.setattr("src.config.settings.AWSCredentialManager", _raise_aws_error)

    aws = AWSSettings()
    assert aws.REGION == "us-east-1"
