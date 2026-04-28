"""Tests for AWSCredentialManager behavior."""

from types import SimpleNamespace

import pytest

from src.utils.aws_credentials import AWSCredentialManager
from src.utils.exceptions import AWSError


def _reset_singleton() -> None:
    AWSCredentialManager._instance = None


def test_load_credentials_success(monkeypatch: pytest.MonkeyPatch) -> None:
    _reset_singleton()
    frozen = SimpleNamespace(access_key="AK", secret_key="SK", token="TK")
    creds = SimpleNamespace(get_frozen_credentials=lambda: frozen)
    session = SimpleNamespace(get_credentials=lambda: creds, region_name="ap-southeast-2")
    monkeypatch.setattr("src.utils.aws_credentials.boto3.Session", lambda: session)
    monkeypatch.delenv("ECS_CONTAINER_METADATA_URI", raising=False)

    mgr = AWSCredentialManager()
    out = mgr.get_credentials()
    assert out["aws_access_key"] == "AK"
    assert out["aws_region"] == "ap-southeast-2"
    assert mgr.region == "ap-southeast-2"


def test_no_credentials_and_profile_error_are_wrapped(monkeypatch: pytest.MonkeyPatch) -> None:
    _reset_singleton()
    session = SimpleNamespace(get_credentials=lambda: None, region_name=None)
    monkeypatch.setattr("src.utils.aws_credentials.boto3.Session", lambda: session)
    with pytest.raises(AWSError, match="No AWS credentials found"):
        AWSCredentialManager()

    _reset_singleton()

    def _boom():
        raise RuntimeError("config profile foo could not be found")

    monkeypatch.setattr("src.utils.aws_credentials.boto3.Session", _boom)
    with pytest.raises(AWSError, match="Hint: AWS_PROFILE is set"):
        AWSCredentialManager()
