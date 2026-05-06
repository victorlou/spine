"""Tests for ``PythonSDKService`` with an injectable fake SDK module."""

import sys
import types
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.config.config_models import PythonSDKConfig, ResourceConfig, SourceConfig, SourceType
from src.service.base_service import ServiceError
from src.service.python_sdk_service import PythonSDKService


def _make_settings() -> object:
    return SimpleNamespace()


def test_init_requires_sdk_config() -> None:
    with pytest.raises(ServiceError, match="SDK configuration is required"):
        PythonSDKService(
            settings=_make_settings(),
            source_name="s",
            source_config=SourceConfig.model_construct(
                type=SourceType.PYTHON_SDK,
                base_url="https://example.com",
                resources={},
                sdk=None,
            ),
            redis_context=MagicMock(),
        )


def test_get_base_url_and_headers() -> None:
    cfg = SourceConfig(
        type=SourceType.PYTHON_SDK,
        base_url="https://example.com",
        resources={},
        sdk=PythonSDKConfig(module="os", class_name="Path"),
    )
    svc = PythonSDKService(
        settings=_make_settings(), source_name="my", source_config=cfg, redis_context=MagicMock()
    )
    assert svc.get_base_url() == "python-sdk://my"
    assert svc.get_headers() == {}


def test_get_sdk_client_and_fetch_data() -> None:
    class _Client:
        def __init__(self, **kwargs) -> None:
            self.kwargs = kwargs

        def get_items(self) -> dict:
            return {"data": {"rows": [1, 2]}}

    mod = types.ModuleType("tests_fake_sdk")
    mod._Client = _Client
    sys.modules["tests_fake_sdk"] = mod
    try:
        cfg = SourceConfig(
            type=SourceType.PYTHON_SDK,
            base_url="https://example.com",
            resources={
                "rows": ResourceConfig(
                    method="get_items",
                    path="/",
                )
            },
            sdk=PythonSDKConfig(
                module="tests_fake_sdk",
                class_name="_Client",
            ),
        )
        svc = PythonSDKService(
            settings=_make_settings(), source_name="s", source_config=cfg, redis_context=MagicMock()
        )
        out = svc.fetch_data("rows", full_response=True)
        assert out == {"data": {"rows": [1, 2]}}
    finally:
        del sys.modules["tests_fake_sdk"]


def test_fetch_data_with_response_key() -> None:
    class _C:
        def m(self) -> dict:
            return {"body": {"items": [{"a": 1}]}}

    mod = types.ModuleType("tests_fake_sdk2")
    mod._C = _C
    sys.modules["tests_fake_sdk2"] = mod
    try:
        cfg = SourceConfig(
            type=SourceType.PYTHON_SDK,
            base_url="https://example.com",
            resources={
                "r": ResourceConfig(
                    method="m",
                    path="/",
                    response_key="body.items",
                )
            },
            sdk=PythonSDKConfig(module="tests_fake_sdk2", class_name="_C"),
        )
        svc = PythonSDKService(
            settings=_make_settings(), source_name="s", source_config=cfg, redis_context=MagicMock()
        )
        out = svc.fetch_data("r")
        assert isinstance(out, list)
    finally:
        del sys.modules["tests_fake_sdk2"]


def test_make_request_not_supported() -> None:
    cfg = SourceConfig(
        type=SourceType.PYTHON_SDK,
        base_url="https://example.com",
        resources={"r": ResourceConfig(method="m", path="/")},
        sdk=PythonSDKConfig(module="os", class_name="Path"),
    )
    svc = PythonSDKService(
        settings=_make_settings(), source_name="s", source_config=cfg, redis_context=MagicMock()
    )
    with pytest.raises(ServiceError, match="make_request is not applicable"):
        svc.make_request("GET", "/x")
