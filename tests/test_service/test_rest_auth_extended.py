"""Additional REST auth branches: OAuth JWT exchange and bearer refresh."""

import base64
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.service.rest_service import RestService
from src.utils.exceptions import ServiceError


def _bare_rest_service(**auth_fields) -> RestService:
    svc = RestService.__new__(RestService)
    auth = SimpleNamespace(**auth_fields)
    svc.settings = SimpleNamespace(api=SimpleNamespace(TIMEOUT=5))
    svc.config = SimpleNamespace(
        base_url="https://api.example.com", auth=auth, headers={}, resources={}
    )
    svc.redis_context = object()
    svc.logger = MagicMock()
    svc._session = MagicMock()
    svc._auth_token = None
    svc._token_expiry = None
    return svc


def test_oauth_jwt_missing_jwt_config_raises() -> None:
    pem_b64 = base64.b64encode(b"-----BEGIN X-----\ndata\n-----END X-----").decode("ascii")
    svc = _bare_rest_service(
        type="oauth_jwt",
        private_key=pem_b64,
        token_url="https://id/t",
        jwt_config=None,
        header_name="Authorization",
        header_format="Bearer {token}",
    )
    with pytest.raises(ServiceError, match="jwt_config is required"):
        svc._get_auth_token()


def test_oauth_jwt_token_post_fails_wrapped(monkeypatch: pytest.MonkeyPatch) -> None:
    pem_b64 = base64.b64encode(b"-----BEGIN X-----\nk\n-----END X-----").decode("ascii")
    svc = _bare_rest_service(
        type="oauth_jwt",
        private_key=pem_b64,
        token_url="https://id/t",
        jwt_config=SimpleNamespace(provider="dummy", algorithm="HS256"),
        header_name="A",
        header_format="{token}",
    )
    fake_provider = SimpleNamespace(
        build_jwt_payload=lambda *a: {},
        build_jwt_headers=lambda *a: {},
        build_token_exchange_data=lambda *a: "x=1",
        build_request_headers=lambda *a: {},
    )
    monkeypatch.setattr("src.service.rest_service.get_provider", lambda _p: fake_provider)
    monkeypatch.setattr("src.service.rest_service.jwt.encode", lambda **k: "jwt")
    post_resp = MagicMock()
    post_resp.ok = False
    post_resp.text = "err"
    post_resp.raise_for_status.side_effect = RuntimeError("http")
    svc.session.post.return_value = post_resp
    with pytest.raises(ServiceError, match="Authentication failed"):
        svc._get_auth_token()


def test_oauth_jwt_happy_path(monkeypatch: pytest.MonkeyPatch) -> None:
    pem_b64 = base64.b64encode(b"-----BEGIN X-----\nk\n-----END X-----").decode("ascii")
    svc = _bare_rest_service(
        type="oauth_jwt",
        private_key=pem_b64,
        token_url="https://id/t",
        jwt_config=SimpleNamespace(provider="dummy", algorithm="HS256"),
        header_name="A",
        header_format="{token}",
    )
    fake_provider = SimpleNamespace(
        build_jwt_payload=lambda *a: {},
        build_jwt_headers=lambda *a: {},
        build_token_exchange_data=lambda *a: "x=1",
        build_request_headers=lambda *a: {},
    )
    monkeypatch.setattr("src.service.rest_service.get_provider", lambda _p: fake_provider)
    monkeypatch.setattr("src.service.rest_service.jwt.encode", lambda **k: "jwt")
    post_resp = MagicMock()
    post_resp.ok = True
    post_resp.raise_for_status = MagicMock()
    post_resp.json.return_value = {"access_token": "tok", "expires_in": 120}
    svc.session.post.return_value = post_resp
    assert svc._get_auth_token() == "tok"


def test_bearer_refresh_updates_token(monkeypatch: pytest.MonkeyPatch) -> None:
    svc = _bare_rest_service(
        type="bearer_token",
        token_url="https://id/t",
        client_id="c",
        client_secret="s",
        refresh_token="r",
        bearer_token=None,
        header_name="H",
        header_format="{token}",
    )
    post_resp = MagicMock()
    post_resp.raise_for_status = MagicMock()
    post_resp.json.return_value = {"access_token": "refreshed", "expires_in": 9000}
    svc.session.post.return_value = post_resp
    assert svc._get_auth_token() == "refreshed"
