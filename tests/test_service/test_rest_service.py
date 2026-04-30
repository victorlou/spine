"""Targeted unit tests for RestService core branches."""

import base64
import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.config.config_models import ResourceConfig
from src.service.base_service import ServiceError
from src.service.rest_service import RestService


def _service(auth=None, headers=None):
    svc = RestService.__new__(RestService)
    svc.settings = SimpleNamespace(api=SimpleNamespace(TIMEOUT=5))
    svc.config = SimpleNamespace(
        base_url="https://api.example.com",
        auth=auth,
        headers=headers or {"X-Test": "1"},
        resources={},
    )
    svc.redis_context = object()
    svc.logger = MagicMock()
    svc._session = MagicMock()
    svc._auth_token = None
    svc._token_expiry = None
    return svc


def test_get_base_url_and_decode_private_key() -> None:
    auth = SimpleNamespace(private_key=base64.b64encode(b"line1\\nline2").decode("utf-8"))
    svc = _service(auth=auth)
    assert svc.get_base_url() == "https://api.example.com"
    assert svc._decode_private_key() == "line1\nline2"


def test_decode_private_key_errors() -> None:
    svc = _service(auth=SimpleNamespace(private_key=None))
    with pytest.raises(ServiceError, match="private_key is required"):
        svc._decode_private_key()

    svc = _service(auth=SimpleNamespace(private_key="//8="))
    with pytest.raises(ServiceError, match="Failed to decode base64 private key"):
        svc._decode_private_key()


def test_get_headers_adds_bearer_auth(monkeypatch: pytest.MonkeyPatch) -> None:
    auth = SimpleNamespace(
        type="bearer_token", header_name="Authorization", header_format="Bearer {token}"
    )
    svc = _service(auth=auth, headers={"X-Test": "1"})
    monkeypatch.setattr(
        "src.service.rest_service.resolve_headers_dict", lambda *_a, **_k: {"X-Test": "1"}
    )
    monkeypatch.setattr(svc, "_get_auth_token", lambda: "abc")

    headers = svc.get_headers()

    assert headers["Authorization"] == "Bearer abc"
    assert headers["X-Test"] == "1"


def test_get_auth_token_cached_and_basic_branch() -> None:
    auth = SimpleNamespace(type="basic", client_id="id", client_secret="secret")
    svc = _service(auth=auth)

    token = svc._get_auth_token()
    assert token == base64.b64encode(b"id:secret").decode()
    assert svc._token_expiry is not None

    svc._auth_token = "cached"
    svc._token_expiry = datetime.now(UTC) + timedelta(minutes=5)
    assert svc._get_auth_token() == "cached"


def test_get_auth_token_requires_auth() -> None:
    svc = _service(auth=None)
    with pytest.raises(ServiceError, match="Authentication configuration is required"):
        svc._get_auth_token()


def test_reset_auth_and_format_request_params() -> None:
    svc = _service(auth=None)
    svc._auth_token = "x"
    svc._token_expiry = datetime.now(UTC)
    svc._reset_auth()
    assert svc._auth_token is None
    assert svc._token_expiry is None

    class _Param:
        def format_request_value(self, value):
            return f"f:{value}"

    resource = SimpleNamespace(request_inputs={"k": _Param()})
    formatted = svc._format_request_params({"k": "v", "plain": 1, "_internal": 2}, resource)
    assert formatted == {"k": "f:v", "plain": 1}


def test_substitute_path_parameters() -> None:
    svc = _service(auth=None)
    path = svc._substitute_path_parameters("/store/{store}/item/{id}", {"store": "10", "id": 7})
    assert path == "/store/10/item/7"
    with pytest.raises(ServiceError, match="Missing path parameter"):
        svc._substitute_path_parameters("/x/{missing}", {"other": 1})


def test_poll_snapshot_success_and_json_error() -> None:
    svc = _service(auth=None)
    svc.config.resources = {
        "status": SimpleNamespace(path="/status", method="GET"),
    }
    response = SimpleNamespace(json=lambda: {"state": "done"})
    svc.make_request = MagicMock(return_value=response)
    assert svc.poll_snapshot("status", {"id": "1"}) == {"state": "done"}

    bad_response = SimpleNamespace(json=MagicMock(side_effect=ValueError("bad")))
    svc.make_request = MagicMock(return_value=bad_response)
    with pytest.raises(ServiceError, match="Invalid JSON in snapshot poll response"):
        svc.poll_snapshot("status", {"id": "1"})


def _http_response(**kwargs) -> MagicMock:
    r = MagicMock()
    r.ok = kwargs.get("ok", True)
    r.status_code = kwargs.get("status_code", 200)
    hdrs = MagicMock()
    hdrs.get.side_effect = lambda k, d=None: kwargs.get("headers", {}).get(k, d)
    r.headers = hdrs
    data = kwargs.get("json_data", {})
    r.json.return_value = data
    raw = json.dumps(data) if not isinstance(data, str) else data
    r.content = raw.encode() if isinstance(raw, str) else b""
    r.text = raw if isinstance(raw, str) else json.dumps(data)
    el = MagicMock()
    el.total_seconds.return_value = 0.01
    r.elapsed = el
    r.raw = SimpleNamespace(version=11)
    return r


def _make_request_service(session: MagicMock) -> RestService:
    svc = RestService.__new__(RestService)
    svc.settings = SimpleNamespace(
        api=SimpleNamespace(TIMEOUT=5, MAX_RETRIES=1, INITIAL_DELAY=0, RETRY_BACKOFF=2)
    )
    svc.source_name = "api"
    svc.config = SimpleNamespace(headers={})
    svc.logger = MagicMock()
    svc.redis_context = MagicMock()
    svc.audit_recorder = None
    svc.get_headers = MagicMock(return_value={"H": "1", "Content-Type": "application/json"})
    svc._format_request_params = lambda params, res: params  # type: ignore[assignment]
    svc._reset_auth = MagicMock()
    object.__setattr__(svc, "_session", session)
    return svc


def test_make_request_get_wraps_object_response(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("src.service.rest_service.time.sleep", lambda s: None)
    session = MagicMock()
    session.get.return_value = _http_response(json_data={"a": 1})
    svc = _make_request_service(session)
    res = ResourceConfig(method="GET", path="/x", request_inputs={})
    out = svc._make_request(res, "https://api.example.com/x", {}, resource_name="r1")
    assert out == [{"a": 1}]


def test_make_request_get_with_response_key_missing_returns_empty() -> None:
    session = MagicMock()
    session.get.return_value = _http_response(json_data={"other": 1})
    svc = _make_request_service(session)
    res = ResourceConfig(method="GET", path="/x", request_inputs={}, response_key="data.items")
    out = svc._make_request(res, "https://api.example.com/x", {}, resource_name="r1")
    assert out == []


def test_make_request_post_json_body(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("src.service.rest_service.time.sleep", lambda s: None)
    session = MagicMock()
    session.post.return_value = _http_response(json_data={"ok": True})
    svc = _make_request_service(session)
    svc.get_headers = MagicMock(return_value={"Content-Type": "application/json"})
    res = ResourceConfig(
        method="POST",
        path="/x",
        request_inputs={},
    )
    svc._make_request(res, "https://api.example.com/x", {}, resource_name="r1")
    session.post.assert_called_once()
    kw = session.post.call_args.kwargs
    assert "json" in kw


def test_make_request_skip_encoding_builds_query_string(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("src.service.rest_service.time.sleep", lambda s: None)
    session = MagicMock()
    session.get.return_value = _http_response(json_data=[1])
    svc = _make_request_service(session)
    res = ResourceConfig(
        method="GET",
        path="/x",
        skip_encoding_params=True,
        request_inputs={},
    )
    svc._make_request(res, "https://api.example.com/x", {"k": ["a", "b"]}, resource_name="r1")
    url = session.get.call_args.args[0]
    assert "k=a" in url and "k=b" in url


def test_make_request_invalid_json_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("src.service.rest_service.time.sleep", lambda s: None)
    session = MagicMock()
    bad = _http_response(ok=True, json_data={})
    bad.json.side_effect = ValueError("not json")
    session.get.return_value = bad
    svc = _make_request_service(session)
    res = ResourceConfig(method="GET", path="/x", request_inputs={})
    with pytest.raises(ServiceError, match="Invalid JSON response"):
        svc._make_request(res, "https://api.example.com/x", {}, resource_name="r1")


def test_make_request_auth_retry_branch(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("src.service.rest_service.time.sleep", lambda s: None)
    fail = _http_response(ok=False, status_code=401, json_data={"error": "invalid_token"})
    fail.raise_for_status.side_effect = Exception("http error")
    ok = _http_response(json_data={"ok": True})
    session = MagicMock()
    session.get.side_effect = [fail, ok]
    svc = _make_request_service(session)
    svc.settings.api.MAX_RETRIES = 2
    res = ResourceConfig(method="GET", path="/x", request_inputs={})
    svc._make_request(res, "https://api.example.com/x", {}, resource_name="r1")
    assert session.get.call_count == 2
