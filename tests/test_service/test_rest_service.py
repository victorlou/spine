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


# ---------------------------------------------------------------------------
# get_headers — api_key does not inject Authorization header
# ---------------------------------------------------------------------------


def test_get_headers_no_auth_header_for_api_key_type(monkeypatch: pytest.MonkeyPatch) -> None:
    auth = SimpleNamespace(type="api_key", client_id="key123")
    svc = _service(auth=auth, headers={})
    monkeypatch.setattr("src.service.rest_service.resolve_headers_dict", lambda *_a, **_k: {})
    headers = svc.get_headers()
    assert "Authorization" not in headers


# ---------------------------------------------------------------------------
# _resolve_request_body
# ---------------------------------------------------------------------------


def test_resolve_request_body_builds_from_body_inputs(monkeypatch: pytest.MonkeyPatch) -> None:
    from src.config.config_models import RequestInputConfig

    res = ResourceConfig(
        method="POST",
        path="/x",
        request_inputs={
            "q_param": RequestInputConfig(value="v", location="query"),
            "b_key": RequestInputConfig(value="static_val", location="body"),
        },
    )
    svc = _service(auth=None)
    resolver = MagicMock()
    resolver.resolve.side_effect = lambda v: v
    monkeypatch.setattr("src.service.rest_service.get_resolver", lambda *_: resolver)
    monkeypatch.setattr(
        "src.service.rest_service.resolve_request_body",
        lambda body, resolver, overrides, exclude_keys: overrides,
    )
    result = svc._resolve_request_body(res, {"b_key": "provided"})
    assert "b_key" in result
    assert result["b_key"] == "provided"


def test_resolve_request_body_returns_empty_when_no_body_inputs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.config.config_models import RequestInputConfig

    res = ResourceConfig(
        method="GET",
        path="/x",
        request_inputs={"q_param": RequestInputConfig(value="v", location="query")},
    )
    svc = _service(auth=None)
    monkeypatch.setattr("src.service.rest_service.get_resolver", lambda *_: MagicMock())
    result = svc._resolve_request_body(res, {})
    assert result == {}


# ---------------------------------------------------------------------------
# fetch_data — delegates and unknown resource raises ServiceError
# ---------------------------------------------------------------------------


def test_fetch_data_returns_list_from_make_request(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("src.service.rest_service.time.sleep", lambda s: None)
    session = MagicMock()
    session.get.return_value = _http_response(json_data=[{"id": 1}])
    svc = _make_request_service(session)
    res = ResourceConfig(method="GET", path="/x", request_inputs={})
    svc.config.resources = {"r1": res}
    svc.config.base_url = "https://api.example.com"
    monkeypatch.setattr(
        svc,
        "_resolve_resource_request",
        lambda rn, params=None: (res, "https://api.example.com/x", {}),
    )
    monkeypatch.setattr(svc, "_make_request", lambda *a, **k: [{"id": 1}])
    result = svc.fetch_data("r1")
    assert result == [{"id": 1}]


def test_fetch_data_unknown_resource_raises_service_error() -> None:
    svc = _service(auth=None)
    svc.config.resources = {}
    with pytest.raises(ServiceError, match="Request failed"):
        svc.fetch_data("missing")


# ---------------------------------------------------------------------------
# _make_request — 5xx raises without auth retry
# ---------------------------------------------------------------------------


def test_make_request_5xx_raises_without_retry(monkeypatch: pytest.MonkeyPatch) -> None:
    import requests as requests_lib

    monkeypatch.setattr("src.service.rest_service.time.sleep", lambda s: None)
    http_err = requests_lib.exceptions.HTTPError("500 Server Error")
    err_resp = MagicMock()
    err_resp.status_code = 500
    http_err.response = err_resp

    session = MagicMock()
    fail = _http_response(ok=False, status_code=500, json_data={"error": "internal"})
    fail.raise_for_status.side_effect = http_err
    session.get.return_value = fail

    svc = _make_request_service(session)
    svc.settings.api.MAX_RETRIES = 2
    res = ResourceConfig(method="GET", path="/x", request_inputs={})
    with pytest.raises(requests_lib.exceptions.HTTPError):
        svc._make_request(res, "https://api.example.com/x", {}, resource_name="r1")
    assert session.get.call_count == 1


# ---------------------------------------------------------------------------
# _make_request — response_key and list passthrough
# ---------------------------------------------------------------------------


def test_make_request_response_key_extracts_nested_list(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("src.service.rest_service.time.sleep", lambda s: None)
    session = MagicMock()
    session.get.return_value = _http_response(json_data={"items": [{"id": 1}]})
    svc = _make_request_service(session)
    res = ResourceConfig(method="GET", path="/x", request_inputs={}, response_key="items")
    out = svc._make_request(res, "https://api.example.com/x", {}, resource_name="r1")
    assert out == [{"id": 1}]


def test_make_request_list_response_returned_directly(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("src.service.rest_service.time.sleep", lambda s: None)
    session = MagicMock()
    session.get.return_value = _http_response(json_data=[1, 2, 3])
    svc = _make_request_service(session)
    res = ResourceConfig(method="GET", path="/x", request_inputs={})
    out = svc._make_request(res, "https://api.example.com/x", {}, resource_name="r1")
    assert out == [1, 2, 3]


# ---------------------------------------------------------------------------
# Audit recording — triggered when audit_recorder is set
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "extra_headers",
    [
        pytest.param(None, id="success"),
        pytest.param({"x-envoy-upstream-service-time": "42"}, id="upstream_timing"),
        pytest.param({"x-envoy-upstream-service-time": "not-a-number"}, id="invalid_timing"),
    ],
)
def test_make_request_audit_recorder(
    monkeypatch: pytest.MonkeyPatch,
    extra_headers: dict | None,
) -> None:
    """Audit trail records request and response; invalid upstream timing hits parse fallback only."""
    monkeypatch.setattr("src.service.rest_service.time.sleep", lambda s: None)
    session = MagicMock()
    kw: dict = {"json_data": {"id": 1}}
    if extra_headers:
        kw["headers"] = extra_headers
    session.get.return_value = _http_response(**kw)
    svc = _make_request_service(session)
    svc.audit_recorder = MagicMock()
    res = ResourceConfig(method="GET", path="/x", request_inputs={})
    svc._make_request(res, "https://api.example.com/x", {}, resource_name="r1")
    svc.audit_recorder.record_request.assert_called_once()
    svc.audit_recorder.record_response.assert_called_once()


# ---------------------------------------------------------------------------
# _get_auth_token — api_key branch stores client_id as token
# ---------------------------------------------------------------------------


def test_get_auth_token_api_key_branch() -> None:
    auth = SimpleNamespace(type="api_key", client_id="my-key-123")
    svc = _service(auth=auth)
    token = svc._get_auth_token()
    assert token == "my-key-123"
    assert svc._auth_token == "my-key-123"


# ---------------------------------------------------------------------------
# _get_bearer_token — simple token (no refresh)
# ---------------------------------------------------------------------------


def test_get_bearer_token_uses_static_token() -> None:
    auth = SimpleNamespace(
        type="bearer_token",
        bearer_token="static-tok",
        token_url=None,
        client_id=None,
        client_secret=None,
        refresh_token=None,
    )
    svc = _service(auth=auth)
    token = svc._get_bearer_token()
    assert token == "static-tok"
    assert svc._auth_token == "static-tok"


# ---------------------------------------------------------------------------
# get_headers — oauth_jwt branch injects auth header
# ---------------------------------------------------------------------------


def test_get_headers_oauth_jwt_branch(monkeypatch: pytest.MonkeyPatch) -> None:
    auth = SimpleNamespace(
        type="oauth_jwt", header_name="Authorization", header_format="Bearer {token}"
    )
    svc = _service(auth=auth, headers={})
    monkeypatch.setattr("src.service.rest_service.resolve_headers_dict", lambda *_a, **_k: {})
    monkeypatch.setattr(svc, "_get_auth_token", lambda: "jwt-token")
    headers = svc.get_headers()
    assert headers["Authorization"] == "Bearer jwt-token"


# ---------------------------------------------------------------------------
# poll_snapshot — missing resource and missing path branches
# ---------------------------------------------------------------------------


def test_poll_snapshot_resource_not_found_raises() -> None:
    svc = _service(auth=None)
    svc.config.resources = {}
    with pytest.raises(ServiceError, match="Resource not found"):
        svc.poll_snapshot("nonexistent", {})


def test_poll_snapshot_missing_path_raises() -> None:
    svc = _service(auth=None)
    svc.config.resources = {"r": SimpleNamespace(path=None, method="GET")}
    with pytest.raises(ServiceError, match="path is required"):
        svc.poll_snapshot("r", {})


# ---------------------------------------------------------------------------
# _make_request — endpoint headers are merged
# ---------------------------------------------------------------------------


def test_make_request_endpoint_headers_merged(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("src.service.rest_service.time.sleep", lambda s: None)
    session = MagicMock()
    session.get.return_value = _http_response(json_data=[1])
    svc = _make_request_service(session)
    res = ResourceConfig(method="GET", path="/x", request_inputs={}, headers={"X-Custom": "val"})
    svc._make_request(res, "https://api.example.com/x", {}, resource_name="r1")
    call_headers = session.get.call_args.kwargs["headers"]
    assert call_headers.get("X-Custom") == "val"


# ---------------------------------------------------------------------------
# _make_request — return_full_response returns raw JSON dict
# ---------------------------------------------------------------------------


def test_make_request_return_full_response_returns_dict(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("src.service.rest_service.time.sleep", lambda s: None)
    session = MagicMock()
    session.get.return_value = _http_response(json_data={"meta": 1, "data": [1, 2]})
    svc = _make_request_service(session)
    res = ResourceConfig(method="GET", path="/x", request_inputs={})
    out = svc._make_request(
        res, "https://api.example.com/x", {}, resource_name="r1", return_full_response=True
    )
    assert out == {"meta": 1, "data": [1, 2]}


# ---------------------------------------------------------------------------
# _make_request — non-200 response with non-JSON error body
# ---------------------------------------------------------------------------


def test_make_request_non_200_non_json_body_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("src.service.rest_service.time.sleep", lambda s: None)
    import requests as requests_lib

    session = MagicMock()
    resp = _http_response(ok=False, status_code=400, json_data={})
    resp.json.side_effect = ValueError("not json")
    resp.text = "Bad Request plain text"
    http_err = requests_lib.exceptions.HTTPError("400 Bad Request")
    err_resp = MagicMock()
    err_resp.status_code = 400
    http_err.response = err_resp
    resp.raise_for_status.side_effect = http_err
    session.get.return_value = resp

    svc = _make_request_service(session)
    res = ResourceConfig(method="GET", path="/x", request_inputs={})
    with pytest.raises(requests_lib.exceptions.HTTPError):
        svc._make_request(res, "https://api.example.com/x", {}, resource_name="r1")


# ---------------------------------------------------------------------------
# _make_request — POST with form-urlencoded content-type
# ---------------------------------------------------------------------------


def test_make_request_post_form_urlencoded(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("src.service.rest_service.time.sleep", lambda s: None)
    session = MagicMock()
    session.post.return_value = _http_response(json_data={"ok": True})
    svc = _make_request_service(session)
    svc.get_headers = MagicMock(return_value={"Content-Type": "application/x-www-form-urlencoded"})
    res = ResourceConfig(method="POST", path="/x", request_inputs={})
    svc._make_request(res, "https://api.example.com/x", {"k": "v"}, resource_name="r1")
    kw = session.post.call_args.kwargs
    assert "data" in kw
    assert "json" not in kw


# ---------------------------------------------------------------------------
# _refresh_token — exchanges refresh credentials for access token
# ---------------------------------------------------------------------------


def test_refresh_token_exchanges_credentials() -> None:
    svc = _service(auth=None)
    svc.settings.api.TIMEOUT = 5
    svc.config.auth = SimpleNamespace(
        token_url="https://auth.example.com/token",
        refresh_token="reftok",
        client_id="cid",
        client_secret="csec",
    )
    token_resp = MagicMock()
    token_resp.raise_for_status = MagicMock()
    token_resp.json.return_value = {"access_token": "newtoken", "expires_in": 3600}
    svc._session = MagicMock()
    svc.session.post.return_value = token_resp
    svc._refresh_token()
    assert svc._auth_token == "newtoken"
    assert svc._token_expiry is not None


# ---------------------------------------------------------------------------
# _resolve_resource_request — builds URL and resolves params
# ---------------------------------------------------------------------------


def test_resolve_resource_request_builds_url(monkeypatch: pytest.MonkeyPatch) -> None:
    from src.config.config_models import RequestInputConfig

    svc = _service(auth=None)
    svc.config.resources = {
        "users": ResourceConfig(
            method="GET",
            path="/users",
            request_inputs={"limit": RequestInputConfig(value="10", location="query")},
        )
    }
    monkeypatch.setattr(
        "src.service.rest_service.get_resolver",
        lambda *_: SimpleNamespace(resolve=lambda v: v),
    )
    resource, url, _params = svc._resolve_resource_request("users")
    assert "/users" in url
    assert resource.method == "GET"


def test_resolve_resource_request_unknown_resource_raises() -> None:
    svc = _service(auth=None)
    svc.config.resources = {}
    with pytest.raises(ServiceError, match="Resource not found"):
        svc._resolve_resource_request("missing")


# ---------------------------------------------------------------------------
# _resolve_resource_request — POST with body params and path param substitution
# ---------------------------------------------------------------------------


def test_resolve_resource_request_post_resolves_body_params(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.config.config_models import RequestInputConfig

    svc = _service(auth=None)
    svc.config.resources = {
        "submit": ResourceConfig(
            method="POST",
            path="/submit",
            request_inputs={
                "body_field": RequestInputConfig(value="x", location="body"),
            },
        )
    }
    monkeypatch.setattr(
        "src.service.rest_service.get_resolver",
        lambda *_: SimpleNamespace(resolve=lambda v: v),
    )
    resource, _url, params = svc._resolve_resource_request(
        "submit", parameters={"body_field": "val"}
    )
    assert resource.method == "POST"
    assert "body_field" in params


def test_resolve_resource_request_substitutes_path_params(monkeypatch: pytest.MonkeyPatch) -> None:
    from src.config.config_models import RequestInputConfig

    svc = _service(auth=None)
    svc.config.resources = {
        "item": ResourceConfig(
            method="GET",
            path="/items/{item_id}",
            request_inputs={
                "item_id": RequestInputConfig(value="42", location="path"),
            },
        )
    }
    monkeypatch.setattr(
        "src.service.rest_service.get_resolver",
        lambda *_: SimpleNamespace(resolve=lambda v: v),
    )
    _, url, _ = svc._resolve_resource_request("item")
    assert "/items/42" in url


# ---------------------------------------------------------------------------
# _get_bearer_token — returns cached valid token; raises when no bearer_token
# ---------------------------------------------------------------------------


def test_get_bearer_token_returns_cached_when_valid() -> None:
    from datetime import UTC, datetime, timedelta

    auth = SimpleNamespace(
        type="bearer_token",
        bearer_token="static",
        token_url=None,
        client_id=None,
        client_secret=None,
        refresh_token=None,
    )
    svc = _service(auth=auth)
    svc._auth_token = "cached-tok"
    svc._token_expiry = datetime.now(UTC) + timedelta(hours=1)
    assert svc._get_bearer_token() == "cached-tok"


def test_get_bearer_token_missing_bearer_token_raises() -> None:
    auth = SimpleNamespace(
        type="bearer_token",
        bearer_token=None,
        token_url=None,
        client_id=None,
        client_secret=None,
        refresh_token=None,
    )
    svc = _service(auth=auth)
    with pytest.raises(ServiceError, match="bearer_token is required"):
        svc._get_bearer_token()


# ---------------------------------------------------------------------------
# _make_request — skip_encoding with scalar (non-list) value
# ---------------------------------------------------------------------------


def test_make_request_skip_encoding_scalar_value(monkeypatch: pytest.MonkeyPatch) -> None:
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
    svc._make_request(res, "https://api.example.com/x", {"k": "scalar_val"}, resource_name="r1")
    url = session.get.call_args.args[0]
    assert "k=scalar_val" in url


# ---------------------------------------------------------------------------
# _get_auth_token — bearer_token elif branch (lines 271-274)
# ---------------------------------------------------------------------------


def test_get_auth_token_bearer_token_branch() -> None:
    """Calling _get_auth_token with auth.type='bearer_token' uses _get_bearer_token."""
    auth = SimpleNamespace(
        type="bearer_token",
        bearer_token="my-bearer-tok",
        token_url=None,
        client_id=None,
        client_secret=None,
        refresh_token=None,
    )
    svc = _service(auth=auth)
    token = svc._get_auth_token()
    assert token == "my-bearer-tok"


# ---------------------------------------------------------------------------
# _get_bearer_token — refresh credentials trigger _refresh_token (line 305)
# ---------------------------------------------------------------------------


def test_get_bearer_token_calls_refresh_when_credentials_present() -> None:
    auth = SimpleNamespace(
        type="bearer_token",
        bearer_token=None,
        token_url="https://auth.example.com/token",
        client_id="cid",
        client_secret="csec",
        refresh_token="reftok",
    )
    svc = _service(auth=auth)
    svc.settings.api.TIMEOUT = 5
    svc._session = MagicMock()
    token_resp = MagicMock()
    token_resp.raise_for_status = MagicMock()
    token_resp.json.return_value = {"access_token": "refreshed", "expires_in": 3600}
    svc.session.post.return_value = token_resp
    token = svc._get_bearer_token()
    assert token == "refreshed"


# ---------------------------------------------------------------------------
# _get_auth_token — unsupported auth type raises ServiceError (line 274)
# ---------------------------------------------------------------------------


def test_get_auth_token_unsupported_type_raises() -> None:
    auth = SimpleNamespace(type="custom_auth")
    svc = _service(auth=auth)
    with pytest.raises(ServiceError, match=r"(Unsupported|Authentication failed)"):
        svc._get_auth_token()
