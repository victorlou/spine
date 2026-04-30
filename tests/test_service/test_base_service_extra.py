"""Exercise ``BaseSourceService`` session setup and ``make_request`` error paths."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
import requests

from src.service.base_service import BaseSourceService
from src.utils.exceptions import ServiceError


class _ConcreteService(BaseSourceService):
    def get_base_url(self) -> str:
        return "https://api.example.com"

    def get_headers(self) -> dict:
        return {"X": "1"}

    def fetch_data(self, resource_name: str, parameters=None, *, full_response: bool = False):
        return {}


def _api_settings() -> SimpleNamespace:
    return SimpleNamespace(MAX_RETRIES=3, RETRY_BACKOFF=2.0, TIMEOUT=30)


@pytest.fixture
def settings_obj() -> SimpleNamespace:
    return SimpleNamespace(api=_api_settings())


def test_setup_session_and_lazy_session(settings_obj: SimpleNamespace) -> None:
    svc = _ConcreteService(settings_obj)
    s1 = svc.session
    s2 = svc.session
    assert s1 is s2
    assert "https://" in s1.adapters


def test_make_request_success(settings_obj: SimpleNamespace) -> None:
    svc = _ConcreteService(settings_obj)
    resp = MagicMock()
    resp.ok = True
    resp.status_code = 200
    svc.session.request = MagicMock(return_value=resp)
    out = svc.make_request("GET", "/items")
    assert out is resp


def test_make_request_api_error_non_json_body(settings_obj: SimpleNamespace) -> None:
    svc = _ConcreteService(settings_obj)
    resp = MagicMock()
    resp.ok = False
    resp.status_code = 502
    resp.json.side_effect = ValueError("not json")
    resp.text = "bad gateway html"
    svc.session.request = MagicMock(return_value=resp)
    with pytest.raises(ServiceError, match="API request failed"):
        svc.make_request("GET", "/bad")


def test_make_request_api_error_json(settings_obj: SimpleNamespace) -> None:
    svc = _ConcreteService(settings_obj)
    resp = MagicMock()
    resp.ok = False
    resp.status_code = 500
    resp.json.return_value = {"err": "x"}
    svc.session.request = MagicMock(return_value=resp)
    with pytest.raises(ServiceError, match="API request failed"):
        svc.make_request("POST", "/bad")


def test_make_request_401_triggers_reset_when_marker_present(settings_obj: SimpleNamespace) -> None:
    svc = _ConcreteService(settings_obj)
    svc._reset_auth = MagicMock()  # type: ignore[attr-defined]
    resp = MagicMock()
    resp.ok = False
    resp.status_code = 401
    resp.json.return_value = {"msg": "invalid_token"}
    svc.session.request = MagicMock(return_value=resp)
    with pytest.raises(ServiceError) as exc:
        svc.make_request("GET", "/x")
    assert exc.value.is_retryable is True
    svc._reset_auth.assert_called_once()


def test_make_request_timeout(settings_obj: SimpleNamespace) -> None:
    svc = _ConcreteService(settings_obj)
    svc.session.request = MagicMock(side_effect=requests.exceptions.Timeout("tmo"))
    with pytest.raises(ServiceError, match="timed out"):
        svc.make_request("GET", "/slow")


def test_make_request_connection_error(settings_obj: SimpleNamespace) -> None:
    svc = _ConcreteService(settings_obj)
    svc.session.request = MagicMock(side_effect=requests.exceptions.ConnectionError("down"))
    with pytest.raises(ServiceError, match="Connection failed"):
        svc.make_request("GET", "/x")


def test_make_request_generic_request_exception(settings_obj: SimpleNamespace) -> None:
    svc = _ConcreteService(settings_obj)
    svc.session.request = MagicMock(side_effect=requests.exceptions.RequestException("misc"))
    with pytest.raises(ServiceError, match="Request failed"):
        svc.make_request("GET", "/x")


def test_poll_snapshot_not_supported(settings_obj: SimpleNamespace) -> None:
    svc = _ConcreteService(settings_obj)
    with pytest.raises(ServiceError, match="not supported"):
        svc.poll_snapshot("r", {})


def test_extra_service_init_kwargs_default() -> None:
    assert _ConcreteService.extra_service_init_kwargs() == {}
