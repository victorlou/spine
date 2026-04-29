"""Tests for Retry-After parsing and rate-limit header extraction."""

from unittest.mock import MagicMock

from src.service.rate_limit_http import (
    extract_rate_limit_headers,
    parse_retry_after_seconds,
    rate_limit_context_from_response,
    rate_limit_observability_for_error_response,
)


def test_parse_retry_after_seconds_integer() -> None:
    assert parse_retry_after_seconds("2", retry_after_max=100) == 2.0


def test_parse_retry_after_seconds_respects_cap() -> None:
    assert parse_retry_after_seconds("999999", retry_after_max=10) == 10.0


def test_parse_retry_after_seconds_invalid_returns_none() -> None:
    assert parse_retry_after_seconds("not-a-date", retry_after_max=100) is None


def test_extract_rate_limit_headers_whitelist_only() -> None:
    headers = {
        "Retry-After": "5",
        "X-RateLimit-Remaining": "0",
        "X-RateLimit-Reset": "1234567890",
        "Authorization": "Bearer secret",
        "Content-Type": "application/json",
    }
    out = extract_rate_limit_headers(headers, mask=False)
    assert "Authorization" not in out
    assert out["Retry-After"] == "5"
    assert out["X-RateLimit-Remaining"] == "0"


def test_rate_limit_context_from_response() -> None:
    mock_response = MagicMock()
    mock_response.headers = {
        "Retry-After": "2",
        "X-RateLimit-Remaining": "0",
    }
    ctx = rate_limit_context_from_response(mock_response, retry_after_max=60)
    assert ctx["retry_after_seconds"] == 2.0
    assert "rate_limit_headers" in ctx
    assert "0" in ctx["rate_limit_headers"]["X-RateLimit-Remaining"]


def test_observability_429_warns_even_without_headers() -> None:
    mock_response = MagicMock()
    mock_response.status_code = 429
    mock_response.headers = {}
    ctx, use_rl_log = rate_limit_observability_for_error_response(
        mock_response, retry_after_max=60
    )
    assert ctx == {}
    assert use_rl_log is True


def test_observability_503_warns_even_without_headers() -> None:
    mock_response = MagicMock()
    mock_response.status_code = 503
    mock_response.headers = {}
    ctx, use_rl_log = rate_limit_observability_for_error_response(
        mock_response, retry_after_max=60
    )
    assert ctx == {}
    assert use_rl_log is True


def test_observability_413_with_retry_after() -> None:
    mock_response = MagicMock()
    mock_response.status_code = 413
    mock_response.headers = {"Retry-After": "3"}
    ctx, use_rl_log = rate_limit_observability_for_error_response(
        mock_response, retry_after_max=60
    )
    assert ctx["retry_after_seconds"] == 3.0
    assert use_rl_log is True


def test_observability_413_without_headers() -> None:
    mock_response = MagicMock()
    mock_response.status_code = 413
    mock_response.headers = {}
    ctx, use_rl_log = rate_limit_observability_for_error_response(
        mock_response, retry_after_max=60
    )
    assert ctx == {}
    assert use_rl_log is False


def test_observability_413_x_ratelimit_only() -> None:
    mock_response = MagicMock()
    mock_response.status_code = 413
    mock_response.headers = {"X-RateLimit-Remaining": "0"}
    ctx, use_rl_log = rate_limit_observability_for_error_response(
        mock_response, retry_after_max=60
    )
    assert "rate_limit_headers" in ctx
    assert use_rl_log is True


def test_observability_other_status_no_context() -> None:
    mock_response = MagicMock()
    mock_response.status_code = 404
    mock_response.headers = {"Retry-After": "9"}
    ctx, use_rl_log = rate_limit_observability_for_error_response(
        mock_response, retry_after_max=60
    )
    assert ctx == {}
    assert use_rl_log is False
