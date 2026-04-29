"""Tests for Retry-After parsing and rate-limit header extraction."""

from unittest.mock import MagicMock

from src.service.rate_limit_http import (
    extract_rate_limit_headers,
    parse_retry_after_seconds,
    rate_limit_context_from_response,
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
