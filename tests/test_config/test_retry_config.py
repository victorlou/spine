"""Tests for RetryConfig HTTP transport fields."""

from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from src.config.config_models import RetryConfig
from src.config.settings import APISettings


def test_retry_config_defaults() -> None:
    r = RetryConfig()
    assert r.max_attempts == 3
    assert r.honor_retry_after_header is True
    assert r.max_retry_after_seconds == 21600
    assert r.max_backoff_seconds == 120.0
    assert r.backoff_jitter_seconds == 0.0


def test_retry_config_validation_max_retry_after_ge_one() -> None:
    with pytest.raises(ValidationError):
        RetryConfig(max_retry_after_seconds=0)


def test_api_settings_update_from_config_maps_http_retry_fields() -> None:
    retry = RetryConfig(
        max_attempts=5,
        initial_delay=2.0,
        backoff_factor=3.0,
        honor_retry_after_header=False,
        max_retry_after_seconds=120,
        max_backoff_seconds=45.0,
        backoff_jitter_seconds=2.0,
    )
    config = SimpleNamespace(defaults=SimpleNamespace(retry=retry))
    api = APISettings()
    api.update_from_config(config)
    assert api.MAX_RETRIES == 5
    assert api.INITIAL_DELAY == 2.0
    assert api.RETRY_BACKOFF == 3.0
    assert api.HONOR_RETRY_AFTER_HEADER is False
    assert api.MAX_RETRY_AFTER_SECONDS == 120
    assert api.MAX_BACKOFF_SECONDS == 45.0
    assert api.BACKOFF_JITTER_SECONDS == 2.0
