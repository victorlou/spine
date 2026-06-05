"""Tests for the telemetry config model and its place in the defaults schema."""

import pytest
from pydantic import ValidationError

from src.config.config_loader import ConfigLoader
from src.config.config_models import DefaultsConfig
from src.config.telemetry import TelemetryConfig


def test_defaults_disabled_and_present_on_defaults_config():
    defaults = DefaultsConfig()
    assert defaults.telemetry.enabled is False
    assert defaults.telemetry.protocol == "grpc"
    assert defaults.telemetry.traces_enabled is True
    assert defaults.telemetry.metrics_enabled is True
    assert defaults.telemetry.service_name == "spine"


def test_enabled_with_all_signals_off_is_rejected():
    with pytest.raises(ValidationError):
        TelemetryConfig(enabled=True, traces_enabled=False, metrics_enabled=False)


def test_enabled_with_one_signal_is_accepted():
    cfg = TelemetryConfig(enabled=True, traces_enabled=False, metrics_enabled=True)
    assert cfg.metrics_enabled is True


def test_arbitrary_resource_attributes_round_trip():
    cfg = TelemetryConfig(
        enabled=True,
        resource_attributes={"team": "data-platform", "deployment.region": "ap-southeast-2"},
    )
    assert cfg.resource_attributes["team"] == "data-platform"
    assert cfg.resource_attributes["deployment.region"] == "ap-southeast-2"


def test_endpoint_env_var_resolution(monkeypatch):
    """${VAR:-default} in the YAML endpoint is resolved by the config loader."""
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://collector:4317")
    loader = ConfigLoader()
    processed = loader._process_config(
        {
            "telemetry": {
                "enabled": True,
                "endpoint": "${OTEL_EXPORTER_OTLP_ENDPOINT:-http://localhost:4317}",
            }
        }
    )
    cfg = TelemetryConfig(**processed["telemetry"])
    assert cfg.endpoint == "http://collector:4317"


def test_endpoint_env_var_default_when_unset():
    loader = ConfigLoader()
    processed = loader._process_config(
        {"telemetry": {"endpoint": "${OTEL_EXPORTER_OTLP_ENDPOINT:-http://localhost:4317}"}}
    )
    cfg = TelemetryConfig(**processed["telemetry"])
    assert cfg.endpoint == "http://localhost:4317"
