"""Tests for settings behavior."""

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from src.config.settings import (
    APISettings,
    DatabricksSettings,
    Settings,
    _resolve_pipeline_config_dir,
    get_settings,
)
from src.utils.exceptions import ConfigError
from tests.conftest import write_minimal_pipeline_config_dir

_RETRY_DEFAULTS_BLOCK = (
    'version: "1.0"\n'
    "defaults:\n"
    "  retry:\n"
    "    max_attempts: 5\n"
    "    initial_delay: 2.0\n"
    "    backoff_factor: 3.0\n"
    "  loading:\n"
    '    destination: "local"\n'
    '    format: "delta"\n'
    '    write_mode: "overwrite"\n'
    '    storage_root: ".spine/out"\n'
    "  context:\n"
    '    type: "redis"\n'
    "    ttl: 3600\n"
    '    prefix: "p:"\n'
    "    redis: { host: localhost, port: 6379, db: 0 }\n"
)

_API_SOURCE_FILE = {
    "api.yml": (
        'type: "rest_api"\n'
        'base_url: "https://ex.com"\n'
        "resources: { u: { path: /u, method: GET, response_type: json } }\n"
    ),
}


def _write_minimal_pipeline_config_dir(cfg_dir: Path) -> None:
    write_minimal_pipeline_config_dir(
        cfg_dir, defaults_yaml=_RETRY_DEFAULTS_BLOCK, sources=_API_SOURCE_FILE
    )


def test_settings_model_has_no_embedded_aws_field() -> None:
    """AWS for Spark is resolved in SparkManager / AWSCredentialManager, not on Settings."""
    assert "aws" not in Settings.model_fields


def test_settings_loading_destinations_property_uses_pipeline_config() -> None:
    settings_like = SimpleNamespace(
        _pipeline_config=SimpleNamespace(get_effective_loading_destinations=lambda: {"s3", "gcs"})
    )
    assert Settings.loading_destinations.fget(settings_like) == {"s3", "gcs"}


def test_settings_loading_destinations_empty_when_no_pipeline_config() -> None:
    bare = SimpleNamespace(_pipeline_config=None)
    assert Settings.loading_destinations.fget(bare) == set()


def test_resolve_pipeline_config_dir_absolute_normalized(tmp_path: Path) -> None:
    d = tmp_path / "abs_cfg"
    d.mkdir()
    resolved = _resolve_pipeline_config_dir(str(d))
    assert resolved == d.resolve()


def test_resolve_pipeline_config_dir_relative_under_repo_config(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    fake_root = tmp_path / "repo"
    (fake_root / "config" / "staging").mkdir(parents=True)
    monkeypatch.setattr(
        "src.config.settings.repository_root",
        lambda: fake_root.resolve(),
    )
    out = _resolve_pipeline_config_dir("staging")
    assert out == (fake_root / "config" / "staging").resolve()


def test_api_settings_update_from_config_applies_retry_from_pipeline() -> None:
    api = APISettings()
    retry = SimpleNamespace(max_attempts=7, initial_delay=1.5, backoff_factor=3.5)
    cfg = SimpleNamespace(defaults=SimpleNamespace(retry=retry))
    api.update_from_config(cfg)
    assert api.MAX_RETRIES == 7
    assert api.INITIAL_DELAY == 1.5
    assert api.RETRY_BACKOFF == 3.5


def test_api_settings_update_from_config_skips_when_defaults_missing() -> None:
    api = APISettings()
    api.MAX_RETRIES = 99
    api.update_from_config(SimpleNamespace(defaults=None))
    assert api.MAX_RETRIES == 99


def test_api_settings_update_from_config_skips_when_retry_missing() -> None:
    api = APISettings()
    api.MAX_RETRIES = 88
    api.update_from_config(SimpleNamespace(defaults=SimpleNamespace(retry=None)))
    assert api.MAX_RETRIES == 88


def test_databricks_settings_get_warehouse_id_requires_value() -> None:
    ds = DatabricksSettings(WAREHOUSE_ID="")
    with pytest.raises(ValueError, match="DATABRICKS_WAREHOUSE_ID"):
        ds.get_warehouse_id()


def test_databricks_settings_get_warehouse_id_returns_value() -> None:
    ds = DatabricksSettings(WAREHOUSE_ID="wh-abc")
    assert ds.get_warehouse_id() == "wh-abc"


def test_databricks_settings_workspace_client_success() -> None:
    sentinel = object()
    with patch("src.config.settings.WorkspaceClient", autospec=True) as WC:
        WC.return_value = sentinel
        ds = DatabricksSettings(
            HOST="https://dbc.example.com",
            CLIENT_ID="cid",
            CLIENT_SECRET="sec",
        )
        assert ds.initialize_databricks_workspace_client() is sentinel
        WC.assert_called_once_with(
            host="https://dbc.example.com",
            client_id="cid",
            client_secret="sec",
        )


def test_databricks_settings_workspace_client_missing_credentials_wraps_config_error() -> None:
    ds = DatabricksSettings(HOST="", CLIENT_ID="", CLIENT_SECRET="")
    with pytest.raises(ConfigError, match="Failed to initialize Databricks workspace client"):
        ds.initialize_databricks_workspace_client()


def test_settings_loads_pipeline_config_and_updates_api_retries(tmp_path: Path) -> None:
    cfg_dir = tmp_path / "cfg"
    _write_minimal_pipeline_config_dir(cfg_dir)
    s = Settings(CONFIG_PATH=str(cfg_dir.resolve()))
    assert s.pipeline_config.version == "1.0"
    assert s.api.MAX_RETRIES == 5
    assert s.api.INITIAL_DELAY == 2.0
    assert s.api.RETRY_BACKOFF == 3.0


def test_settings_load_config_wraps_non_config_exception(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg_dir = tmp_path / "cfg"
    _write_minimal_pipeline_config_dir(cfg_dir)

    def boom_loader():
        m = MagicMock()
        m.load_config.side_effect = RuntimeError("boom")
        return m

    monkeypatch.setattr("src.config.settings.ConfigLoader", boom_loader)
    with pytest.raises(ConfigError, match="Failed to load pipeline configuration") as ei:
        Settings(CONFIG_PATH=str(cfg_dir.resolve()))
    assert ei.value.operation == "_load_config"
    assert isinstance(ei.value.original_error, RuntimeError)


def test_settings_load_config_reraises_config_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg_dir = tmp_path / "cfg"
    _write_minimal_pipeline_config_dir(cfg_dir)
    err = ConfigError(message="bad yaml day", operation="load_config")

    def failing_loader():
        m = MagicMock()
        m.load_config.side_effect = err
        return m

    monkeypatch.setattr("src.config.settings.ConfigLoader", failing_loader)
    with pytest.raises(ConfigError, match="bad yaml day"):
        Settings(CONFIG_PATH=str(cfg_dir.resolve()))


def test_get_settings_cache_same_selection(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    cfg_dir = tmp_path / "cfg"
    _write_minimal_pipeline_config_dir(cfg_dir)
    monkeypatch.setenv("CONFIG_PATH", str(cfg_dir.resolve()))
    a = get_settings(selection={"api": None})
    b = get_settings(selection={"api": None})
    assert a is b


def test_get_settings_cache_distinct_for_different_selection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg_dir = tmp_path / "cfg"
    _write_minimal_pipeline_config_dir(cfg_dir)
    monkeypatch.setenv("CONFIG_PATH", str(cfg_dir.resolve()))
    full = get_settings(selection=None)
    scoped = get_settings(selection={"api": {"u"}})
    assert full is not scoped
