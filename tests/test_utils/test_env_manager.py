"""Tests for environment resolution and JSON-style secret expansion."""

import json
import os

import pytest

from src.utils.env_manager import (
    _parse_env_list_if_json,
    load_pipeline_dotenv,
    process_environment_variables,
    resolve_env_var,
)
from src.utils.exceptions import ConfigError


def test_parse_env_list_if_json() -> None:
    assert _parse_env_list_if_json('["a","b"]') == ["a", "b"]
    assert _parse_env_list_if_json("[invalid json") == "[invalid json"
    assert _parse_env_list_if_json("plain,csv") == "plain,csv"
    assert _parse_env_list_if_json(42) == 42


def test_resolve_env_var_substitution(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MYVAR", "hello")
    assert resolve_env_var("${MYVAR}") == "hello"
    monkeypatch.delenv("MYVAR", raising=False)
    assert resolve_env_var("${MYVAR:-fallback}") == "fallback"
    assert resolve_env_var("${MISSING:-}") == ""
    assert resolve_env_var(123) == 123


def test_resolve_env_var_required_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("REQVAR_xyz", raising=False)
    with pytest.raises(ConfigError, match="REQVAR_xyz"):
        resolve_env_var("${REQVAR_xyz}")


def test_resolve_env_var_empty_reference_logged(monkeypatch: pytest.MonkeyPatch, caplog) -> None:
    import logging

    caplog.set_level(logging.WARNING)
    assert resolve_env_var("prefix${}suffix") == "prefix${}suffix"


def test_resolve_env_var_unexpected_error_returns_original(monkeypatch: pytest.MonkeyPatch) -> None:
    import re

    monkeypatch.setattr(re, "sub", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")))
    assert resolve_env_var("${X:-y}") == "${X:-y}"


def test_process_environment_variables_json_expansion(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = json.dumps({"FROM_JSON": "expanded"})
    monkeypatch.setenv("SECRET_BUNDLE", payload)
    process_environment_variables()
    assert os.environ.get("FROM_JSON") == "expanded"
    monkeypatch.delenv("FROM_JSON", raising=False)
    monkeypatch.delenv("SECRET_BUNDLE", raising=False)


def test_load_pipeline_dotenv_calls_dotenv(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = []

    def fake_load(path, override=False):
        calls.append((str(path), override))

    monkeypatch.setattr("dotenv.load_dotenv", fake_load)
    load_pipeline_dotenv()
    assert len(calls) == 2
    assert calls[0][1] is False and calls[1][1] is False
