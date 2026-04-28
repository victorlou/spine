"""Tests for CLI entrypoints and selection parsing in ``src.main``."""

import json
from types import SimpleNamespace

from click.testing import CliRunner

import src.main as main_module


class _FakeDynamicHandler:
    def __init__(self, settings, selection=None, record_limit=None, backfill_mode=False):
        self.settings = settings
        self.selection = selection
        self.record_limit = record_limit
        self.backfill_mode = backfill_mode
        self.execution_plan = SimpleNamespace(
            summarize=lambda: {"total_stages": 1, "total_resources": 1, "stages": []}
        )

    def validate(self) -> None:
        return None

    def handle(self):
        return {
            "status": "success",
            "sources": {},
            "message": "handled",
        }


def _json_payload(output: str) -> dict:
    start = output.find("{")
    end = output.rfind("}")
    assert start != -1 and end != -1, output
    return json.loads(output[start : end + 1])


def test_parse_select_supports_single_double_colon_and_ignores_empty_items() -> None:
    parsed = main_module.parse_select("users,posts:daily,stats::hourly, , users:recent")

    assert parsed["users"] == {"recent"}
    assert parsed["posts"] == {"daily"}
    assert parsed["stats"] == {"hourly"}


def test_parse_select_source_only_means_all_resources() -> None:
    parsed = main_module.parse_select("orders")

    assert parsed == {"orders": None}


def test_main_cli_show_plan_uses_real_click_entry_with_temp_config(
    monkeypatch, minimal_pipeline_config_dir
) -> None:
    monkeypatch.setenv("CONFIG_PATH", str(minimal_pipeline_config_dir))
    monkeypatch.setattr(main_module, "DynamicHandler", _FakeDynamicHandler)
    monkeypatch.setattr(main_module, "load_pipeline_dotenv", lambda: None)
    monkeypatch.setattr(main_module, "process_environment_variables", lambda: None)

    runner = CliRunner()
    result = runner.invoke(main_module.main, ["--show-plan"])

    assert result.exit_code == 0, result.output
    payload = _json_payload(result.output)
    assert payload["status"] == "success"
    assert payload["message"] == "Execution plan generated"
    assert payload["execution_plan"]["total_resources"] == 1


def test_main_cli_validate_only_returns_success_json(
    monkeypatch, minimal_pipeline_config_dir
) -> None:
    monkeypatch.setenv("CONFIG_PATH", str(minimal_pipeline_config_dir))
    monkeypatch.setattr(main_module, "DynamicHandler", _FakeDynamicHandler)
    monkeypatch.setattr(main_module, "load_pipeline_dotenv", lambda: None)
    monkeypatch.setattr(main_module, "process_environment_variables", lambda: None)

    runner = CliRunner()
    result = runner.invoke(main_module.main, ["--validate-only"])

    assert result.exit_code == 0, result.output
    payload = _json_payload(result.output)
    assert payload["status"] == "success"
    assert payload["message"] == "Configuration validation successful"
