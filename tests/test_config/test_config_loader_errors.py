"""Error and edge paths in ``ConfigLoader`` (directory shape, YAML, source validation)."""

from pathlib import Path

import pytest
import yaml

from src.config.config_loader import ConfigLoader
from src.utils.exceptions import ConfigError


def test_load_config_path_not_found(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="not found"):
        ConfigLoader().load_config(tmp_path / "missing_dir")


def test_load_config_path_is_file(tmp_path: Path) -> None:
    f = tmp_path / "not_dir"
    f.write_text("x", encoding="utf-8")
    with pytest.raises(ConfigError, match="must be a directory"):
        ConfigLoader().load_config(f)


def test_load_config_missing_defaults_yml(tmp_path: Path) -> None:
    d = tmp_path / "cfg"
    d.mkdir()
    (d / "sources").mkdir()
    with pytest.raises(ConfigError, match=r"defaults\.yml not found"):
        ConfigLoader().load_config(d)


def test_defaults_yml_not_mapping(tmp_path: Path) -> None:
    d = tmp_path / "cfg"
    d.mkdir()
    (d / "defaults.yml").write_text("[]\n", encoding="utf-8")
    (d / "sources").mkdir()
    with pytest.raises(ConfigError, match="must contain a YAML object"):
        ConfigLoader().load_config(d)


def test_source_file_invalid_yaml(tmp_path: Path) -> None:
    d = tmp_path / "cfg"
    d.mkdir()
    (d / "defaults.yml").write_text(
        "\n".join(
            [
                'version: "1.0"',
                "defaults:",
                "  loading: { destination: local, format: delta, write_mode: overwrite, storage_root: x }",
            ]
        ),
        encoding="utf-8",
    )
    (d / "sources").mkdir()
    (d / "sources" / "bad.yml").write_text("{{invalid", encoding="utf-8")
    with pytest.raises(ConfigError, match="Failed to parse source YAML"):
        ConfigLoader().load_config(d)


def test_source_file_bad_keys(tmp_path: Path) -> None:
    d = tmp_path / "cfg"
    d.mkdir()
    (d / "defaults.yml").write_text(
        "\n".join(
            [
                'version: "1.0"',
                "defaults:",
                "  loading: { destination: local, format: delta, write_mode: overwrite, storage_root: x }",
            ]
        ),
        encoding="utf-8",
    )
    (d / "sources").mkdir()
    (d / "sources" / "bad.yml").write_text(
        yaml.safe_dump({"type": "rest_api", "resources": {}, "extra_bad": True}),
        encoding="utf-8",
    )
    with pytest.raises(ConfigError, match="invalid top-level keys"):
        ConfigLoader().load_config(d)


def test_selection_warns_and_filters(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, caplog
) -> None:
    import logging

    caplog.set_level(logging.WARNING)
    base = tmp_path / "cfg"
    base.mkdir()
    (base / "defaults.yml").write_text(
        "\n".join(
            [
                'version: "1.0"',
                "defaults:",
                "  loading:",
                '    destination: "local"',
                '    format: "delta"',
                '    write_mode: "overwrite"',
                '    storage_root: ".spine/out"',
                "  context:",
                '    type: "redis"',
                "    ttl: 3600",
                '    prefix: "p:"',
                "    redis: { host: localhost, port: 6379, db: 0 }",
            ]
        ),
        encoding="utf-8",
    )
    src = base / "sources"
    src.mkdir()
    (src / "only.yml").write_text(
        "\n".join(
            [
                'type: "rest_api"',
                'base_url: "https://ex.com"',
                "resources: { u: { path: /u, method: GET, response_type: json } }",
            ]
        ),
        encoding="utf-8",
    )
    cfg = ConfigLoader().load_config(base, selection={"only": None, "ghost": None})
    assert "only" in cfg.sources and "ghost" not in cfg.sources
    assert "not found" in caplog.text.lower() or "missing" in caplog.text.lower()


def test_defaults_yml_invalid_yaml_syntax(tmp_path: Path) -> None:
    d = tmp_path / "cfg"
    d.mkdir()
    (d / "defaults.yml").write_text("{{invalid_unclosed", encoding="utf-8")
    (d / "sources").mkdir()
    with pytest.raises(ConfigError, match="Failed to parse YAML"):
        ConfigLoader().load_config(d)


def test_source_file_empty_mapping(tmp_path: Path) -> None:
    d = tmp_path / "cfg"
    d.mkdir()
    (d / "defaults.yml").write_text(
        "\n".join(
            [
                'version: "1.0"',
                "defaults:",
                "  loading: { destination: local, format: delta, write_mode: overwrite, storage_root: x }",
            ]
        ),
        encoding="utf-8",
    )
    (d / "sources").mkdir()
    (d / "sources" / "empty.yml").write_text("{}\n", encoding="utf-8")
    with pytest.raises(ConfigError, match="non-empty YAML object"):
        ConfigLoader().load_config(d)


def test_source_file_not_mapping(tmp_path: Path) -> None:
    d = tmp_path / "cfg"
    d.mkdir()
    (d / "defaults.yml").write_text(
        "\n".join(
            [
                'version: "1.0"',
                "defaults:",
                "  loading: { destination: local, format: delta, write_mode: overwrite, storage_root: x }",
            ]
        ),
        encoding="utf-8",
    )
    (d / "sources").mkdir()
    (d / "sources" / "scalar.yml").write_text("[1, 2]\n", encoding="utf-8")
    with pytest.raises(ConfigError, match="non-empty YAML object"):
        ConfigLoader().load_config(d)


def test_source_file_missing_required_keys(tmp_path: Path) -> None:
    d = tmp_path / "cfg"
    d.mkdir()
    (d / "defaults.yml").write_text(
        "\n".join(
            [
                'version: "1.0"',
                "defaults:",
                "  loading: { destination: local, format: delta, write_mode: overwrite, storage_root: x }",
            ]
        ),
        encoding="utf-8",
    )
    (d / "sources").mkdir()
    (d / "sources" / "only_type.yml").write_text(
        yaml.safe_dump({"type": "rest_api"}),
        encoding="utf-8",
    )
    with pytest.raises(ConfigError, match="missing required keys"):
        ConfigLoader().load_config(d)


def test_process_config_required_env_var_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("SPINE_COVERAGE_REQUIRED_ENV_MISSING", raising=False)
    d = tmp_path / "cfg"
    d.mkdir()
    (d / "defaults.yml").write_text(
        "\n".join(
            [
                'version: "1.0"',
                "defaults:",
                "  loading: { destination: local, format: delta, write_mode: overwrite, storage_root: x }",
            ]
        ),
        encoding="utf-8",
    )
    (d / "sources").mkdir()
    (d / "sources" / "api.yml").write_text(
        "\n".join(
            [
                'type: "rest_api"',
                'base_url: "${SPINE_COVERAGE_REQUIRED_ENV_MISSING}"',
                "resources: { u: { path: /u, method: GET, response_type: json } }",
            ]
        ),
        encoding="utf-8",
    )
    with pytest.raises(ConfigError, match="Required environment variable"):
        ConfigLoader().load_config(d)


def test_process_config_wraps_config_error_from_resolve(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    d = tmp_path / "cfg"
    d.mkdir()
    (d / "defaults.yml").write_text(
        "\n".join(
            [
                'version: "1.0"',
                "defaults:",
                "  loading: { destination: local, format: delta, write_mode: overwrite, storage_root: x }",
            ]
        ),
        encoding="utf-8",
    )
    (d / "sources").mkdir()
    (d / "sources" / "api.yml").write_text(
        "\n".join(
            [
                'type: "rest_api"',
                'base_url: "https://ex.com"',
                "resources: { u: { path: /u, method: GET, response_type: json } }",
            ]
        ),
        encoding="utf-8",
    )

    def boom(_value):
        raise ConfigError(message="substitution blocked", operation="traverse")

    monkeypatch.setattr("src.config.config_loader.resolve_env_var", boom)
    with pytest.raises(ConfigError, match="substitution blocked"):
        ConfigLoader().load_config(d)


def test_no_source_yml_files_logs_warning(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    import logging

    caplog.set_level(logging.WARNING)
    d = tmp_path / "cfg"
    d.mkdir()
    (d / "defaults.yml").write_text(
        "\n".join(
            [
                'version: "1.0"',
                "defaults:",
                "  loading:",
                '    destination: "local"',
                '    format: "delta"',
                '    write_mode: "overwrite"',
                '    storage_root: ".spine/out"',
                "  context:",
                '    type: "redis"',
                "    ttl: 3600",
                '    prefix: "p:"',
                "    redis: { host: localhost, port: 6379, db: 0 }",
            ]
        ),
        encoding="utf-8",
    )
    (d / "sources").mkdir()
    ConfigLoader().load_config(d)
    assert "no valid source YAML" in caplog.text.lower() or "No valid source" in caplog.text
