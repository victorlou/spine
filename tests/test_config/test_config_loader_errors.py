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
