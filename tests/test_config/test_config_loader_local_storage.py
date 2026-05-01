"""Tests for resolving local loading storage_root in ConfigLoader."""

from pathlib import Path

from src.config.config_loader import ConfigLoader


def test_resolve_local_loading_storage_paths_relative_defaults() -> None:
    loader = ConfigLoader()
    cfg = {
        "defaults": {
            "loading": {
                "destination": "local",
                "storage_root": ".spine/local-output",
                "prefix": "src/res",
            }
        }
    }
    project_root = Path("/tmp/myproject")
    loader._resolve_local_loading_storage_paths(cfg, layout_root=project_root)
    assert cfg["defaults"]["loading"]["storage_root"] == str(
        (project_root / ".spine/local-output").resolve()
    )


def test_resolve_local_loading_storage_paths_absolute_unchanged() -> None:
    loader = ConfigLoader()
    cfg = {
        "defaults": {
            "loading": {
                "destination": "local",
                "storage_root": "/var/data",
                "prefix": "src/res",
            }
        }
    }
    loader._resolve_local_loading_storage_paths(cfg, layout_root=Path("/tmp/any"))
    assert cfg["defaults"]["loading"]["storage_root"] == "/var/data"


def test_resolve_local_loading_storage_paths_skips_s3() -> None:
    loader = ConfigLoader()
    cfg = {"defaults": {"loading": {"destination": "s3", "storage_root": "rel", "prefix": "a/b"}}}
    loader._resolve_local_loading_storage_paths(cfg, layout_root=Path("/tmp/c"))
    assert cfg["defaults"]["loading"]["storage_root"] == "rel"


def test_resolve_local_loading_storage_paths_resource(tmp_path: Path) -> None:
    loader = ConfigLoader()
    cfg = {
        "defaults": {},
        "sources": {
            "api": {
                "type": "rest_api",
                "resources": {
                    "r1": {
                        "method": "GET",
                        "path": "/x",
                        "loading": {
                            "destination": "local",
                            "storage_root": "out/data",
                            "prefix": "a/b",
                        },
                    }
                },
            }
        },
    }
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "config").mkdir()
    loader._resolve_local_loading_storage_paths(cfg, layout_root=repo.resolve())
    assert cfg["sources"]["api"]["resources"]["r1"]["loading"]["storage_root"] == str(
        (repo / "out/data").resolve()
    )


def test_resolve_disk_config_paths_relative_streaming_path(tmp_path: Path) -> None:
    """Relative defaults.streaming.disk_config.path is anchored to the defaults.yml directory."""
    loader = ConfigLoader()
    cfg_dir = tmp_path / "cfg"
    cfg_dir.mkdir()
    defaults_file = cfg_dir / "defaults.yml"
    defaults_file.write_text("version: '1.0'\ndefaults: {}\n", encoding="utf-8")
    raw = {
        "defaults": {"streaming": {"disk_config": {"path": "spill_relative"}}},
        "sources": {},
    }
    out = loader._resolve_disk_config_paths(raw, defaults_file)
    assert out["defaults"]["streaming"]["disk_config"]["path"] == str(
        (cfg_dir / "spill_relative").resolve()
    )


def test_resolve_local_loading_skips_non_string_storage_root() -> None:
    loader = ConfigLoader()
    cfg = {"defaults": {"loading": {"destination": "local", "storage_root": 12345, "prefix": "a"}}}
    loader._resolve_local_loading_storage_paths(cfg, layout_root=Path("/tmp/root"))
    assert cfg["defaults"]["loading"]["storage_root"] == 12345


def test_resolve_local_loading_skips_non_dict_nested_resource(tmp_path: Path) -> None:
    loader = ConfigLoader()
    cfg = {
        "defaults": {},
        "sources": {
            "api": {
                "type": "rest_api",
                "resources": {"bad": "not_a_dict"},
            }
        },
    }
    loader._resolve_local_loading_storage_paths(cfg, layout_root=tmp_path)
    assert cfg["sources"]["api"]["resources"]["bad"] == "not_a_dict"


def test_resolve_local_loading_skips_when_source_entry_not_mapping(tmp_path: Path) -> None:
    loader = ConfigLoader()
    cfg = {"defaults": {}, "sources": {"broken": "not_a_source_mapping"}}
    loader._resolve_local_loading_storage_paths(cfg, layout_root=tmp_path)
    assert cfg["sources"]["broken"] == "not_a_source_mapping"


def test_resolve_local_loading_skips_when_resources_not_mapping(tmp_path: Path) -> None:
    loader = ConfigLoader()
    cfg = {
        "defaults": {},
        "sources": {"api": {"type": "rest_api", "resources": ["not", "a", "dict"]}},
    }
    loader._resolve_local_loading_storage_paths(cfg, layout_root=tmp_path)
    assert cfg["sources"]["api"]["resources"] == ["not", "a", "dict"]
