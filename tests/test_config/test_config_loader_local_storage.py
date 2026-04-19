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
    config_root = Path("/tmp/spine_cfg")
    loader._resolve_local_loading_storage_paths(cfg, config_root)
    assert cfg["defaults"]["loading"]["storage_root"] == str(
        (config_root / ".spine/local-output").resolve()
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
    loader._resolve_local_loading_storage_paths(cfg, Path("/tmp/any"))
    assert cfg["defaults"]["loading"]["storage_root"] == "/var/data"


def test_resolve_local_loading_storage_paths_skips_s3() -> None:
    loader = ConfigLoader()
    cfg = {"defaults": {"loading": {"destination": "s3", "storage_root": "rel", "prefix": "a/b"}}}
    loader._resolve_local_loading_storage_paths(cfg, Path("/tmp/c"))
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
    root = tmp_path / "cfgdir"
    root.mkdir()
    loader._resolve_local_loading_storage_paths(cfg, root.resolve())
    assert cfg["sources"]["api"]["resources"]["r1"]["loading"]["storage_root"] == str(
        (root / "out/data").resolve()
    )
