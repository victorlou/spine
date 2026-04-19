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
