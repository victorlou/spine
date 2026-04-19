"""Tests for LoadingConfig (S3 and local destinations)."""

from pathlib import Path

import pytest
from pydantic import ValidationError

from src.config.config_models import LoadingConfig


def test_loading_config_s3_valid() -> None:
    cfg = LoadingConfig(
        destination="s3",
        bucket="b",
        prefix="src/res",
        format="delta",
    )
    assert cfg.bucket == "b"
    assert cfg.prefix == "src/res"


def test_loading_config_s3_requires_bucket() -> None:
    with pytest.raises(ValidationError) as ei:
        LoadingConfig(destination="s3", prefix="a/b", format="delta")
    assert "bucket" in str(ei.value).lower()


def test_loading_config_s3_requires_prefix_shape() -> None:
    with pytest.raises(ValidationError) as ei:
        LoadingConfig(destination="s3", bucket="b", prefix="onlyone", format="delta")
    assert "prefix" in str(ei.value).lower() or "source_name" in str(ei.value).lower()


def test_loading_config_local_valid(tmp_path: Path) -> None:
    cfg = LoadingConfig(
        destination="local",
        storage_root=str(tmp_path.resolve()),
        prefix="src/res",
        format="delta",
    )
    assert cfg.storage_root == str(tmp_path.resolve())


def test_loading_config_local_requires_storage_root() -> None:
    with pytest.raises(ValidationError) as ei:
        LoadingConfig(destination="local", prefix="a/b", format="delta")
    assert "storage_root" in str(ei.value).lower()


def test_loading_config_local_allows_relative_storage_root() -> None:
    """Relative paths are allowed on the model; ConfigLoader resolves against CONFIG_PATH."""
    cfg = LoadingConfig(
        destination="local",
        storage_root=".spine/local-output",
        prefix="a/b",
        format="delta",
    )
    assert cfg.storage_root == ".spine/local-output"


def test_loading_config_merge_requires_keys() -> None:
    with pytest.raises(ValidationError):
        LoadingConfig(
            destination="local",
            storage_root="/tmp",
            prefix="a/b",
            format="delta",
            write_mode="merge",
        )
