"""Tests for LoadingConfig destination validation and aliases."""

from pathlib import Path

import pytest
from pydantic import ValidationError

from src.config.config_models import LoadingConfig, ResourceConfig, SourceConfig, SourceType


def test_loading_config_s3_valid() -> None:
    cfg = LoadingConfig(
        destination="s3",
        s3_bucket="b",
        prefix="src/res",
        format="delta",
    )
    assert cfg.bucket == "b"
    assert cfg.s3_bucket == "b"
    assert cfg.prefix == "src/res"


def test_loading_config_s3_canonicalizes_bucket_name() -> None:
    cfg = LoadingConfig(
        destination="s3",
        s3_bucket="  my-bucket/  ",
        prefix="a/b",
        format="delta",
    )
    assert cfg.s3_bucket == "my-bucket"
    assert cfg.bucket == "my-bucket"


def test_loading_config_s3_requires_bucket() -> None:
    with pytest.raises(ValidationError) as ei:
        LoadingConfig(destination="s3", prefix="a/b", format="delta")
    assert "s3_bucket" in str(ei.value).lower() or "bucket" in str(ei.value).lower()


def test_loading_config_s3_bucket_alias_normalizes_to_s3_bucket() -> None:
    cfg = LoadingConfig(destination="s3", bucket="alias-bucket", prefix="a/b", format="delta")
    assert cfg.bucket == "alias-bucket"
    assert cfg.s3_bucket == "alias-bucket"


def test_loading_config_s3_bucket_alias_conflict_errors() -> None:
    with pytest.raises(ValidationError, match="bucket and s3_bucket"):
        LoadingConfig(
            destination="s3",
            s3_bucket="s3-value",
            bucket="alias-value",
            prefix="a/b",
            format="delta",
        )


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
    """Relative paths are allowed on the model; ConfigLoader resolves against repository root."""
    cfg = LoadingConfig(
        destination="local",
        storage_root=".spine/local-output",
        prefix="a/b",
        format="delta",
    )
    assert cfg.storage_root == ".spine/local-output"


def test_loading_config_gcs_bucket_alias_only() -> None:
    cfg = LoadingConfig(destination="gcs", bucket="gcs-alias", prefix="a/b", format="delta")
    assert cfg.bucket == "gcs-alias"
    assert cfg.gcs_bucket == "gcs-alias"


def test_loading_config_gcs_canonical_bucket_only() -> None:
    cfg = LoadingConfig(destination="gcs", gcs_bucket="gcs-main", prefix="a/b", format="delta")
    assert cfg.bucket == "gcs-main"
    assert cfg.gcs_bucket == "gcs-main"


def test_loading_config_gcs_bucket_conflict_errors() -> None:
    with pytest.raises(ValidationError, match="bucket and gcs_bucket"):
        LoadingConfig(
            destination="gcs",
            bucket="alias-value",
            gcs_bucket="gcs-value",
            prefix="a/b",
            format="delta",
        )


def test_loading_config_azure_bucket_alias_maps_to_container() -> None:
    cfg = LoadingConfig(
        destination="azure",
        bucket="container-alias",
        azure_account="acct",
        prefix="a/b",
        format="delta",
    )
    assert cfg.bucket == "container-alias"
    assert cfg.azure_container == "container-alias"


def test_loading_config_blob_destination_alias_normalizes_to_azure() -> None:
    cfg = LoadingConfig(
        destination="blob",
        bucket="container-alias",
        azure_account="acct",
        prefix="a/b",
        format="delta",
    )
    assert cfg.destination == "azure_blob"
    assert cfg.azure_container == "container-alias"


def test_loading_config_azure_blob_destination_alias_normalizes_to_azure() -> None:
    cfg = LoadingConfig(
        destination="azure_blob",
        bucket="container-alias",
        azure_account="acct",
        prefix="a/b",
        format="delta",
    )
    assert cfg.destination == "azure_blob"
    assert cfg.azure_container == "container-alias"


def test_loading_config_azure_canonicalizes_case_and_slashes() -> None:
    cfg = LoadingConfig(
        destination="azure_blob",
        azure_container="/MyContainer/",
        azure_account=" MyAccount ",
        prefix="a/b",
        format="delta",
    )
    assert cfg.azure_container == "mycontainer"
    assert cfg.azure_account == "myaccount"
    assert cfg.bucket == "mycontainer"


def test_loading_config_azure_bucket_conflict_errors() -> None:
    with pytest.raises(ValidationError, match="bucket and azure_container"):
        LoadingConfig(
            destination="azure",
            bucket="alias-container",
            azure_container="real-container",
            azure_account="acct",
            prefix="a/b",
            format="delta",
        )


def test_loading_config_merge_requires_keys() -> None:
    with pytest.raises(ValidationError):
        LoadingConfig(
            destination="local",
            storage_root="/tmp",
            prefix="a/b",
            format="delta",
            write_mode="merge",
        )


def test_loading_config_local_prefix_optional() -> None:
    cfg = LoadingConfig(
        destination="local",
        storage_root="/tmp/spine",
        prefix=None,
        format="delta",
    )
    assert cfg.prefix is None


def test_loading_config_disabled_skips_bucket_and_prefix_rules() -> None:
    cfg = LoadingConfig(
        enabled=False,
        destination="s3",
        format="delta",
        prefix="any",
    )
    assert cfg.enabled is False


def test_loading_config_rejects_unknown_destination() -> None:
    with pytest.raises(ValidationError, match="Unsupported loading destination"):
        LoadingConfig(
            destination="sftp",
            format="delta",
            enabled=True,
        )


def test_rest_source_config_strips_trailing_slash_from_base_url() -> None:
    src = SourceConfig(
        enabled=True,
        type=SourceType.REST_API,
        base_url="https://api.example.com/v1/",
        resources={
            "r": ResourceConfig(
                enabled=True,
                path="/x",
                method="GET",
            ),
        },
    )
    assert str(src.base_url) == "https://api.example.com/v1"
