"""Tests for loader object store and loading_base_uri."""

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from src.config.config_models import LoadingConfig, LoadingFormat
from src.loader.object_store import SparkFilesystemObjectStore, loading_base_uri
from src.loader.object_store_loader import ObjectStoreLoader, retry_on_transient_storage_error
from src.utils.exceptions import LoaderError


def test_loading_base_uri_s3() -> None:
    cfg = LoadingConfig(
        destination="s3",
        s3_bucket="my-bucket",
        prefix="a/b",
        format="delta",
    )
    assert loading_base_uri(cfg) == "s3a://my-bucket"


def test_loading_base_uri_s3_identity_from_validated_config() -> None:
    """Whitespace/slash trimming happens in LoadingConfig validation, not in loading_base_uri."""
    cfg = LoadingConfig(
        destination="s3",
        s3_bucket="  my-bucket  ",
        prefix="a/b",
        format="delta",
    )
    assert loading_base_uri(cfg) == "s3a://my-bucket"


def test_loading_base_uri_s3_missing_bucket_model_construct() -> None:
    """Guard when config bypasses validation (e.g. model_construct)."""
    cfg = LoadingConfig.model_construct(
        destination="s3",
        format=LoadingFormat.DELTA,
        enabled=True,
        s3_bucket=None,
        gcs_bucket=None,
        bucket=None,
        azure_container=None,
        azure_account=None,
        storage_root=None,
        prefix="a/b",
    )
    with pytest.raises(ValueError, match="s3_bucket is required"):
        loading_base_uri(cfg)


def test_loading_base_uri_local(tmp_path: Path) -> None:
    root = tmp_path / "out"
    root.mkdir()
    cfg = LoadingConfig(
        destination="local",
        storage_root=str(root),
        prefix="a/b",
        format="delta",
    )
    uri = loading_base_uri(cfg)
    assert uri == root.resolve().as_uri().rstrip("/")


def test_loading_base_uri_local_missing_root_model_construct() -> None:
    cfg = LoadingConfig.model_construct(
        destination="local",
        format=LoadingFormat.DELTA,
        enabled=True,
        storage_root=None,
        prefix="a/b",
    )
    with pytest.raises(ValueError, match="storage_root is required"):
        loading_base_uri(cfg)


def test_loading_base_uri_gcs() -> None:
    cfg = LoadingConfig(
        destination="gcs",
        gcs_bucket="my-gcs-bucket",
        prefix="a/b",
        format="delta",
    )
    assert loading_base_uri(cfg) == "gs://my-gcs-bucket"


def test_loading_base_uri_gcs_missing_bucket_model_construct() -> None:
    cfg = LoadingConfig.model_construct(
        destination="gcs",
        format=LoadingFormat.DELTA,
        enabled=True,
        gcs_bucket=None,
        s3_bucket=None,
        bucket=None,
        prefix="a/b",
    )
    with pytest.raises(ValueError, match="gcs_bucket is required"):
        loading_base_uri(cfg)


def test_loading_base_uri_azure() -> None:
    cfg = LoadingConfig(
        destination="azure_blob",
        azure_container="mycontainer",
        azure_account="myaccount",
        prefix="a/b",
        format="delta",
    )
    uri = loading_base_uri(cfg)
    assert uri == "abfs://mycontainer@myaccount.dfs.core.windows.net"


def test_loading_base_uri_blob_destination_alias() -> None:
    cfg = LoadingConfig(
        destination="blob",
        bucket="mycontainer",
        azure_account="myaccount",
        prefix="a/b",
        format="delta",
    )
    assert cfg.destination == "azure_blob"
    uri = loading_base_uri(cfg)
    assert uri == "abfs://mycontainer@myaccount.dfs.core.windows.net"


def test_loading_base_uri_azure_destination_alias() -> None:
    cfg = LoadingConfig(
        destination="azure",
        bucket="mycontainer",
        azure_account="myaccount",
        prefix="a/b",
        format="delta",
    )
    assert cfg.destination == "azure_blob"
    uri = loading_base_uri(cfg)
    assert uri == "abfs://mycontainer@myaccount.dfs.core.windows.net"


def test_loading_base_uri_azure_missing_container_model_construct() -> None:
    cfg = LoadingConfig.model_construct(
        destination="azure_blob",
        format=LoadingFormat.DELTA,
        enabled=True,
        azure_container=None,
        azure_account="myaccount",
        bucket=None,
        prefix="a/b",
    )
    with pytest.raises(ValueError, match="azure_container is required"):
        loading_base_uri(cfg)


def test_loading_base_uri_azure_missing_account_model_construct() -> None:
    cfg = LoadingConfig.model_construct(
        destination="azure_blob",
        format=LoadingFormat.DELTA,
        enabled=True,
        azure_container="mycontainer",
        azure_account=None,
        bucket=None,
        prefix="a/b",
    )
    with pytest.raises(ValueError, match="azure_account is required"):
        loading_base_uri(cfg)


def test_loading_base_uri_unknown_destination() -> None:
    cfg = LoadingConfig.model_construct(
        destination="sftp",
        format=LoadingFormat.DELTA,
        enabled=True,
    )
    with pytest.raises(ValueError, match="Unsupported"):
        loading_base_uri(cfg)


def test_resolve_path_joins_and_trailing_slash(loader_spark: MagicMock) -> None:
    store = SparkFilesystemObjectStore(loader_spark)
    assert store.resolve_path("s3a://b", "p", "q") == "s3a://b/p/q"
    assert store.resolve_path("s3a://b/", "p/", "/q/") == "s3a://b/p/q"
    assert store.resolve_path("file:///tmp", trailing_slash=True) == "file:///tmp/"


def test_exists(loader_spark: MagicMock) -> None:
    jvm = loader_spark.sparkContext._jvm
    fs = MagicMock()
    fs.exists.return_value = True
    jvm.org.apache.hadoop.fs.FileSystem.get.return_value = fs
    path_obj = MagicMock()
    jvm.org.apache.hadoop.fs.Path.return_value = path_obj

    store = SparkFilesystemObjectStore(loader_spark)
    assert store.exists("s3a://b/path") is True
    jvm.org.apache.hadoop.fs.Path.assert_called_once()
    fs.exists.assert_called_once_with(path_obj)


def test_delete_when_present(loader_spark: MagicMock) -> None:
    jvm = loader_spark.sparkContext._jvm
    fs = MagicMock()
    fs.exists.return_value = True
    jvm.org.apache.hadoop.fs.FileSystem.get.return_value = fs
    path_obj = MagicMock()
    jvm.org.apache.hadoop.fs.Path.return_value = path_obj

    store = SparkFilesystemObjectStore(loader_spark)
    store.delete("s3a://b/tmp", recursive=True)
    fs.delete.assert_called_once_with(path_obj, True)


def test_move_success(loader_spark: MagicMock) -> None:
    jvm = loader_spark.sparkContext._jvm
    fs = MagicMock()
    fs.rename.return_value = True
    jvm.org.apache.hadoop.fs.FileSystem.get.return_value = fs
    src = MagicMock()
    dst = MagicMock()
    jvm.org.apache.hadoop.fs.Path.side_effect = [src, dst]

    store = SparkFilesystemObjectStore(loader_spark)
    store.move("s3a://b/a", "s3a://b/b")
    fs.rename.assert_called_once_with(src, dst)


def test_move_failure_raises_loader_error(loader_spark: MagicMock) -> None:
    jvm = loader_spark.sparkContext._jvm
    fs = MagicMock()
    fs.rename.return_value = False
    jvm.org.apache.hadoop.fs.FileSystem.get.return_value = fs
    jvm.org.apache.hadoop.fs.Path.side_effect = [MagicMock(), MagicMock()]

    store = SparkFilesystemObjectStore(loader_spark)
    with pytest.raises(LoaderError, match="Failed to move"):
        store.move("s3a://b/a", "s3a://b/b")


def test_glob_first_part_file(loader_spark: MagicMock) -> None:
    jvm = loader_spark.sparkContext._jvm
    fs = MagicMock()
    status = MagicMock()
    status.getPath.return_value.toString.return_value = "s3a://b/out/part-00000"
    fs.globStatus.return_value = [status]
    jvm.org.apache.hadoop.fs.FileSystem.get.return_value = fs

    store = SparkFilesystemObjectStore(loader_spark)
    assert store.glob_first_part_file("s3a://b/out") == "s3a://b/out/part-00000"


def test_glob_first_part_file_empty(loader_spark: MagicMock) -> None:
    jvm = loader_spark.sparkContext._jvm
    fs = MagicMock()
    fs.globStatus.return_value = []
    jvm.org.apache.hadoop.fs.FileSystem.get.return_value = fs

    store = SparkFilesystemObjectStore(loader_spark)
    assert store.glob_first_part_file("s3a://b/out") is None


def test_is_empty_directory(loader_spark: MagicMock) -> None:
    jvm = loader_spark.sparkContext._jvm
    fs = MagicMock()
    fs.exists.return_value = False
    jvm.org.apache.hadoop.fs.FileSystem.get.return_value = fs

    store = SparkFilesystemObjectStore(loader_spark)
    assert store.is_empty_directory("s3a://b/missing") is True


def test_object_store_loader_generate_table_path_uses_object_store() -> None:
    loader = ObjectStoreLoader()
    loader.spark = MagicMock(name="spark")
    loader._object_store = MagicMock(name="object_store")
    loader._object_store.resolve_path.return_value = "s3a://bucket/rest_api/foo/"

    path = loader._generate_table_path("s3a://bucket", "foo", source_type="rest_api")

    assert path == "s3a://bucket/rest_api/foo/"
    loader._object_store.resolve_path.assert_called_once_with(
        "s3a://bucket", "rest_api/foo", trailing_slash=True
    )


def test_retry_on_transient_storage_error_retries_then_succeeds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("src.loader.object_store_loader.time.sleep", lambda _: None)
    attempts = {"count": 0}

    @retry_on_transient_storage_error(max_retries=3, delay=0)
    def flaky_operation():
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise RuntimeError("SocketTimeoutException while writing")
        return "ok"

    assert flaky_operation() == "ok"
    assert attempts["count"] == 3
