"""Tests for the cloud-agnostic destination preflight."""

from __future__ import annotations

from pathlib import Path
from time import sleep
from typing import Any, List
from unittest.mock import MagicMock

import pytest

from src.config.config_models import LoadingConfig
from src.loader import destination_preflight
from src.loader.destination_preflight import preflight_destinations
from src.utils.exceptions import HandlerError
from tests.conftest import clear_managed_platform_env

_MINIMAL_ADC_JSON = (
    '{"type":"authorized_user","client_id":"cid","client_secret":"x","refresh_token":"rt"}'
)


def _install_minimal_gcs_adc(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Deterministic workstation ADC so GCS preflight reaches the JVM stub in CI."""
    monkeypatch.setenv("HOME", str(tmp_path))
    clear_managed_platform_env(monkeypatch)
    adc_dir = tmp_path / ".config" / "gcloud"
    adc_dir.mkdir(parents=True)
    (adc_dir / "application_default_credentials.json").write_text(
        _MINIMAL_ADC_JSON, encoding="utf-8"
    )


class _FakeFs:
    """Minimal stand-in for org.apache.hadoop.fs.FileSystem."""

    def __init__(
        self,
        *,
        exists_returns: bool = True,
        exists_raises: Exception | None = None,
        list_raises: Exception | None = None,
        create_raises: Exception | None = None,
    ) -> None:
        self._exists_returns = exists_returns
        self._exists_raises = exists_raises
        self._list_raises = list_raises
        self._create_raises = create_raises
        self.list_calls: List[Any] = []
        self.delete_calls: List[Any] = []
        self.write_calls: List[bytes] = []

    def exists(self, _path: Any) -> bool:
        if self._exists_raises:
            raise self._exists_raises
        return self._exists_returns

    def listStatus(self, path: Any) -> List[Any]:
        self.list_calls.append(path)
        if self._list_raises:
            raise self._list_raises
        return []

    def create(self, path: Any, _overwrite: bool) -> Any:
        if self._create_raises:
            raise self._create_raises
        out = MagicMock()
        out.write.side_effect = lambda b: self.write_calls.append(b)
        return out

    def delete(self, path: Any, _recursive: bool) -> bool:
        self.delete_calls.append(path)
        return True


def _build_fake_spark(fs: _FakeFs) -> MagicMock:
    spark = MagicMock(name="SparkSession")
    spark.sparkContext._jsc.hadoopConfiguration.return_value = MagicMock(name="hadoop_conf")
    jvm = spark.sparkContext._jvm
    path_obj = MagicMock(name="path_obj")
    path_obj.toUri.return_value = MagicMock(name="uri")
    jvm.org.apache.hadoop.fs.Path.return_value = path_obj
    jvm.org.apache.hadoop.fs.FileSystem.get.return_value = fs
    return spark


@pytest.mark.parametrize(
    "config,expected_dest",
    [
        (LoadingConfig(destination="s3", s3_bucket="my-bucket"), "s3"),
        (LoadingConfig(destination="gcs", gcs_bucket="my-gcs"), "gcs"),
        (
            LoadingConfig(destination="azure_blob", azure_container="ctr", azure_account="acct"),
            "azure_blob",
        ),
    ],
)
def test_preflight_destinations_passes_when_bucket_reachable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    config: LoadingConfig,
    expected_dest: str,
) -> None:
    if config.destination == "gcs":
        _install_minimal_gcs_adc(tmp_path, monkeypatch)
    fs = _FakeFs(exists_returns=True)
    spark = _build_fake_spark(fs)

    preflight_destinations(spark, [config])

    assert fs.list_calls, "listStatus should be called when the destination root exists"
    assert config.destination == expected_dest


@pytest.mark.parametrize(
    "config",
    [
        LoadingConfig(destination="s3", s3_bucket="missing-bucket"),
        LoadingConfig(destination="gcs", gcs_bucket="missing-bucket"),
        LoadingConfig(destination="azure_blob", azure_container="ctr", azure_account="acct"),
    ],
)
def test_preflight_destinations_wraps_list_failures_with_destination_context(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    config: LoadingConfig,
) -> None:
    if config.destination == "gcs":
        _install_minimal_gcs_adc(tmp_path, monkeypatch)
    fs = _FakeFs(list_raises=RuntimeError("403 Forbidden"))
    spark = _build_fake_spark(fs)

    with pytest.raises(HandlerError) as excinfo:
        preflight_destinations(spark, [config])

    err = excinfo.value
    assert err.operation == "destination_preflight"
    assert err.details["destination"] == config.destination
    assert "403 Forbidden" in str(err)
    assert "Cannot list destination" in str(err)


@pytest.mark.slow
def test_preflight_destinations_times_out_filesystem_get(monkeypatch: pytest.MonkeyPatch) -> None:
    fs = _FakeFs()
    spark = _build_fake_spark(fs)
    config = LoadingConfig(destination="s3", s3_bucket="my-bucket")
    monkeypatch.setenv("SPINE_DESTINATION_PREFLIGHT_FILESYSTEM_TIMEOUT_SECONDS", "0.01")

    def _slow_get(*_args, **_kwargs):
        sleep(0.2)
        return fs

    spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get.side_effect = _slow_get

    with pytest.raises(HandlerError, match="timed out"):
        preflight_destinations(spark, [config])


def test_preflight_destinations_list_failure_s3_message_includes_bucket() -> None:
    fs = _FakeFs(list_raises=RuntimeError("permission denied"))
    spark = _build_fake_spark(fs)
    config = LoadingConfig(destination="s3", s3_bucket="my-bucket")

    with pytest.raises(HandlerError) as excinfo:
        preflight_destinations(spark, [config])

    assert "Cannot list destination" in str(excinfo.value)
    assert excinfo.value.details["s3_bucket"] == "my-bucket"


def test_preflight_destinations_always_calls_list_even_when_exists_false() -> None:
    """Regression: read probe must not skip listStatus when exists(bucket) is false (S3A empty bucket)."""
    fs = _FakeFs(exists_returns=False)
    spark = _build_fake_spark(fs)
    config = LoadingConfig(destination="s3", s3_bucket="my-bucket")

    preflight_destinations(spark, [config])

    assert len(fs.list_calls) == 1


def test_preflight_destinations_deduplicates_repeated_configs() -> None:
    fs = _FakeFs(exists_returns=True)
    spark = _build_fake_spark(fs)
    cfg1 = LoadingConfig(destination="s3", s3_bucket="my-bucket", prefix="src/a")
    cfg2 = LoadingConfig(destination="s3", s3_bucket="my-bucket", prefix="src/b")

    preflight_destinations(spark, [cfg1, cfg2])

    # Only one bucket is probed even though two configs target it.
    assert len(fs.list_calls) == 1


def test_preflight_destinations_write_probe_writes_and_deletes_marker() -> None:
    fs = _FakeFs(exists_returns=True)
    spark = _build_fake_spark(fs)
    config = LoadingConfig(destination="s3", s3_bucket="my-bucket")

    preflight_destinations(spark, [config], write_probe=True)

    assert fs.write_calls == [b"spine-preflight"]
    assert fs.delete_calls, "preflight marker should be cleaned up"


def test_preflight_destinations_write_probe_failure_raises_handler_error() -> None:
    fs = _FakeFs(exists_returns=True, create_raises=RuntimeError("AccessDenied"))
    spark = _build_fake_spark(fs)
    config = LoadingConfig(destination="s3", s3_bucket="my-bucket")

    with pytest.raises(HandlerError, match="Cannot write"):
        preflight_destinations(spark, [config], write_probe=True)


def test_preflight_destinations_local_destination_uses_filesystem_check(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    storage_root = tmp_path / "out"
    storage_root.mkdir()
    config = LoadingConfig(destination="local", storage_root=str(storage_root))

    seen: list[Path] = []

    def _fake_check(path: Path) -> None:
        seen.append(path)

    monkeypatch.setattr(destination_preflight, "check_local_storage_root", _fake_check)

    preflight_destinations(None, [config])

    assert seen == [Path(str(storage_root))]


def test_preflight_destinations_local_storage_root_required() -> None:
    config = LoadingConfig.model_construct(destination="local", storage_root=None)

    with pytest.raises(HandlerError, match="storage_root is required"):
        preflight_destinations(None, [config])


def test_preflight_destinations_requires_spark_for_object_store() -> None:
    config = LoadingConfig(destination="s3", s3_bucket="my-bucket")

    with pytest.raises(HandlerError, match="Spark session is required"):
        preflight_destinations(None, [config])


def test_preflight_gcs_fails_fast_without_adc_on_workstation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Regression: avoid long JVM hangs when only `gcloud auth login` was run, not ADC."""
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)
    clear_managed_platform_env(monkeypatch)

    spark = MagicMock(name="unused_spark")
    config = LoadingConfig(destination="gcs", gcs_bucket="test-bucket-dinho")

    with pytest.raises(HandlerError) as excinfo:
        preflight_destinations(spark, [config])

    err = excinfo.value
    assert err.details.get("step") == "gcs_adc_precheck"
    assert "application-default" in str(err).lower()


def test_preflight_gcs_rejects_bucket_with_gs_scheme() -> None:
    spark = _build_fake_spark(_FakeFs())
    config = LoadingConfig(destination="gcs", gcs_bucket="gs://whoops-not-here")

    with pytest.raises(HandlerError) as excinfo:
        preflight_destinations(spark, [config])

    assert excinfo.value.details.get("step") == "destination_identity_precheck"


def test_preflight_gcs_rejects_uppercase_bucket_name() -> None:
    spark = _build_fake_spark(_FakeFs())
    config = LoadingConfig(destination="gcs", gcs_bucket="My-Bucket")

    with pytest.raises(HandlerError, match="DNS bucket pattern"):
        preflight_destinations(spark, [config])


def test_preflight_s3_rejects_bucket_with_s3_scheme() -> None:
    spark = _build_fake_spark(_FakeFs())
    config = LoadingConfig(destination="s3", s3_bucket="s3://nope/extra")

    with pytest.raises(HandlerError) as excinfo:
        preflight_destinations(spark, [config])

    assert excinfo.value.details.get("step") == "destination_identity_precheck"


def test_preflight_gcs_rejects_malformed_adc_json(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("HOME", str(tmp_path))
    clear_managed_platform_env(monkeypatch)
    adc_dir = tmp_path / ".config" / "gcloud"
    adc_dir.mkdir(parents=True)
    (adc_dir / "application_default_credentials.json").write_text("{not json", encoding="utf-8")

    spark = _build_fake_spark(_FakeFs())
    config = LoadingConfig(destination="gcs", gcs_bucket="my-gcs")

    with pytest.raises(HandlerError) as excinfo:
        preflight_destinations(spark, [config])

    assert excinfo.value.details.get("step") == "gcs_credential_json_precheck"


def test_preflight_gcs_accepts_minimal_json_object_for_adc_precheck(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("HOME", str(tmp_path))
    clear_managed_platform_env(monkeypatch)
    adc_dir = tmp_path / ".config" / "gcloud"
    adc_dir.mkdir(parents=True)
    (adc_dir / "application_default_credentials.json").write_text(
        '{"type":"authorized_user","client_id":"x"}',
        encoding="utf-8",
    )

    spark = _build_fake_spark(_FakeFs())
    config = LoadingConfig(destination="gcs", gcs_bucket="my-gcs")

    preflight_destinations(spark, [config])


def test_preflight_gcs_rejects_malformed_gac_json(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    clear_managed_platform_env(monkeypatch)
    bad = tmp_path / "bad.json"
    bad.write_text("{", encoding="utf-8")
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", str(bad))

    spark = _build_fake_spark(_FakeFs())
    config = LoadingConfig(destination="gcs", gcs_bucket="my-gcs")

    with pytest.raises(HandlerError) as excinfo:
        preflight_destinations(spark, [config])

    assert excinfo.value.details.get("step") == "gcs_credential_json_precheck"


def test_preflight_gcs_precheck_errors_when_gac_points_to_missing_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("HOME", str(tmp_path))
    missing = tmp_path / "nope.json"
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", str(missing))
    clear_managed_platform_env(monkeypatch)

    spark = MagicMock()
    config = LoadingConfig(destination="gcs", gcs_bucket="my-bucket")

    with pytest.raises(HandlerError, match="GOOGLE_APPLICATION_CREDENTIALS"):
        preflight_destinations(spark, [config])


def test_preflight_gcs_compute_engine_auth_fails_fast_off_gcp(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)
    monkeypatch.setenv("SPINE_GCS_AUTH_TYPE", "COMPUTE_ENGINE")
    clear_managed_platform_env(monkeypatch)

    spark = MagicMock()
    config = LoadingConfig(destination="gcs", gcs_bucket="my-bucket")

    with pytest.raises(HandlerError, match="COMPUTE_ENGINE"):
        preflight_destinations(spark, [config])


def test_preflight_gcs_allows_missing_adc_on_cloud_run_signal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)
    monkeypatch.setenv("K_SERVICE", "spine-worker")
    monkeypatch.delenv("SPINE_GCS_AUTH_TYPE", raising=False)

    fs = _FakeFs(exists_returns=True)
    spark = _build_fake_spark(fs)
    config = LoadingConfig(destination="gcs", gcs_bucket="my-gcs")

    preflight_destinations(spark, [config])
    assert len(fs.list_calls) == 1


def test_preflight_destinations_skips_disabled_and_unknown_destinations() -> None:
    fs = _FakeFs(exists_returns=True)
    spark = _build_fake_spark(fs)

    # destinations of an unknown kind are silently skipped (the loader factory enforces
    # the destination set elsewhere); ``None`` configs are filtered upfront.
    preflight_destinations(spark, [None])

    assert not fs.list_calls


@pytest.mark.parametrize(
    "env_key,env_value",
    [
        ("FUNCTION_TARGET", "handler"),
        ("FUNCTION_NAME", "worker"),
        ("GAE_ENV", "standard"),
        ("GKE_METADATA_HOST", "169.254.169.254"),
    ],
)
def test_is_gcp_managed_identity_environment_signals_true(
    monkeypatch: pytest.MonkeyPatch,
    env_key: str,
    env_value: str,
) -> None:
    clear_managed_platform_env(monkeypatch)
    monkeypatch.delenv("K_SERVICE", raising=False)
    monkeypatch.delenv("FUNCTION_TARGET", raising=False)
    monkeypatch.delenv("FUNCTION_NAME", raising=False)
    monkeypatch.delenv("GAE_ENV", raising=False)
    monkeypatch.delenv("GKE_METADATA_HOST", raising=False)
    monkeypatch.setenv(env_key, env_value)

    assert destination_preflight._is_gcp_managed_identity_environment() is True


def test_is_gcp_managed_identity_environment_false_without_signals(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clear_managed_platform_env(monkeypatch)
    monkeypatch.delenv("K_SERVICE", raising=False)
    monkeypatch.delenv("FUNCTION_TARGET", raising=False)
    monkeypatch.delenv("FUNCTION_NAME", raising=False)
    monkeypatch.delenv("GAE_ENV", raising=False)
    monkeypatch.delenv("GKE_METADATA_HOST", raising=False)

    assert destination_preflight._is_gcp_managed_identity_environment() is False


def test_filesystem_timeout_seconds_invalid_env_uses_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SPINE_DESTINATION_PREFLIGHT_FILESYSTEM_TIMEOUT_SECONDS", "bad-value")

    assert (
        destination_preflight._filesystem_timeout_seconds()
        == destination_preflight._DEFAULT_FILESYSTEM_TIMEOUT_SECONDS
    )


def test_validate_google_credentials_json_file_rejects_empty_file(
    tmp_path: Path,
) -> None:
    empty = tmp_path / "empty.json"
    empty.write_text("   ", encoding="utf-8")

    with pytest.raises(HandlerError, match="credential file is empty"):
        destination_preflight._validate_google_credentials_json_file(empty, {"destination": "gcs"})


def test_validate_google_credentials_json_file_rejects_non_object_json(
    tmp_path: Path,
) -> None:
    non_object = tmp_path / "list.json"
    non_object.write_text('["not-an-object"]', encoding="utf-8")

    with pytest.raises(HandlerError, match="must be an object"):
        destination_preflight._validate_google_credentials_json_file(
            non_object, {"destination": "gcs"}
        )


def test_validate_google_credentials_json_file_wraps_read_oserror(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_path = Path("/unreadable/adc.json")
    monkeypatch.setattr(
        Path,
        "read_text",
        lambda *_a, **_k: (_ for _ in ()).throw(OSError("permission denied")),
    )

    with pytest.raises(HandlerError, match="Cannot read Google credential file"):
        destination_preflight._validate_google_credentials_json_file(
            fake_path, {"destination": "gcs"}
        )


def test_preflight_destinations_write_probe_cleanup_failure_is_best_effort() -> None:
    fs = _FakeFs(exists_returns=True)
    fs.delete = MagicMock(side_effect=RuntimeError("cleanup failed"))  # type: ignore[method-assign]
    spark = _build_fake_spark(fs)
    config = LoadingConfig(destination="s3", s3_bucket="my-bucket")

    preflight_destinations(spark, [config], write_probe=True)

    assert fs.write_calls == [b"spine-preflight"]
    fs.delete.assert_called_once()


def test_preflight_destinations_wraps_loading_base_uri_value_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = LoadingConfig(destination="s3", s3_bucket="my-bucket")
    spark = _build_fake_spark(_FakeFs())
    monkeypatch.setattr(
        destination_preflight,
        "loading_base_uri",
        lambda _cfg: (_ for _ in ()).throw(ValueError("bad uri")),
    )

    with pytest.raises(HandlerError, match="Invalid loading destination configuration"):
        preflight_destinations(spark, [config])
