"""Tests for DefaultsConfig.loading built-in default."""

from src.config.config_models import DefaultsConfig, LoadingConfig


def test_defaults_config_loading_factory_is_local_delta() -> None:
    d = DefaultsConfig()
    assert d.loading.destination == "local"
    assert d.loading.format == "delta"
    assert d.loading.write_mode == "overwrite"
    assert d.loading.compression == "snappy"
    assert d.loading.storage_root == ".spine/local-output"
    assert d.loading.prefix == "default/output"
    assert d.loading.bucket is None


def test_defaults_config_explicit_loading_overrides() -> None:
    d = DefaultsConfig(
        loading=LoadingConfig(
            destination="s3",
            bucket="b",
            prefix="src/res",
            format="delta",
        )
    )
    assert d.loading.destination == "s3"
    assert d.loading.bucket == "b"
