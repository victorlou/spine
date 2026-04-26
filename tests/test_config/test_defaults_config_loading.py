"""Tests for DefaultsConfig.loading built-in default."""

from src.config.config_models import DefaultsConfig, LoadingConfig, ResourceConfig


def test_defaults_config_loading_factory_is_local_delta() -> None:
    d = DefaultsConfig()
    assert d.loading.destination == "local"
    assert d.loading.format == "delta"
    assert d.loading.write_mode == "overwrite"
    assert d.loading.compression == "snappy"
    assert d.loading.storage_root == ".spine/local-output"
    assert d.loading.prefix is None
    assert d.loading.bucket is None
    assert d.loading.s3_bucket is None
    assert d.loading.enabled is True


def test_defaults_config_explicit_loading_overrides() -> None:
    d = DefaultsConfig(
        loading=LoadingConfig(
            destination="s3",
            s3_bucket="b",
            prefix="src/res",
            format="delta",
        )
    )
    assert d.loading.destination == "s3"
    assert d.loading.bucket == "b"
    assert d.loading.s3_bucket == "b"


def test_resource_config_inherits_defaults_loading_when_loading_omitted() -> None:
    defaults = DefaultsConfig().model_dump()
    r = ResourceConfig(
        method="GET",
        path="/api",
        _defaults=defaults,
    )
    assert r.loading is not None
    assert r.loading.destination == "local"
    assert r.loading.prefix is None


def test_resource_config_merge_respects_loading_enabled_false() -> None:
    defaults = DefaultsConfig().model_dump()
    r = ResourceConfig(
        method="GET",
        path="/api",
        loading={"enabled": False},
        _defaults=defaults,
    )
    assert r.loading is not None
    assert r.loading.enabled is False
    assert r.loading.destination == "local"
