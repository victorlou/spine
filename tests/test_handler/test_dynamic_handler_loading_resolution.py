"""Tests for DynamicHandler loading resolution behavior."""

from types import SimpleNamespace

from src.config.config_models import LoadingConfig
from src.handler.dynamic_handler import DynamicHandler


def _make_handler(default_loading: LoadingConfig) -> DynamicHandler:
    handler = DynamicHandler.__new__(DynamicHandler)
    handler.config = SimpleNamespace(defaults=SimpleNamespace(loading=default_loading))
    return handler


def test_resolve_resource_loading_sets_default_prefix_for_gcs() -> None:
    handler = _make_handler(
        LoadingConfig(destination="gcs", gcs_bucket="gcs-bucket", prefix=None, format="delta")
    )
    resource_config = SimpleNamespace(loading=None)

    resolved = handler._resolve_resource_loading("my_source", "my_resource", resource_config)

    assert resolved is not None
    assert resolved.prefix == "my_source/my_resource"


def test_resolve_resource_loading_sets_default_prefix_for_azure_blob_alias() -> None:
    handler = _make_handler(
        LoadingConfig(
            destination="blob",
            azure_container="container",
            azure_account="account",
            prefix=None,
            format="delta",
        )
    )
    resource_config = SimpleNamespace(loading=None)

    resolved = handler._resolve_resource_loading("my_source", "my_resource", resource_config)

    assert resolved is not None
    assert resolved.destination == "azure_blob"
    assert resolved.prefix == "my_source/my_resource"
