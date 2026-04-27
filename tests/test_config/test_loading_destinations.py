"""Tests for canonical loading destination helpers."""

from src.config.loading_destinations import (
    OBJECT_STORE_DESTINATIONS,
    is_object_store_destination,
    normalize_loading_destination,
)


def test_normalize_loading_destination_aliases() -> None:
    assert normalize_loading_destination("azure") == "azure_blob"
    assert normalize_loading_destination("blob") == "azure_blob"
    assert normalize_loading_destination("  Azure  ") == "azure_blob"
    assert normalize_loading_destination("azure_blob") == "azure_blob"


def test_object_store_destinations_membership() -> None:
    assert OBJECT_STORE_DESTINATIONS == frozenset({"s3", "local", "gcs", "azure_blob"})


def test_is_object_store_destination() -> None:
    assert is_object_store_destination("s3")
    assert is_object_store_destination("blob")
    assert not is_object_store_destination("snowflake")
