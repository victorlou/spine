"""Tests for loader registration and factory dispatch."""

from types import SimpleNamespace

import pytest

from src.loader.loader_factory import LoaderFactory
from src.loader.object_store_loader import ObjectStoreLoader
from src.utils.exceptions import LoaderError


def test_create_loader_known_destinations() -> None:
    for dest in ("s3", "gcs", "azure_blob", "local"):
        loader = LoaderFactory.create_loader(SimpleNamespace(destination=dest))
        assert isinstance(loader, ObjectStoreLoader)


def test_create_loader_unknown_raises() -> None:
    with pytest.raises(LoaderError, match="Unsupported loader"):
        LoaderFactory.create_loader(SimpleNamespace(destination="not_a_registered_loader"))


def test_register_loader_custom() -> None:
    class _Mini(ObjectStoreLoader):
        pass

    key = "_tmp_test_loader_dest_"
    LoaderFactory.register_loader(key, _Mini)
    try:
        assert isinstance(LoaderFactory.create_loader(SimpleNamespace(destination=key)), _Mini)
    finally:
        LoaderFactory._loader_types.pop(key, None)
