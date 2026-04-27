"""
Factory for creating loader instances based on configuration.
"""

from typing import ClassVar, Dict, Type

from src.config.config_models import LoadingConfig
from src.loader.base_loader import BaseLoader, LoaderError
from src.loader.object_store_loader import ObjectStoreLoader


class LoaderFactory:
    """Factory for creating loader instances."""

    # Canonical LoadingConfig.destination values (aliases are normalized in LoadingConfig).
    _loader_types: ClassVar[Dict[str, Type[BaseLoader]]] = {
        "s3": ObjectStoreLoader,
        "gcs": ObjectStoreLoader,
        "azure_blob": ObjectStoreLoader,
        "local": ObjectStoreLoader,
    }

    @classmethod
    def create_loader(cls, config: LoadingConfig) -> BaseLoader:
        """
        Create a loader instance based on configuration.

        Args:
            config: Loading configuration

        Returns:
            BaseLoader: Loader instance

        Raises:
            LoaderError: If loader type is not supported
        """
        if config.destination not in cls._loader_types:
            raise LoaderError(f"Unsupported loader type: {config.destination}")

        loader_class = cls._loader_types[config.destination]
        return loader_class()

    @classmethod
    def register_loader(cls, destination_type: str, loader_class: Type[BaseLoader]) -> None:
        """
        Register a new loader type.

        Args:
            destination_type: Type identifier for the loader
            loader_class: Loader class implementation
        """
        cls._loader_types[destination_type] = loader_class
