"""
Factory for creating loader instances based on configuration.
"""

from typing import ClassVar, Dict, Type

from src.config.config_models import LoadingConfig
from src.loader.base_loader import BaseLoader, LoaderError
from src.loader.s3_loader import S3Loader


class LoaderFactory:
    """Factory for creating loader instances."""

    # Map of loader types to their implementations
    _loader_types: ClassVar[Dict[str, Type[BaseLoader]]] = {
        "s3": S3Loader,
        "local": S3Loader,
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
