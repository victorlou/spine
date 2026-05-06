from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Dict, Optional, Type

from pyspark.sql import SparkSession

from src.config.config_models import LoadingConfig, LoadingFormat
from src.load_strategy.base_load_strategy import BaseLoadStrategy
from src.utils.exceptions import LoaderError

from .delta_strategy import DeltaStrategy
from .iceberg_strategy import IcebergStrategy

if TYPE_CHECKING:
    from src.loader.object_store import ObjectStore


class LoadStrategyFactory:
    """Factory class to create load strategy instances based on loading format."""

    _load_strategies: ClassVar[Dict[LoadingFormat, Type[BaseLoadStrategy]]] = {
        LoadingFormat.DELTA: DeltaStrategy,
        LoadingFormat.ICEBERG: IcebergStrategy,
    }

    @classmethod
    def create_load_strategy(
        cls,
        spark: SparkSession,
        object_store: ObjectStore,
        base_uri: str,
        config: LoadingConfig,
        source_type: Optional[str],
    ) -> BaseLoadStrategy:
        """
        Create a load strategy for the configured table format.
        """
        strategy_class = cls._load_strategies.get(config.format)
        if not strategy_class:
            raise LoaderError(f"Unsupported load strategy format: {config.format}")
        return strategy_class(spark, object_store, base_uri, config, source_type)
