from typing import Any, ClassVar, Dict, Type

from pyspark.sql import SparkSession

from src.config.config_models import LoadingConfig, LoadingFormat
from src.load_strategy import BaseLoadStrategy
from src.loader import ObjectStore

from .delta_strategy import DeltaStrategy


class LoadStrategyFactory:
    """
    Factory class to create load strategy instances based on the specified type.
    """

    _load_strategies: ClassVar[Dict[str, Type[BaseLoadStrategy]]] = {
        LoadingFormat.DELTA: DeltaStrategy
    }

    @classmethod
    def create_load_strategy(
        cls,
        spark: SparkSession,
        object_store: ObjectStore,
        base_uri: str,
        config: LoadingConfig,
        source_type: str,
    ) -> BaseLoadStrategy:
        """
        Create an instance of a load strategy based on the specified type.

        Args:
            spark (SparkSession): The Spark session to use for loading data.
            object_store (ObjectStore): The object store to interact with for loading data.
            base_uri (str): The base URI for the data source.
            config (LoadingConfig): The configuration for loading data.
            source_type (str): The type of load strategy to create.

        Returns:
            BaseLoadStrategy: An instance of the specified load strategy.
        """

        strategy_class = cls._load_strategies.get(config.format)
        if not strategy_class:
            raise ValueError(f"Unsupported load strategy type: {source_type}")
        return strategy_class(spark, object_store, base_uri, config, source_type)
