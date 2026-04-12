"""
Factory for creating service instances based on configuration.
"""

from typing import Any, ClassVar, Dict, Optional, Type

from src.config.config_models import SourceConfig, SourceType
from src.config.settings import Settings
from src.service.base_service import BaseSourceService, ServiceError
from src.service.hana_service import HanaService
from src.service.postgres_service import PostgresService
from src.service.python_sdk_service import PythonSDKService
from src.service.rest_service import RestService
from src.utils.redis_context import RedisContextManager


class ServiceFactory:
    """Factory for creating service instances."""

    # Map of service types to their implementations (keys must match SourceType values)
    _service_types: ClassVar[Dict[str, Type[BaseSourceService]]] = {
        SourceType.REST_API.value: RestService,
        SourceType.PYTHON_SDK.value: PythonSDKService,
        SourceType.POSTGRESQL.value: PostgresService,
        SourceType.HANA.value: HanaService,
    }

    @classmethod
    def create_service(
        cls,
        settings: Settings,
        source_name: str,
        config: SourceConfig,
        redis_context: RedisContextManager,
        audit_recorder: Optional[Any] = None,
    ) -> BaseSourceService:
        """
        Create a service instance based on configuration.

        Args:
            settings: Application settings instance
            source_name: Name of the source
            config: Source configuration
            redis_context: Redis context manager
            audit_recorder: Optional audit recorder for request/response trail (passed via service extra kwargs)

        Returns:
            BaseSourceService: Service instance

        Raises:
            ServiceError: If service type is not supported
        """
        type_key = config.type.value
        if type_key not in cls._service_types:
            raise ServiceError(
                f"Unsupported service type '{config.type}' for source '{source_name}'"
            )

        service_class = cls._service_types[type_key]
        extra = service_class.extra_service_init_kwargs(
            audit_recorder=audit_recorder,
            redis_context=redis_context,
            settings=settings,
            source_name=source_name,
            config=config,
        )
        return service_class(
            settings,
            source_name,
            config,
            redis_context=redis_context,
            **extra,
        )

    @classmethod
    def register_service(cls, service_type: str, service_class: Type[BaseSourceService]) -> None:
        """
        Register a new service type.

        Args:
            service_type: Type identifier for the service
            service_class: Service class implementation
        """
        cls._service_types[service_type] = service_class
