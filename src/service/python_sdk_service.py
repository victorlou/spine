"""
Python SDK service for making API calls through Python libraries.
"""

import importlib
from typing import Any, Dict, Optional

from src.config.config_models import SourceConfig
from src.config.settings import Settings
from src.service.base_service import BaseSourceService, ServiceError
from src.utils.api_response import dict_response_key_to_records
from src.utils.dynamic_values import get_resolver, resolve_headers_dict
from src.utils.logger import get_logger
from src.utils.redis_context import RedisContextManager


class PythonSDKService(BaseSourceService):
    """
    Service for interacting with Python SDKs/libraries.
    Instantiates the SDK and calls methods based on configuration.
    """

    def __init__(
        self,
        settings: Settings,
        source_name: str,
        source_config: SourceConfig,
        redis_context: RedisContextManager,
    ):
        """
        Initialize the Python SDK service.

        Args:
            settings: Application settings instance
            source_name: Name of the source
            source_config: Source-specific configuration
            redis_context: Redis context manager
        """
        super().__init__(settings)
        self.source_name = source_name
        self.config = source_config
        self.logger = get_logger(self.__class__.__name__)
        self.redis_context = redis_context
        self._sdk_client: Any = None

        if not self.config.sdk:
            raise ServiceError("SDK configuration is required for python_sdk source type")

    def get_base_url(self) -> str:
        """
        Get the base URL for the API.
        For Python SDKs, this returns a placeholder since we don't use HTTP.

        Returns:
            str: Placeholder URL
        """
        return "python-sdk://" + self.source_name

    def get_headers(self) -> Dict[str, str]:
        """
        Get request headers.
        For Python SDKs, headers are not used (they're for HTTP requests).

        Returns:
            Dict[str, str]: Empty dict
        """
        return {}

    def _get_sdk_client(self) -> Any:
        """
        Get or create the SDK client instance.

        Returns:
            Any: SDK client instance

        Raises:
            ServiceError: If SDK cannot be imported or instantiated
        """
        if self._sdk_client is not None:
            return self._sdk_client

        try:
            # Import the module
            module = importlib.import_module(self.config.sdk.module)

            # Get the class
            sdk_class = getattr(module, self.config.sdk.class_name)

            # Resolve auth parameters (may contain dynamic values)
            resolver = get_resolver(self.redis_context)
            auth_params = resolve_headers_dict(self.config.sdk.auth, resolver=resolver)
            # Resolve init kwargs
            init_kwargs = resolve_headers_dict(self.config.sdk.init_kwargs, resolver=resolver)

            # Merge auth into init kwargs
            init_kwargs.update(auth_params)

            self.logger.debug(
                "Instantiating SDK client",
                extra_fields={
                    "module": self.config.sdk.module,
                    "class": self.config.sdk.class_name,
                },
            )

            # Instantiate the SDK client
            self._sdk_client = sdk_class(**init_kwargs)

            # If the SDK has a login method, call it
            if hasattr(self._sdk_client, "login"):
                self.logger.debug("Calling SDK login method")
                self._sdk_client.login()

            self.logger.info("Successfully initialized SDK client")
            return self._sdk_client

        except ImportError as e:
            raise ServiceError(
                f"Failed to import SDK module '{self.config.sdk.module}': {e!s}",
                operation="import_sdk",
                service_name=self.__class__.__name__,
            ) from e
        except AttributeError as e:
            raise ServiceError(
                f"Class '{self.config.sdk.class_name}' not found in module '{self.config.sdk.module}': {e!s}",
                operation="get_sdk_class",
                service_name=self.__class__.__name__,
            ) from e
        except Exception as e:
            raise ServiceError(
                f"Failed to instantiate SDK client: {e!s}",
                operation="instantiate_sdk",
                service_name=self.__class__.__name__,
                original_error=e,
            ) from e

    def fetch_data(
        self,
        resource_name: str,
        parameters: Optional[Dict[str, Any]] = None,
        *,
        full_response: bool = False,
    ) -> Dict[str, Any] | list[Any]:
        """
        Fetch data from the configured SDK method.

        Args:
            resource_name: Name of the resource to run (method name in configuration).
            parameters: Optional parameters for the method call
            full_response: If True, return the raw SDK result (dict/list) without response_key extraction
                or list normalization. Scalars are wrapped as [{"value": result}] for a consistent top-level type.

        Returns:
            Dict[str, Any] | list[Any]: Response data

        Raises:
            ServiceError: If the request fails
        """
        if resource_name not in self.config.resources:
            raise ServiceError(f"Resource not found in configuration: {resource_name}")

        resource = self.config.resources[resource_name]

        try:
            # Get SDK client
            client = self._get_sdk_client()

            # Get method name from resource configuration
            method_name = resource.method

            if not hasattr(client, method_name):
                raise ServiceError(
                    f"Method '{method_name}' not found on SDK client",
                    operation=f"{method_name}",
                    service_name=self.__class__.__name__,
                )

            # Get the method
            method = getattr(client, method_name)

            # Resolve parameters from request_inputs
            resolved_params = resource.resolve_parameters(
                redis_context=self.redis_context,
                params=parameters or {},
                param_dict=resource.request_inputs,
            )

            self.logger.debug(
                "Calling SDK method",
                extra_fields={
                    "method": method_name,
                    "parameters": resolved_params,
                    "source": self.source_name,
                    "resource_name": resource_name,
                },
            )

            # Call the method with resolved parameters
            # Most Python SDKs use keyword arguments, but we support both patterns
            if resolved_params:
                # Try keyword arguments first (most common pattern)
                try:
                    result = method(**resolved_params)
                except TypeError as e:
                    # If keyword args fail, try positional (for methods like get_stats(date))
                    if len(resolved_params) == 1:
                        param_value = next(iter(resolved_params.values()))
                        result = method(param_value)
                    else:
                        # Multiple params but keyword failed - re-raise with better error
                        raise ServiceError(
                            f"Failed to call SDK method '{method_name}' with parameters: {e!s}",
                            operation=method_name,
                            service_name=self.__class__.__name__,
                            original_error=e,
                        ) from e
            else:
                result = method()

            self.logger.trace(
                "SDK method response",
                extra_fields={
                    "method": method_name,
                    "response_type": type(result).__name__,
                    "response_preview": str(result)[:200] if result else None,
                },
            )

            if full_response:
                if isinstance(result, (dict, list)):
                    return result
                return [{"value": result}]

            # Handle response data extraction
            if resource.response_key and isinstance(result, dict):
                out, missing = dict_response_key_to_records(result, resource.response_key)
                if missing:
                    self.logger.debug(
                        f"Response key '{resource.response_key}' not found in response - returning empty data",
                        extra_fields={
                            "resource_name": resource_name,
                            "response_keys": (
                                list(result.keys()) if isinstance(result, dict) else None
                            ),
                        },
                    )
                    return []
                return out
            elif isinstance(result, list):
                return result
            elif isinstance(result, dict):
                return [result]
            else:
                # For scalar values, wrap in a list with a single dict
                return [{"value": result}]

        except ServiceError:
            raise
        except Exception as e:
            raise ServiceError(
                f"SDK method call failed: {e!s}",
                operation=f"{resource.method}",
                service_name=self.__class__.__name__,
                original_error=e,
            ) from e

    def make_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
        **kwargs,
    ):
        """
        Make a request (not used for Python SDKs, but required by base class).
        For Python SDKs, use fetch_data instead.

        Raises:
            ServiceError: Always, as this method is not applicable to Python SDKs
        """
        raise ServiceError(
            "make_request is not applicable to Python SDK services. Use fetch_data instead.",
            operation="make_request",
            service_name=self.__class__.__name__,
        )
