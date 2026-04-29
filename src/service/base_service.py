"""
Base class for source connector services.
Provides common functionality and error handling for outbound requests.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.config.settings import Settings
from src.service.rate_limit_http import rate_limit_context_from_response
from src.utils.exceptions import ServiceError
from src.utils.logger import get_logger
from src.utils.url_join import join_http_base_and_path


class BaseSourceService(ABC):
    """
    Abstract base class for source services.
    Defines common interface and shared functionality.
    """

    def __init__(self, settings: Settings):
        """
        Initialize the source service with settings.

        Args:
            settings: Application settings instance
        """
        self.settings = settings
        self.logger = get_logger(self.__class__.__name__)
        self._session: Optional[requests.Session] = None

    def _setup_session(self) -> requests.Session:
        """
        Set up requests session with retry configuration.

        This method configures comprehensive retry logic for all API calls using urllib3.Retry.
        This is the ONLY retry mechanism that should be used for API calls. Do not wrap API
        calls with additional retry logic (like BaseHandler.with_retry) as that may cause:
        1. More retries than intended
        2. Inconsistent backoff behavior
        3. Potential race conditions between retry mechanisms

        The retry strategy handles:
        - Network-level issues (timeouts, connection errors)
        - HTTP status codes (5xx errors, rate limits)
        - Authentication failures (via ServiceError.is_retryable)

        For authentication failures (401):
        1. The RestService resets auth state before the retry
        2. This retry mechanism handles the actual retry
        3. The next attempt gets fresh auth tokens automatically
        4. Time-based signatures are regenerated on each attempt
        """
        session = requests.Session()

        # Define comprehensive retry strategy
        retry_strategy = Retry(
            total=self.settings.api.MAX_RETRIES,
            backoff_factor=self.settings.api.RETRY_BACKOFF,
            backoff_max=self.settings.api.MAX_BACKOFF_SECONDS,
            backoff_jitter=self.settings.api.BACKOFF_JITTER_SECONDS,
            retry_after_max=self.settings.api.MAX_RETRY_AFTER_SECONDS,
            respect_retry_after_header=self.settings.api.HONOR_RETRY_AFTER_HEADER,
            # 401 handled separately to allow auth reset
            status_forcelist=[429, 500, 502, 503, 504, 507, 508, 509],
            allowed_methods=["GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS"],
            raise_on_status=False,  # Let us handle status codes for proper auth reset
            # Retry on connection errors, timeouts, and read errors
            connect=self.settings.api.MAX_RETRIES,
            read=self.settings.api.MAX_RETRIES,
            redirect=3,
        )

        # Configure adapter with retry strategy
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=10,  # Connection pooling settings
            pool_maxsize=10,
            pool_block=False,
        )

        # Mount adapter for both HTTP and HTTPS
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        return session

    @property
    def session(self) -> requests.Session:
        """Lazily create the HTTP session (only sources that call make_request need it)."""
        if self._session is None:
            self._session = self._setup_session()
        return self._session

    @classmethod
    def extra_service_init_kwargs(
        cls, *, audit_recorder: Optional[Any] = None, **kwargs: Any
    ) -> Dict[str, Any]:
        """
        Optional constructor arguments for this service type.

        Used by ServiceFactory so cross-cutting options (e.g. audit) do not require
        per-class branches in the factory.
        """
        return {}

    @abstractmethod
    def get_base_url(self) -> str:
        """
        Get the base URL for the API.
        Must be implemented by concrete classes.

        Returns:
            str: The base URL
        """
        pass

    @abstractmethod
    def get_headers(self) -> Dict[str, str]:
        """
        Get the headers for API requests.
        Must be implemented by concrete classes.

        Returns:
            Dict[str, str]: The headers
        """
        pass

    def make_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> requests.Response:
        """
        Make an HTTP request to the API.

        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint
            params: Optional query parameters
            data: Optional request body
            **kwargs: Additional arguments to pass to requests

        Returns:
            requests.Response: The API response

        Raises:
            ServiceError: If the request fails
        """
        url = join_http_base_and_path(self.get_base_url(), endpoint)
        headers = self.get_headers()

        try:
            response = self.session.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                json=data,
                timeout=self.settings.api.TIMEOUT,
                **kwargs,
            )

            if not response.ok:
                # Get detailed error information
                error_info = {"status_code": response.status_code, "url": url, "method": method}

                try:
                    error_body = response.json()
                    error_info["response"] = error_body
                except ValueError:
                    error_info["response_text"] = response.text[:200]

                if response.status_code in (429, 503):
                    error_info.update(
                        rate_limit_context_from_response(
                            response,
                            retry_after_max=self.settings.api.MAX_RETRY_AFTER_SECONDS,
                        )
                    )
                    self.logger.warning(
                        "API request failed (rate limited or unavailable)",
                        extra_fields=error_info,
                    )
                else:
                    self.logger.error("API request failed", extra_fields=error_info)

                # Check for auth failure that needs token reset
                auth_needs_reset = response.status_code == 401 and any(
                    keyword in str(error_info.get("response", "")).lower()
                    for keyword in ["expired", "invalid_token", "signature", "timestamp"]
                )

                # If this is a RestService and auth needs reset, do it
                if auth_needs_reset and hasattr(self, "_reset_auth"):
                    self._reset_auth()

                # Determine if error is retryable
                is_retryable = (
                    response.status_code >= 500  # Server errors
                    or response.status_code == 429  # Rate limits
                    or response.status_code in [408, 423, 425, 449]  # Timeout/lock errors
                    or auth_needs_reset  # Auth errors that we can retry
                )

                # Add auth-specific context to error info
                if response.status_code == 401:
                    error_info["auth_context"] = {
                        "is_retryable": is_retryable,
                        "retry_strategy": "refresh_token" if auth_needs_reset else "none",
                        "auth_reset": auth_needs_reset,
                    }

                raise ServiceError(
                    message=f"API request failed with status {response.status_code}",
                    operation=f"{method} {endpoint}",
                    service_name=self.__class__.__name__,
                    is_retryable=is_retryable,
                    details=error_info,
                )

            return response

        except requests.exceptions.Timeout as e:
            error_info = {"url": url, "timeout": self.settings.api.TIMEOUT, "error": str(e)}
            self.logger.error("Request timed out", extra_fields=error_info)
            raise ServiceError(
                message=f"Request timed out after {self.settings.api.TIMEOUT}s",
                operation=f"{method} {endpoint}",
                service_name=self.__class__.__name__,
                is_retryable=True,  # Timeouts are retryable
                details=error_info,
                original_error=e,
            ) from e

        except requests.exceptions.ConnectionError as e:
            error_info = {"url": url, "error": str(e)}
            self.logger.error("Connection error", extra_fields=error_info)
            raise ServiceError(
                message="Connection failed",
                operation=f"{method} {endpoint}",
                service_name=self.__class__.__name__,
                is_retryable=True,  # Connection errors are retryable
                details=error_info,
                original_error=e,
            ) from e

        except requests.exceptions.RequestException as e:
            error_info = {"url": url, "error": str(e), "error_type": type(e).__name__}
            self.logger.error("Request failed", extra_fields=error_info)
            raise ServiceError(
                message="Request failed",
                operation=f"{method} {endpoint}",
                service_name=self.__class__.__name__,
                is_retryable=True,  # Most request errors are retryable
                details=error_info,
                original_error=e,
            ) from e

    def __del__(self):
        """Cleanup method to ensure the session is closed."""
        if getattr(self, "_session", None) is not None:
            self._session.close()

    def poll_snapshot(self, resource_name: str, parameters: Dict[str, Any]) -> Any:
        """
        Poll snapshot status for a resource (dict payload, e.g. JSON object).

        HTTP-based services override this; others raise. DynamicHandler uses this
        instead of calling make_request directly.
        """
        raise ServiceError(
            "Snapshot polling is not supported for this source type",
            operation="poll_snapshot",
            service_name=self.__class__.__name__,
            details={"resource_name": resource_name},
        )

    @abstractmethod
    def fetch_data(
        self,
        resource_name: str,
        parameters: Optional[Dict[str, Any]] = None,
        *,
        full_response: bool = False,
    ) -> Dict[str, Any] | list[Any]:
        """
        Fetch data for the specified resource.
        Must be implemented by concrete classes.

        Args:
            resource_name: Name of the configured resource to run (YAML key for REST/SDK, etc.).
            parameters: Optional parameters for the request
            full_response: If True, return full response without extraction. Default False.

        Returns:
            Dict[str, Any] | list[Any]: Response data (extracted/normalized if full_response=False).

        Raises:
            ServiceError: If the request fails or response is invalid
        """
        pass
