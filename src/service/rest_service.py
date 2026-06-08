"""
Generic REST service for making API calls based on configuration.
"""

import base64
import re
import time
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any, Dict, Optional

import jwt
import requests

from src.audit import (
    ApiRequestRecord,
    ApiResponseRecord,
    mask_headers,
    request_preview_from_payload,
)
from src.auth.jwt_providers import get_provider
from src.config.config_models import ResourceConfig, SourceConfig
from src.config.settings import Settings
from src.service.base_service import BaseSourceService, ServiceError
from src.utils.data_utils import dict_response_key_to_records
from src.utils.dynamic_values import get_resolver, resolve_headers_dict, resolve_request_body
from src.utils.logger import REDACTED_PLACEHOLDER, get_logger, redact_text
from src.utils.redis_context import RedisContextManager
from src.utils.url_join import join_http_base_and_path


class RestService(BaseSourceService):
    """
    Generic REST service that uses configuration to make API calls.
    Supports different authentication methods and request patterns.
    """

    def __init__(
        self,
        settings: Settings,
        source_name: str,
        source_config: SourceConfig,
        redis_context: RedisContextManager,
        audit_recorder: Optional[Any] = None,
    ):
        """
        Initialize the REST service with settings and source configuration.

        Args:
            settings: Application settings instance
            source_name: Name of the source
            source_config: Source-specific configuration
            redis_context: Redis context manager
            audit_recorder: Optional audit recorder for request/response trail
        """
        super().__init__(settings)
        self.source_name = source_name
        self.config = source_config
        self.logger = get_logger(self.__class__.__name__)
        self._auth_token: Any = None
        self._token_expiry = None
        self.redis_context = redis_context
        self.audit_recorder = audit_recorder

    @classmethod
    def extra_service_init_kwargs(
        cls, *, audit_recorder: Optional[Any] = None, **kwargs: Any
    ) -> Dict[str, Any]:
        return {"audit_recorder": audit_recorder}

    def poll_snapshot(self, resource_name: str, parameters: Dict[str, Any]) -> Any:
        """
        Poll snapshot status via the configured HTTP method and path for this resource.

        Matches legacy DynamicHandler behavior (make_request + JSON parse).
        """
        if resource_name not in self.config.resources:
            raise ServiceError(
                f"Resource not found in configuration: {resource_name}",
                operation="poll_snapshot",
                service_name=self.__class__.__name__,
            )
        resource_config = self.config.resources[resource_name]
        if not resource_config.path:
            raise ServiceError(
                "path is required on the resource to use snapshot polling",
                operation="poll_snapshot",
                service_name=self.__class__.__name__,
                details={"resource_name": resource_name},
            )
        response = self.make_request(
            str(resource_config.method),
            endpoint=resource_config.path,
            params=parameters,
        )
        try:
            data = response.json()
        except ValueError as e:
            raise ServiceError(
                f"Invalid JSON in snapshot poll response: {e!s}",
                operation="poll_snapshot",
                service_name=self.__class__.__name__,
                is_retryable=True,
            ) from e
        self.logger.debug(
            "Snapshot poll response",
            extra_fields={
                "resource_name": resource_name,
                "response_keys": (list(data.keys()) if isinstance(data, dict) else None),
                "response_type": type(data).__name__,
                "response_content": data,
            },
        )
        return data

    def get_base_url(self) -> str:
        """
        Get the base URL for the API.

        Returns:
            str: The base URL from configuration
        """
        return str(self.config.base_url)

    def get_headers(self, resolver: Optional[Any] = None) -> Dict[str, str]:
        """
        Get request headers including authentication.

        Args:
            resolver: Optional resolver to reuse.

        Returns:
            Dict[str, str]: Request headers
        """
        headers = resolve_headers_dict(self.config.headers, self.redis_context, resolver=resolver)

        # Add authentication if configured
        if self.config.auth:
            if self.config.auth.type == "oauth_jwt":
                # Get auth token and add with configured format
                auth_token = self._get_auth_token()
                headers[self.config.auth.header_name] = self.config.auth.header_format.format(
                    token=auth_token
                )
            elif self.config.auth.type == "bearer_token":
                auth_token = self._get_auth_token()
                headers[self.config.auth.header_name] = self.config.auth.header_format.format(
                    token=auth_token
                )

        return headers

    def _decode_private_key(self) -> str:
        """
        Decode the base64-encoded private key from auth config.

        Returns:
            Decoded private key string

        Raises:
            ServiceError: If private_key is missing or decoding fails
        """
        if not self.config.auth.private_key:
            raise ServiceError("private_key is required for oauth_jwt authentication")
        try:
            self.logger.trace(
                "Decoding private key",
                extra_fields={
                    "length": len(self.config.auth.private_key),
                    "is_base64": self.config.auth.private_key.endswith("="),
                },
            )
            return (
                base64.b64decode(self.config.auth.private_key).decode("utf-8").replace("\\n", "\n")
            )
        except Exception as e:
            raise ServiceError(f"Failed to decode base64 private key: {e!s}") from e

    def _get_auth_token(self) -> str:
        """
        Get or refresh the authentication token.

        Returns:
            str: Authentication token

        Raises:
            ServiceError: If authentication fails
        """
        if self._auth_token and self._token_expiry and datetime.now(UTC) < self._token_expiry:
            return self._auth_token

        try:
            if not self.config.auth:
                raise ServiceError("Authentication configuration is required")

            if self.config.auth.type == "oauth_jwt":
                private_key = self._decode_private_key()
                jwt_config = self.config.auth.jwt_config
                if not jwt_config:
                    raise ServiceError("jwt_config is required for oauth_jwt authentication")

                provider = get_provider(jwt_config.provider)
                current_time = int(time.time())

                jwt_payload = provider.build_jwt_payload(self.config.auth, jwt_config, current_time)
                jwt_headers = provider.build_jwt_headers(jwt_config)
                jwt_token = jwt.encode(
                    payload=jwt_payload,
                    key=private_key,
                    algorithm=jwt_config.algorithm,
                    headers=jwt_headers,
                )

                token_data = provider.build_token_exchange_data(
                    jwt_token, self.config.auth, jwt_config
                )
                request_headers = provider.build_request_headers(self.config.auth)

                self.logger.debug(
                    "Requesting access token",
                    extra_fields={"token_url": str(self.config.auth.token_url)},
                )

                response = self.session.post(
                    url=str(self.config.auth.token_url),
                    headers=request_headers,
                    data=token_data,
                    timeout=self.settings.api.TIMEOUT,
                )

                if not response.ok:
                    self.logger.error(
                        "Token request failed",
                        extra_fields={
                            "status_code": response.status_code,
                            "response": response.text[:200],  # Limit response text
                        },
                    )
                response.raise_for_status()

                token_info = response.json()
                self._auth_token = token_info["access_token"]
                self._token_expiry = datetime.now(UTC) + timedelta(
                    seconds=token_info.get("expires_in", 252000)
                )

                self.logger.debug(
                    "Successfully obtained access token",
                    extra_fields={
                        "expires_in": token_info.get("expires_in"),
                        "expiry": self._token_expiry.isoformat(),
                    },
                )

                return self._auth_token
            elif self.config.auth.type == "basic":
                # Basic authentication - encode credentials
                credentials = f"{self.config.auth.client_id}:{self.config.auth.client_secret}"
                self._auth_token = base64.b64encode(credentials.encode()).decode()
                self._token_expiry = datetime.now(UTC) + timedelta(
                    hours=24
                )  # Basic auth doesn't expire
                return self._auth_token
            elif self.config.auth.type == "api_key":
                # API key authentication - use as is
                self._auth_token = self.config.auth.client_id  # Use client_id as API key
                self._token_expiry = datetime.now(UTC) + timedelta(
                    hours=24
                )  # API key doesn't expire
                return self._auth_token
            elif self.config.auth.type == "bearer_token":
                return self._get_bearer_token()

            raise ServiceError(f"Unsupported authentication type: {self.config.auth.type}")

        except Exception as e:
            raise ServiceError(f"Authentication failed: {e!s}") from e

    def _get_bearer_token(self) -> str:
        """
        Get bearer token with auto-detection of refresh capability.
        If refresh credentials are provided, automatically refreshes when expired.

        Returns:
            str: Valid access token

        Raises:
            ServiceError: If authentication or refresh fails
        """
        # Check if we have a valid cached token
        if self._auth_token and self._token_expiry and datetime.now(UTC) < self._token_expiry:
            return self._auth_token

        if not self.config.auth:
            raise ServiceError("Authentication configuration is required")

        if all(
            [
                self.config.auth.token_url,
                self.config.auth.client_id,
                self.config.auth.client_secret,
                self.config.auth.refresh_token,
            ]
        ):
            self._refresh_token()
        else:
            if not self.config.auth.bearer_token:
                raise ServiceError("bearer_token is required for bearer_token authentication")
            self._auth_token = self.config.auth.bearer_token
            self._token_expiry = datetime.now(UTC) + timedelta(hours=24)

        return self._auth_token

    def _refresh_token(self) -> None:
        """Refresh access token using refresh credentials."""
        if not self.config.auth:
            raise ServiceError("Authentication configuration is required")

        payload = {
            "grant_type": "refresh_token",
            "refresh_token": self.config.auth.refresh_token,
            "client_id": self.config.auth.client_id,
            "client_secret": self.config.auth.client_secret,
        }

        content_type = getattr(self.config.auth, "token_request_content_type", "json")

        # RFC 6749 specifies form-encoded as the OAuth2 standard for token requests;
        # some providers also accept JSON as a convenience. Use token_request_content_type
        # in the source auth config to select the format the provider requires.
        if content_type == "form":
            response = self.session.post(
                url=str(self.config.auth.token_url),
                data=payload,
                timeout=self.settings.api.TIMEOUT,
            )
        else:
            response = self.session.post(
                url=str(self.config.auth.token_url),
                headers={"Content-Type": "application/json"},
                json=payload,
                timeout=self.settings.api.TIMEOUT,
            )
        response.raise_for_status()

        # Handle OAuth2 response formats and set token with expiry buffer
        token_data = response.json().get("data", response.json())
        self._auth_token = token_data["access_token"]
        expires_in = token_data.get("expires_in", 86400)
        self._token_expiry = datetime.now(UTC) + timedelta(seconds=expires_in - 300)

    def _reset_auth(self) -> None:
        """Reset authentication state to force new token acquisition."""
        self._auth_token = None
        self._token_expiry = None
        self.logger.debug("Reset authentication state")

    def _format_request_params(
        self, params: Dict[str, Any], resource: ResourceConfig
    ) -> Dict[str, Any]:
        """
        Format request parameters based on resource configuration.

        Args:
            params: Parameters to format
            resource: Resource configuration

        Returns:
            Dict[str, Any]: Formatted parameters
        """
        formatted = {}

        for key, value in params.items():
            # Skip internal parameters
            if key.startswith("_"):
                continue

            # Get input config if available (from request_inputs)
            param_config = resource.request_inputs.get(key)

            if param_config:
                # Delegate to parameter config for consistent formatting
                formatted[key] = param_config.format_request_value(value)
            else:
                # No configuration - use value as is
                formatted[key] = value

        return formatted

    def _substitute_path_parameters(self, path_template: str, path_params: Dict[str, Any]) -> str:
        """
        Substitute path parameters into URL path template.

        Args:
            path_template: Path template with placeholders (e.g., "store/{storeNbr}/gtin/{gtin}/available")
            path_params: Resolved path parameters dictionary

        Returns:
            str: Substituted path with parameter values inserted

        Raises:
            ServiceError: If any path parameter placeholder is missing a value
        """
        substituted_path = path_template

        # Find all placeholders in the path template
        placeholders = re.findall(r"\{(\w+)\}", path_template)

        # Substitute each placeholder with its value
        for placeholder in placeholders:
            if placeholder not in path_params:
                raise ServiceError(
                    f"Missing path parameter '{placeholder}' for path template '{path_template}'",
                    operation="substitute_path_parameters",
                    service_name=self.__class__.__name__,
                )
            # Replace all occurrences of {placeholder} with the value
            substituted_path = substituted_path.replace(
                f"{{{placeholder}}}", str(path_params[placeholder])
            )

        return substituted_path

    def _resolve_request_body(
        self,
        resource: ResourceConfig,
        formatted_params: Dict[str, Any],
        resolver: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """
        Resolve the request body for a POST request from body inputs only.

        Body is built from request_inputs with location=body: each key's value
        comes from formatted_params or is resolved from the input config value.
        Then exclude_from_request_body is applied.

        Args:
            resource: Resource configuration (body inputs only; no request_body)
            formatted_params: Formatted request parameters (resolved context, includes body keys)
            resolver: Optional ValueResolver instance for consistent timestamps

        Returns:
            Dict[str, Any]: Fully resolved request body ready to send
        """
        r = resolver or get_resolver(self.redis_context)
        body_inputs = resource.get_inputs_by_location("body")
        if not body_inputs:
            return {}
        overrides = {}
        for name, config in body_inputs.items():
            if name in formatted_params:
                overrides[name] = formatted_params[name]
            elif config.value is not None:
                v = config.value
                # Backfill-style: { value: template, backfill: {...} } — resolve the inner "value"
                if isinstance(v, dict) and "backfill" in v and "value" in v:
                    overrides[name] = r.resolve(v["value"])
                elif isinstance(v, str) and "{{" in v and "}}" in v:
                    overrides[name] = v
                else:
                    overrides[name] = r.resolve(v)
        return resolve_request_body(
            {},
            resolver=r,
            overrides=overrides,
            exclude_keys=resource.exclude_from_request_body,
        )

    def _make_request(
        self,
        resource: ResourceConfig,
        url: str,
        params: Dict[str, Any],
        *,
        resource_name: str,
        return_full_response: bool = False,
    ) -> Dict[str, Any] | list[Any]:
        """
        Make HTTP request for the configured resource.

        Args:
            resource: Resource configuration (path, method, request_inputs, etc.)
            url: Full URL for the request
            params: Request parameters
            resource_name: Logical resource key from configuration
            return_full_response: If True, return full JSON response; else extract and normalize per response_key.

        Returns:
            Full JSON (dict/list) if return_full_response else extracted/normalized data (list).

        Raises:
            requests.exceptions.RequestException: If the request fails
        """
        retry_count = 0
        max_retries = self.settings.api.MAX_RETRIES
        flag = resource.skip_encoding_params
        resolver = get_resolver(self.redis_context)

        while retry_count <= max_retries:
            try:
                # Get base headers and merge with endpoint-specific headers if any
                headers = self.get_headers(resolver=resolver)
                if resource.headers:
                    # Use pre-resolved endpoint headers (resolved at handler level)
                    # Convert to strings for HTTP header compatibility
                    endpoint_headers = {
                        name: str(value) if not isinstance(value, str) else value
                        for name, value in resource.headers.items()
                    }
                    headers.update(endpoint_headers)

                # Format parameters based on request type and configuration
                formatted_params = self._format_request_params(params, resource)

                request_id = str(uuid.uuid4())
                _audit_payload: Optional[Dict[str, Any]] = None

                # Log detailed request information at TRACE level (masked headers to avoid leaking secrets)
                self.logger.trace(
                    "Making API request",
                    extra_fields={
                        "request_id": request_id,
                        "method": resource.method,
                        "url": url,
                        "headers": mask_headers(headers),
                        "raw_params": params,
                        "formatted_params": formatted_params,
                        "source": self.source_name,
                        "path": resource.path,
                        "resource_name": resource_name,
                        "attempt": retry_count + 1,
                        "max_attempts": max_retries + 1,
                    },
                )

                # Make the request (GET or POST)
                if resource.method.upper() == "POST":
                    # Prepare request body using the new resolution logic
                    request_body = self._resolve_request_body(
                        resource, formatted_params, resolver=resolver
                    )

                    self.logger.trace(
                        "POST request body",
                        extra_fields={
                            "request_body": {
                                k: v[:100] if isinstance(v, str) and len(v) > 100 else v
                                for k, v in request_body.items()
                            }
                        },
                    )

                    content_type = headers.get("Content-Type", "").lower()

                    post_kwargs = {
                        "url": url,
                        "headers": headers,
                        "timeout": self.settings.api.TIMEOUT,
                    }

                    # Assign body to the correct parameter based on type
                    if content_type == "application/x-www-form-urlencoded":
                        post_kwargs["data"] = request_body
                    else:
                        post_kwargs["json"] = request_body

                    _audit_payload = request_body
                    # Make the call
                    response = self.session.post(**post_kwargs)

                else:
                    # For GET requests, optionally build URL manually to avoid encoding special characters
                    # This is necessary for RESTLi APIs that require unencoded parameters
                    if flag:
                        query_parts = []
                        for key, value in formatted_params.items():
                            if isinstance(value, (list, tuple)):
                                for item in value:
                                    query_parts.append(f"{key}={item}")
                            else:
                                query_parts.append(f"{key}={value}")
                        query_string = "&".join(query_parts)
                        url = f"{url}?{query_string}"

                    _audit_payload = formatted_params
                    response = self.session.get(
                        url,
                        params=formatted_params if not flag else None,
                        headers=headers,
                        timeout=self.settings.api.TIMEOUT,
                    )

                # Audit: API-shaped request/response trail for Delta (api_requests / api_responses).
                # The persisted `endpoint` column is the REST path from configuration (ResourceConfig.path),
                # not the pipeline "resource" YAML key—unchanged from pre-rename behavior for schema stability.
                if self.audit_recorder is not None:
                    latency_ms = int(response.elapsed.total_seconds() * 1000)
                    raw_version = getattr(response.raw, "version", None)
                    http_version_str = (
                        "1.1"
                        if raw_version == 11
                        else (
                            "2"
                            if raw_version == 20
                            else str(raw_version) if raw_version is not None else None
                        )
                    )
                    server_timing = response.headers.get("Server-Timing")
                    content_type = response.headers.get("Content-Type")
                    content_length = len(response.content)
                    upstream_time_ms = None
                    for header_name in (
                        "x-envoy-upstream-service-time",
                        "X-Response-Time",
                        "X-Process-Time",
                    ):
                        val = response.headers.get(header_name)
                        if val is not None:
                            try:
                                upstream_time_ms = int(float(val))
                                break
                            except (ValueError, TypeError):
                                pass
                    request_preview_str = request_preview_from_payload(_audit_payload)
                    masked_headers = mask_headers(headers)
                    response_headers_dict = dict(response.headers) if response.headers else {}
                    masked_response_headers = mask_headers(response_headers_dict)
                    masked_req = [k for k, v in masked_headers.items() if v == REDACTED_PLACEHOLDER]
                    masked_res = [
                        k for k, v in masked_response_headers.items() if v == REDACTED_PLACEHOLDER
                    ]
                    self.logger.trace(
                        "Header masking applied",
                        extra_fields={
                            "request_header_keys": list(headers.keys()),
                            "request_masked": masked_req,
                            "response_header_keys": list(response_headers_dict.keys()),
                            "response_masked": masked_res,
                        },
                    )
                    response_preview: Optional[str] = None
                    if content_length <= 1024 * 1024 and response.text:
                        try:
                            parsed = response.json()
                            fields = request_preview_from_payload(parsed)
                            response_preview = fields if fields else None
                        except (ValueError, TypeError):
                            pass
                    now_ts = datetime.now(UTC)
                    req_record = ApiRequestRecord(
                        request_id=request_id,
                        timestamp=now_ts,
                        method=resource.method,
                        url=redact_text(url),
                        endpoint=resource.path or "",
                        headers=masked_headers,
                        request_preview=request_preview_str,
                        source=self.source_name,
                        attempt=retry_count + 1,
                        http_version=http_version_str,
                        latency_ms=latency_ms,
                    )
                    resp_record = ApiResponseRecord(
                        request_id=request_id,
                        timestamp=now_ts,
                        status_code=response.status_code,
                        response_headers=masked_response_headers,
                        response_preview=response_preview,
                        content_length=content_length,
                        content_type=content_type,
                        upstream_time_ms=upstream_time_ms,
                        server_timing=server_timing,
                    )
                    self.audit_recorder.record_request(req_record)
                    self.audit_recorder.record_response(resp_record)

                # Log response
                self.logger.debug(
                    "Received response",
                    extra_fields={
                        "status_code": response.status_code,
                        "content_length": len(response.content),
                    },
                )
                self.logger.trace(
                    "Response data",
                    extra_fields={
                        "content_type": response.headers.get("content-type"),
                        "response_preview": response.text[:200] if len(response.text) > 0 else None,
                    },
                )

                # Handle non-200 responses
                if not response.ok:
                    error_detail = ""
                    try:
                        error_data = response.json()
                        error_detail = f": {error_data}"
                    except ValueError:
                        error_detail = f": {response.text[:200]}"

                    self.logger.error(
                        "Request failed",
                        extra_fields={
                            "status_code": response.status_code,
                            "error_detail": error_detail,
                            "method": resource.method,
                            "url": url,
                            "attempt": retry_count + 1,
                            "max_attempts": max_retries + 1,
                        },
                    )

                    # Check for auth failure
                    is_auth_failure = response.status_code == 401 and any(
                        keyword in str(error_detail).lower()
                        for keyword in ["expired", "invalid_token", "signature", "timestamp"]
                    )

                    # Handle auth failure with retry
                    if is_auth_failure:
                        if retry_count < max_retries:
                            self._reset_auth()  # Reset auth state
                            retry_count += 1
                            delay = self.settings.api.INITIAL_DELAY * (
                                self.settings.api.RETRY_BACKOFF ** (retry_count - 1)
                            )
                            self.logger.info(
                                "Auth failed - will retry with fresh credentials",
                                extra_fields={
                                    "attempt": retry_count,
                                    "max_attempts": max_retries + 1,
                                    "delay": delay,
                                },
                            )
                            time.sleep(delay)
                            continue  # Retry with fresh auth
                        else:
                            self.logger.error(
                                "Max retries exceeded for auth failure",
                                extra_fields={
                                    "attempts": retry_count + 1,
                                    "max_attempts": max_retries + 1,
                                },
                            )

                    # For non-auth failures or if we're out of retries, raise
                    response.raise_for_status()

                # Log successful retry if this was a retry attempt
                if retry_count > 0:
                    self.logger.debug(
                        "Successfully recovered from auth failure",
                        extra_fields={
                            "method": resource.method,
                            "url": url,
                            "attempts_used": retry_count,
                            "max_attempts": max_retries + 1,
                        },
                    )

                # Parse and return response data
                try:
                    data = response.json()
                except ValueError as e:
                    self.logger.error(
                        "Failed to parse JSON response",
                        extra_fields={
                            "error": str(e),
                            "content_type": response.headers.get("content-type"),
                            "response_preview": (
                                response.text[:200] if len(response.text) > 0 else None
                            ),
                        },
                    )
                    raise ServiceError(f"Invalid JSON response: {e!s}") from e

                if return_full_response:
                    return data

                # Handle response data extraction and return
                if resource.response_key and isinstance(data, dict):
                    out, missing = dict_response_key_to_records(data, resource.response_key)
                    if missing:
                        # For iteration scenarios, some combinations may have no data
                        # Return empty list instead of throwing error
                        self.logger.debug(
                            f"Response key '{resource.response_key}' not found in response - returning empty data",
                            extra_fields={
                                "path": resource.path,
                                "resource_name": resource_name,
                                "response_keys": (
                                    list(data.keys()) if isinstance(data, dict) else None
                                ),
                                "response_code": (
                                    data.get("code") if isinstance(data, dict) else None
                                ),
                                "response_message": (
                                    data.get("message") if isinstance(data, dict) else None
                                ),
                            },
                        )
                        return []
                    return out
                elif isinstance(data, list):
                    return data
                else:
                    return [data] if data else []

            except requests.exceptions.RequestException as e:
                # Only retry auth failures - let the session handle other retries
                if (
                    retry_count < max_retries
                    and isinstance(e, requests.exceptions.HTTPError)
                    and e.response.status_code == 401
                ):
                    retry_count += 1
                    delay = self.settings.api.INITIAL_DELAY * (
                        self.settings.api.RETRY_BACKOFF ** (retry_count - 1)
                    )
                    self.logger.warning(
                        "Auth failed - will retry with fresh credentials",
                        extra_fields={
                            "attempt": retry_count,
                            "max_attempts": max_retries + 1,
                            "delay": delay,
                            "error": str(e),
                        },
                    )
                    time.sleep(delay)
                    continue

                self.logger.error(
                    "Request failed",
                    extra_fields={
                        "error": str(e),
                        "method": resource.method,
                        "url": url,
                        "attempt": retry_count + 1,
                    },
                )
                raise  # Let other errors propagate

        # Safety check - should never get here due to loop condition
        raise ServiceError(
            message="Max retries exceeded",
            operation=f"{resource.method} {url}",
            service_name=self.__class__.__name__,
            is_retryable=False,
            details={"attempts": retry_count + 1, "max_attempts": max_retries + 1},
        )

    def _resolve_resource_request(
        self, resource_name: str, parameters: Optional[Dict[str, Any]] = None
    ) -> tuple[ResourceConfig, str, Dict[str, Any]]:
        """
        Resolve resource_name to ResourceConfig, parameters by location, and build request URL.

        Returns:
            Tuple of (resource, url, params_for_request).
        """
        if resource_name not in self.config.resources:
            raise ServiceError(f"Resource not found in configuration: {resource_name}")

        resource = self.config.resources[resource_name]

        path_params = resource.resolve_parameters(
            redis_context=self.redis_context,
            param_dict=resource.get_inputs_by_location("path"),
            params=parameters,
        )
        query_params = resource.resolve_parameters(
            redis_context=self.redis_context,
            param_dict=resource.get_inputs_by_location("query"),
            params=parameters,
        )

        if resource.method.upper() == "POST":
            body_params = {}
            if parameters:
                for body_key, body_config in resource.get_inputs_by_location("body").items():
                    if body_key in parameters:
                        body_params[body_key] = body_config.format_request_value(
                            parameters[body_key]
                        )
            params_for_request = {**query_params, **body_params}
        else:
            params_for_request = query_params

        path = resource.path
        if path_params:
            path = self._substitute_path_parameters(path, path_params)

        url = join_http_base_and_path(str(self.config.base_url), path)
        return resource, url, params_for_request

    def fetch_data(
        self,
        resource_name: str,
        parameters: Optional[Dict[str, Any]] = None,
        *,
        full_response: bool = False,
    ):
        """
        Fetch data for the configured resource.

        Args:
            resource_name: Name of the resource to run (YAML key for REST).
            parameters: Optional parameters for the request (path, query, body).
            full_response: If True, return full JSON response (no response_key extraction).
                Default False (extract and normalize per resource.response_key).

        Returns:
            Full JSON (dict/list) if full_response=True, else extracted/normalized data (list).

        Raises:
            ServiceError: If the request fails
        """
        try:
            resource, url, params_for_request = self._resolve_resource_request(
                resource_name, parameters
            )
            return self._make_request(
                resource,
                url,
                params_for_request,
                resource_name=resource_name,
                return_full_response=full_response,
            )
        except Exception as e:
            res = self.config.resources.get(resource_name)
            method = res.method if res else "GET"
            raise ServiceError(
                "Request failed", operation=f"{method} {resource_name}", original_error=e
            ) from e
