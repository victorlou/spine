"""
Utilities for resolving dynamic values at runtime.

Supports two syntaxes:
- Load-time: `${VAR}` / `${VAR:-default}` (resolved in config_loader)
- Runtime: `{{ expr }}` (Jinja2) for most values; flat DATE and SOURCE/PAGINATION use dict structure
"""

import base64
import os
import time
import uuid
from datetime import UTC, datetime, timedelta
from enum import Enum
from typing import Annotated, Any, Dict, List, Literal, Optional, Union

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from databricks.sdk import WorkspaceClient
from jinja2 import Environment, StrictUndefined
from pydantic import BaseModel, Field, ValidationInfo, field_validator

from src.utils.exceptions import ResolverError
from src.utils.logger import get_logger
from src.utils.query_utils import format_query_ref_key
from src.utils.redis_context import RedisContextManager


class DynamicValueType(str, Enum):
    """Types of dynamic values that can be resolved at runtime."""

    NOW_UNIX = "NOW_UNIX"  # Current time as Unix timestamp
    NOW_ISO = "NOW_ISO"  # Current time as ISO 8601 string
    NOW_MS = "NOW_MS"  # Millisecond timestamp
    UUID = "UUID"  # Generates UUID v4
    RSA_SIGN = "RSA_SIGN"  # RSA signature with configurable inputs
    DATE = "DATE"  # Dynamic date with optional offset
    DATABRICKS = "DATABRICKS"  # Delta Table from Databricks
    SOURCE = "SOURCE"  # Dynamic source value resolved from
    PAGINATION = "PAGINATION"  # Pagination configuration for page parameters


class DateOperation(str, Enum):
    """Types of date operations supported."""

    TODAY = "TODAY"
    DAYS_AGO = "DAYS_AGO"
    DAYS_FUTURE = "DAYS_FUTURE"
    LINKEDIN_PREVIOUS_MONTH_RANGE = (
        "LINKEDIN_PREVIOUS_MONTH_RANGE"  # Full previous month as date range string
    )
    MONTH_START = "MONTH_START"
    MONTH_END = "MONTH_END"
    PREVIOUS_MONTH_START = "PREVIOUS_MONTH_START"  # First day of the previous month
    PREVIOUS_MONTH_END = "PREVIOUS_MONTH_END"  # Last day of the previous month
    PREVIOUS_SUNDAY = "PREVIOUS_SUNDAY"  # Sunday of the previous week
    PREVIOUS_SATURDAY = "PREVIOUS_SATURDAY"  # Saturday of the previous week


class DateConfig(BaseModel):
    """Configuration for a dynamic date value."""

    operation: DateOperation
    days: Optional[int] = 0  # Number of days for offset operations
    format: Optional[str] = "%Y-%m-%d"  # Optional date format (YYYY-MM-DD by default)


class DatabricksDeltaTableConfig(BaseModel):
    """Configuration for a Databricks Delta Table dynamic value."""

    query_ref: str


class FilterOperator(str, Enum):
    """Supported filter operations."""

    EQUALS = "eq"
    NOT_EQUALS = "neq"
    GREATER_THAN = "gt"
    LESS_THAN = "lt"
    GREATER_EQUALS = "gte"
    LESS_EQUALS = "lte"
    CONTAINS = "contains"
    IN = "in"


class FilterType(str, Enum):
    """Types of filtering supported."""

    COLUMN = "column"
    PARAMS = "params"
    EXPRESSION = "expression"


class FilterValueSource(BaseModel):
    """Configuration for parameter-based value sources."""

    source: str
    field: str


class FilterConfig(BaseModel):
    """
    Enhanced filter configuration supporting multiple filter types.

    Three types of filtering are supported:
    1. Column filtering: Direct filtering on DataFrame columns
    2. Params filtering: Filter using the _params column (for parent context)
    3. Expression filtering: Raw SQL-like filter expressions
    """

    type: FilterType = FilterType.COLUMN
    field: str  # Field to filter on or full expression for type=expression
    operator: FilterOperator = FilterOperator.EQUALS
    value_source: Union[str, FilterValueSource]  # Parameter name, static value, or source config
    value_type: Literal["static", "parameter"] = "parameter"

    @field_validator("operator")
    @classmethod
    def validate_operator_for_type(cls, v: FilterOperator, info: ValidationInfo) -> FilterOperator:
        """Ensure operator is only specified for column filtering."""
        if (
            "type" in info.data
            and info.data["type"] != FilterType.COLUMN
            and v != FilterOperator.EQUALS
        ):
            raise ValueError("operator can only be specified for column filtering")
        return v

    @property
    def is_params_filter(self) -> bool:
        """Whether this filter uses parent context (_params column)."""
        return self.type == FilterType.PARAMS

    @property
    def params_key(self) -> Optional[str]:
        """Get the _params key for parent context filtering."""
        if self.is_params_filter and isinstance(self.value_source, FilterValueSource):
            return f"{self.value_source.source}__{self.value_source.field}"
        return None


class SourceConfig(BaseModel):
    """Configuration for resolving values from other sources (resources)."""

    source: str  # Source endpoint for dynamic values
    field: str  # Field to extract from source
    filter: Optional[FilterConfig] = None  # Optional filter to apply when resolving


class DynamicValue(BaseModel):
    """Configuration for a dynamic value."""

    dynamic_value: DynamicValueType
    description: Optional[str] = None


class ComplexDynamicValue(BaseModel):
    """Configuration for a complex dynamic value with parameters."""

    type: DynamicValueType
    inputs: Optional[Dict[str, Any]] = None
    key: Optional[str] = None
    algorithm: Optional[str] = None
    date_config: Optional[DateConfig] = None  # Added for date operations
    databricks_config: Optional[DatabricksDeltaTableConfig] = (
        None  # Added for databricks delta table operations
    )
    source_config: Optional[SourceConfig] = (
        None  # Added for resolving values from other sources (resources)
    )
    pagination_config: Optional[Dict[str, Any]] = (
        None  # Added for pagination configuration (PaginationConfig as dict to avoid circular import)
    )


class DynamicValueResolver:
    """Resolves dynamic values with consistent timestamps within a resolution cycle."""

    def __init__(self, redis_context: RedisContextManager):
        """
        Initialize resolver with current timestamp and context.

        Args:
            redis_context: Redis context manager for data retrieval
            source_name: Name of the current source (used for relative endpoint references)
        """
        self._now = datetime.now(UTC)
        self._now_ts = int(self._now.timestamp())
        self._now_ms = int(time.time() * 1000)
        self._logger = get_logger(__name__)
        self.databricks_client: WorkspaceClient | None = None
        self.redis_context = redis_context

    def get_timestamp(self, type: DynamicValueType) -> str:
        """Get consistent timestamp of specified type."""
        if type == DynamicValueType.NOW_UNIX:
            return str(self._now_ts)
        elif type == DynamicValueType.NOW_ISO:
            return self._now.isoformat()
        elif type == DynamicValueType.NOW_MS:
            return str(self._now_ms)
        raise ValueError(f"Not a timestamp type: {type}")

    def get_date(self, config: DateConfig) -> str:
        """
        Get a date string based on the configuration.

        Args:
            config: Date configuration specifying operation and offset

        Returns:
            str: Date in YYYY-MM-DD format
        """
        if config.operation == DateOperation.TODAY:
            base_date = self._now
        elif config.operation == DateOperation.DAYS_AGO:
            base_date = self._now - timedelta(days=config.days or 0)
        elif config.operation == DateOperation.DAYS_FUTURE:
            base_date = self._now + timedelta(days=config.days or 0)
        elif config.operation == DateOperation.LINKEDIN_PREVIOUS_MONTH_RANGE:
            # Get previous month date range in format: (start:(day:D,month:M,year:Y),end:(day:D,month:M,year:Y))
            first_of_current = self._now.replace(day=1)
            last_of_previous = first_of_current - timedelta(days=1)  # Last day of previous month
            first_of_previous = last_of_previous.replace(day=1)  # First day of previous month

            start_str = f"start:(day:{first_of_previous.day},month:{first_of_previous.month},year:{first_of_previous.year})"
            end_str = f"end:(day:{last_of_previous.day},month:{last_of_previous.month},year:{last_of_previous.year})"
            return f"({start_str},{end_str})"
        elif config.operation == DateOperation.MONTH_START:
            base_date = self._now.replace(day=1, hour=0, minute=0, second=0)
        elif config.operation == DateOperation.MONTH_END:
            # move to first of next month, then back one second to end of current month
            next_month = self._now.replace(month=self._now.month % 12 + 1, day=1)
            base_date = next_month - timedelta(seconds=1)
        elif config.operation == DateOperation.PREVIOUS_MONTH_START:
            first_of_current = self._now.replace(day=1)
            last_of_previous = first_of_current - timedelta(days=1)
            base_date = last_of_previous.replace(day=1, hour=0, minute=0, second=0)
        elif config.operation == DateOperation.PREVIOUS_MONTH_END:
            first_of_current = self._now.replace(day=1)
            base_date = first_of_current - timedelta(seconds=1)
        elif config.operation == DateOperation.PREVIOUS_SUNDAY:
            # Calculate the most recent Sunday (start of current week)
            # weekday(): Mon=0, ..., Sun=6
            days_since_sunday = (self._now.weekday() + 1) % 7
            current_week_sunday = self._now - timedelta(days=days_since_sunday)

            # Previous week's Sunday is 7 days before current week's Sunday
            previous_week_start = current_week_sunday - timedelta(days=7)
            return previous_week_start.strftime("%Y-%m-%d")

        elif config.operation == DateOperation.PREVIOUS_SATURDAY:
            # Calculate the most recent Sunday (start of current week)
            days_since_sunday = (self._now.weekday() + 1) % 7
            current_week_sunday = self._now - timedelta(days=days_since_sunday)

            # Previous week's Sunday is 7 days before current week's Sunday
            previous_week_start = current_week_sunday - timedelta(days=7)
            # Previous week's Saturday is 6 days after previous week's Sunday
            previous_week_end = previous_week_start + timedelta(days=6)

            return previous_week_end.strftime("%Y-%m-%d")

        else:
            raise ValueError(f"Unsupported date operation: {config.operation}")

        return base_date.strftime(config.format or "%Y-%m-%d")

    def resolve_databricks_delta_table_value(
        self, databricks_delta_table_config: DatabricksDeltaTableConfig
    ) -> Any:
        """
        Handle the Databricks Delta Table dynamic value resolution.

        Validates the configuration and delegates to _process_databricks_config
        for execution. This is the entry point for resolving DATABRICKS type
        dynamic values.

        Args:
            databricks_delta_table_config: Databricks Delta Table configuration
                                          containing required catalog, schema, and table names

        Returns:
            list: Aggregated result data from the Databricks query execution

        Raises:
            ValueError: If required configuration parameters (catalog, schema, table) are missing
                       or if query execution fails
        """
        if not databricks_delta_table_config.query_ref:
            raise ValueError("DATABRICKS requires query_ref parameter")

        query_ref = databricks_delta_table_config.query_ref

        formatted_query_ref_key = format_query_ref_key(query_ref)

        if self.redis_context:
            cached_result = self.redis_context.get(key=formatted_query_ref_key)

            return cached_result

        raise ResolverError(
            f"Redis Context is required to resolve Databricks Delta Table value for query_ref: {query_ref}",
            details={"query_ref": query_ref, "key": formatted_query_ref_key},
            operation="DynamicValueResolver.resolve_databricks_delta_table_value",
        )

    def compute_rsa_signature(self, inputs: List[str], key: str, algorithm: str = "SHA256") -> str:
        """
        Compute RSA signature for the given inputs.

        Args:
            inputs: List of input strings to sign
            key: Private key (base64 encoded or PEM format)
            algorithm: Hash algorithm to use

        Returns:
            str: Base64 encoded signature
        """
        # Join inputs with newlines and add trailing newline
        input_data = "\n".join(inputs) + "\n"

        self._logger.trace(
            "Computing RSA signature",
            extra_fields={"input_data": input_data, "algorithm": algorithm},
        )

        try:
            # Try base64 decode first, fallback to assuming PEM format
            try:
                decoded_key = base64.b64decode(key).decode("utf-8")
            except Exception:
                decoded_key = key

            # Load the private key
            private_key = serialization.load_pem_private_key(
                decoded_key.encode(), password=None, backend=default_backend()
            )

            # Ensure it's an RSA private key
            if not isinstance(private_key, rsa.RSAPrivateKey):
                raise ValueError("Only RSA private keys are supported for signing")

            # Sign the input data
            signature = private_key.sign(
                input_data.encode(), padding.PKCS1v15(), getattr(hashes, algorithm)()
            )

            # Base64 encode the signature
            encoded_signature = base64.b64encode(signature).decode()

            signature_preview = (
                encoded_signature[:50] + "..." if len(encoded_signature) > 50 else encoded_signature
            )
            self._logger.trace(
                "Generated signature",
                extra_fields={
                    "signature_length": len(encoded_signature),
                    "signature_preview": signature_preview,
                },
            )

            return encoded_signature

        except Exception as e:
            raise ValueError(f"Failed to compute RSA signature: {e!s}") from e

    def resolve(self, value: Any) -> Any:
        """
        Resolve a dynamic value to its concrete value at runtime.

        Args:
            value: The value to resolve, either a static value or a dynamic value

        Returns:
            Any: The resolved value
        """

        # Handle simple static values
        if not isinstance(value, (str, dict, DynamicValue, ComplexDynamicValue)):
            return value

        # Handle dictionary format for complex values
        if isinstance(value, dict) and not isinstance(value, (DynamicValue, ComplexDynamicValue)):
            # Handle flat DATE format (type, operation, days at top level) - used by request_body backfill
            if "type" in value:
                try:
                    dynamic_type = DynamicValueType(value["type"].upper())
                    if dynamic_type == DynamicValueType.DATE:
                        date_config = DateConfig(
                            operation=value.get("operation", "today"),
                            days=value.get("days", 0),
                            format=value.get("format"),
                        )
                        return self.get_date(date_config)
                except ValueError:
                    pass

            # Parse as ComplexDynamicValue for SOURCE and PAGINATION (structure only; resolution elsewhere)
            try:
                parsed_value = ComplexDynamicValue(**value)
                value = parsed_value
            except (ValueError, TypeError):
                pass

        # Handle ComplexDynamicValue: PAGINATION returns initial page, SOURCE falls through
        if isinstance(value, ComplexDynamicValue):
            if value.type == DynamicValueType.PAGINATION:
                return 1

        return value


class EnvProxy:
    """Proxy for environment variables in Jinja templates. Use env.VAR_NAME or env['VAR_NAME']."""

    def __getattr__(self, name: str) -> str:
        return os.environ.get(name, "")

    def __getitem__(self, name: str) -> str:
        return os.environ.get(name, "")

    def get(self, name: str, default: str = "") -> str:
        return os.environ.get(name, default)


class JinjaValueResolver:
    """
    Resolves {{ expr }} template strings at runtime using Jinja2.

    Provides built-in functions: now_iso, now_ms, now_unix, uuid, date,
    databricks, rsa_sign. Uses env for environment variables.
    """

    def __init__(
        self,
        redis_context: Optional[RedisContextManager] = None,
        *,
        legacy_resolver: Optional[DynamicValueResolver] = None,
    ):
        if legacy_resolver is not None:
            self._legacy_resolver = legacy_resolver
        else:
            if redis_context is None:
                raise ValueError("Either redis_context or legacy_resolver must be provided")
            self._legacy_resolver = DynamicValueResolver(redis_context=redis_context)
        self._logger = get_logger(__name__)
        self._env = self._build_jinja_environment()

    def _build_jinja_environment(self) -> Environment:
        """Build Jinja environment with custom globals."""
        env = Environment(
            undefined=StrictUndefined,
            trim_blocks=True,
            lstrip_blocks=True,
        )

        def now_iso() -> str:
            return self._legacy_resolver.get_timestamp(DynamicValueType.NOW_ISO)

        def now_ms() -> str:
            return self._legacy_resolver.get_timestamp(DynamicValueType.NOW_MS)

        def now_unix() -> str:
            return self._legacy_resolver.get_timestamp(DynamicValueType.NOW_UNIX)

        def uuid_func() -> str:
            return str(uuid.uuid4())

        def date(
            operation: str,
            days: int = 0,
            format: Optional[str] = None,
        ) -> str:
            op_name = operation.upper().replace("-", "_")
            op = DateOperation(op_name)
            config = DateConfig(operation=op, days=days, format=format)
            return self._legacy_resolver.get_date(config)

        def databricks(query_ref: str) -> Any:
            config = DatabricksDeltaTableConfig(query_ref=query_ref)
            return self._legacy_resolver.resolve_databricks_delta_table_value(config)

        def rsa_sign(
            key: str,
            inputs: list,
            algorithm: str = "SHA256",
        ) -> str:
            resolved_inputs = [str(x) for x in inputs]
            return self._legacy_resolver.compute_rsa_signature(
                inputs=resolved_inputs, key=key, algorithm=algorithm
            )

        env.globals.update(
            now_iso=now_iso,
            now_ms=now_ms,
            now_unix=now_unix,
            uuid=uuid_func,
            date=date,
            databricks=databricks,
            rsa_sign=rsa_sign,
            env=EnvProxy(),
        )
        return env

    def resolve(self, template: str, context: Optional[Dict[str, Any]] = None) -> Any:
        """
        Render a Jinja template string to its resolved value.

        When the template is a single expression (e.g. "{{ databricks('x') }}"), uses
        compile_expression to preserve the raw return type (list, etc.). For mixed
        templates (e.g. "prefix_{{ now_iso() }}") returns a string.

        Args:
            template: String containing {{ expr }} (e.g. "{{ now_iso() }}")
            context: Optional extra context for the template

        Returns:
            Resolved value (raw type for single expr, str for mixed templates)
        """
        if not isinstance(template, str) or "{{" not in template or "}}" not in template:
            return template

        ctx = context or {}
        stripped = template.strip()

        # Single expression: preserve raw return type (e.g. list from databricks())
        if stripped.startswith("{{") and stripped.endswith("}}"):
            inner = stripped[2:-2].strip()
            if inner and "{{" not in inner:
                try:
                    expr = self._env.compile_expression(inner)
                    return expr(**ctx)
                except Exception:
                    pass  # Fall through to render

        try:
            t = self._env.from_string(template)
            return t.render(**ctx)
        except Exception as e:
            self._logger.error(
                "Failed to resolve Jinja template",
                extra_fields={"template": template[:200], "error": str(e)},
            )
            raise


class ValueResolver:
    """
    Resolves dynamic values (Jinja or legacy) with one timestamp per cycle.
    Use get_resolver(redis_context), then resolve(value) or resolve_headers_dict().
    """

    def __init__(self, redis_context: RedisContextManager):
        self._legacy_resolver = DynamicValueResolver(redis_context=redis_context)
        self._jinja_resolver = JinjaValueResolver(legacy_resolver=self._legacy_resolver)

    def resolve(self, value: Any, context: Optional[Dict[str, Any]] = None) -> Any:
        # Handle value-based format (e.g. {value: "{{ date('...') }}", backfill: {...}})
        # Resolve inner value with this same resolver so one timestamp per cycle is preserved
        if isinstance(value, dict) and "value" in value and "type" not in value:
            return self.resolve(value["value"], context=context)
        if isinstance(value, str) and "{{" in value and "}}" in value:
            return self._jinja_resolver.resolve(value, context=context)
        return self._legacy_resolver.resolve(value)


def get_resolver(redis_context: RedisContextManager) -> ValueResolver:
    """Return a ValueResolver that shares one timestamp per cycle."""
    return ValueResolver(redis_context=redis_context)


def resolve_headers_dict(
    d: Dict[str, Any],
    redis_context: Optional[RedisContextManager] = None,
    resolver: Optional["ValueResolver"] = None,
) -> Dict[str, Any]:
    """Resolve dict with one resolver. Provide resolver or redis_context."""
    if resolver is not None:
        r = resolver
    elif redis_context is not None:
        r = get_resolver(redis_context)
    else:
        raise ValueError("Either redis_context or resolver must be provided")
    return {k: r.resolve(v) for k, v in d.items()}


def resolve_request_body(
    raw_body: Dict[str, Any],
    resolver: "ValueResolver",
    overrides: Optional[Dict[str, Any]] = None,
    exclude_keys: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Resolve a request body template using two-pass resolution (service-agnostic).

    Pass 1: Resolve non-Jinja values (dicts, statics). Jinja strings are skipped
    because they may reference other body values.
    Merge overrides into the result (e.g. backfill dates from params or context).
    Pass 2: Resolve Jinja strings with body context (resolved values from Pass 1
    plus overrides are now available).
    Optionally strip keys listed in exclude_keys (e.g. backfill-only fields).

    Callers: rest_service uses this with formatted_params and exclude_from_request_body;
    dynamic_handler uses it with context-based overrides and exclude_keys=None.

    Args:
        raw_body: Request body template (e.g. endpoint.request_body).
        resolver: ValueResolver instance (e.g. from get_resolver(redis_context)).
        overrides: Dict to merge after Pass 1. None treated as {}.
        exclude_keys: Keys to remove from the result before returning. None = do not strip.

    Returns:
        Fully resolved request body.
    """
    overrides = overrides or {}
    if not raw_body:
        # Body built only from overrides (e.g. body inputs)
        request_body = dict(overrides)
        # Resolve Jinja in any overrides value
        body_context = {k: v for k, v in request_body.items() if isinstance(v, str)}
        for k, v in list(request_body.items()):
            if isinstance(v, str) and "{{" in v and "}}" in v:
                request_body[k] = resolver.resolve(v, context=body_context)
    else:
        request_body = {}
        # Pass 1: Resolve non-Jinja values
        for k, v in raw_body.items():
            if isinstance(v, str) and "{{" in v and "}}" in v:
                continue  # Jinja strings resolved in Pass 2
            request_body[k] = resolver.resolve(v)
        request_body.update(overrides)
        # Pass 2: Resolve Jinja strings with body context
        body_context = {k: v for k, v in request_body.items() if isinstance(v, str)}
        for k, v in raw_body.items():
            if isinstance(v, str) and "{{" in v and "}}" in v:
                request_body[k] = resolver.resolve(v, context=body_context)
        # Also ensure overriding Jinja values are resolved
        for k, v in overrides.items():
            if isinstance(v, str) and "{{" in v and "}}" in v:
                request_body[k] = resolver.resolve(v, context=body_context)

    if exclude_keys:
        for key in exclude_keys:
            request_body.pop(key, None)

    return request_body


# Maintain backward compatibility
# Dict[str, Any] allows nested dictionaries with flexible field types (including dynamic values)
# union_mode="left_to_right" ensures that Pydantic tries to match types in the order they are defined unlike the default behavior 'smart' which may lead to unexpected type resolution
DynamicOrStaticValue = Annotated[
    Union[str, int, float, bool, DynamicValue, ComplexDynamicValue, Dict[str, Any], List[Any]],
    Field(union_mode="left_to_right"),
]
