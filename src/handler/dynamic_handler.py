"""
Configuration-driven handler for orchestrating data ingestion.
"""

import os
import signal
from datetime import UTC, datetime
from typing import Any, Dict, List, Optional, Set, Union

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

from src.audit import AuditRecorder
from src.collector import (
    DiskStreamingDataCollector,
    RawDataBatch,
    RawDataCollector,
    StreamingRawDataCollector,
)
from src.config.config_models import (
    BatchSizeMode,
    InputConfig,
    PaginationConfig,
    PaginationType,
    ResourceConfig,
    SourceConfig,
    SourceType,
)
from src.config.settings import Settings
from src.handler.base_handler import BaseHandler, HandlerError
from src.loader.loader_factory import LoaderFactory
from src.parser.spark_parser import SparkParser
from src.planner.execution_plan import ExecutionPlan, ResourceMetadata
from src.service.service_factory import ServiceFactory
from src.utils.backfill_dates import generate_backfill_date_pairs
from src.utils.data_utils import (
    build_parent_context_from_parameters,
    get_nested_value,
    set_nested_value,
)
from src.utils.dynamic_values import (
    ComplexDynamicValue,
    DynamicOrStaticValue,
    DynamicValue,
    DynamicValueType,
    FilterConfig,
    FilterOperator,
    FilterType,
    FilterValueSource,
    ValueResolver,
    get_resolver,
    resolve_request_body,
)
from src.utils.exceptions import GracefulShutdownError
from src.utils.redis_context import RedisContextManager
from src.utils.snapshot_poller import (
    SnapshotError,
    SnapshotPoller,
    SnapshotTimeoutError,
)


def _sigterm_handler(signum: int, frame: Any) -> None:
    """Raise GracefulShutdownError so handle()'s finally block runs and audit is flushed."""
    raise GracefulShutdownError("Pipeline terminated (SIGTERM)")


class DynamicHandler(BaseHandler):
    """Handler that uses configuration to orchestrate data ingestion."""

    def __init__(
        self,
        settings: Settings,
        selection: Optional[Dict[str, Optional[Set[str]]]] = None,
        record_limit: Optional[int] = None,
        backfill_mode: bool = False,
    ):
        """
        Initialize the configuration handler.

        Args:
            settings: Application settings instance containing pipeline configuration
            selection: Optional selection structure mapping source names to resource name sets.
                       None means all resources, Set[str] means specific resource names.
            record_limit: Optional limit on fetch operations per resource (for development/testing)
            backfill_mode: If True, use backfill date ranges for resources that have backfill config.
        """
        super().__init__(
            parser=None,
            loader=None,
            destination=None,
            max_retries=settings.api.MAX_RETRIES,
            retry_delay=settings.api.INITIAL_DELAY,
            retry_backoff=settings.api.RETRY_BACKOFF,
        )
        self.settings = settings
        self.config = settings.pipeline_config
        self.record_limit = record_limit
        self.backfill_mode = backfill_mode
        self.spark_manager = None
        self.spark: SparkSession | None = None
        self._setup_spark()

        # Initialize Redis context
        if self.config.defaults.context.type == "redis" and self.config.defaults.context.redis:
            redis_config = self.config.defaults.context.redis.model_dump()
            self.redis_context = RedisContextManager(
                redis_config=redis_config,
                prefix=self.config.defaults.context.prefix,
                default_ttl=self.config.defaults.context.ttl,
            )
            self.logger.debug(
                "Initialized Redis context",
                extra_fields={
                    "prefix": self.config.defaults.context.prefix,
                    "ttl": self.config.defaults.context.ttl,
                },
            )
        else:
            raise HandlerError(
                f"Unsupported context type: {self.config.defaults.context.type}. "
                "Only 'redis' is currently supported."
            )

        # Create execution plan
        self.execution_plan = ExecutionPlan(self.config, self.redis_context, selection=selection)

    def _apply_request_limit(
        self,
        items: List[Any],
        limit: Optional[int],
        item_type: str,
        source_name: str,
        resource_name: str,
    ) -> List[Any]:
        """
        Apply request limit to a list of items.

        Args:
            items: List of items to limit
            limit: Optional limit on number of items
            item_type: Type description for logging (e.g., "request contexts", "batch values")
            source_name: Source name for logging
            resource_name: Resource name for logging

        Returns:
            List[Any]: Limited list of items
        """
        if limit is None:
            return items

        original_count = len(items)
        limited_items = items[:limit]

        if len(limited_items) < original_count:
            log_message = f"Limited {item_type} from {original_count} to {len(limited_items)} for {limit} limit"

            extra_fields = {
                "source": source_name,
                "resource_name": resource_name,
                "original_count": original_count,
                "limited_count": len(limited_items),
                "limit": limit,
            }

            self.logger.info(log_message, extra_fields=extra_fields)

        return limited_items

    def _parse_parent_resource_ref(
        self, source_endpoint: str, current_source: str
    ) -> tuple[str, str]:
        """
        Parse a parent resource reference to extract parent source name and resource name.

        Args:
            source_endpoint: Reference string (format: "source.resource" or "resource" within current source)
            current_source: Current source name (used as default if not in source_endpoint)

        Returns:
            tuple[str, str]: (parent_source_name, parent_resource_name)
        """
        if "." in source_endpoint:
            parent_source_name, parent_resource = source_endpoint.split(".")
        else:
            parent_source_name = current_source
            parent_resource = source_endpoint
        return parent_source_name, parent_resource

    def _get_value_resolver(self) -> ValueResolver:
        """Return the cycle's resolver (set at run start) or create one."""
        return getattr(self, "_current_value_resolver", None) or get_resolver(self.redis_context)

    def _resolve_resource_header_values(
        self, headers: Dict[str, DynamicOrStaticValue], source_name: str, resource_name: str
    ) -> Dict[str, Any]:
        """
        Resolve resource-specific header values that have source config.

        Headers with source config are resolved from parent resources.
        This allows headers to be dynamically populated from other upstream resource data.

        Args:
            headers: Dictionary of header configurations
            source_name: Source name for context
            resource_name: Resource name for context

        Returns:
            Dict[str, Any]: Headers with source config values resolved

        Raises:
            HandlerError: If header resolution fails
        """
        resolved_headers = {}
        resolver = self._get_value_resolver()

        for header_name, header_value in headers.items():
            # Check if this header has a source config
            if (
                header_value is not None
                and isinstance(header_value, ComplexDynamicValue)
                and (
                    header_value.type == DynamicValueType.SOURCE
                    and header_value.source_config is not None
                )
            ):
                try:
                    # Resolve the header value from the upstream resource reference
                    source_endpoint = f"{header_value.source_config.source}"
                    parent_source_name, parent_resource = self._parse_parent_resource_ref(
                        source_endpoint, source_name
                    )

                    redis_key = self._get_redis_key(parent_source_name, parent_resource)
                    source_data = self.redis_context.get(redis_key, spark=self.spark)

                    if source_data is None:
                        raise HandlerError(
                            f"Required data from parent ref '{source_endpoint}' not found for header '{header_name}'",
                            details={
                                "header": header_name,
                                "source": source_endpoint,
                                "resource_name": resource_name,
                            },
                        )

                    # Extract the field value from source data
                    # Headers must have a single value, not multiple
                    field = header_value.source_config.field
                    filter_config = header_value.source_config.filter

                    if isinstance(source_data, DataFrame):
                        # Apply filtering if configured
                        if filter_config:
                            source_data = self._apply_parameter_filter(
                                df=source_data,
                                input_name=header_name,
                                filter_config=filter_config,
                                filter_value=None,
                            )

                        # Extract from DataFrame - get only the first value
                        if field not in source_data.columns:
                            raise ValueError(
                                f"Field {field} not found in DataFrame for header '{header_name}'. "
                                f"Available columns: {source_data.columns}"
                            )
                        df_filtered = source_data.filter(col(field).isNotNull())
                        values = df_filtered.select(field).collect()
                        if values:
                            # Use only the first value for header (headers must be single-valued)
                            resolved_headers[header_name] = str(values[0][field])
                        else:
                            raise HandlerError(
                                f"No values found in field '{field}' for header '{header_name}'",
                                details={
                                    "header": header_name,
                                    "field": field,
                                    "resource_name": resource_name,
                                },
                            )
                    elif isinstance(source_data, list):
                        # Extract from list - get only the first value
                        if source_data and isinstance(source_data[0], dict):
                            if field not in source_data[0]:
                                raise ValueError(
                                    f"Field {field} not found in list data for header '{header_name}'. "
                                    f"Available fields: {list(source_data[0].keys())}"
                                )
                            # Use only the first item's value for header (headers must be single-valued)
                            resolved_headers[header_name] = str(source_data[0][field])
                        else:
                            raise HandlerError(
                                f"Invalid list data format for header '{header_name}'",
                                details={"header": header_name, "resource_name": resource_name},
                            )
                    else:
                        raise HandlerError(
                            f"Unsupported data type for header '{header_name}': {type(source_data)}",
                            details={
                                "header": header_name,
                                "type": type(source_data).__name__,
                                "resource_name": resource_name,
                            },
                        )

                    self.logger.trace(
                        f"Resolved resource header '{header_name}' from source '{source_endpoint}'",
                        extra_fields={
                            "header": header_name,
                            "source": source_endpoint,
                            "field": field,
                            "resource_name": resource_name,
                            "resolved_value": str(resolved_headers[header_name])[:100],
                        },
                    )

                except HandlerError:
                    raise
                except Exception as e:
                    raise HandlerError(
                        f"Failed to resolve header '{header_name}' from source config",
                        operation="resolve_resource_header_values",
                        details={
                            "header": header_name,
                            "resource_name": resource_name,
                            "error": str(e),
                        },
                    ) from e
            else:
                # Use shared resolver so timestamps match across headers
                resolved_headers[header_name] = resolver.resolve(header_value)

        return resolved_headers

    def _create_service(self, source_name: str, source_config: SourceConfig):
        """
        Create a service instance for the given source.

        Args:
            source_name: Name of the source
            source_config: Source configuration

        Returns:
            BaseSourceService: Service instance

        Raises:
            HandlerError: If service creation fails
        """
        try:
            audit_recorder = getattr(self, "_audit_recorder", None)
            return ServiceFactory.create_service(
                self.settings,
                source_name,
                source_config,
                redis_context=self.redis_context,
                audit_recorder=audit_recorder,
            )
        except Exception as e:
            raise HandlerError.from_error(
                e,
                f"Failed to create service for source '{source_name}'",
                is_retryable=False,  # Service creation failures are typically configuration issues
            ) from e

    def _resolve_nested_value_in_dict(self, input_name: str, value: Any) -> Any:
        """
        Recursively resolve nested values within a dict structure.

        Handles cases where an input's value is a dict containing nested "value" fields
        that need to be resolved independently (e.g., nested dynamic values like PAGINATION).

        This processes dict structures by:
        1. Iterating through each key-value pair
        2. For nested dicts containing a "value" key, resolving that value independently
        3. Rebuilding the dict with resolved values
        4. Recursively handling deeper nesting

        Args:
            input_name: Name of the request input (for logging)
            value: The input value (potentially containing nested structures)

        Returns:
            Any: The value with all nested values resolved
        """
        if not isinstance(value, dict):
            return value

        resolved = {}
        resolver = self._get_value_resolver()

        for key, val in value.items():
            if isinstance(val, dict):
                # Check if this nested dict has a "value" key that needs resolution
                if "value" in val:
                    nested_value = val.get("value")

                    # Resolve the nested value independently
                    try:
                        resolved_nested = resolver.resolve(nested_value)

                        # Rebuild the dict, keeping other properties and updating the resolved value
                        resolved[key] = resolved_nested

                        self.logger.debug(
                            f"Resolved nested value in '{input_name}.{key}'",
                            extra_fields={
                                "original_value_type": type(nested_value).__name__,
                                "resolved_value": (
                                    str(resolved_nested)[:100] if resolved_nested else None
                                ),
                            },
                        )
                    except Exception as e:
                        self.logger.warning(
                            f"Failed to resolve nested value in '{input_name}.{key}': {e!s}",
                            extra_fields={"key": key, "error": str(e)},
                        )
                        resolved[key] = val
                else:
                    # Recursively resolve deeper nested structures
                    resolved[key] = self._resolve_nested_value_in_dict(input_name, val)
            else:
                # Resolve Jinja strings at any nesting level
                if isinstance(val, str) and "{{" in val and "}}" in val:
                    try:
                        resolved[key] = resolver.resolve(val)
                    except Exception as e:
                        self.logger.warning(
                            f"Failed to resolve nested value in '{input_name}.{key}'",
                            extra_fields={"key": key, "error": str(e)},
                        )
                        resolved[key] = val
                else:
                    resolved[key] = val

        return resolved

    def _resolve_parameter_values_list(
        self,
        input_name: str,
        input_config: InputConfig,
        source_name: str,
        resource_name: str,
        context: Optional[Dict[str, Any]] = None,
        filter_value: Optional[str] = None,
        *,
        for_batch_expansion: bool = False,
    ) -> List[Any]:
        """
        Resolve a list of values for any request input type, including nested values.

        Handles:
        - Static lists: Returns input_config.value directly
        - Static dicts with nested dynamic values: Resolves nested values first
        - Dynamic from parent resources: Resolves from Redis, applies filtering
        - Dynamic from other sources: Resolves via dynamic value resolver

        For inputs with dict values, independently resolves any nested "value" fields
        before returning the dict. This allows nested dynamic values (like PAGINATION configs)
        to be resolved separately.

        Args:
            input_name: Name of the request input
            input_config: Input configuration
            source_name: Source name for context
            resource_name: Resource name for context
            context: Optional request context (may contain pre-resolved values)
            filter_value: Optional filter value for scoping parent resource data
            for_batch_expansion: When True (batch_inputs expansion), resolve body Jinja immediately
                so expressions like ``{{ databricks('ref') }}`` return a list of values for batching.

        Returns:
            List[Any]: List of input values (always returns a list)

        Raises:
            HandlerError: If input processing fails
        """
        context = context or {}

        # Check if already resolved in context (from previous expansion)
        if input_name in context:
            value = context[input_name]
            return value if isinstance(value, list) else [value]

        # Static list input
        if input_config.is_static_list():
            values = input_config.value
            if isinstance(values, list):
                # Resolve nested values in each dict item if applicable
                resolved_values = []
                for item in values:
                    if isinstance(item, dict):
                        resolved_item = self._resolve_nested_value_in_dict(input_name, item)
                        resolved_values.append(resolved_item)
                    else:
                        resolved_values.append(item)
                return resolved_values
            else:
                # Single static dict value - resolve nested values within it
                if isinstance(values, dict):
                    values = self._resolve_nested_value_in_dict(input_name, values)
                return [values]

        # Dynamic input from parent resource
        source_config = input_config.get_source_config()
        if source_config:
            return self._resolve_from_parent_resource(
                input_name=input_name,
                input_config=input_config,
                source_config=source_config,
                source_name=source_name,
                resource_name=resource_name,
                filter_value=filter_value,
            )

        # Dynamic input: Jinja string, or dict/ComplexDynamicValue (legacy structure)
        if input_config.value is not None:
            if isinstance(input_config.value, (DynamicValue, ComplexDynamicValue, dict)):
                # Skip SOURCE type - handled above as parent resource
                if (
                    isinstance(input_config.value, ComplexDynamicValue)
                    and input_config.value.type == DynamicValueType.SOURCE
                ):
                    raise HandlerError(
                        "SOURCE type dynamic value should be handled via source_config, not direct value",
                        details={"input": input_name},
                    )

            resolver = self._get_value_resolver()

            # Defer Jinja resolution for body inputs; outbound-request services and _resolve_request_body_context resolve the full body with context.
            # Skip deferral when expanding batch_inputs: values must be concrete lists before batching.
            if (
                not for_batch_expansion
                and getattr(input_config, "location", None) == "body"
                and isinstance(input_config.value, str)
                and "{{" in input_config.value
                and "}}" in input_config.value
            ):
                resolved_value = input_config.value
            else:
                resolved_value = resolver.resolve(input_config.value)

            # If resolved value is a dict, resolve any nested values within it
            if isinstance(resolved_value, dict):
                resolved_value = self._resolve_nested_value_in_dict(input_name, resolved_value)

            # Normalize to list
            if isinstance(resolved_value, list):
                return resolved_value
            return [resolved_value] if resolved_value is not None else []

        # Single static value
        if input_config.value is not None:
            value = input_config.value
            # If it's a dict, resolve nested values
            if isinstance(value, dict):
                value = self._resolve_nested_value_in_dict(input_name, value)
            return [value]

        raise HandlerError(f"No source or value defined for input '{input_name}'")

    def _resolve_from_parent_resource(
        self,
        input_name: str,
        input_config: InputConfig,
        source_config: Any,
        source_name: str,
        resource_name: str,
        filter_value: Optional[str] = None,
    ) -> List[Any]:
        """
        Resolve input values from a parent resource.

        This treats parent resources as just another input source - no special handling needed.

        Args:
            input_name: Name of the request input
            input_config: Input configuration
            source_config: Source configuration for parent resource
            source_name: Source name for context
            resource_name: Resource name for context
            filter_value: Optional filter value for scoping parent data

        Returns:
            List[Any]: List of input values
        """
        # Get parent resource data from Redis
        source_endpoint = f"{source_config.source}"
        parent_source_name, parent_resource = self._parse_parent_resource_ref(
            source_endpoint, source_name
        )

        redis_key = self._get_redis_key(parent_source_name, parent_resource)
        source_data = self.redis_context.get(redis_key, spark=self.spark)

        if source_data is None:
            raise HandlerError(
                f"Required data from parent ref '{source_endpoint}' not found",
                details={"input": input_name},
            )

        # Handle DataFrame source data (most common case)
        if isinstance(source_data, DataFrame):
            return self._extract_values_from_dataframe(
                df=source_data,
                input_name=input_name,
                input_config=input_config,
                source_config=source_config,
                filter_value=filter_value,
            )

        # Handle list data
        if isinstance(source_data, list):
            return self._extract_values_from_list(
                data=source_data, input_name=input_name, source_config=source_config
            )

        raise HandlerError(
            f"Data from parent ref '{source_endpoint}' has unsupported type: {type(source_data)}",
            details={
                "input": input_name,
                "source": source_endpoint,
                "type": type(source_data).__name__,
            },
        )

    def _resolve_filter_value(
        self,
        filter_config: FilterConfig,
        batch_value: Optional[str] = None,
    ) -> Any:
        """
        Resolve the filter value based on configuration.

        Args:
            filter_config: Filter configuration
            batch_value: Optional batch value for parameter filtering

        Returns:
            Any: Resolved filter value
        """
        if filter_config.value_type == "static":
            resolver = self._get_value_resolver()
            return resolver.resolve(filter_config.value_source)
        elif filter_config.value_type == "parameter":
            return batch_value if batch_value is not None else filter_config.value_source
        else:
            raise HandlerError(f"Unsupported value_type: {filter_config.value_type}")

    def _build_filter_expression(self, field: str, operator: FilterOperator, value: Any) -> str:
        """
        Build a Spark SQL filter expression.

        Args:
            field: Field name to filter on
            operator: Filter operator to apply
            value: Value to filter by

        Returns:
            str: SQL filter expression
        """
        operators = {
            FilterOperator.EQUALS: lambda f, v: f"{f} = '{v}'",
            FilterOperator.NOT_EQUALS: lambda f, v: f"{f} != '{v}'",
            FilterOperator.GREATER_THAN: lambda f, v: f"{f} > '{v}'",
            FilterOperator.LESS_THAN: lambda f, v: f"{f} < '{v}'",
            FilterOperator.GREATER_EQUALS: lambda f, v: f"{f} >= '{v}'",
            FilterOperator.LESS_EQUALS: lambda f, v: f"{f} <= '{v}'",
            FilterOperator.CONTAINS: lambda f, v: f"array_contains({f}, '{v}')",
            FilterOperator.IN: lambda f, v: f"{f} IN ({', '.join(repr(x) for x in v)})",
        }

        if operator not in operators:
            raise HandlerError(f"Unsupported operator: {operator}")

        return operators[operator](field, value)

    def _extract_values_from_dataframe(
        self,
        df: DataFrame,
        input_name: str,
        input_config: InputConfig,
        source_config: Any,
        filter_value: Optional[str] = None,
    ) -> List[Any]:
        """
        Extract input values from a DataFrame source with filtering.
        """
        # Get field and filter from configuration
        field = source_config.field
        filter_config = source_config.filter

        # Validate that field is specified
        if not field:
            raise ValueError(f"No field specified in configuration for input {input_name}")

        # Apply filtering if configured
        if filter_config:
            df = self._apply_parameter_filter(
                df=df, input_name=input_name, filter_config=filter_config, filter_value=filter_value
            )

        # Get values from the configured field
        if field not in df.columns:
            raise ValueError(
                f"Field {field} not found in DataFrame. Available columns: {df.columns}"
            )

        df = df.filter(col(field).isNotNull())
        values = df.select(field).distinct().collect()
        return [row[field] for row in values]

    def _apply_parameter_filter(
        self, df: DataFrame, input_name: str, filter_config: Any, filter_value: Optional[str] = None
    ) -> DataFrame:
        """
        Apply filtering to a DataFrame for input value extraction.

        Supports column-based, expression-based, and _params filtering.
        """
        try:
            if filter_config.type == FilterType.COLUMN:
                # Direct column filtering with operator support
                filter_field = filter_config.field
                if filter_field not in df.columns:
                    raise ValueError(f"Filter field {filter_field} not found in DataFrame")

                value = self._resolve_filter_value(filter_config, filter_value)
                filter_expr = self._build_filter_expression(
                    filter_field, filter_config.operator, value
                )
                df = df.filter(filter_expr)

            elif filter_config.type == FilterType.EXPRESSION:
                # Raw expression filtering
                df = df.filter(filter_config.field)

            elif filter_config.type == FilterType.PARAMS:
                # PARAMS filtering using _params column
                if "_params" not in df.columns:
                    raise ValueError("_params column not found in DataFrame")

                params_key = filter_config.params_key
                if not params_key:
                    raise ValueError(
                        "Invalid params filter configuration - missing source or field"
                    )

                # Parse _params JSON and filter by the parent context
                df = df.filter(df["_params"].isNotNull())
                df = df.selectExpr(
                    "*", "from_json(_params, 'map<string,array<string>>') as params_map"
                )
                df = df.filter(f"array_contains(params_map['{params_key}'], '{filter_value}')")

            self.logger.debug(
                f"Applied {filter_config.type.value} filter",
                extra_fields={
                    "input": input_name,
                    "field": filter_config.field,
                    "operator": (
                        filter_config.operator.value
                        if filter_config.type == FilterType.COLUMN
                        else None
                    ),
                    "filtered_count": df.count(),
                },
            )

            return df

        except Exception as e:
            raise HandlerError(
                f"Failed to apply filter for input '{input_name}'",
                operation="filter_input",
                details={
                    "filter_type": filter_config.type.value,
                    "field": filter_config.field,
                    "error": str(e),
                },
            ) from e

    def _extract_values_from_list(
        self, data: List[Dict[str, Any]], input_name: str, source_config: Any
    ) -> List[Any]:
        """
        Extract input values from a list source.

        Args:
            data: Source list data
            input_name: Name of the request input
            source_config: Source configuration

        Returns:
            List[Any]: List of input values
        """
        field = source_config.field

        # Validate that field is specified
        if not field:
            raise HandlerError(
                f"No field specified in configuration for input '{input_name}'",
                details={"input": input_name},
            )

        # Extract and format values
        values = []
        for record in data:
            if not isinstance(record, dict):
                raise HandlerError(
                    "Invalid record format in source data",
                    details={"input": input_name, "record_type": type(record).__name__},
                )

            if field not in record:
                raise HandlerError(
                    f"Required field '{field}' not found in source data",
                    details={"input": input_name, "available_fields": list(record.keys())},
                )

            values.append(str(record[field]))

        self.logger.debug(
            f"Processed list values for {input_name}",
            extra_fields={"value_count": len(values), "sample": values[:2] if values else None},
        )

        return values

    def _probe_source_connectivity(
        self, source_name: str, source_config: SourceConfig, service: Any
    ) -> None:
        """
        Probe that the source can run at least one resource (minimal fetch with resolved inputs).

        Args:
            source_name: Name of the source
            source_config: Source configuration
            service: Service instance to test

        Raises:
            HandlerError: If the connectivity probe fails
        """
        try:
            for resource_name, resource_config in source_config.resources.items():
                has_dependencies = any(
                    param.has_source_config() for param in resource_config.request_inputs.values()
                )

                if not has_dependencies:
                    self.logger.debug(
                        f"Probing source connectivity for {source_name} using resource: {resource_name}"
                    )

                    if source_config.type in (SourceType.POSTGRESQL, SourceType.HANA):
                        try:
                            service.connect()
                            self.logger.debug(
                                f"Successfully validated database connectivity for {source_name}"
                            )
                            return
                        except Exception as e:
                            raise HandlerError(
                                f"Database connectivity probe failed for {source_name}: {e!s}"
                            ) from e
                        finally:
                            try:
                                service.close()
                            except Exception:
                                pass

                    # Prepare minimal parameters (resolve Jinja/dynamic values)
                    parameters = {}
                    for name, param in resource_config.request_inputs.items():
                        if param.value is not None:
                            val = param.value
                            if (isinstance(val, str) and "{{" in val and "}}" in val) or isinstance(
                                val, (DynamicValue, ComplexDynamicValue, dict)
                            ):
                                if not (
                                    isinstance(val, ComplexDynamicValue)
                                    and val.type == DynamicValueType.SOURCE
                                ):
                                    val = get_resolver(self.redis_context).resolve(param.value)
                            parameters[name] = (
                                val[0] if isinstance(val, list) and len(val) == 1 else val
                            )

                    # Make a test fetch with minimal data
                    try:
                        service.fetch_data(resource_name, parameters)
                        self.logger.debug(
                            f"Successfully validated source connectivity for {source_name}"
                        )
                        return
                    except Exception as e:
                        raise HandlerError(
                            f"Source connectivity probe failed for {source_name} "
                            f"using resource {resource_name}: {e!s}"
                        ) from e

            raise HandlerError(
                f"No suitable resource found for connectivity probe in {source_name}. "
                "All resources have dependencies."
            )

        except Exception as e:
            raise HandlerError(f"Source connectivity probe failed for {source_name}: {e!s}") from e

    def _get_redis_key(self, source_name: str, resource_name: str) -> str:
        """
        Generate Redis key for cached resource data.

        Args:
            source_name: Name of the data source
            resource_name: Name of the resource

        Returns:
            str: Redis key for the resource
        """
        return f"{self.config.defaults.context.prefix}{source_name}:{resource_name}"

    def _split_into_batches(self, values: List[Any], batch_size: int) -> List[List[Any]]:
        """
        Split a list of values into batches of specified size.

        Args:
            values: List of values to split into batches
            batch_size: Size of each batch

        Returns:
            List[List[Any]]: List of batches, each containing up to batch_size items
        """
        if not values:
            return []
        return [values[i : i + batch_size] for i in range(0, len(values), batch_size)]

    def _build_parent_context(
        self,
        resource_config: ResourceConfig,
        parameters: Dict[str, Any],
        request_context: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Build parent context for parsing from parameters and request context.

        Args:
            resource_config: Resource configuration
            parameters: Resolved parameters for the request
            request_context: Request context (may contain iteration params)

        Returns:
            Dict[str, Any]: Parent context for parsing
        """
        # Build parent context from parameters using shared utility
        parent_context = build_parent_context_from_parameters(resource_config, parameters)

        # Add iteration parameters to context for data tracking
        if request_context:
            parent_context.update(request_context)

        return parent_context

    def _parse_data(
        self,
        raw_data: Union[List[Dict[str, Any]], Dict[str, Any]],
        resource_meta: ResourceMetadata,
        service: Any,
        parent_context: Optional[Dict[str, Any]] = None,
        request_context: Optional[Dict[str, Any]] = None,
    ) -> Optional[DataFrame]:
        """
        Centralized method to parse raw fetched data into a Spark DataFrame.

        Extracts fields and builds schema but does NOT apply transformations.
        Transformations are applied separately after all data is collected.

        Args:
            raw_data: Raw payload from the source (e.g. decoded response body)
            resource_meta: Metadata for the resource being processed
            service: Service instance used for the fetch
            parent_context: Optional parent context for parameter tracking
            request_context: Optional dictionary containing request parameters and body

        Returns:
            Optional[DataFrame]: Parsed data as a Spark DataFrame or None

        Raises:
            HandlerError: If parsing fails
        """
        try:
            if not raw_data:
                self.logger.debug(
                    "No data to parse",
                    extra_fields={
                        "source": resource_meta.source_name,
                        "resource_name": resource_meta.resource_name,
                    },
                )
                return None

            # Create parser instance
            if self.spark:
                parser = SparkParser(
                    config=resource_meta.config,
                    spark=self.spark,
                    source_name=service.source_name,
                    resource_name=resource_meta.resource_name,
                    execution_plan=self.execution_plan,
                    redis_context=self.redis_context,
                )

                data_df = parser.parse(raw_data, parent_context)

                # Create empty DataFrame with correct schema if no data
                if data_df is None:
                    self.logger.warning(
                        f"No data returned for resource {resource_meta.resource_name}, creating empty DataFrame",
                        extra_fields={"resource_name": resource_meta.resource_name},
                    )
                    schema = parser._build_target_schema(parent_context=parent_context)
                    data_df = self.spark.createDataFrame([], schema)

                return data_df

        except Exception as e:
            raise HandlerError.from_error(
                e,
                "Failed to parse data",
                operation="parse_data",
                details={
                    "source": resource_meta.source_name,
                    "resource_name": resource_meta.resource_name,
                },
            ) from e

    def _handle_snapshot(
        self, resource_meta: ResourceMetadata, service: Any, params: Dict[str, Any]
    ) -> Any:
        """
        Handle snapshot polling for a resource (REST snapshot flows).

        Args:
            resource_meta: Resource metadata
            service: Source service instance
            params: Request parameters

        Returns:
            Dict[str, Any]: Parsed payload after snapshot is ready

        Raises:
            HandlerError: If snapshot processing fails
        """
        resource_config = resource_meta.config
        snapshot_config = resource_config.snapshot

        if not snapshot_config:
            raise HandlerError("No snapshot configuration found")

        try:
            # Log snapshot polling configuration
            self.logger.debug(
                "Starting snapshot polling",
                extra_fields={
                    "source": resource_meta.source_name,
                    "resource_name": resource_meta.resource_name,
                    "max_wait_time": f"{snapshot_config.max_time} seconds",
                    "initial_interval": f"{snapshot_config.interval} seconds",
                    "backoff_factor": snapshot_config.backoff_factor,
                    "max_interval": f"{snapshot_config.max_interval} seconds",
                    "ready_condition": snapshot_config.ready_condition,
                    "error_condition": snapshot_config.error_condition,
                },
            )

            def get_snapshot(poll_params: Dict[str, Any]) -> Any:
                return service.poll_snapshot(resource_meta.resource_name, poll_params)

            # Initialize poller and wait for completion
            poller = SnapshotPoller(
                config=snapshot_config, logger=self.logger, get_snapshot=get_snapshot
            )

            return poller.wait_for_completion(params)

        except SnapshotTimeoutError as e:
            raise HandlerError(
                "Snapshot timed out",
                operation="snapshot_polling",
                details={
                    "source": resource_meta.source_name,
                    "resource_name": resource_meta.resource_name,
                    "max_time": snapshot_config.max_time,
                    "last_response": e.last_response,
                },
                original_error=e,
                is_retryable=True,  # Allow retry of the entire operation
            ) from e
        except SnapshotError as e:
            raise HandlerError(
                "Snapshot failed",
                operation="snapshot_polling",
                details={
                    "source": resource_meta.source_name,
                    "resource_name": resource_meta.resource_name,
                    "response": e.response,
                },
                original_error=e,
                is_retryable=False,  # Don't retry if snapshot entered error state
            ) from e
        except Exception as e:
            raise HandlerError.from_error(
                e,
                "Failed to process snapshot resource",
                operation="snapshot_polling",
                details={
                    "source": resource_meta.source_name,
                    "resource_name": resource_meta.resource_name,
                },
            ) from e

    def _generate_all_request_contexts(
        self,
        resource_meta: ResourceMetadata,
        service: Any,
        use_backfill: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        Generate all request contexts by expanding parameter values via cartesian product.

        When use_backfill is True and the resource has backfill_config, first generates paired
        date ranges (one context per pair), then for each date context runs cartesian
        expansion over batch_inputs so date pairs and other params are not cartesian.
        """
        batch_inputs = resource_meta.batch_inputs
        backfill_config = resource_meta.backfill_config

        # Backfill layer: list of date contexts [{startDate, endDate}, ...] or [{}]
        if use_backfill and backfill_config:
            try:
                date_pairs = generate_backfill_date_pairs(
                    backfill_config, redis_context=self.redis_context
                )
            except Exception as e:
                self.logger.warning(
                    "Backfill date generation failed, using single default range",
                    extra_fields={
                        "resource_name": resource_meta.resource_name,
                        "error": str(e),
                    },
                )
                date_pairs = [{}]
            if not date_pairs:
                self.logger.warning(
                    "Backfill produced no date pairs (e.g. start > end), skipping backfill",
                    extra_fields={"resource_name": resource_meta.resource_name},
                )
                date_pairs = [{}]
        else:
            date_pairs = [{}]

        self._current_value_resolver = get_resolver(self.redis_context)
        # For each date context, run cartesian product over batch_inputs (if any)
        all_contexts: List[Dict[str, Any]] = []
        for date_context in date_pairs:
            if not batch_inputs:
                all_contexts.append({**date_context})
                continue
            contexts = [date_context]
            prev_input_name = None

            for input_name, batch_size in batch_inputs.items():
                input_config = resource_meta.config.request_inputs.get(input_name)
                if not input_config:
                    self.logger.warning(f"Request input {input_name} not found in request_inputs")
                    continue
                new_contexts = []

                for context in contexts:
                    try:
                        # Extract filter value for nested filtering
                        filter_value = (
                            self._extract_filter_value(prev_input_name, context)
                            if prev_input_name
                            else None
                        )

                        # Resolve input values (handles all types)
                        values = self._resolve_parameter_values_list(
                            input_name=input_name,
                            input_config=input_config,
                            source_name=service.source_name,
                            resource_name=resource_meta.resource_name,
                            context=context,
                            filter_value=filter_value,
                            for_batch_expansion=True,
                        )

                        if not values:
                            self.logger.warning(f"No values found for input {input_name}")
                            continue

                        # Batch and expand (cartesian product)
                        for batch in self._split_into_batches(
                            values, batch_size if batch_size != BatchSizeMode.ALL else len(values)
                        ):
                            new_contexts.append({**context, input_name: batch})

                    except Exception as e:
                        self.logger.error(
                            f"Failed to resolve values for input {input_name}: {e!s}",
                            extra_fields={
                                "input": input_name,
                                "error": str(e),
                                "error_type": type(e).__name__,
                            },
                        )

                prev_input_name = input_name
                contexts = new_contexts

            all_contexts.extend(contexts)

        if not all_contexts and not batch_inputs and date_pairs == [{}]:
            all_contexts = [{}]

        self.logger.info(
            f"Generating request contexts for {resource_meta.resource_name}",
            extra_fields={
                "batch_inputs": list(batch_inputs.keys()) if batch_inputs else [],
                "context_count": len(all_contexts),
            },
        )

        # Apply limit to final contexts list
        if self.record_limit is not None:
            original_count = len(all_contexts)
            all_contexts = self._apply_request_limit(
                items=all_contexts,
                limit=self.record_limit,
                item_type="request contexts",
                source_name=service.source_name,
                resource_name=resource_meta.resource_name,
            )
            if len(all_contexts) < original_count:
                self.logger.info(
                    f"Limited request contexts from {original_count} to {len(all_contexts)} for {self.record_limit} fetch operation(s) per resource",
                    extra_fields={
                        "source": service.source_name,
                        "resource_name": resource_meta.resource_name,
                        "original_count": original_count,
                        "limited_count": len(all_contexts),
                        "request_limit": self.record_limit,
                    },
                )

        self.logger.debug(
            f"Generated {len(all_contexts)} request contexts for {resource_meta.resource_name}"
        )
        return all_contexts

    def _extract_filter_value(self, prev_input_name: str, context: Dict[str, Any]) -> Optional[str]:
        """Extract filter value from previous input's context for nested filtering."""
        prev_value = context.get(prev_input_name)
        if isinstance(prev_value, list) and prev_value:
            return prev_value[0]
        return prev_value if prev_value is not None else None

    def _get_filter_value_from_context(
        self,
        resource_config: ResourceConfig,
        input_name: str,
        context: Dict[str, Any],
    ) -> Optional[str]:
        """
        Derive filter_value for an input that has a SOURCE filter with value_type 'parameter'.

        Finds the request input whose source_config matches the filter's value_source
        (same source and field), then returns that input's value from context (so the
        parent DataFrame can be filtered by the current request's value, e.g. snapshotId).
        """
        input_config = resource_config.request_inputs.get(input_name)
        if not input_config:
            return None
        source_config = input_config.get_source_config()
        if not source_config or not source_config.filter:
            return None
        filter_config = source_config.filter
        if getattr(filter_config, "value_type", None) != "parameter":
            return None
        value_source = getattr(filter_config, "value_source", None)
        if not isinstance(value_source, FilterValueSource):
            return None
        # Find input in this resource whose source_config matches value_source (source + field)
        for name, pconfig in resource_config.request_inputs.items():
            sc = pconfig.get_source_config()
            if not sc:
                continue
            if sc.source == value_source.source and sc.field == value_source.field:
                raw = context.get(name)
                if isinstance(raw, list) and raw:
                    return str(raw[0]) if raw[0] is not None else None
                return str(raw) if raw is not None else None
        return None

    def _extract_pagination_info(
        self, response_data: Dict[str, Any], pagination_config: PaginationConfig
    ) -> Optional[Dict[str, Any]]:
        """
        Extract pagination metadata from a paginated payload using configurable field names.

        Args:
            response_data: Full JSON response body (or equivalent dict) from the source
            pagination_config: Pagination configuration with field name mappings

        Returns:
            Dict with pagination info (page, total_page, page_size, total_number) or None if not found
        """
        try:
            page_info = get_nested_value(response_data, pagination_config.page_info_path)
            if not page_info or not isinstance(page_info, dict):
                return None

            # Extract fields using configurable field names
            current_page = None
            total_pages = None
            page_size = None
            total_records = None

            # Extract current page if field is configured
            if pagination_config.response_page_field:
                current_page = page_info.get(pagination_config.response_page_field)

            # Extract total pages (required)
            total_pages = page_info.get(pagination_config.response_total_pages_field)

            # Extract optional fields if configured
            if pagination_config.response_page_size_field:
                page_size = page_info.get(pagination_config.response_page_size_field)
            if pagination_config.response_total_records_field:
                total_records = page_info.get(pagination_config.response_total_records_field)

            pagination_info = {
                "page": current_page,
                "total_page": total_pages,
                "page_size": page_size,
                "total_number": total_records,
            }

            # Validate required fields (only total_pages is required)
            if pagination_info["total_page"] is None:
                self.logger.warning(
                    "Pagination info missing required field: total_pages",
                    extra_fields={
                        "page_info": page_info,
                        "page_info_path": pagination_config.page_info_path,
                        "response_total_pages_field": pagination_config.response_total_pages_field,
                        "available_fields": (
                            list(page_info.keys()) if isinstance(page_info, dict) else None
                        ),
                    },
                )
                return None

            return pagination_info

        except Exception as e:
            self.logger.warning(
                "Failed to extract pagination info",
                extra_fields={"error": str(e), "page_info_path": pagination_config.page_info_path},
            )
            return None

    def _find_pagination_in_nested_value(
        self, value: Any, current_path: str = ""
    ) -> Optional[tuple[Dict[str, Any], str]]:
        """
        Recursively search for pagination config in nested dict structures.
        Returns tuple of (pagination_config_dict, field_path) if found.
        field_path is the dot-notation path to the pagination field (e.g., "Paging.PageNo")

        Important: When pagination_config is found, we exclude ".value" from the path
        since pagination_config is the terminal point, not a nested field within "value"

        Args:
            value: The value to search (could be dict, ComplexDynamicValue, etc.)
            current_path: Current path in the nested structure (for building field path)

        Returns:
            Optional[tuple[Dict[str, Any], str]]: Tuple of (pagination_config, field_path) if found, None otherwise
        """
        # If it's already a ComplexDynamicValue, check for pagination_config
        if isinstance(value, ComplexDynamicValue):
            if hasattr(value, "pagination_config") and value.pagination_config is not None:
                # For direct pagination, field_path is just the parameter name
                return (value.pagination_config, current_path)

        # If it's a dict, try to convert to ComplexDynamicValue first
        elif isinstance(value, dict):
            # Try to parse as ComplexDynamicValue to properly handle nested structures
            parsed_value = None
            try:
                parsed_value = ComplexDynamicValue(**value)
            except (ValueError, TypeError):
                pass  # Not a ComplexDynamicValue

            # If successfully parsed, check for pagination_config
            if (
                parsed_value
                and hasattr(parsed_value, "pagination_config")
                and parsed_value.pagination_config is not None
            ):
                return (parsed_value.pagination_config, current_path)

            # Check if this dict directly has pagination_config key
            if "pagination_config" in value:
                return (value["pagination_config"], current_path)

            # Recursively search in nested dicts
            for key, val in value.items():
                # Skip "value" key when building path - it's not part of the actual parameter structure
                # The "value" key is only used in configuration, not in the actual request parameters
                if key == "value":
                    # Recurse into "value" but don't add it to the path
                    # However, if pagination is found inside "value", return current_path
                    # (which is the key that contains this "value")
                    result = self._find_pagination_in_nested_value(val, current_path)
                else:
                    # Build the path as we traverse
                    new_path = f"{current_path}.{key}" if current_path else key
                    result = self._find_pagination_in_nested_value(val, new_path)

                if result is not None:
                    return result

        return None

    def _resolve_request_body_context(
        self,
        resource_config: ResourceConfig,
        context: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Resolve request body values for use in request_context (transformations).

        Builds body from request_inputs with location=body; values from context
        or resolved from config. Does NOT strip exclude_from_request_body keys
        so transformations like add_column_from_request can use them.

        Args:
            resource_config: Resource configuration (body inputs only)
            context: Iteration context (may contain backfill date overrides)

        Returns:
            Dict[str, Any]: Resolved request body values
        """
        body_inputs = resource_config.get_inputs_by_location("body")
        if not body_inputs:
            return {}
        r = get_resolver(self.redis_context)
        overrides = {}
        for name, config in body_inputs.items():
            if name in context:
                overrides[name] = context[name]
            elif config.value is not None:
                v = config.value
                if isinstance(v, dict) and "backfill" in v and "value" in v:
                    overrides[name] = r.resolve(v["value"])
                elif isinstance(v, str) and "{{" in v and "}}" in v:
                    overrides[name] = v
                else:
                    overrides[name] = r.resolve(v)
        return resolve_request_body({}, resolver=r, overrides=overrides, exclude_keys=None)

    def _make_single_request(
        self,
        resource_config: ResourceConfig,
        context: Dict[str, Any],
        service: Any,
        collector: Union[RawDataCollector, StreamingRawDataCollector, DiskStreamingDataCollector],
        resource_meta: ResourceMetadata,
        source_config: SourceConfig,
    ) -> None:
        """
        Perform a single fetch for the resource (with pagination support if enabled).

        Args:
            resource_config: Resource configuration
            context: Request context containing parameter values
            service: Service instance
            collector: Data collector
            resource_meta: Resource metadata
        """
        self._current_value_resolver = get_resolver(self.redis_context)
        # Resolve resource-level headers merged with source defaults before the fetch
        if resource_config.headers or source_config.headers:
            headers_to_resolve = {
                **(source_config.headers or {}),
                **(resource_config.headers or {}),
            }

            resolved_resource_headers = self._resolve_resource_header_values(
                headers_to_resolve,
                source_name=resource_meta.source_name,
                resource_name=resource_meta.resource_name,
            )
            # Update resource config headers with resolved values
            resource_config.headers = resolved_resource_headers

        # Build resolved request context from request_inputs (single resolution path)
        parameters = {}
        for input_name, input_config in resource_config.request_inputs.items():
            if input_name in context:
                value = context[input_name]
                if input_config.location == "path":
                    # Path inputs must be single values
                    if isinstance(value, list):
                        if len(value) == 1:
                            parameters[input_name] = value[0]
                        elif len(value) > 1:
                            raise ValueError(
                                f"Path input '{input_name}' received multiple values in context: {value}. "
                                "Path inputs must be single values. Check batch_size configuration."
                            )
                        else:
                            raise ValueError(
                                f"Path input '{input_name}' received empty list in context"
                            )
                    else:
                        parameters[input_name] = value
                else:
                    parameters[input_name] = value
            else:
                filter_value = self._get_filter_value_from_context(
                    resource_config, input_name, context
                )
                values = self._resolve_parameter_values_list(
                    input_name=input_name,
                    input_config=input_config,
                    source_name=service.source_name,
                    resource_name=resource_meta.resource_name,
                    context=context,
                    filter_value=filter_value,
                )
                if input_config.location == "path":
                    if len(values) == 1 and input_config.input_format == "single":
                        parameters[input_name] = values[0]
                    elif len(values) > 1:
                        raise ValueError(
                            f"Path input '{input_name}' resolved to multiple values: {values}. "
                            "Path inputs must be single values. Ensure batch_size is configured."
                        )
                    else:
                        parameters[input_name] = values
                else:
                    if len(values) == 1 and input_config.input_format == "single":
                        parameters[input_name] = values[0]
                    else:
                        parameters[input_name] = values

        # Check if pagination is enabled on any input (including nested fields)
        pagination_config = None
        pagination_field_path = (
            None  # Track path to nested pagination field (e.g., "Paging.PageNo")
        )

        # Check request_inputs for pagination (any location)
        for input_name, input_config in resource_config.request_inputs.items():
            if input_config.pagination:
                pagination_config = input_config.pagination
                pagination_field_path = input_name
                break
            if input_config.value is not None:
                result = self._find_pagination_in_nested_value(input_config.value)
                if result is not None:
                    nested_pagination, nested_path = result
                    if isinstance(nested_pagination, dict):
                        pagination_config = PaginationConfig(**nested_pagination)
                    elif isinstance(nested_pagination, PaginationConfig):
                        pagination_config = nested_pagination
                    pagination_field_path = (
                        f"{input_name}.{nested_path}" if nested_path else input_name
                    )
                    break

        is_pagination_enabled = pagination_config is not None

        # # If pagination is enabled, automatically set page parameter to 1 if not already set
        # temporarily disabled to avoid unnecessary errors
        # if is_pagination_enabled and request_page_param:
        #     if request_page_param not in parameters:
        #         parameters[request_page_param] = 1
        #         self.logger.trace(
        #             f"Auto-set {request_page_param} parameter to 1 for pagination",
        #             extra_fields={
        #                 "resource_name": resource_meta.resource_name,
        #                 "param": request_page_param,
        #             },
        #         )

        # Collect all pages of data
        all_pages_data = []

        try:
            if resource_config.snapshot:
                # Snapshot resources don't support pagination
                full_response = self._handle_snapshot(
                    resource_meta=resource_meta,
                    service=service,
                    params=parameters,
                )
                # Extract data from snapshot response
                if resource_config.response_key and isinstance(full_response, dict):
                    page_data = get_nested_value(full_response, resource_config.response_key)
                    if page_data:
                        all_pages_data = page_data if isinstance(page_data, list) else [page_data]
                elif isinstance(full_response, list):
                    all_pages_data = full_response
                else:
                    all_pages_data = [full_response] if full_response else []
            elif is_pagination_enabled:
                # Pagination-enabled resource: fetch first page and check for more pages
                # request_page_param is set from the parameter name above
                max_pages = pagination_config.max_pages

                # Only PAGE_NUMBER pagination is currently implemented
                if pagination_config.type != PaginationType.PAGE_NUMBER:
                    raise HandlerError(
                        f"Pagination type '{pagination_config.type.value}' is not yet implemented. "
                        f"Only '{PaginationType.PAGE_NUMBER.value}' is currently supported.",
                        operation="pagination",
                        details={
                            "resource_name": resource_meta.resource_name,
                            "pagination_type": pagination_config.type.value,
                        },
                    )

                # Make first request and get full response for pagination metadata
                full_response = service.fetch_data(
                    resource_meta.resource_name, parameters, full_response=True
                )

                # Extract data from first page
                if resource_config.response_key and isinstance(full_response, dict):
                    page_data = get_nested_value(full_response, resource_config.response_key)
                    if page_data:
                        all_pages_data = page_data if isinstance(page_data, list) else [page_data]
                elif isinstance(full_response, list):
                    all_pages_data = full_response
                else:
                    all_pages_data = [full_response] if full_response else []

                # Extract pagination info
                pagination_info = self._extract_pagination_info(full_response, pagination_config)

                if pagination_info:
                    current_page = pagination_info.get("page")  # May be None
                    total_pages = pagination_info["total_page"]

                    # Log pagination info
                    self.logger.trace(
                        "Pagination detected",
                        extra_fields={
                            "resource_name": resource_meta.resource_name,
                            "current_page": current_page,
                            "total_pages": total_pages,
                            "page_size": pagination_info.get("page_size"),
                            "total_number": pagination_info.get("total_number"),
                        },
                    )

                    # Check if we need to fetch more pages
                    if total_pages > 1:
                        # Determine how many pages to fetch
                        pages_to_fetch = total_pages - 1  # Already have page 1
                        if max_pages is not None:
                            pages_to_fetch = min(pages_to_fetch, max_pages - 1)

                        # Fetch remaining pages
                        for page_num in range(2, 2 + pages_to_fetch):
                            try:
                                page_params = parameters.copy()
                                if pagination_field_path:
                                    set_nested_value(page_params, pagination_field_path, page_num)

                                self.logger.debug(
                                    f"Fetching page {page_num} of {total_pages}",
                                    extra_fields={
                                        "resource_name": resource_meta.resource_name,
                                        "page": page_num,
                                        "total_pages": total_pages,
                                        "pagination_field_path": pagination_field_path,
                                    },
                                )

                                # Fetch page data
                                page_response = service.fetch_data(
                                    resource_meta.resource_name, page_params
                                )

                                # Add page data to collection
                                if page_response:
                                    if isinstance(page_response, list):
                                        all_pages_data.extend(page_response)
                                    else:
                                        all_pages_data.append(page_response)

                            except Exception as e:
                                self.logger.warning(
                                    f"Failed to fetch page {page_num}",
                                    extra_fields={
                                        "resource_name": resource_meta.resource_name,
                                        "page": page_num,
                                        "error": str(e),
                                        "error_type": type(e).__name__,
                                    },
                                )
                                # Continue with next page instead of failing completely
                                continue

                        pages_fetched = pages_to_fetch + 1  # +1 for the initial page
                        self.logger.info(
                            f"Completed pagination: fetched {pages_fetched} of {total_pages} pages",
                            extra_fields={
                                "resource_name": resource_meta.resource_name,
                                "pages_fetched": pages_fetched,
                                "total_pages": total_pages,
                                "max_pages_limit": max_pages,
                                "total_records": len(all_pages_data),
                            },
                        )
                else:
                    self.logger.debug(
                        "Pagination enabled but page_info not found, treating as single page",
                        extra_fields={
                            "resource_name": resource_meta.resource_name,
                            "page_info_path": pagination_config.page_info_path,
                        },
                    )
            else:
                # Standard request without pagination
                raw_data = service.fetch_data(resource_meta.resource_name, parameters)
                if raw_data:
                    all_pages_data = raw_data if isinstance(raw_data, list) else [raw_data]
        except Exception as e:
            self.logger.error(
                "Failed to fetch data for request",
                extra_fields={
                    "resource_name": resource_meta.resource_name,
                    "source": service.source_name,
                    "context": context,
                    "error": str(e),
                    "error_type": type(e).__name__,
                },
            )
            raise

        # Create parent context for parsing
        parent_context = self._build_parent_context(
            resource_config=resource_config, parameters=parameters, request_context=context
        )

        # Collect all pages data as a single batch
        if all_pages_data:
            request_context_dict = {
                "parameters": parameters,
                "request_body": self._resolve_request_body_context(resource_config, context),
                "iteration_params": context,
            }
            collector.add_batch(
                RawDataBatch(
                    raw_data=all_pages_data,
                    parent_context=parent_context,
                    request_context=request_context_dict,
                )
            )

    def _is_database_source(self, source_config: SourceConfig) -> bool:
        return source_config.type in (SourceType.POSTGRESQL, SourceType.HANA)

    def _build_database_dataframe(
        self,
        service: Any,
        resource_meta: ResourceMetadata,
        request_contexts: List[Dict[str, Any]],
    ) -> Optional[DataFrame]:
        """
        Connect, extract via Spark JDBC / HANA driver, project to configured fields (string typed).
        """
        resource_config = resource_meta.config
        fields = resource_config.fields
        if not fields:
            raise HandlerError(
                "Database resources require fields (output schema) in configuration",
                operation="process_resource",
                details={"resource_name": resource_meta.resource_name},
            )
        schema = str(resource_config.database_schema).strip()
        table = str(resource_config.database_table).strip()
        all_df: Optional[DataFrame] = None
        service.connect()
        try:
            for _ctx in request_contexts:
                df = service.extract_table(
                    schema=schema,
                    table=table,
                    select_query=resource_config.database_select_query,
                    spark_session=self.spark,
                )
                select_cols = []
                for f in fields:
                    if f.source not in df.columns:
                        raise HandlerError(
                            f"Configured field source {f.source!r} not in extract columns: {list(df.columns)}",
                            operation="process_resource",
                            details={
                                "resource_name": resource_meta.resource_name,
                                "source": resource_meta.source_name,
                            },
                        )
                    select_cols.append(col(f.source).cast("string").alias(f.name))
                batch_df = df.select(*select_cols)
                all_df = (
                    batch_df
                    if all_df is None
                    else all_df.unionByName(batch_df, allowMissingColumns=True)
                )
            return all_df
        finally:
            service.close()

    def _process_resource(
        self, resource_meta: ResourceMetadata, service: Any, source_config: SourceConfig
    ) -> Dict[str, Any]:
        """
        Process a single resource's data flow using Redis for context.

        Args:
            resource_meta: Metadata for the resource to process
            service: Service instance to use

        Returns:
            Dict[str, Any]: Results of processing

        Raises:
            HandlerError: If processing fails
        """
        use_streaming = False
        collector = None

        source_name = resource_meta.source_name
        resource_name = resource_meta.resource_name
        resource_config = resource_meta.config

        # Log resource processing start
        self.logger.debug(
            f"Processing resource: {resource_name}",
            extra_fields={
                "source": source_name,
                "method": resource_config.method,
                "is_snapshot": resource_config.snapshot is not None,
            },
        )

        try:
            if self.spark is None:
                raise HandlerError("Spark session is not initialized for processing")

            is_db = self._is_database_source(source_config)

            # Check if streaming is enabled for this resource
            streaming_config = resource_config.get_streaming_config(self.config.defaults.streaming)
            has_batch_inputs = self.execution_plan.has_batch_inputs(source_name, resource_name)
            if is_db:
                use_streaming = False
            else:
                use_streaming = streaming_config.enable_streaming and has_batch_inputs

            # Initialize appropriate collector based on streaming mode
            if is_db:
                collector = None
            elif use_streaming:
                if streaming_config.mode == "disk":
                    # Use disk-based collector for memory efficiency
                    collector = DiskStreamingDataCollector(
                        disk_path=self.config.defaults.streaming.disk_config.path,
                        resource_key=f"{source_name}_{resource_name}",
                        file_size_threshold=self.config.defaults.streaming.disk_config.file_size_threshold,
                        spark=self.spark,
                        redis_context=self.redis_context,
                        resource_meta=resource_meta,
                        service=service,
                        execution_plan=self.execution_plan,
                    )
                    self.logger.trace(
                        "Using disk-based streaming collector for resource",
                        extra_fields={
                            "resource_name": resource_name,
                            "streaming_mode": "disk",
                            "disk_path": self.config.defaults.streaming.disk_config.path,
                            "file_size_threshold": self.config.defaults.streaming.disk_config.file_size_threshold,
                        },
                    )
                else:  # streaming_config.mode == "redis"
                    # Use Redis-based collector
                    collector = StreamingRawDataCollector(
                        redis_context=self.redis_context,
                        resource_key=f"{source_name}_{resource_name}",
                        flush_threshold=streaming_config.flush_threshold,
                        spark=self.spark,
                        resource_meta=resource_meta,
                        service=service,
                        execution_plan=self.execution_plan,
                    )
                    self.logger.trace(
                        "Using Redis-based streaming collector for resource",
                        extra_fields={
                            "resource_name": resource_name,
                            "streaming_mode": "redis",
                            "flush_threshold": streaming_config.flush_threshold,
                        },
                    )
            else:
                collector = RawDataCollector()
                self.logger.trace(
                    "Using standard collector for resource",
                    extra_fields={"resource_name": resource_name},
                )

            # Decide whether to use backfill date ranges (manual --backfill or auto on first write)
            use_backfill = False
            if not is_db and resource_meta.backfill_config:
                if self.backfill_mode:
                    use_backfill = True
                    self.logger.debug(
                        "Backfill enabled by CLI for resource",
                        extra_fields={"resource_name": resource_name, "source": source_name},
                    )
                elif resource_config.loading and self.record_limit is None:
                    try:
                        loader = LoaderFactory.create_loader(resource_config.loading)
                        if hasattr(loader, "set_spark_session") and self.spark:
                            loader.set_spark_session(self.spark)
                        if hasattr(loader, "destination_exists"):
                            source_config = self.execution_plan.get_source_config(source_name)
                            source_type = source_config.type if source_config else None
                            exists = loader.destination_exists(
                                resource_config.loading, source_type=source_type
                            )
                            if not exists:
                                use_backfill = True
                                self.logger.debug(
                                    "Auto-backfill: destination empty, using backfill date ranges",
                                    extra_fields={
                                        "resource_name": resource_name,
                                        "source": source_name,
                                    },
                                )
                    except Exception as e:
                        self.logger.trace(
                            "Could not check destination for auto-backfill, skipping",
                            extra_fields={
                                "resource_name": resource_name,
                                "error": str(e),
                            },
                        )
                elif not resource_config.loading and self.record_limit is None:
                    # Snapshot trigger pattern: resource has no loading but dependents do
                    # Check dependents' destinations; if any is empty, trigger backfill
                    dependent_loadings = self.execution_plan.get_dependent_loading_configs(
                        source_name, resource_name
                    )
                    for loading_config, source_type in dependent_loadings:
                        try:
                            loader = LoaderFactory.create_loader(loading_config)
                            if hasattr(loader, "set_spark_session") and self.spark:
                                loader.set_spark_session(self.spark)
                            if hasattr(loader, "destination_exists"):
                                exists = loader.destination_exists(
                                    loading_config, source_type=source_type
                                )
                                if not exists:
                                    use_backfill = True
                                    self.logger.debug(
                                        "Auto-backfill: dependent destination empty, using backfill date ranges",
                                        extra_fields={
                                            "resource_name": resource_name,
                                            "source": source_name,
                                        },
                                    )
                                    break
                        except Exception as e:
                            self.logger.trace(
                                "Could not check dependent destination for auto-backfill, skipping",
                                extra_fields={
                                    "resource_name": resource_name,
                                    "dependent_loading": str(loading_config.prefix),
                                    "error": str(e),
                                },
                            )

            # Generate all request contexts (handles single and nested batching, and backfill)
            request_contexts = self._generate_all_request_contexts(
                resource_meta=resource_meta,
                service=service,
                use_backfill=use_backfill,
            )
            total_contexts = len(request_contexts)
            failed_context_count = 0

            # Process each context (HTTP/SDK only; database sources use Spark extract_table)
            if not is_db:
                for context in request_contexts:
                    try:
                        self._make_single_request(
                            resource_config=resource_config,
                            context=context,
                            service=service,
                            collector=collector,
                            resource_meta=resource_meta,
                            source_config=source_config,
                        )
                    except Exception as e:
                        import traceback

                        traceback.print_exc()
                        failed_context_count += 1
                        self.logger.error(
                            "Failed to process request context",
                            extra_fields={
                                "resource_name": resource_name,
                                "source": source_name,
                                "context": context,
                                "error": str(e),
                                "error_type": type(e).__name__,
                                "will_continue": True,
                            },
                        )
                        # Continue with next context
                        continue

            # Centralized parsing of all collected data (or Spark JDBC path for database sources)
            # Optimization: parse / extract without transformations first, then transform once
            all_data_df = None
            if is_db:
                all_data_df = self._build_database_dataframe(
                    service, resource_meta, request_contexts
                )
            elif collector is not None and not collector.is_empty():
                if use_streaming and isinstance(
                    collector, (StreamingRawDataCollector, DiskStreamingDataCollector)
                ):
                    # For streaming collectors (both Redis and Disk-based), finalize returns the complete dataset
                    all_data_df = collector.finalize()
                    self.logger.debug(
                        "Finalized streaming collector",
                        extra_fields={
                            "has_transformations": bool(resource_meta.config.transformations),
                            "record_count": all_data_df.count() if all_data_df else 0,
                            "collector_type": type(collector).__name__,
                        },
                    )
                else:
                    # For standard collector, parse all batches
                    self.logger.trace(
                        "Processing collected batches",
                        extra_fields={
                            "batch_count": len(collector.batches),  # type: ignore
                            "has_transformations": bool(resource_meta.config.transformations),
                        },
                    )

                    # Parse each batch (schema mapping + field extraction only)
                    for batch in collector.batches:  # type: ignore
                        batch_df = self._parse_data(
                            raw_data=batch.raw_data,
                            resource_meta=resource_meta,
                            service=service,
                            parent_context=batch.parent_context,
                            request_context=batch.request_context,
                        )
                        if batch_df is not None:
                            all_data_df = (
                                batch_df
                                if all_data_df is None
                                else all_data_df.unionByName(batch_df, allowMissingColumns=True)
                            )

            if all_data_df is not None:
                if resource_meta.config.transformations:
                    self.logger.trace(
                        "Applying transformations to complete data",
                        extra_fields={
                            "record_count": all_data_df.count(),
                            "transformation_count": len(resource_meta.config.transformations),
                        },
                    )

                    if use_streaming and isinstance(
                        collector, (StreamingRawDataCollector, DiskStreamingDataCollector)
                    ):
                        request_context = collector.request_context
                    elif is_db:
                        request_context = {
                            "parameters": request_contexts[0] if request_contexts else {},
                            "request_body": {},
                        }
                    else:
                        request_context = (
                            collector.batches[0].request_context
                            if collector is not None
                            and hasattr(collector, "batches")
                            and collector.batches
                            else None
                        )  # type: ignore

                    if self.spark:
                        parser = SparkParser(
                            config=resource_meta.config,
                            spark=self.spark,
                            source_name=service.source_name,
                            resource_name=resource_meta.resource_name,
                            execution_plan=self.execution_plan,
                            redis_context=self.redis_context,
                        )
                        all_data_df = parser._apply_transformations(
                            all_data_df, request_context=request_context
                        )

                all_data_df = all_data_df.cache()

            # Store and process results
            try:
                if all_data_df is not None:
                    record_count = all_data_df.count()

                    if record_count > 0:
                        # Store in Redis only when this resource has downstream dependents (needed for SOURCE params)
                        dependent_resources = self.execution_plan.get_dependent_resources(
                            source_name, resource_name
                        )
                        if dependent_resources:
                            redis_key = self._get_redis_key(source_name, resource_name)
                            self.redis_context.store(key=redis_key, data=all_data_df)
                        else:
                            self.logger.trace(
                                "Skipping Redis store (no downstream dependents)",
                                extra_fields={
                                    "source": source_name,
                                    "resource_name": resource_name,
                                },
                            )

                        # Load data if we have records and loading is configured
                        location = None
                        if self.record_limit is None and resource_config.loading:
                            loader = LoaderFactory.create_loader(resource_config.loading)
                            if hasattr(loader, "set_spark_session"):
                                loader.set_spark_session(self.spark)

                            # Get source type from source config to prefix the path
                            source_config = self.execution_plan.get_source_config(source_name)
                            source_type = source_config.type if source_config else None

                            location = loader.load(
                                data=all_data_df,
                                config=resource_config.loading,
                                source_type=source_type,
                            )

                        status = "partial_failure" if failed_context_count > 0 else "success"
                        return {
                            "count": record_count,
                            "location": location,
                            "timestamp": datetime.now(UTC).isoformat(),
                            "status": status,
                        }

                status = (
                    "failed"
                    if total_contexts > 0 and failed_context_count == total_contexts
                    else "success"
                )
                return {
                    "count": 0,
                    "timestamp": datetime.now(UTC).isoformat(),
                    "warning": "No data returned from source",
                    "status": status,
                }
            finally:
                # CRITICAL: Unpersist cached DataFrame to free memory
                if all_data_df is not None:
                    try:
                        all_data_df.unpersist(blocking=True)
                        self.logger.trace(
                            "Unpersisted cached DataFrame",
                            extra_fields={"resource_name": resource_name, "source": source_name},
                        )
                    except Exception as e:
                        self.logger.warning(
                            "Failed to unpersist DataFrame",
                            extra_fields={"error": str(e), "resource_name": resource_name},
                        )

        except Exception as e:
            # Wrap exception with context - caller will handle logging
            # Cleanup will be performed in finally block
            raise HandlerError.from_error(
                e,
                f"Failed to process resource '{resource_name}' for source '{source_name}'",
                operation="process_resource",
                details={"resource_name": resource_name, "source": source_name},
            ) from e
        finally:
            # Ensure cleanup is performed if not already done
            if (
                use_streaming
                and collector is not None
                and isinstance(collector, DiskStreamingDataCollector)
            ):
                if not collector.cleaned_up:
                    try:
                        self.logger.debug(
                            "Performing final cleanup of disk collector in finally block",
                            extra_fields={
                                "resource_name": resource_name,
                                "source": source_name,
                                "cleaned_up_flag": collector.cleaned_up,
                            },
                        )
                        collector.cleanup_disk_path()
                    except Exception as cleanup_error:
                        self.logger.error(
                            "Failed to cleanup disk collector in finally block",
                            extra_fields={
                                "resource_name": resource_name,
                                "source": source_name,
                                "cleanup_error": str(cleanup_error),
                            },
                        )

    def cleanup(self):
        """Clean up resources used by the handler."""
        try:
            # Clean up Redis context
            if hasattr(self, "redis_context"):
                self.redis_context.cleanup()

            # Clean up Spark session
            if self.spark:
                try:
                    self.spark_manager.stop_session()
                except Exception as e:
                    self.logger.warning(f"Failed to stop Spark session: {e!s}")

            super().cleanup()

        except Exception as e:
            self.logger.error(f"Error during cleanup: {e!s}")

    def handle(self) -> Dict[str, Any]:
        """
        Handle the data ingestion flow based on configuration.

        Returns:
            Dict[str, Any]: Results of the operation

        Raises:
            HandlerError: If handling fails
        """
        results = {
            "timestamp": datetime.now(UTC).isoformat(),
            "sources": {},
            "execution_plan": self.execution_plan.summarize(),
        }

        self._audit_recorder = AuditRecorder()
        _prev_sigterm = None

        try:
            if hasattr(signal, "SIGTERM"):
                _prev_sigterm = signal.signal(signal.SIGTERM, _sigterm_handler)
            # Process each stage in order
            for stage in self.execution_plan.stages:
                self.logger.info(
                    f"Processing stage {stage.stage_number}",
                    extra_fields={
                        "resources": [
                            f"{ep.source_name}.{ep.resource_name}" for ep in stage.resources
                        ]
                    },
                )

                # Process all resources in this stage
                for resource_meta in stage.resources:
                    source_name = resource_meta.source_name
                    resource_name = resource_meta.resource_name

                    # Initialize source results if needed
                    if source_name not in results["sources"]:
                        results["sources"][source_name] = {"resources": {}, "status": "pending"}

                    source_results = results["sources"][source_name]
                    source_config = self.execution_plan.get_source_config(source_name)

                    if source_config is None:
                        error_details = {
                            "source": source_name,
                            "resource_name": resource_name,
                            "stage": stage.stage_number,
                            "error": "Source configuration not found",
                        }

                        source_results["status"] = "failed"
                        source_results["error"] = error_details

                        self.logger.error(
                            f"Source configuration not found for source '{source_name}'",
                            extra_fields=error_details,
                        )
                        continue

                    try:
                        # Create service for this source if needed
                        service = self._create_service(source_name, source_config)

                        # Process the resource
                        resource_result = self._process_resource(
                            resource_meta=resource_meta,
                            service=service,
                            source_config=source_config,
                        )

                        source_results["resources"][resource_name] = resource_result

                        if resource_result.get("status") == "failed":
                            source_results["status"] = "failed"
                            if "error" not in source_results:
                                source_results["error"] = {
                                    "source": source_name,
                                    "resource_name": resource_name,
                                    "stage": stage.stage_number,
                                    "error": resource_result.get("warning", "Resource failed"),
                                }

                        # Debug resource completion
                        self.logger.debug(
                            f"Completed resource: {resource_name}",
                            extra_fields={
                                "result_summary": {
                                    "status": resource_result.get("status", "success"),
                                    "record_count": resource_result.get("count", 0),
                                    "location": resource_result.get("location"),
                                }
                            },
                        )

                    except Exception as e:
                        error_details = {
                            "source": source_name,
                            "resource_name": resource_name,
                            "stage": stage.stage_number,
                            "error": str(e),
                        }

                        # Add error context if available
                        if isinstance(e, HandlerError):
                            error_details.update(
                                {
                                    "operation": e.operation,
                                    "details": e.details,
                                    "original_error": (
                                        str(e.original_error) if e.original_error else None
                                    ),
                                }
                            )

                        source_results["status"] = "failed"
                        source_results["error"] = error_details

                        self.logger.error(
                            f"Failed to process resource '{resource_name}' for source '{source_name}'",
                            extra_fields=error_details,
                        )

                        # Continue with next resource instead of failing entire pipeline
                        continue

                # Mark successful sources
                for source_name in results["sources"]:
                    if results["sources"][source_name]["status"] == "pending":
                        results["sources"][source_name]["status"] = "success"

            # Set overall status
            results["status"] = (
                "failed"
                if any(s["status"] == "failed" for s in results["sources"].values())
                else "success"
            )

            return results

        except Exception as e:
            error_msg = "Failed to handle data ingestion"
            error_details = {"error": str(e)}

            if isinstance(e, HandlerError):
                error_details.update(
                    {
                        "operation": e.operation,
                        "details": e.details,
                        "original_error": str(e.original_error) if e.original_error else None,
                    }
                )

            self.logger.error(error_msg, extra_fields=error_details)

            results["status"] = "failed"
            results["error"] = error_details

            return results

        finally:
            if _prev_sigterm is not None:
                try:
                    signal.signal(signal.SIGTERM, _prev_sigterm)
                except Exception:
                    pass
            try:
                audit_recorder = getattr(self, "_audit_recorder", None)
                control_bucket = os.getenv("S3_CONTROL_BUCKET")
                if audit_recorder is not None and self.spark is not None and control_bucket:
                    audit_recorder.flush(self.spark, control_bucket)
            except KeyboardInterrupt:
                self.logger.warning("Audit flush interrupted")
                raise
            except Exception as e:
                self.logger.error(
                    "Failed to flush audit trail",
                    extra_fields={"error": str(e), "error_type": type(e).__name__},
                )
            self.cleanup()

    def validate(self) -> None:
        """
        Validate the pipeline configuration without executing it.

        This method performs the following validations:
        1. Validates all source configurations that are part of the execution plan
        2. Probes source connectivity for each selected source
        3. Checks resource dependencies and parameter relationships
        4. Validates schema definitions
        5. Verifies and tests loader configurations
        6. Tests Redis connectivity if Redis context is configured
        7. Validates execution plan for circular dependencies

        Raises:
            HandlerError: If validation fails
        """
        try:
            self.logger.info("Starting configuration validation")

            # Validate Redis configuration
            self.logger.debug("Validating Redis context configuration")
            try:
                self.redis_context.validate_connection()
                self.logger.debug("Redis context validation successful")
            except Exception as e:
                raise HandlerError(f"Redis context validation failed: {e!s}") from e

            # Validate execution plan
            self.logger.debug("Validating execution plan")
            plan_summary = self.execution_plan.summarize()
            self.logger.debug(
                "Execution plan validation successful",
                extra_fields={
                    "total_stages": plan_summary["total_stages"],
                    "total_resources": plan_summary["total_resources"],
                },
            )

            # Track unique S3 buckets to validate
            s3_buckets = set()

            # Get the list of sources that are actually part of the execution plan
            selected_sources = {
                resource_meta.source_name
                for stage in self.execution_plan.stages
                for resource_meta in stage.resources
            }

            # Validate each selected source
            for source_name in selected_sources:
                source_config = self.config.sources[source_name]
                self.logger.debug(f"Validating source: {source_name}")

                # Create service instance and test connectivity
                try:
                    service = self._create_service(source_name, source_config)
                    self._probe_source_connectivity(source_name, source_config, service)
                except Exception as e:
                    raise HandlerError(
                        f"Invalid service configuration for {source_name}: {e!s}"
                    ) from e

                # Validate each resource's configuration
                for resource_name, resource_config in source_config.resources.items():
                    self.logger.debug(f"Validating resource: {resource_name}")

                    # Validate schema fields
                    if not resource_config.fields:
                        raise HandlerError(f"No schema defined for resource: {resource_name}")

                    for field in resource_config.fields:
                        if not field.name:
                            raise HandlerError(
                                f"Invalid schema field in {resource_name}: name is required"
                            )

                    # Validate loading configuration (if provided)
                    if resource_config.loading:
                        if resource_config.loading.destination == "s3":
                            if not resource_config.loading.bucket:
                                raise HandlerError(
                                    f"Missing S3 bucket configuration for resource: {resource_name}"
                                )
                            s3_buckets.add(resource_config.loading.bucket)

                self.logger.info(f"Successfully validated source: {source_name}")

            # Test S3 connectivity for all unique buckets
            for bucket in s3_buckets:
                self._test_s3_connectivity(bucket)

            self.logger.info("Configuration validation completed successfully")

        except Exception as e:
            raise HandlerError(f"Configuration validation failed: {e!s}") from e

        finally:
            self.cleanup()
