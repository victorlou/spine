"""
Configuration models for data pipeline.
Uses Pydantic for validation and type safety.
"""

import ast
import copy
import json
from enum import Enum, StrEnum
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Set, Union

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    HttpUrl,
    TypeAdapter,
    ValidationInfo,
    field_validator,
    model_validator,
)

from src.config.incremental_extract import (
    IncrementalExtractConfig,
    IncrementalWatermarkCursorStrategy,
)
from src.config.loading_schema import (
    OBJECT_STORE_DESTINATIONS,
    normalize_azure_account_label,
    normalize_azure_container_label,
    normalize_loading_destination,
    normalize_object_store_bucket_label,
)
from src.config.telemetry import TelemetryConfig
from src.utils.dynamic_values import (
    ComplexDynamicValue,
    DynamicOrStaticValue,
    DynamicSourceReference,
    DynamicValueType,
    get_resolver,
)
from src.utils.query_utils import is_databricks_source_ref, parse_databricks_source_ref
from src.utils.redis_context import RedisContextManager

QUERIES_DIR: str = "queries"


class RetryConfig(BaseModel):
    """Retry configuration settings."""

    max_attempts: int = Field(default=3, ge=1)
    initial_delay: float = Field(default=1.0, gt=0)
    backoff_factor: float = Field(default=2.0, gt=1)


class LoadingFormat(str, Enum):
    """Supported loading formats."""

    DELTA = "delta"
    ICEBERG = "iceberg"


INCREMENTAL_DESTINATION_COLUMN_CURSOR_FORMATS: frozenset[LoadingFormat] = frozenset(
    (LoadingFormat.DELTA, LoadingFormat.ICEBERG)
)


class LoadingConfig(BaseModel):
    """
    Data loading configuration.
    For object storage destinations, prefix should follow ``source_name/resource_name``
    (e.g. ``my_source/users``). When ``prefix`` is omitted or left blank after merge, the handler
    fills it at runtime with ``{source_name}/{resource_name}`` for that resource. Set ``prefix``
    explicitly only when overriding that default path. When a non-empty ``prefix`` is set in
    YAML, it must still satisfy the ``source_name/resource_name`` segment rules validated below.

    For file-based formats (Parquet), the actual data will be stored in a 'data'
    subdirectory under this prefix.

    Supported destinations and required fields:
    - ``s3``    → requires ``s3_bucket`` (or alias ``bucket``)
    - ``local`` → requires ``storage_root``
    - ``gcs``   → requires ``gcs_bucket`` (or alias ``bucket``)
    - ``azure_blob`` (aliases: ``blob``, ``azure``) → requires ``azure_container`` (or alias ``bucket``) and ``azure_account``

    Save modes for Delta format:
    - **overwrite** (default): Replace all existing data in the table
    - **append**: Add new data without removing existing data
    - **merge**: Update existing rows and insert new ones based on primary keys (upsert).
      Requires merge_keys to be specified.

    Schema evolution is automatically enabled for all modes, allowing new columns
    to be added to existing tables.
    """

    enabled: bool = Field(
        default=True,
        description=(
            "When false, skip loader writes for this resource (use after merging defaults). "
            "Omit ``loading`` on a resource to inherit defaults; set ``enabled: false`` to opt out."
        ),
    )
    destination: str
    format: LoadingFormat = LoadingFormat.DELTA
    write_mode: Literal["overwrite", "append", "merge", "ignore", "error"] = "overwrite"
    compression: Optional[str] = "snappy"
    prefix: Optional[str] = Field(
        default=None,
        description=(
            "Object-store path prefix before the format-specific layout (e.g. ``source/resource``). "
            "Omit or leave unset so the handler uses ``{source_name}/{resource_name}`` for this resource. "
            "When set explicitly, must be at least two path segments and must not include a ``data`` segment."
        ),
    )
    storage_root: Optional[str] = Field(
        default=None,
        description=(
            "Filesystem directory for destination: local (Spark file://). "
            "May be relative to the repository root (directory containing ``src/``); "
            "ConfigLoader resolves it before validation."
        ),
    )
    bucket: Optional[str] = None  # Generic alias bucket/container name
    s3_bucket: Optional[str] = Field(
        default=None,
        description="Canonical S3 bucket name for destination: s3 (Spark s3a://).",
    )
    gcs_bucket: Optional[str] = Field(
        default=None,
        description="GCS bucket name for destination: gcs (Spark gs://).",
    )
    azure_container: Optional[str] = Field(
        default=None,
        description="Azure Blob Storage container name for destination: azure_blob (Spark abfs://).",
    )
    azure_account: Optional[str] = Field(
        default=None,
        description="Azure storage account name for destination: azure_blob (Spark abfs://).",
    )
    output_partitions: Optional[int] = Field(
        default=None,
        ge=1,
        description=(
            "Optional Spark partition count before Delta/Iceberg writes (implemented as coalesce). "
            "Applies to append, overwrite, and merge: merge uses this on the incoming source "
            "DataFrame only (the existing target table is unchanged). Unset preserves upstream "
            "partitioning—for example parallel JDBC reads. When set, narrows partitions toward "
            "this cap (coalesce never increases partition count—if the DataFrame has fewer "
            "partitions than this value, the count stays unchanged). To run more write tasks than "
            "the JDBC extract produced, use parallel table_read_options or an explicit Spark "
            "repartition (shuffle), not a larger output_partitions alone. Typical output file "
            "count follows task parallelism but is not strictly equal to this value. Raw Parquet "
            "file loads still use a single writer partition in the object-store loader."
        ),
    )
    merge_keys: Optional[List[str]] = Field(
        default=None,
        description="List of column names to use as primary keys for merge operations. Required when write_mode is 'merge'.",
    )
    force_nondeterministic_deduplication: bool = Field(
        default=False,
        description="Enable deduplication during merge operations using merge_keys as the matching criteria. When True, duplicate records matching the merge key are deduplicated with non-deterministic row selection. When False, all records are preserved without deduplication.",
    )

    @model_validator(mode="after")
    def normalize_destination_alias_fields(self) -> "LoadingConfig":
        """Normalize generic/provider aliases and reject conflicting values."""
        if not self.enabled:
            return self

        if self.destination == "s3":
            if self.bucket and self.s3_bucket and self.bucket != self.s3_bucket:
                raise ValueError("bucket and s3_bucket cannot both be set with different values")
            effective = self.s3_bucket or self.bucket
            object.__setattr__(self, "s3_bucket", effective)
            object.__setattr__(self, "bucket", effective)

        elif self.destination == "gcs":
            if self.bucket and self.gcs_bucket and self.bucket != self.gcs_bucket:
                raise ValueError("bucket and gcs_bucket cannot both be set with different values")
            effective = self.gcs_bucket or self.bucket
            object.__setattr__(self, "gcs_bucket", effective)
            object.__setattr__(self, "bucket", effective)

        elif self.destination == "azure_blob":
            if self.bucket and self.azure_container and self.bucket != self.azure_container:
                raise ValueError(
                    "bucket and azure_container cannot both be set with different values"
                )
            effective = self.azure_container or self.bucket
            object.__setattr__(self, "azure_container", effective)
            object.__setattr__(self, "bucket", effective)

        return self

    @model_validator(mode="after")
    def normalize_object_store_identity_fields(self) -> "LoadingConfig":
        """Normalize bucket/container/account/storage labels at validation time."""
        dest = self.destination
        if dest == "s3":
            nb = normalize_object_store_bucket_label(self.s3_bucket)
            object.__setattr__(self, "s3_bucket", nb)
            object.__setattr__(self, "bucket", nb)
        elif dest == "gcs":
            nb = normalize_object_store_bucket_label(self.gcs_bucket)
            object.__setattr__(self, "gcs_bucket", nb)
            object.__setattr__(self, "bucket", nb)
        elif dest == "azure_blob":
            nc = normalize_azure_container_label(self.azure_container)
            na = normalize_azure_account_label(self.azure_account)
            object.__setattr__(self, "azure_container", nc)
            object.__setattr__(self, "bucket", nc)
            object.__setattr__(self, "azure_account", na)
        elif dest == "local" and self.storage_root is not None:
            object.__setattr__(self, "storage_root", str(self.storage_root).strip())
        return self

    @field_validator("destination")
    @classmethod
    def normalize_destination_aliases(cls, v: str) -> str:
        """Normalize destination aliases to canonical destination identifiers."""
        return normalize_loading_destination(v)

    @field_validator("prefix")
    @classmethod
    def validate_prefix(cls, v: Optional[str], info: ValidationInfo) -> Optional[str]:
        """Validate prefix format for object storage when a non-empty prefix is supplied."""
        if not info.data.get("enabled", True):
            return v
        # ``destination`` is validated before ``prefix``; aliases are already normalized.
        dest = info.data.get("destination")
        if dest is not None and dest in OBJECT_STORE_DESTINATIONS:
            if v is None or not str(v).strip():
                return None

            # Remove any leading/trailing slashes
            v = str(v).strip("/")

            # Check prefix structure (should be at least source/resource)
            parts = v.split("/")
            if len(parts) < 2:
                raise ValueError(
                    "prefix must follow the pattern 'source_name/resource_name' "
                    "(e.g., 'my_source/users')"
                )

            # Ensure 'data' is not included in the prefix
            # Delta appends /data under the prefix; Iceberg writes to the prefix root — neither should include 'data' explicitly
            if "data" in parts:
                raise ValueError(
                    "prefix should not include 'data' directory - it will be automatically appended"
                )

        return v

    @model_validator(mode="after")
    def validate_merge_keys(self) -> "LoadingConfig":
        """
        Validate that merge_keys is provided when write_mode is 'merge'.

        Returns:
            LoadingConfig: Validated configuration instance

        Raises:
            ValueError: If write_mode is 'merge' but merge_keys is not provided or is empty
        """
        if not self.enabled:
            return self
        if self.write_mode == "merge":
            if not self.merge_keys:
                raise ValueError(
                    "merge_keys is required when write_mode is 'merge'. "
                    "Please provide a list of column names to use as primary keys."
                )
            if len(self.merge_keys) == 0:
                raise ValueError("merge_keys must be a non-empty list when write_mode is 'merge'.")
        return self

    @model_validator(mode="after")
    def validate_destination_storage_fields(self) -> "LoadingConfig":
        """Require the appropriate storage fields for each object store destination."""
        if not self.enabled:
            return self
        if self.destination not in OBJECT_STORE_DESTINATIONS:
            valid = ", ".join(sorted(OBJECT_STORE_DESTINATIONS))
            raise ValueError(
                f"Unsupported loading destination '{self.destination}'. Valid destinations: {valid}"
            )
        if self.destination == "s3":
            if not self.s3_bucket:
                raise ValueError("s3_bucket (or bucket alias) is required for S3 destination")
        elif self.destination == "local":
            if not self.storage_root:
                raise ValueError("storage_root is required for local destination")
        elif self.destination == "gcs":
            if not self.gcs_bucket:
                raise ValueError("gcs_bucket (or bucket alias) is required for GCS destination")
        elif self.destination == "azure_blob":
            if not self.azure_container:
                raise ValueError(
                    "azure_container (or bucket alias) is required for Azure destination"
                )
            if not self.azure_account:
                raise ValueError("azure_account is required for Azure destination")
        return self

    def destination_dedup_key(self) -> tuple[str, ...]:
        """Stable identity for a destination so the same bucket/root is probed once."""
        if self.destination == "s3":
            return ("s3", self.s3_bucket or "")
        if self.destination == "gcs":
            return ("gcs", self.gcs_bucket or "")
        if self.destination == "azure_blob":
            return ("azure_blob", self.azure_container or "", self.azure_account or "")
        if self.destination == "local":
            root = self.storage_root or ""
            if not root:
                return ("local", "")
            return ("local", str(Path(root).expanduser().resolve()))
        return (self.destination,)

    def destination_details(self) -> Dict[str, Any]:
        """Operator-readable destination context for errors and logs."""
        if self.destination == "s3":
            return {"destination": "s3", "s3_bucket": self.s3_bucket}
        if self.destination == "gcs":
            return {"destination": "gcs", "gcs_bucket": self.gcs_bucket}
        if self.destination == "azure_blob":
            return {
                "destination": "azure_blob",
                "azure_container": self.azure_container,
                "azure_account": self.azure_account,
            }
        if self.destination == "local":
            return {"destination": "local", "storage_root": self.storage_root}
        return {"destination": self.destination}

    model_config = {"validate_assignment": True}


class SchemaField(BaseModel):
    """
    Schema field definition.
    Specifies mapping between source data fields and output columns.
    """

    name: str  # Name of the field in the output
    source: str  # Name of the field in the source data


class TransformationType(str, Enum):
    """Supported transformation types."""

    ADD_COLUMN = "add_column"
    ADD_COLUMN_FROM_REQUEST = "add_column_from_request"


class Transformation(BaseModel):
    """
    Data transformation definition.

    Supports two types of transformations:
    1. add_column: Add a static or dynamic value as a new column
       Example:
         type: add_column
         name: timestamp
         value: "{{ now_iso() }}"

    2. add_column_from_request: Add values from the request parameters/body
       Example:
         type: add_column_from_request
         name: request_date  # Name of the new column
         source: start_date  # Key in parameters or request_body to extract
         location: parameters  # Where to look for the value: 'parameters' or 'request_body'
         data_type: string  # Optional: how to format the value (string, integer, float, array)
    """

    type: TransformationType
    name: str  # Name of the output column

    # For add_column type
    value: Optional[Any] = None

    # For add_column_from_request type
    source: Optional[str] = None  # Key to extract from parameters/request_body
    location: Optional[Literal["parameters", "request_body"]] = None
    data_type: Optional[Literal["string", "integer", "float", "array"]] = (
        "string"  # Optional type conversion
    )

    @field_validator("value")
    @classmethod
    def validate_value_for_add_column(cls, v: Optional[Any], info: ValidationInfo) -> Optional[Any]:
        """Ensure value is provided for add_column type."""
        if info.data.get("type") == TransformationType.ADD_COLUMN and v is None:
            raise ValueError("value is required for add_column transformation")
        return v

    @field_validator("source", "location")
    @classmethod
    def validate_request_fields(cls, v: Optional[str], info: ValidationInfo) -> Optional[str]:
        """Ensure source and location are provided for add_column_from_request type."""
        if info.data.get("type") == TransformationType.ADD_COLUMN_FROM_REQUEST:
            if v is None:
                field_name = info.field_name
                raise ValueError(
                    f"{field_name} is required for add_column_from_request transformation"
                )
        return v

    def get_value(self) -> Any:
        """Get the value for add_column transformations (Jinja or static)."""
        if self.type != TransformationType.ADD_COLUMN:
            return None
        return self.value


class PreprocessorType(Enum):
    CONCAT = "concat"


class PreprocessConfig(BaseModel):
    """
    Configuration for preprocessing steps on parameter values.
    """

    type: PreprocessorType
    prefix: Optional[str] = None
    separator: Optional[str] = None

    @model_validator(mode="after")
    def validate_type_dependencies(self):
        """Validate dependencies after all fields are set."""
        if self.type == PreprocessorType.CONCAT and not self.separator:
            raise ValueError("separator is required when type is 'concat'")
        return self


class SnapshotConfig(BaseModel):
    """Configuration for snapshot-based polling."""

    max_time: int = Field(
        default=300, description="Maximum time in seconds to wait for snapshot completion"
    )
    interval: int = Field(default=60, description="Initial interval between checks in seconds")
    backoff_factor: float = Field(
        default=1.5, description="Multiplier for exponential backoff between retries"
    )
    max_interval: int = Field(default=300, description="Maximum interval between checks in seconds")
    ready_condition: str = Field(
        description="Python expression to evaluate against response to determine readiness"
    )
    error_condition: Optional[str] = Field(
        default=None, description="Optional expression that indicates a terminal error state"
    )

    @field_validator("ready_condition", "error_condition")
    @classmethod
    def validate_condition(cls, v: Optional[str]) -> Optional[str]:
        """Validate that conditions are valid Python expressions."""
        if v is not None:
            try:
                ast.parse(v, mode="eval")
            except SyntaxError as e:
                raise ValueError(f"Invalid Python expression: {e}") from e
        return v


class PaginationType(str, Enum):
    """Supported pagination types."""

    PAGE_NUMBER = "page_number"  # Page-based pagination (page=1, page=2, etc.)
    CURSOR = "cursor"  # Cursor-based pagination (cursor tokens)
    OFFSET = "offset"  # Offset-based pagination (offset=0, offset=100, etc.)
    TIME_BASED = "time_based"  # Time-based pagination (since=timestamp, until=timestamp)


class PaginationConfig(BaseModel):
    """Configuration for pagination support."""

    type: PaginationType = Field(
        default=PaginationType.PAGE_NUMBER, description="Type of pagination strategy to use"
    )
    page_info_path: str = Field(
        description="Path to pagination metadata in response (e.g., 'data.page_info')"
    )
    max_pages: Optional[int] = Field(
        default=None, description="Optional limit on number of pages to fetch"
    )
    response_page_field: Optional[str] = Field(
        default="page",
        description="Field name in page_info response that contains the current page number (optional - only total_pages is required). Only used for PAGE_NUMBER type.",
    )
    response_total_pages_field: str = Field(
        default="total_page",
        description="Field name in page_info response that contains the total number of pages. Only used for PAGE_NUMBER type.",
    )
    response_page_size_field: Optional[str] = Field(
        default="page_size",
        description="Field name in page_info response that contains the page size (optional)",
    )
    response_total_records_field: Optional[str] = Field(
        default="total_number",
        description="Field name in page_info response that contains the total number of records (optional)",
    )


class BatchSizeMode(str, Enum):
    """Batch size modes for parameter processing."""

    ALL = "all"  # Process all items in a single batch


class RequestFormatType(str, Enum):
    """Supported request format types."""

    STRING = "string"
    ARRAY = "array"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    JSON_STRING = "json_string"


class RequestFormatConfig(BaseModel):
    """
    Configuration for request value formatting.
    Attributes:
    type: How to format the value(s) in the API request
        - "string": Convert to string
        - "array": Ensure value is an array
        - "integer": Convert to integer
        - "float": Convert to float
        - "json_string": Convert arrays or dicts to JSON string
    preprocess: Optional list of preprocessing steps to apply before formatting
    """

    type: RequestFormatType
    preprocess: Optional[List[PreprocessConfig]] = None


class InputConfig(BaseModel):
    """Configuration for a request input (value, format, batching).

    Attributes:
        value: Static or dynamic value for the input. For batching, provide a list of values.
                Can be a simple value (str, int, float, bool), a list, a dict, or a dynamic value.
                Dict values can contain nested fields that are themselves DynamicOrStaticValue types,
                enabling nested parameter structures with independent value resolution.
        input_format: How the parameter value should be interpreted
            - "single": Treat value as a single item
            - "array": Treat value as a list of items
        request_format: How to format the value(s) in the API request (``RequestFormatConfig``; shorthand strings are normalized at validation)
        batch_size: Optional batch size for processing. Static lists are automatically batched.
        pagination: Optional pagination configuration (typically used for page parameters)

    Note:
        The parameter type (static vs dynamic) is automatically inferred from the value structure:
        - If value has source_config or is a ComplexDynamicValue → dynamic
        - Otherwise → static

    Nested Parameter Support:
        When a parameter value is a dict (e.g., Paging: {Limit: 1000, PageNo: {value: PAGINATION_TYPE}}),
        the handler will automatically resolve any nested "value" fields independently before using
        the parameter in requests. This allows nested dynamic values to be resolved separately from
        the parent parameter structure, enabling complex parameter configurations.
    """

    value: DynamicOrStaticValue  # Static or dynamic value
    input_format: Literal["single", "array"] = "single"  # Format of the input parameter
    request_format: Optional[RequestFormatConfig] = Field(
        default=None,
        description="Request formatting; use ``{type: string}`` or shorthand ``string`` at YAML load time.",
    )
    batch_size: Optional[Union[int, BatchSizeMode]] = (
        None  # Batch size for processing. Use BatchSizeMode.ALL to process all items in a single batch.
    )
    include_as_field: bool = Field(
        default=False,
        description="If true, include this parameter as a field in the output schema (prefixed with '_')",
    )
    pagination: Optional["PaginationConfig"] = Field(
        default=None,
        description="Pagination configuration for this parameter (typically used for page parameters)",
    )

    @field_validator("request_format", mode="before")
    @classmethod
    def normalize_request_format(cls, v: Any) -> Optional[RequestFormatConfig]:
        """Accept ``RequestFormatConfig``, a format name string, or a dict from YAML."""
        if v is None:
            return None
        if isinstance(v, RequestFormatConfig):
            return v
        if isinstance(v, RequestFormatType):
            return RequestFormatConfig(type=v)
        if isinstance(v, str):
            try:
                return RequestFormatConfig(type=RequestFormatType(v))
            except ValueError as e:
                allowed = ", ".join(repr(t.value) for t in RequestFormatType)
                raise ValueError(
                    f"request_format string must be one of [{allowed}], got {v!r}"
                ) from e
        if isinstance(v, dict):
            return RequestFormatConfig.model_validate(v)
        raise TypeError(
            f"request_format must be null, str, dict, or RequestFormatConfig, got {type(v).__name__}"
        )

    @model_validator(mode="after")
    def extract_pagination_from_value(self):
        """Extract pagination config from value if type is PAGINATION."""
        # If pagination is already set, don't override
        if self.pagination is not None:
            return self

        # Check if value is a ComplexDynamicValue with PAGINATION type
        if (
            isinstance(self.value, ComplexDynamicValue)
            and self.value.type == DynamicValueType.PAGINATION
        ):
            if self.value.pagination_config:
                # Convert dict to PaginationConfig
                self.pagination = PaginationConfig(**self.value.pagination_config)

        return self

    def format_request_value(self, value: Any) -> Any:
        """
        Format a value for use in an API request.

        This applies preprocessing steps (if configured) before format conversion.
        """
        if value is None:
            return None

        # Apply preprocessing steps sequentially if configured
        # Below implementation is for simple types like str, int, float, list
        # Complex types (dict, etc.) are not processed here
        # If preprocessing is configured, the perceived value will be a list (regardless of the input type of the value) and after applying preprocessing steps
        if (
            self.request_format
            and self.request_format.preprocess
            and isinstance(value, (str, int, float, list))
        ):
            processed_value = value if (isinstance(value, list)) else [value]

            for step in self.request_format.preprocess:
                if step.type == PreprocessorType.CONCAT and step.separator:
                    # Concatenate list into a single string
                    processed_value = [step.separator.join(str(v) for v in processed_value)]
                else:
                    # Raise error for unsupported preprocessing types
                    raise ValueError(
                        f"Unsupported preprocessing type: {step.type}. Supported types are: {', '.join([t.value for t in PreprocessorType])}"
                    )

            value = processed_value

        format_config = self.request_format
        if not format_config:
            return value

        # Apply format conversion based on type
        format_type = format_config.type

        if format_type == "string":
            # If converting array to string, take the first element
            if isinstance(value, list) and len(value) > 0:
                return str(value[0])
            return str(value)
        elif format_type == "integer":
            return int(value[0] if isinstance(value, list) else value)
        elif format_type == "float":
            return float(value[0] if isinstance(value, list) else value)
        elif format_type == "boolean":
            raw = value[0] if isinstance(value, list) else value
            return bool(raw) if raw is not None else None
        elif format_type == "array":
            # Don't wrap if already an array
            return value if isinstance(value, list) else [value]
        elif format_type == "json_string":
            # Convert arrays or dicts to JSON string
            if isinstance(value, (list, dict)):
                return json.dumps(value)
            return str(value)

        return value

    def has_source_config(self) -> bool:
        """
        Check if this parameter has a source configuration defined.

        Returns:
            bool: True if value is a ComplexDynamicValue with SOURCE type and source_config is defined
        """
        return isinstance(self.value, ComplexDynamicValue) and (
            self.value.type == DynamicValueType.SOURCE and self.value.source_config is not None
        )

    def get_source_config(self) -> Optional[DynamicSourceReference]:
        """
        Get the source configuration if this parameter has one defined.

        Returns:
            Optional[DynamicSourceReference]: Reference to another source's field when value is a
            ComplexDynamicValue with SOURCE type and ``source_config`` is set.
        """
        if isinstance(self.value, ComplexDynamicValue) and (
            self.value.type == DynamicValueType.SOURCE and self.value.source_config is not None
        ):
            return self.value.source_config
        return None

    def get_databricks_query_refs(self) -> List[str]:
        """
        Extract Databricks query refs from parameter value (Jinja strings).

        Returns:
            List of query_ref strings (e.g. ["uk_aus_nz_store_locations"])
        """
        import re

        refs: List[str] = []
        if isinstance(self.value, str) and "databricks(" in self.value:
            refs.extend(re.findall(r"databricks\s*\(\s*['\"]([^'\"]+)['\"]", self.value))
        return refs

    def get_databricks_column(self) -> Optional[str]:
        """
        Extract the ``column=`` argument from a ``databricks(...)`` Jinja value, if present.

        Returns:
            The column name selected via ``{{ databricks('ref', column='col') }}``, or None.
        """
        import re

        if isinstance(self.value, str) and "databricks(" in self.value:
            match = re.search(r"column\s*=\s*['\"]([^'\"]+)['\"]", self.value)
            if match:
                return match.group(1)
        return None

    def get_databricks_source_ref(self) -> Optional[str]:
        """
        Return the databricks query_ref when this input's SOURCE targets a databricks table.

        Detects ``source_config.source`` of the form ``databricks:<ref>`` (used by lookups) so the
        planner can load and execute the query at plan build like other databricks refs.
        """
        source_config = self.get_source_config()
        if source_config and is_databricks_source_ref(source_config.source):
            return parse_databricks_source_ref(source_config.source)
        return None

    def has_filter_config(self) -> bool:
        """
        Check if this parameter has a filter configuration defined.

        Returns:
            bool: True if value is a ComplexDynamicValue with SOURCE type, source_config is defined,
                  and filter is configured
        """
        if isinstance(self.value, ComplexDynamicValue):
            return (
                self.value.type == DynamicValueType.SOURCE
                and self.value.source_config is not None
                and self.value.source_config.filter is not None
            )
        return False

    def get_filter_config(self) -> Optional[Any]:
        """
        Get the filter configuration if this parameter has one defined.

        Returns:
            Optional[Any]: Filter configuration if value is a ComplexDynamicValue with SOURCE type,
                          source_config is defined, and filter is configured. None otherwise.
        """
        if isinstance(self.value, ComplexDynamicValue):
            if (
                self.value.type == DynamicValueType.SOURCE
                and self.value.source_config is not None
                and self.value.source_config.filter is not None
            ):
                return self.value.source_config.filter
        return None

    def is_static_list(self) -> bool:
        """Check if this parameter is a static list (not dynamic)."""
        return isinstance(self.value, list) and not self.get_source_config()


RequestInputLocation = Literal["path", "query", "body"]


class RequestInputConfig(InputConfig):
    """
    Request input configuration: InputConfig plus location in the HTTP request.

    Used for the unified request_inputs dict. Each input has the same resolution
    and batching behavior; location determines where the value goes (path, query, or body).
    If location is omitted, default is query for GET and body for POST (set by ResourceConfig validator).
    """

    location: Optional[RequestInputLocation] = Field(
        default=None,
        description="Where this input is sent: path, query, or body. Default: query (GET) or body (POST).",
    )
    correlate: Optional[str] = Field(
        default=None,
        description=(
            "Correlation group id. Inputs sharing a correlate group are iterated row-by-row "
            "(zipped) from the same multi-column databricks query instead of producing a "
            "cartesian product. Each member must select a column via "
            "{{ databricks('ref', column='...') }} and share the same query and batch_size."
        ),
    )


class JWTConfig(BaseModel):
    """JWT-specific configuration."""

    provider: Literal["jwt_bearer", "roundel"] = "jwt_bearer"
    algorithm: str = "RS256"
    version: str = "2.0"
    headers: Dict[str, str] = Field(default_factory=lambda: {"alg": "RS256", "typ": "JWT"})
    token_exchange: Dict[str, str] = Field(
        default_factory=lambda: {"grant_type": "password", "scope": "profile email openid"}
    )


class AuthConfig(BaseModel):
    """Authentication configuration."""

    type: str  # "oauth_jwt", "basic", "api_key", "bearer_token"
    token_url: Optional[HttpUrl] = (
        None  # Required for oauth_jwt, optional for bearer_token (auto-refresh)
    )
    client_id: Optional[str] = (
        None  # Required for oauth_jwt, basic, api_key; optional for bearer_token (auto-refresh)
    )
    client_secret: Optional[str] = (
        None  # Required for oauth_jwt, basic; optional for bearer_token (auto-refresh)
    )
    issuer: Optional[str] = None  # Required for oauth_jwt
    private_key: Optional[str] = None  # Base64 encoded private key for oauth_jwt
    bearer_token: Optional[str] = (
        None  # Required for bearer_token auth (unless refresh credentials provided)
    )
    refresh_token: Optional[str] = None  # Optional for bearer_token (enables auto-refresh)
    token_request_content_type: Literal["json", "form"] = Field(
        default="json",
        description=(
            "Content type for token refresh requests: 'json' sends application/json "
            "(request body as JSON), 'form' sends application/x-www-form-urlencoded "
            "(request body as form data). Most OAuth2 providers expect 'form'."
        ),
    )
    header_name: str = "Authorization"  # Name of the auth header
    header_format: str = "Bearer {token}"  # Format string for the auth header value
    jwt_config: Optional[JWTConfig] = None  # JWT-specific settings, required for oauth_jwt

    @model_validator(mode="after")
    def validate_auth_config(self) -> "AuthConfig":
        """Validate authentication configuration based on type."""
        if self.type == "oauth_jwt":
            if not self.jwt_config:
                raise ValueError("jwt_config is required for oauth_jwt authentication")

            if not self.issuer:
                raise ValueError("issuer is required for oauth_jwt authentication")

            if not self.private_key:
                raise ValueError("private_key is required for oauth_jwt authentication")

            if not self.token_url:
                raise ValueError("token_url is required for oauth_jwt authentication")

            # roundel requires client_id/client_secret; jwt_bearer/google do not
            if self.jwt_config.provider == "roundel":
                if not self.client_id:
                    raise ValueError("client_id is required for roundel oauth_jwt authentication")
                if not self.client_secret:
                    raise ValueError(
                        "client_secret is required for roundel oauth_jwt authentication"
                    )

        elif self.type == "basic":
            if not self.client_id:
                raise ValueError("client_id is required for basic authentication")
            if not self.client_secret:
                raise ValueError("client_secret is required for basic authentication")

        elif self.type == "api_key":
            if not self.client_id:
                raise ValueError("client_id is required for api_key authentication")

        elif self.type == "bearer_token":
            has_refresh_credentials = all(
                [self.token_url, self.client_id, self.client_secret, self.refresh_token]
            )
            if not self.bearer_token and not has_refresh_credentials:
                raise ValueError(
                    "bearer_token authentication requires either a static bearer_token "
                    "or all refresh credentials (token_url, client_id, client_secret, refresh_token)"
                )

        return self


class EnsureParamValuesInOutputConfig(BaseModel):
    """
    Configuration to ensure all parameter values are present in the output dataframe.

    When enabled, this ensures that all values from the specified parameter are present
    in the output dataframe. If the API doesn't return data for a parameter value,
    a row with null values for all other fields will be added.

    Attributes:
        enabled: Whether to ensure all parameter values are in the output (default: False)
        param_name: Name of the parameter to track (e.g., "barcode")
        output_field: Name of the field in the output dataframe that should match param values (e.g., "barcode_number")
    """

    enabled: bool = Field(
        default=False, description="Whether to ensure all parameter values are in the output"
    )
    param_name: str = Field(description="Name of the parameter to track")
    output_field: str = Field(
        description="Name of the field in the output dataframe that should match param values"
    )


class PythonSDKConfig(BaseModel):
    """Configuration for Python SDK integration."""

    module: str = Field(description="Python module path (e.g., 'requests')")
    class_name: str = Field(description="Class name to instantiate (e.g., 'Session')")
    auth: Dict[str, Any] = Field(
        default_factory=dict,
        description="Authentication parameters passed to SDK constructor (e.g., email, password)",
    )
    init_kwargs: Dict[str, Any] = Field(
        default_factory=dict, description="Additional keyword arguments for SDK initialization"
    )


class TableReadOptions(BaseModel):
    """
    Optional Spark ``DataFrameReader.jdbc`` read tuning for large relational table extracts.

    Use either **range partitioning** (partition_column + bounds + num_partitions) or
    **predicates** (list of WHERE fragments for parallel reads), not both.
    Honored for relational sources that extract via Spark JDBC (PostgreSQL, HANA). Other
    database kinds may reject this block at validation until they use the same read path.
    """

    fetch_size: Optional[int] = Field(
        default=None,
        ge=1,
        description="JDBC fetchSize hint passed through Spark connection properties.",
    )
    partition_column: Optional[str] = Field(
        default=None,
        description="Numeric (or date-castable) column for Spark parallel range reads.",
    )

    @field_validator("partition_column", mode="before")
    @classmethod
    def normalize_partition_column(cls, v: object) -> Optional[str]:
        if v is None:
            return None
        stripped = str(v).strip()
        return stripped if stripped else None

    lower_bound: Optional[Union[int, float]] = Field(
        default=None,
        description="Inclusive lower bound for partition_column (operator-supplied).",
    )
    upper_bound: Optional[Union[int, float]] = Field(
        default=None,
        description="Inclusive upper bound for partition_column (operator-supplied).",
    )
    num_partitions: Optional[int] = Field(
        default=None,
        ge=1,
        description="Number of JDBC partitions when using partition_column.",
    )
    predicates: Optional[List[str]] = Field(
        default=None,
        description="Spark JDBC predicates (WHERE fragments); mutually exclusive with range mode.",
    )
    use_on_incremental_warm: bool = Field(
        default=False,
        description=(
            "When true, warm incremental_extract JDBC reads apply predicates and partition_column "
            "range as configured. When false (default), those parallel modes are omitted on warm "
            "reads so small bounded extracts use a single JDBC partition; fetch_size is unchanged. "
            "Cold incremental loads always honor predicates and range mode."
        ),
    )

    def uses_parallel_read(self) -> bool:
        """True when Spark will use column range or predicate-based JDBC partitioning for this resource."""
        if self.predicates is not None and len(self.predicates) > 0:
            return True
        return bool(self.partition_column)

    @model_validator(mode="after")
    def validate_partitioning(self) -> "TableReadOptions":
        if self.predicates is not None and len(self.predicates) == 0:
            raise ValueError("table_read_options.predicates must be non-empty when set")

        has_pred = self.predicates is not None and len(self.predicates) > 0
        has_range = bool(self.partition_column)

        if has_pred and has_range:
            raise ValueError(
                "table_read_options: use either predicates or partition_column range mode, not both"
            )
        if has_range:
            if self.num_partitions is None:
                raise ValueError(
                    "table_read_options.num_partitions is required when partition_column is set"
                )
            if self.lower_bound is None or self.upper_bound is None:
                raise ValueError(
                    "table_read_options.lower_bound and upper_bound are required when "
                    "partition_column is set"
                )
            if self.lower_bound > self.upper_bound:
                raise ValueError(
                    "table_read_options.lower_bound must be less than or equal to upper_bound "
                    "(Spark JDBC range partitioning)"
                )
        return self

    def effective_for_incremental_warm_jdbc_read(self) -> "TableReadOptions":
        """
        Options passed to Spark JDBC for a **warm** incremental extract.

        When :attr:`use_on_incremental_warm` is false (default), returns a copy with
        ``predicates`` and range-partition fields cleared so the read stays single-partition;
        ``fetch_size`` is preserved. When true, returns ``self``.
        """
        if self.use_on_incremental_warm:
            return self
        return self.model_copy(
            update={
                "predicates": None,
                "partition_column": None,
                "lower_bound": None,
                "upper_bound": None,
                "num_partitions": None,
            }
        )


class ResourceConfig(BaseModel):
    """Configuration for a single ingest resource (REST, SDK, or future source types)."""

    enabled: bool = Field(default=True, description="Whether this resource is enabled")
    path: Optional[str] = None  # Optional for Python SDK resources
    method: Union[Literal["GET", "POST"], str] = (
        "GET"  # For REST: "GET"/"POST", for Python SDK: method name
    )
    response_type: Literal["json", "csv"] = "json"
    response_key: Optional[str] = None
    skip_encoding_params: bool = Field(
        default=False,
        description="Whether to skip URL encoding of parameters and directly insert them into the URL",
    )

    # Headers specific to this resource (overrides source-level headers)
    headers: Optional[Dict[str, DynamicOrStaticValue]] = None

    # Unified request inputs: path, query, and body. Each has location and same resolution/batching.
    request_inputs: Dict[str, RequestInputConfig] = Field(
        default_factory=dict,
        description="All dynamic request inputs (path, query, body). Location determines where each goes.",
    )

    # Keys to exclude from the final request body before sending (e.g. backfill-only date fields)
    exclude_from_request_body: Optional[List[str]] = Field(
        default=None,
        description="Request body keys to strip before sending (used for backfill-only fields that drive date generation but shouldn't be sent to the API)",
    )

    # Fields to extract from response or database extract. If not specified, all fields are used.
    fields: Optional[List[SchemaField]] = Field(
        default=None,
        description=(
            "Fields to extract from REST responses or JDBC/HANA extracts. "
            "If not specified, all fields from the response or extract are used (string-typed for databases)."
        ),
    )
    transformations: List[Transformation] = Field(default_factory=list)
    loading: Optional[LoadingConfig] = Field(
        default=None,
        description="Loading configuration. If not specified, data will only be stored in Redis.",
    )

    # Snapshot configuration for resources that require polling (REST)
    snapshot: Optional[SnapshotConfig] = None

    # Streaming configuration for memory-efficient processing
    streaming: Optional["StreamingConfig"] = None

    # Configuration to ensure all parameter values are in output dataframe
    ensure_param_values_in_output: Optional[EnsureParamValuesInOutputConfig] = Field(
        default=None,
        description="Configuration to ensure all parameter values are present in the output dataframe",
    )

    # Relational database extract target (used when source type is a database kind)
    database_schema: Optional[str] = Field(
        default=None,
        description="Database schema for JDBC/HANA extract (required for database source types on this resource).",
    )
    database_table: Optional[str] = Field(
        default=None,
        description="Database table or view name for extract (required for database source types on this resource).",
    )

    @field_validator("database_schema", "database_table", mode="before")
    @classmethod
    def strip_db_table_identifier(cls, v: object) -> Optional[str]:
        if v is None:
            return None
        stripped = str(v).strip()
        return stripped if stripped else None

    database_select_query: Optional[str] = Field(
        default=None,
        description=(
            "Optional SELECT for Spark JDBC extract; schema/table stay used for logging. "
            "Use a plain SELECT statement (trailing semicolons stripped); Spine wraps it as "
            "``(query) alias`` when needed. When set, Spark JDBC LIMIT/OFFSET pushdown is "
            "disabled so ``LIMIT``/``OFFSET`` in the operator SQL work with nested subqueries."
        ),
    )
    database_where_predicate: Optional[str] = Field(
        default=None,
        description=(
            "Optional SQL boolean for the **physical** ``database_schema``/``database_table`` read "
            "(not used with ``database_select_query``). Applied inside a derived table aliased ``m``; "
            'reference main columns as ``m."COL"`` (HANA) or ``m.col``. Combines with '
            "``table_read_options.predicates`` and ``incremental_extract`` without duplicating ``WHERE``."
        ),
    )

    @field_validator("database_where_predicate", mode="before")
    @classmethod
    def normalize_database_where_predicate_field(cls, v: object) -> Optional[str]:
        if v is None:
            return None
        text = str(v).strip()
        if not text:
            return None
        upper = text.upper()
        if upper.startswith("WHERE "):
            text = text[6:].strip()
        return text if text else None

    table_read_options: Optional[TableReadOptions] = Field(
        default=None,
        description=(
            "Optional table read tuning (Spark JDBC partitioning, fetch size, logging). "
            "Honored for PostgreSQL and HANA sources (Spark read.jdbc). Other database kinds may "
            "reject this block until they support the same read path."
        ),
    )
    incremental_extract: Optional[IncrementalExtractConfig] = Field(
        default=None,
        description=(
            "Optional fetch-stage incremental bounds for database resources (companion CDC table). "
            "Requires append or merge loading and delta format for watermark.cursor destination_column in v1."
        ),
    )

    def resolve_parameters(
        self,
        redis_context: RedisContextManager,
        params: Optional[Dict[str, Any]] = None,
        param_dict: Optional[Dict[str, RequestInputConfig]] = None,
    ) -> Dict[str, Any]:
        """
        Resolve and validate parameters based on configuration.

        Extracts and resolves parameters from the provided params dict based on
        the configuration template in param_dict.
        - params: Pre-resolved values from handler (resolved request context)
        - param_dict: Input configs to extract/resolve. If None, uses self.request_inputs

        Returns:
            Dict[str, Any]: Resolved parameters matching param_dict configuration
        """
        resolved = {}
        params = params or {}

        config_dict = param_dict if param_dict is not None else self.request_inputs

        for input_name, input_config in config_dict.items():
            if isinstance(input_config, InputConfig):
                if input_name in params:
                    resolved[input_name] = input_config.format_request_value(params[input_name])
                elif input_config.value is not None:
                    value = get_resolver(redis_context).resolve(input_config.value)
                    resolved[input_name] = input_config.format_request_value(value)
            else:
                if input_name in params:
                    resolved[input_name] = params[input_name]
                else:
                    resolved[input_name] = input_config

        if param_dict is None:
            for param_name, value in params.items():
                if param_name not in resolved and not param_name.startswith("_"):
                    resolved[param_name] = value

        return resolved

    def get_inputs_by_location(
        self, location: RequestInputLocation
    ) -> Dict[str, RequestInputConfig]:
        """Return request_inputs with the given location (path, query, or body)."""
        return {
            name: config
            for name, config in self.request_inputs.items()
            if config.location == location
        }

    def get_request_input_values_for_backfill(self) -> Dict[str, Any]:
        """
        Flat map of input name to configured ``value`` for backfill detection.

        Merges path, then query, then body inputs. Input names are unique per
        resource, so order only matters if that invariant were violated.

        Returns:
            Empty dict when there are no request_inputs.
        """
        merged: Dict[str, Any] = {}
        for loc in ("path", "query", "body"):
            for name, cfg in self.get_inputs_by_location(loc).items():
                merged[name] = cfg.value
        return merged

    def get_batch_inputs(self) -> Dict[str, RequestInputConfig]:
        """
        Get request inputs that require batching (any location).

        Includes only inputs with batch_size explicitly set. Static list body fields
        (e.g. dimensions, metrics) are not expanded; they are sent as a single value.
        """
        return {
            name: param
            for name, param in self.request_inputs.items()
            if param.batch_size is not None
        }

    def get_streaming_config(self, defaults: "StreamingConfig") -> "StreamingConfig":
        """
        Get streaming config with resource-specific overrides.

        Args:
            defaults: Default streaming configuration

        Returns:
            StreamingConfig: Streaming configuration for this resource
        """
        if self.streaming:
            return StreamingConfig(
                enable_streaming=self.streaming.enable_streaming,
                mode=self.streaming.mode,
                flush_threshold=self.streaming.flush_threshold or defaults.flush_threshold,
                disk_config=self.streaming.disk_config or defaults.disk_config,
            )
        return defaults

    @field_validator("request_inputs", mode="before")
    @classmethod
    def normalize_request_inputs(cls, v: Any) -> Dict[str, Any]:
        """
        Allow shorthand: key: value (scalar or list) is normalized to key: { value: value }.
        Full config dicts (with value, location, batch_size, etc.) are left as-is.
        """
        if not isinstance(v, dict):
            return v
        config_like_keys = (
            "location",
            "batch_size",
            "type",
            "source_config",
            "request_format",
            "input_format",
            "pagination",
            "value",
        )
        out = {}
        for name, raw in v.items():
            if isinstance(raw, RequestInputConfig):
                out[name] = raw
            elif not isinstance(raw, dict):
                out[name] = {"value": raw}
            elif "value" in raw:
                out[name] = raw
            elif any(k in raw for k in config_like_keys):
                out[name] = raw
            else:
                # Dict without "value" and without config keys -> treat whole dict as value
                out[name] = {"value": raw}
        return out

    @field_validator("request_inputs")
    @classmethod
    def validate_request_inputs(cls, v: Dict[str, Any], info: ValidationInfo) -> Dict[str, Any]:
        """
        Validate request_inputs: each entry must be RequestInputConfig with value.
        Location may be None (defaulted in model_validator).
        """
        for name, param in v.items():
            if not isinstance(param, RequestInputConfig):
                continue
            if param.value is None:
                raise ValueError(f"Request input '{name}' must have 'value' specified")
            if param.location is not None and param.location not in ("path", "query", "body"):
                raise ValueError(
                    f"Request input '{name}' location must be 'path', 'query', or 'body'"
                )
        return v

    @model_validator(mode="after")
    def set_default_request_input_locations(self) -> "ResourceConfig":
        """Default location: GET -> query, POST -> body."""
        method = (self.method or "GET").upper()
        default_loc: RequestInputLocation = "body" if method == "POST" else "query"
        for _name, config in self.request_inputs.items():
            if isinstance(config, RequestInputConfig) and config.location is None:
                object.__setattr__(config, "location", default_loc)
        return self

    @model_validator(mode="after")
    def validate_correlate_groups(self) -> "ResourceConfig":
        """
        Validate correlated request-input groups.

        Members of a correlate group are iterated row-by-row (zipped) from a single multi-column
        databricks query, so they must reference the same query, each select a column, and share
        the same batch_size (which drives row iteration).
        """
        groups: Dict[str, List[str]] = {}
        for name, config in self.request_inputs.items():
            correlate = getattr(config, "correlate", None)
            if correlate:
                groups.setdefault(correlate, []).append(name)

        for group, names in groups.items():
            if len(names) < 2:
                raise ValueError(
                    f"correlate group '{group}' must contain at least two request inputs; "
                    f"found {names}"
                )

            query_refs: set[str] = set()
            batch_sizes: set[Any] = set()
            for name in names:
                config = self.request_inputs[name]
                refs = config.get_databricks_query_refs()
                if len(refs) != 1:
                    raise ValueError(
                        f"Request input '{name}' in correlate group '{group}' must reference exactly "
                        "one databricks query via {{ databricks('ref', column='...') }}; "
                        f"found refs {refs}"
                    )
                if config.get_databricks_column() is None:
                    raise ValueError(
                        f"Request input '{name}' in correlate group '{group}' must select a column, "
                        "e.g. {{ databricks('ref', column='col') }}"
                    )
                query_refs.add(refs[0])
                batch_sizes.add(config.batch_size)

            if len(query_refs) != 1:
                raise ValueError(
                    f"correlate group '{group}' inputs must reference the same databricks query; "
                    f"found {sorted(query_refs)}"
                )
            if len(batch_sizes) != 1:
                raise ValueError(
                    f"correlate group '{group}' inputs must share the same batch_size; "
                    f"found {sorted(str(b) for b in batch_sizes)}"
                )
            if next(iter(batch_sizes)) is None:
                raise ValueError(
                    f"correlate group '{group}' inputs must set batch_size; the group drives "
                    "row-by-row iteration over the query result."
                )
        return self

    @model_validator(mode="after")
    def validate_database_where_vs_select_query(self) -> "ResourceConfig":
        dq = (self.database_select_query or "").strip()
        if self.database_where_predicate and dq:
            raise ValueError(
                "database_where_predicate cannot be combined with database_select_query; "
                "include filters in the custom SELECT or omit database_select_query."
            )
        return self

    @model_validator(mode="after")
    def validate_incremental_extract_constraints(self) -> "ResourceConfig":
        """Incremental extract rules vs loading and database_select_query."""
        inc = self.incremental_extract
        if inc is None:
            return self
        if (self.database_select_query or "").strip():
            raise ValueError(
                "incremental_extract cannot be combined with database_select_query; remove one of them."
            )
        loading = self.loading
        if loading is None or not loading.enabled:
            raise ValueError(
                "incremental_extract requires enabled loading (set loading or inherit defaults with enabled true)."
            )
        if loading.write_mode == "overwrite":
            raise ValueError(
                "incremental_extract requires loading.write_mode append or merge, not overwrite."
            )
        if loading.write_mode in ("ignore", "error"):
            raise ValueError(
                f"incremental_extract is not compatible with loading.write_mode {loading.write_mode!r}."
            )
        wm = inc.watermark
        if wm.cursor.strategy == IncrementalWatermarkCursorStrategy.DESTINATION_COLUMN:
            if loading.format not in INCREMENTAL_DESTINATION_COLUMN_CURSOR_FORMATS:
                raise ValueError(
                    "incremental_extract with watermark.cursor.strategy destination_column requires "
                    "loading.format delta or iceberg until other formats support MAX cursor reads; "
                    f"got {loading.format.value!r}."
                )
            ref = wm.cursor.reference_column
            if self.fields:
                field_names = {f.name for f in self.fields}
                if ref not in field_names:
                    raise ValueError(
                        f"incremental_extract watermark.cursor.reference_column {ref!r} must match a "
                        f"configured fields entry name (written column). Names: {sorted(field_names)}."
                    )

        cmeta = inc.correlation.companion_metadata_columns
        if cmeta and self.fields:
            banned = {str(x).strip() for x in cmeta if x is not None and str(x).strip()}
            for f in self.fields:
                if f.source in banned and f.source != wm.column:
                    raise ValueError(
                        f"incremental_extract.fields cannot select companion-only column {f.source!r} "
                        "(listed in correlation.companion_metadata_columns)."
                    )
        if inc.correlation.join_columns and self.fields:
            sources = {f.source for f in self.fields}
            for jc in inc.correlation.join_columns:
                if jc not in sources:
                    raise ValueError(
                        f"incremental_extract correlation.join_columns: {jc!r} must appear as a field "
                        "source when fields is configured."
                    )
        return self

    def __init__(self, **data):
        # Get defaults from parent if available
        defaults = data.pop("_defaults", None)
        if defaults and isinstance(defaults, dict):
            # Deep merge loading config with defaults (inherit when omitted; opt out with enabled: false)
            if "loading" in defaults:
                default_loading = copy.deepcopy(defaults["loading"])
                resource_loading = data.get("loading")

                if resource_loading is None:
                    data["loading"] = default_loading
                else:
                    if not isinstance(resource_loading, dict):
                        resource_loading = {}
                    data["loading"] = {**default_loading, **resource_loading}

        super().__init__(**data)


class SourceType(StrEnum):
    """Built-in pipeline source kinds (YAML `type` field). Extend when adding new service types."""

    REST_API = "rest_api"
    PYTHON_SDK = "python_sdk"
    POSTGRESQL = "postgresql"
    HANA = "hana"


def is_database_source_type(source_type: SourceType) -> bool:
    """
    Return True for source kinds that use the relational database extract path
    (single table or query read per resource run, shared request-context rules).

    Add new database ``SourceType`` values here when wiring a backend through the
    same planner and handler behavior.
    """
    return source_type in (SourceType.POSTGRESQL, SourceType.HANA)


class SourceConfig(BaseModel):
    """Data source configuration."""

    enabled: bool = Field(default=True, description="Whether this source is enabled")
    type: SourceType
    base_url: Optional[HttpUrl] = None  # Optional for Python SDK sources
    sdk: Optional[PythonSDKConfig] = None  # Required for Python SDK sources
    auth: Optional[AuthConfig] = None
    headers: Dict[str, DynamicOrStaticValue] = Field(
        default_factory=lambda: {"Content-Type": "application/json", "Accept": "application/json"}
    )
    resources: Dict[str, ResourceConfig]

    # Relational database connection (see ``is_database_source_type`` for supported kinds)
    host: Optional[str] = Field(default=None, description="Database host")
    port: Optional[Union[int, str]] = Field(default=None, description="Database port")
    username: Optional[str] = Field(default=None, description="Database user")
    password: Optional[str] = Field(default=None, description="Database password")
    database: Optional[str] = Field(
        default=None,
        description=(
            "Database/catalog name. Required for postgresql. "
            "Optional for hana: set to the HANA tenant database name (JDBC databaseName) when "
            "using a shared SQL port; omit when the host:port already targets a single tenant."
        ),
    )
    connection_params: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Extra JDBC properties or URL query parameters (driver-specific).",
    )

    @field_validator("host", "username", "database", mode="before")
    @classmethod
    def strip_db_identifier(cls, v: object) -> Optional[str]:
        if v is None:
            return None
        stripped = str(v).strip()
        return stripped if stripped else None

    @model_validator(mode="after")
    def normalize_rest_base_url(self) -> "SourceConfig":
        """Strip trailing slashes from REST base URLs so path joins stay consistent."""
        if self.base_url is not None:
            normalized = str(self.base_url).rstrip("/")
            if normalized != str(self.base_url):
                object.__setattr__(
                    self,
                    "base_url",
                    TypeAdapter(HttpUrl).validate_python(normalized),
                )
        return self

    @model_validator(mode="after")
    def validate_source_type(self):
        """Validate that source type matches required fields."""
        if self.type == SourceType.REST_API:
            if not self.base_url:
                raise ValueError("base_url is required for rest_api source type")
        elif self.type == SourceType.PYTHON_SDK:
            if not self.sdk:
                raise ValueError("sdk configuration is required for python_sdk source type")
        elif is_database_source_type(self.type):
            if not self.host:
                raise ValueError(f"{self.type.value} source requires host")
            if self.port is None or str(self.port).strip() == "":
                raise ValueError(f"{self.type.value} source requires port")
            if not self.username or self.password is None:
                raise ValueError(f"{self.type.value} source requires username and password")
            if self.type == SourceType.POSTGRESQL and not self.database:
                raise ValueError(f"{self.type.value} source requires database")
            for resource_name, resource in self.resources.items():
                if not resource.enabled:
                    continue
                if not resource.database_schema or not resource.database_table:
                    raise ValueError(
                        f"{self.type.value} resource '{resource_name}' requires non-empty "
                        "database_schema and database_table"
                    )
        for resource_name, resource in self.resources.items():
            if resource.incremental_extract is not None and resource.enabled:
                if not is_database_source_type(self.type):
                    raise ValueError(
                        "incremental_extract is only supported for database source types "
                        f"(resource '{resource_name}' has incremental_extract but source type is {self.type.value})."
                    )
        for resource_name, resource in self.resources.items():
            if resource.snapshot is not None and self.type != SourceType.REST_API:
                raise ValueError(
                    "snapshot polling is only supported for rest_api sources "
                    f"(resource '{resource_name}' has snapshot; source type is {self.type!s})"
                )
        return self

    def __init__(self, **data):
        # Extract defaults before initializing resources
        defaults = data.get("_defaults")

        # Add defaults to each resource's data
        if "resources" in data:
            for _resource_name, resource_data in data["resources"].items():
                if isinstance(resource_data, dict):
                    resource_data["_defaults"] = defaults

        super().__init__(**data)


class QueriesConfig(BaseModel):
    """
    Configuration for predefined queries.
    SQL files must live under the `queries/` subdirectory of the pipeline config root
    (the directory that contains `defaults.yml` and `sources/`), with a `.sql` extension.
    Queries can be referenced in resource parameters.
    """

    name: str = Field(description="Name of the query")
    description: Optional[str] = Field(default=None, description="Description of the query")
    file: str = Field(description="Path to the SQL file containing the query")


class ContextType(str, Enum):
    """Type of context storage."""

    MEMORY = "memory"
    REDIS = "redis"


class RedisConfig(BaseModel):
    """Redis connection configuration."""

    host: str = Field(default="localhost", description="Redis host")
    port: int = Field(default=6379, description="Redis port")
    db: int = Field(default=0, description="Redis database number")
    password: Optional[str] = Field(default=None, description="Redis password")
    ssl: bool = Field(default=False, description="Whether to use SSL")
    socket_timeout: int = Field(default=5, description="Socket timeout in seconds")
    socket_connect_timeout: int = Field(default=5, description="Socket connect timeout in seconds")
    retry_on_timeout: bool = Field(default=True, description="Whether to retry on timeout")
    max_connections: int = Field(default=10, description="Maximum number of connections")


class ContextConfig(BaseModel):
    """Context management configuration."""

    type: ContextType = Field(
        default=ContextType.MEMORY, description="Type of context storage to use"
    )
    ttl: int = Field(default=3600, description="Default TTL for context data in seconds")
    prefix: str = Field(default="pipeline:", description="Prefix for context keys")
    redis: Optional[RedisConfig] = Field(
        default=None, description="Redis configuration if using Redis context"
    )

    @field_validator("redis")
    @classmethod
    def validate_redis_config(
        cls, v: Optional[RedisConfig], info: ValidationInfo
    ) -> Optional[RedisConfig]:
        """Validate Redis configuration is present when using Redis context."""
        if info.data.get("type") == ContextType.REDIS and v is None:
            raise ValueError("Redis configuration required when using Redis context")
        return v


class DiskConfig(BaseModel):
    """Disk configuration for temporary storage."""

    path: str = Field(
        default="../.tmp/disk_streaming",
        description="Directory for temp files; relative paths resolve from the pipeline config dir (e.g. config/).",
    )
    file_size_threshold: int = Field(
        default=100 * 1024 * 1024,  # 100 MB
        description="File size threshold in bytes before flushing to disk",
    )


class StreamingConfig(BaseModel):
    """Streaming configuration for memory-efficient processing."""

    enable_streaming: bool = Field(
        default=True, description="Enable memory-efficient streaming for all resources"
    )
    mode: Literal["redis", "disk"] = Field(
        default="disk",
        description="Streaming mode: 'redis' for Redis-based streaming or 'disk' for disk-based streaming (more memory-efficient)",
    )
    flush_threshold: int = Field(
        default=20,
        description="Flush to Redis every N requests (adjust based on memory constraints)",
    )
    disk_config: DiskConfig = Field(
        default_factory=DiskConfig, description="Disk configuration for temporary storage"
    )


class SparkRuntimeProfile(StrEnum):
    """
    How Spine treats the Spark host for profile selection when ``profile`` is ``auto``.

    ``local_dev`` forces local-style defaults; ``cluster_managed`` forces managed-cluster defaults.
    """

    AUTO = "auto"
    LOCAL_DEV = "local_dev"
    CLUSTER_MANAGED = "cluster_managed"


class ConnectorProvisionMode(StrEnum):
    """Whether Ivy ``--packages`` should pull Hadoop cloud connectors or the cluster supplies them."""

    AUTO = "auto"
    PACKAGES = "packages"
    EXTERNAL = "external"


class SparkRuntimeConfig(BaseModel):
    """
    Defaults for Spark session bootstrap: host profile and symmetric Hadoop connector provisioning.

    Per-destination ``*_connector_mode`` controls whether Ivy pulls artifacts or the cluster
    already provides them. Environment variables (for example ``SPARK_S3_CONNECTOR_MODE``) still
    override YAML when set so CI and bespoke images can force behavior without editing pipeline files.

    S3A endpoint region is not configured here; it follows the AWS credential chain, ``AWS_REGION`` /
    ``AWS_DEFAULT_REGION``, and SparkManager when ``s3`` is a destination (see deployment docs).
    """

    profile: SparkRuntimeProfile = Field(
        default=SparkRuntimeProfile.AUTO,
        description=(
            "``auto`` inspects the process environment (Databricks, EMR, ECS, Kubernetes) and "
            "chooses between local and managed assumptions; set explicitly when detection is wrong."
        ),
    )
    s3_connector_mode: ConnectorProvisionMode = Field(
        default=ConnectorProvisionMode.AUTO,
        description=(
            "S3A / ``hadoop-aws``: ``auto`` uses the same Databricks/EMR ``external`` default as GCS and Azure; "
            "else ``packages``."
        ),
    )
    gcs_connector_mode: ConnectorProvisionMode = Field(
        default=ConnectorProvisionMode.AUTO,
        description="GCS Hadoop connector: ``auto`` uses Databricks/EMR ``external`` defaults; else ``packages``.",
    )
    azure_connector_mode: ConnectorProvisionMode = Field(
        default=ConnectorProvisionMode.AUTO,
        description="ABFS connector: same semantics as ``gcs_connector_mode``.",
    )
    spark_ui_enabled: bool = Field(
        default=False,
        description="When true, enable Spark Web UI (for example http://127.0.0.1:4040) while the session runs.",
    )
    spark_ui_port: Optional[int] = Field(
        default=None,
        ge=1,
        le=65535,
        description="Optional Spark UI port; omit to use Spark default (4040).",
    )
    spark_ui_show_console_progress: bool = Field(
        default=False,
        description="When true, set spark.ui.showConsoleProgress so the driver prints stage progress.",
    )
    spark_event_log_enabled: bool = Field(
        default=False,
        description="When true, write Spark event logs for History Server replay; requires spark_event_log_dir.",
    )
    spark_event_log_dir: Optional[str] = Field(
        default=None,
        description=(
            "Directory or URI for event logs (required when spark_event_log_enabled is true). "
            "Relative paths resolve under the repository root at load time. Use durable storage on clusters."
        ),
    )
    spark_event_log_compress: bool = Field(
        default=True,
        description="When true, compress event log files (spark.eventLog.compress).",
    )

    @model_validator(mode="after")
    def spark_event_log_dir_required_when_enabled(self) -> "SparkRuntimeConfig":
        if self.spark_event_log_enabled:
            d = (self.spark_event_log_dir or "").strip()
            if not d:
                raise ValueError(
                    "spark_event_log_dir is required when spark_event_log_enabled is true"
                )
        return self


class DefaultsConfig(BaseModel):
    """Default configuration values. See ``config/defaults.example.yml`` for a full operator template."""

    retry: RetryConfig = Field(
        default_factory=RetryConfig, description="Default retry configuration"
    )
    loading: LoadingConfig = Field(
        default_factory=lambda: LoadingConfig(
            destination="local",
            format="delta",
            write_mode="overwrite",
            compression="snappy",
            storage_root=".spine/local-output",
            prefix=None,
        ),
        description=(
            "Default loading merged into every resource unless the resource sets loading.enabled "
            "to false. Prefix may be omitted; the handler sets ``{source_name}/{resource_name}`` "
            "before load when prefix is unset for object-store destinations (local, s3, gcs, azure_blob)."
        ),
    )
    context: ContextConfig = Field(
        default_factory=ContextConfig, description="Context management configuration"
    )
    streaming: StreamingConfig = Field(
        default_factory=StreamingConfig,
        description="Streaming configuration for memory-efficient processing",
    )
    spark_runtime: SparkRuntimeConfig = Field(
        default_factory=SparkRuntimeConfig,
        description="Spark host profile and per-cloud connector provisioning (see docs/configuration/loading.md).",
    )
    telemetry: TelemetryConfig = Field(
        default_factory=TelemetryConfig,
        description=(
            "OpenTelemetry (OTLP) producer settings. Disabled by default; see "
            "docs/configuration/telemetry.md. Standard OTEL_* env vars override these values."
        ),
    )


class PipelineConfig(BaseModel):
    """Top-level pipeline configuration."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    config_root: Path = Field(
        exclude=True,
        description="Directory with defaults.yml, sources/, and queries/ (set by ConfigLoader)",
    )
    runtime_selection: Optional[Dict[str, Optional[Set[str]]]] = Field(
        default=None,
        exclude=True,
        description=(
            "CLI selection passed to ConfigLoader.load_config; used so loading destinations match "
            "ExecutionPlan when a disabled source is explicitly selected."
        ),
    )
    version: str
    defaults: DefaultsConfig
    queries: List[QueriesConfig]
    sources: Dict[str, SourceConfig]

    def __init__(self, **data):
        """Initialize pipeline configuration with proper defaults inheritance."""
        # Inject defaults into each source
        if "sources" in data and "defaults" in data:
            defaults_dict = data["defaults"]
            if isinstance(defaults_dict, DefaultsConfig):
                defaults_dict = defaults_dict.model_dump()

            for _source_name, source_data in data["sources"].items():
                if isinstance(source_data, dict):
                    source_data["_defaults"] = defaults_dict

        super().__init__(**data)

    @field_validator("queries", mode="after")
    @classmethod
    def validate_queries(cls, v: List[QueriesConfig], info: ValidationInfo) -> List[QueriesConfig]:
        """Validate that query files exist under config_root/queries/."""
        config_root = info.data.get("config_root")
        if config_root is None:
            raise ValueError("config_root is required to validate queries")
        root = Path(config_root)
        for query in v:
            query_file_path = root / QUERIES_DIR / query.file
            if not query_file_path.is_file():
                raise ValueError(f"Query file not found: {query.file}")
        return v

    def load_query_file(self, query_name: str) -> str:
        """
        Load the SQL content of a predefined query by name.

        Args:
            query_name: Name of the query to load

        Returns:
            str: SQL content of the query

        Raises:
            ValueError: If the query is not found or file does not exist
        """
        query_config = next((q for q in self.queries if q.name == query_name), None)

        if not query_config:
            raise ValueError(f"Query not found: {query_name}")

        query_file_path = self.config_root / QUERIES_DIR / query_config.file

        if not query_file_path.is_file():
            raise ValueError(f"Query file not found: {query_config.file}")

        with open(query_file_path, "r") as file:
            return file.read()

    def get_effective_loading_destinations(self) -> set[str]:
        """
        Return loading destinations used by resources that contribute to this config scope.

        Resource loading is merged with defaults at model initialization.

        Mirrors :meth:`~src.planner.execution_plan.ExecutionPlan._should_include_source` and
        :meth:`~src.planner.execution_plan.ExecutionPlan._should_include_resource` when
        ``runtime_selection`` is set (CLI ``--select``):

        - Explicitly selected disabled sources still contribute destinations.
        - Whole-source selection (value ``None``) still respects per-resource ``enabled``.
        - Explicitly selected disabled resources still contribute destinations.
        """
        destinations: set[str] = set()
        sel = self.runtime_selection
        for source_name, source in self.sources.items():
            if not source.enabled:
                if sel is None or source_name not in sel:
                    continue
            selected_resources = sel.get(source_name) if sel and source_name in sel else None

            for resource_name, resource in source.resources.items():
                if sel is not None and selected_resources is not None:
                    if resource_name not in selected_resources:
                        continue

                if resource.loading is None or not resource.loading.enabled:
                    continue

                if not resource.enabled:
                    if sel is None:
                        continue
                    if selected_resources is None:
                        continue

                destinations.add(resource.loading.destination)
        return destinations


class FieldConfig:
    """Configuration for a data field."""

    def __init__(
        self,
        name: str,
        type: str = "string",
        required: bool = False,
        parent_field: bool = False,
        **kwargs,
    ):
        """
        Initialize field configuration.

        Args:
            name: Field name
            type: Field data type
            required: Whether the field is required
            parent_field: Whether this is a parent field
            **kwargs: Additional field configuration
        """
        self.name = name
        self.type = type
        self.required = required
        self.parent_field = parent_field
        self.config = kwargs
