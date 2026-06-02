"""
Spark-optimized parser for high-performance data transformation.
Uses vectorized operations and DataFrame transformations for better efficiency.
"""

import json
from typing import Any, Dict, List, Optional, Union

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import StringType, StructField, StructType

from src.config.config_models import ResourceConfig, TransformationType
from src.planner.execution_plan import ExecutionPlan
from src.utils.data_utils import (
    add_iteration_context_to_record,
    build_params_json,
    dict_response_key_to_records,
    get_include_as_field_params,
    get_nested_value,
)
from src.utils.dynamic_values import get_resolver
from src.utils.logger import get_logger
from src.utils.redis_context import RedisContextManager


class SparkParser:
    """
    High-performance parser using Spark DataFrame operations.
    All fields are handled as strings for maximum compatibility.
    """

    def __init__(
        self,
        config: ResourceConfig,
        spark: SparkSession,
        source_name: str,
        resource_name: str,
        execution_plan: ExecutionPlan,
        redis_context: RedisContextManager,
    ):
        """
        Initialize the Spark parser.

        Args:
            config: Resource configuration containing schema
            spark: SparkSession instance for DataFrame operations
            source_name: Name of the source
            resource_name: Name of the resource
            execution_plan: Execution plan instance for metadata
        """
        self.config = config
        self.spark = spark
        self.source_name = source_name
        self.resource_name = resource_name
        self.execution_plan = execution_plan
        self.logger = get_logger(self.__class__.__name__)
        self.redis_context = redis_context

    def _extract_all_fields(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract all fields from a record when no schema is specified.
        Uses json.dumps for complex types to preserve null semantics.

        Args:
            record: Record to extract fields from

        Returns:
            Dict[str, Any]: Record with all fields
        """
        return {
            key: (
                json.dumps(value)
                if isinstance(value, (dict, list))
                else str(value) if value is not None else None
            )
            for key, value in record.items()
        }

    def _build_target_schema(self, parent_context: Optional[Dict[str, Any]] = None) -> StructType:
        """
        Build Spark schema for the target DataFrame.
        If no fields are specified, creates schema from first record.

        Returns:
            StructType: Spark schema for the DataFrame
        """
        fields = []

        # Add _params field if needed
        if self.execution_plan.has_parent_inputs(self.source_name, self.resource_name):
            fields.append(StructField("_params", StringType(), True))

        # Add data fields
        if self.config.fields:
            # Use specified fields
            for field in self.config.fields:
                fields.append(StructField(field.name, StringType(), True))
        else:
            # If no fields specified, we'll add them dynamically when processing data
            pass

        # Add transformation fields
        if self.config.transformations:
            for transform in self.config.transformations:
                if transform.type == "add_column":
                    fields.append(StructField(transform.name, StringType(), True))

        # Add iteration context fields only if explicitly configured with include_as_field=True
        include_field_map = get_include_as_field_params(self.config)
        explicit_field_names = {field.name for field in (self.config.fields or [])}

        for output_field_name, _context_field_name in include_field_map.items():
            # Only add if not already explicitly mapped
            if output_field_name not in explicit_field_names:
                fields.append(StructField(output_field_name, StringType(), True))

        return StructType(fields)

    def _log_dataframe_info(self, df: DataFrame, stage: str) -> None:
        """
        Log detailed DataFrame information for debugging.

        Args:
            df: DataFrame to inspect
            stage: Description of the processing stage
        """
        try:
            # Get actual column values for first row (for debugging)
            sample_row = df.limit(1).collect()
            sample_data = sample_row[0].asDict() if sample_row else {}

            self.logger.trace(
                f"DataFrame state at {stage}",
                extra_fields={
                    "columns": df.columns,
                    "schema": str(df.schema),
                    "sample_row": {
                        k: str(v)[:100] + "..." if isinstance(v, str) and len(str(v)) > 100 else v
                        for k, v in sample_data.items()
                    },
                },
            )
        except Exception as e:
            self.logger.warning(
                f"Failed to log DataFrame info for {stage}", extra_fields={"error": str(e)}
            )

    def _get_request_value(
        self,
        source: str,
        location: str,
        data_type: str = "string",
        request_context: Optional[Dict[str, Any]] = None,
    ) -> Optional[str]:
        """
        Get a value from the request context.

        Args:
            source: Key to extract
            location: Where to look ('parameters' or 'request_body')
            data_type: How to format the value (string, integer, float, array)
            request_context: Optional dictionary containing request parameters and body

        Returns:
            Optional[str]: The extracted value as a string, or None if not found
        """
        try:
            request_context = request_context or {}
            # Get the container (parameters or request_body)
            container = request_context.get(location, {})
            if not container:
                self.logger.warning(
                    f"No {location} found in request context",
                    extra_fields={
                        "source": source,
                        "available_context": (
                            list(request_context.keys()) if request_context else None
                        ),
                    },
                )
                return None

            # Extract the value using dot notation if needed
            value = container
            for key in source.split("."):
                if not isinstance(value, dict):
                    self.logger.warning(
                        f"Cannot access key {key} in non-dict value",
                        extra_fields={
                            "value_type": type(value).__name__,
                            "full_path": source,
                            "partial_value": str(value)[:100],
                        },
                    )
                    return None

                value = value.get(key)
                if value is None:
                    self.logger.warning(
                        f"Key {key} not found in {location}",
                        extra_fields={
                            "available_keys": list(container.keys()),
                            "full_path": source,
                            "partial_context": str(container)[:100],
                        },
                    )
                    return None

            # If value is a JSON string, parse it
            if isinstance(value, str):
                try:
                    parsed_value = json.loads(value)
                    if isinstance(parsed_value, (list, dict)):
                        value = parsed_value
                except json.JSONDecodeError:
                    pass  # Keep original string value if not valid JSON

            # Convert to specified type
            if data_type == "integer":
                if isinstance(value, list):
                    # Take first value if array
                    value = value[0] if value else None
                return str(int(value)) if value is not None else None
            elif data_type == "float":
                if isinstance(value, list):
                    # Take first value if array
                    value = value[0] if value else None
                return str(float(value)) if value is not None else None
            elif data_type == "array":
                if isinstance(value, (list, tuple)):
                    return json.dumps(value)
                return json.dumps([value])
            else:  # string or unknown type
                if isinstance(value, (list, tuple)):
                    # Take first value if array
                    value = value[0] if value else None
                if isinstance(value, (dict, list)):
                    return json.dumps(value)
                return str(value) if value is not None else None

        except Exception as e:
            self.logger.warning(
                f"Failed to extract {source} from {location}",
                extra_fields={
                    "error": str(e),
                    "error_type": type(e).__name__,
                    "source": source,
                    "location": location,
                    "data_type": data_type,
                    "context_preview": str(request_context)[:100] if request_context else None,
                },
            )
            return None

    def _apply_transformations(
        self, df: DataFrame, request_context: Optional[Dict[str, Any]] = None
    ) -> DataFrame:
        """
        Apply all configured transformations to the DataFrame.

        Args:
            df: Input DataFrame
            request_context: Optional dictionary containing request parameters and body

        Returns:
            DataFrame: Transformed DataFrame
        """
        if not self.config.transformations:
            return df

        # Log initial state
        self.logger.debug(
            "Starting transformations",
            extra_fields={
                "initial_columns": df.columns,
                "transformation_count": len(self.config.transformations),
            },
        )
        self._log_dataframe_info(df, "before_transformations")

        successful_transforms = []
        skipped_transforms = []

        for transform in self.config.transformations:
            try:
                if transform.type == TransformationType.ADD_COLUMN:
                    # Handle existing add_column transformation
                    resolved_value = get_resolver(self.redis_context).resolve(transform.value)
                    df = df.withColumn(transform.name, lit(str(resolved_value)))
                    successful_transforms.append(
                        {
                            "name": transform.name,
                            "type": transform.type,
                            "value": str(resolved_value),
                        }
                    )

                elif transform.type == TransformationType.ADD_COLUMN_FROM_REQUEST:
                    # Skip if column already exists (e.g. collector added it per batch)
                    if transform.name in df.columns:
                        skipped_transforms.append(
                            {
                                "name": transform.name,
                                "type": transform.type,
                                "source": transform.source,
                                "data_type": transform.data_type,
                                "reason": "column_already_exists",
                            }
                        )
                        continue
                    # Handle add_column_from_request transformation
                    value = self._get_request_value(
                        source=transform.source,
                        location=transform.location,
                        data_type=transform.data_type,
                        request_context=request_context,
                    )
                    if value is not None:
                        df = df.withColumn(transform.name, lit(value))
                        successful_transforms.append(
                            {
                                "name": transform.name,
                                "type": transform.type,
                                "source": transform.source,
                                "value": value,
                                "data_type": transform.data_type,
                            }
                        )
                    else:
                        skipped_transforms.append(
                            {
                                "name": transform.name,
                                "type": transform.type,
                                "source": transform.source,
                                "data_type": transform.data_type,
                                "reason": "missing_value",
                            }
                        )
                        self.logger.warning(
                            f"Skipping transformation {transform.name} due to missing value",
                            extra_fields={
                                "source": transform.source,
                                "location": transform.location,
                                "transform_type": transform.type,
                                "data_type": transform.data_type,
                            },
                        )

                # Log state after each transformation
                self._log_dataframe_info(df, f"after_transform_{transform.name}")

            except Exception as e:
                skipped_transforms.append(
                    {
                        "name": transform.name,
                        "type": transform.type,
                        "reason": "error",
                        "error": str(e),
                    }
                )
                self.logger.error(
                    f"Failed to apply transformation {transform.name}",
                    extra_fields={
                        "type": transform.type,
                        "error": str(e),
                        "error_type": type(e).__name__,
                    },
                )

        # Log final transformation summary
        self.logger.debug(
            "Completed transformations",
            extra_fields={
                "successful_transforms": successful_transforms,
                "skipped_transforms": skipped_transforms,
                "final_columns": df.columns,
            },
        )

        return df

    def _extract_records_and_schema(
        self,
        data: Union[List[Dict[str, Any]], Dict[str, Any]],
        parent_context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Extract records and schema from input data.

        This is the core parsing logic shared by both parse_to_records and parse methods.
        Handles data normalization, field extraction, and schema building.

        Args:
            data: Input data as list of dictionaries or single dictionary
            parent_context: Optional dictionary containing parent context (e.g. account_id)

        Returns:
            Dict[str, Any]: Dictionary containing:
                - "schema": StructType representing the target schema
                - "records": List[Dict[str, Any]] of parsed records
        """
        if not data:
            self.logger.warning("Received empty data to parse")
            return {"schema": self._build_target_schema(parent_context), "records": []}

        # Log incoming data structure
        self.logger.trace(
            "Parsing incoming data",
            extra_fields={
                "data_type": type(data).__name__,
                "is_list": isinstance(data, list),
                "record_count": len(data) if isinstance(data, list) else 1,
                "response_key": self.config.response_key,
                "has_parent_context": bool(parent_context),
                "fields_mode": "specified" if self.config.fields else "all",
            },
        )

        # Convert single dict to list; track whether input was a raw dict so we know
        # whether to apply response_key unwrapping below.
        input_was_dict = isinstance(data, dict)
        records = data if isinstance(data, list) else [data]

        # Handle nested data structures (dot paths via get_nested_value; same as REST/SDK).
        # Only unwrap when the input was a raw dict — if it was already a list the response_key
        # was applied upstream (e.g. by RestService) and re-applying it would look for the key
        # inside each individual record, breaking single-item lists.
        if input_was_dict and isinstance(records[0], dict) and self.config.response_key:
            nested_list, missing = dict_response_key_to_records(
                records[0], self.config.response_key
            )
            if missing:
                self.logger.warning(
                    f"Response key '{self.config.response_key}' not found or None in record",
                    extra_fields={"raw_data": str(records[0])[:100]},
                )
                return {"schema": self._build_target_schema(parent_context), "records": []}
            records = nested_list
            if records and not all(isinstance(r, dict) for r in records):
                self.logger.warning(
                    f"Expected dict records under response_key '{self.config.response_key}'",
                    extra_fields={
                        "types": [type(r).__name__ for r in records[:5]],
                        "value_preview": str(records[0])[:100],
                    },
                )
                return {"schema": self._build_target_schema(parent_context), "records": []}

        # Extract fields according to schema
        processed_records = []
        schema_fields = set()  # Track fields when extracting all

        for record in records:
            processed_record = {}
            ctx = parent_context or {}

            # Build _params dictionary (optimization: only if resource has parent parameters)
            if self.execution_plan.has_parent_inputs(self.source_name, self.resource_name) and (
                params_json := build_params_json(self.config, ctx)
            ):
                processed_record["_params"] = params_json

            # Add iteration context from parent context (only if configured with include_as_field=True)
            include_field_map = get_include_as_field_params(self.config)
            explicit_field_names = {field.name for field in (self.config.fields or [])}
            add_iteration_context_to_record(
                processed_record,
                ctx,
                include_field_map=include_field_map,
                exclude_fields=explicit_field_names,
            )

            # Process fields
            if self.config.fields:
                # Use specified fields with validation
                for field in self.config.fields:
                    value = get_nested_value(record, field.source, required=False)
                    processed_record[field.name] = (
                        json.dumps(value)
                        if isinstance(value, (dict, list))
                        else str(value) if value is not None else None
                    )
            else:
                # Extract all fields
                extracted = self._extract_all_fields(record)
                processed_record.update(extracted)
                schema_fields.update(extracted.keys())

            processed_records.append(processed_record)

        # Build schema dynamically if no fields specified
        schema = self._build_target_schema(parent_context)
        if not self.config.fields and schema_fields:
            fields = []
            if "_params" in schema.fieldNames():
                fields.append(StructField("_params", StringType(), True))
            fields.extend(
                [StructField(field, StringType(), True) for field in sorted(schema_fields)]
            )
            schema = StructType(fields)

        self.logger.trace(
            f"Parsed batch to {len(processed_records)} records",
            extra_fields={
                "record_count": len(processed_records),
                "schema_fields": len(schema.fields),
                "resource_name": self.resource_name,
            },
        )

        return {"schema": schema, "records": processed_records}

    def parse_to_records(
        self,
        data: Union[List[Dict[str, Any]], Dict[str, Any]],
        parent_context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Parse input data into records and schema without creating a DataFrame.

        This method extracts records and schema information directly from the input data,
        avoiding the overhead of DataFrame creation and materialization. The returned
        structure can be easily serialized to disk (e.g., NDJSON format).

        If no fields are specified in the configuration, all fields from the response
        will be extracted. If fields are specified, they must exist in the response.

        Args:
            data: Input data as list of dictionaries or single dictionary
            parent_context: Optional dictionary containing parent context (e.g. account_id)

        Returns:
            Dict[str, Any]: Dictionary containing:
                - "schema": StructType representing the target schema
                - "records": List[Dict[str, Any]] of parsed records
        """
        try:
            return self._extract_records_and_schema(
                data,
                parent_context,
            )
        except Exception as e:
            self.logger.error(
                "Failed to parse data to records",
                extra_fields={
                    "error": str(e),
                    "error_type": type(e).__name__,
                    "input_type": type(data).__name__,
                    "record_count": len(data) if isinstance(data, list) else 1,
                    "fields_mode": "all" if not self.config.fields else "specified",
                },
            )
            raise

    def parse(
        self,
        data: Union[List[Dict[str, Any]], Dict[str, Any]],
        parent_context: Optional[Dict[str, Any]] = None,
        request_context: Optional[Dict[str, Any]] = None,
    ) -> DataFrame:
        """
        Parse input data into a Spark DataFrame.

        If no fields are specified in the configuration, all fields from the response
        will be extracted. If fields are specified, they must exist in the response.

        Args:
            data: Input data as list of dictionaries or single dictionary
            parent_context: Optional dictionary containing parent context (e.g. account_id)
            request_context: Optional dictionary containing request parameters/body for transformations

        Returns:
            DataFrame: Parsed data as a Spark DataFrame
        """
        try:
            # Extract records and schema using shared logic
            result = self._extract_records_and_schema(
                data,
                parent_context,
            )

            # Create DataFrame with schema
            df = self.spark.createDataFrame(result["records"], result["schema"])

            # Ensure all parameter values are present in the output dataframe.
            # If enabled, ensures that all values from the specified parameter are present
            # in the output. If the API doesn't return data for a parameter value,
            # a row with null values for all response fields will be added.
            if (
                self.config.ensure_param_values_in_output
                and self.config.ensure_param_values_in_output.enabled
            ):
                ensure_config = self.config.ensure_param_values_in_output
                try:
                    param_name = ensure_config.param_name
                    output_field = ensure_config.output_field

                    # Get parameter value(s) from request context as JSON array string
                    vals_json = self._get_request_value(param_name, "parameters", data_type="array")
                    if not vals_json:
                        self.logger.debug(
                            f"No parameter values found for '{param_name}', skipping ensure step"
                        )
                    else:
                        vals = json.loads(vals_json) if isinstance(vals_json, str) else vals_json
                        if vals:
                            # Normalize values to strings and create params DataFrame
                            params_records = [{output_field: str(v)} for v in vals]
                            params_schema = StructType(
                                [StructField(output_field, StringType(), True)]
                            )
                            params_df = self.spark.createDataFrame(params_records, params_schema)

                            # Check if output_field exists in the response dataframe
                            if output_field in df.columns:
                                # Use aliases to avoid ambiguous column references during join
                                params_alias = "params_tbl"
                                df_alias = "response_tbl"

                                # Left join: params_df on left (to keep all params even if not in response)
                                joined = params_df.alias(params_alias).join(
                                    df.alias(df_alias),
                                    params_df[output_field] == df[output_field],
                                    how="left",
                                )

                                # Select columns to eliminate ambiguous references:
                                # 1. Use params_tbl version of output_field (the join key from left side)
                                # 2. Use response_tbl versions of all other columns
                                selected_cols = [params_alias + "." + output_field]
                                for col in df.columns:
                                    if col != output_field:
                                        selected_cols.append(df_alias + "." + col)

                                df = joined.select(*selected_cols)

                                self.logger.debug(
                                    "Ensured parameter values in output",
                                    extra_fields={
                                        "param_name": param_name,
                                        "output_field": output_field,
                                        "param_count": len(vals),
                                        "final_row_count": df.count(),
                                    },
                                )
                            else:
                                self.logger.warning(
                                    f"Output field '{output_field}' not found in response dataframe",
                                    extra_fields={
                                        "param_name": param_name,
                                        "available_columns": df.columns,
                                        "requested_output_field": output_field,
                                    },
                                )

                except Exception as e:
                    self.logger.warning(
                        "Failed to ensure parameter values in output",
                        extra_fields={
                            "param_name": ensure_config.param_name,
                            "output_field": ensure_config.output_field,
                            "error": str(e),
                            "error_type": type(e).__name__,
                        },
                    )

            return df

        except Exception as e:
            self.logger.error(
                "Failed to parse data",
                extra_fields={
                    "error": str(e),
                    "error_type": type(e).__name__,
                    "input_type": type(data).__name__,
                    "record_count": len(data) if isinstance(data, list) else 1,
                    "fields_mode": "all" if not self.config.fields else "specified",
                },
            )
            raise

    def get_schema_info(self, parent_context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Get information about the target schema.

        Returns:
            Dict[str, Any]: Schema information
        """
        target_schema = self._build_target_schema(parent_context)

        return {
            "target_schema": str(target_schema),
            "field_count": len(target_schema.fields),
            "field_details": [
                {"name": field.name, "type": str(field.dataType), "nullable": field.nullable}
                for field in target_schema.fields
            ],
            "source_mappings": [
                {
                    "target_field": field.name,
                    "source_path": field.source,
                    "type_conversion": field.type,
                }
                for field in self.config.fields
            ],
        }
