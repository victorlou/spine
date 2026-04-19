"""
Execution planning for the data ingestion pipeline.
Handles dependency resolution and execution order determination.

Plan build requirements:
- Redis: Must be available. Resolved Databricks query values are stored in Redis
  for use when resolving parameters at request time.
- Databricks: Must be reachable if any resource in the plan uses parameters that
  source from Databricks (via databricks query_ref). Queries are resolved during
  plan build and stored in Redis. If Databricks is unavailable, plan construction
  will fail.
"""

from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Dict, List, Optional, Set, Tuple

from src.config.backfill_config import BackfillConfig, get_backfill_config
from src.config.config_models import (
    BatchSizeMode,
    LoadingConfig,
    PipelineConfig,
    RequestInputConfig,
    ResourceConfig,
    SourceConfig,
)
from src.planner.database_request_context import (
    validate_plan_time_static_database_request_context_expansion,
)
from src.utils.databricks_utils import DatabricksUtils
from src.utils.dynamic_values import ComplexDynamicValue, DynamicValueType, FilterConfig
from src.utils.exceptions import PlanningError
from src.utils.logger import get_logger
from src.utils.query_utils import format_query_ref_key, validate_query_content
from src.utils.redis_context import RedisContextManager


@dataclass
class ResourceMetadata:
    """Metadata for a resource in the execution plan."""

    source_name: str
    resource_name: str
    dependencies: Set[str]  # Set of resource ids this resource depends on
    batch_inputs: Dict[str, int]  # Input name -> batch size (only inputs with batch_size set)
    filter_relationships: Dict[str, FilterConfig] = None  # input_name -> filter configuration
    parent_fields: Set[str] = None  # Fields that need to be carried from parent context
    config: ResourceConfig = None  # Reference to the resource configuration
    backfill_config: Optional[BackfillConfig] = (
        None  # Request-body date-range backfill, if configured
    )

    def estimate_request_count(self) -> Optional[int]:
        """
        Estimate total requests based on static parts only.

        Returns:
            Optional[int]: Estimated request count if all static, None if dynamic batching involved
        """
        if not self.batch_inputs:
            return 1

        if not self.config:
            return None

        # If the resource has dependencies, we cannot estimate request count
        # because we don't know how many values will come from earlier requests
        if self.dependencies:
            return None

        # Compute from input values - only works if all are static lists
        static_count = 1
        for input_name, batch_size in self.batch_inputs.items():
            input_config = self.config.request_inputs.get(input_name)
            if not input_config:
                # Found a dynamic input - can't estimate
                return None

            if isinstance(input_config.value, list):
                values = input_config.value
            else:
                values = [input_config.value] if input_config.value is not None else []

            # Calculate number of batches for this input
            num_batches = (
                (len(values) + batch_size - 1) // batch_size
                if batch_size != BatchSizeMode.ALL and batch_size > 0
                else len(values)
            )
            static_count *= num_batches

        return static_count


@dataclass
class ExecutionStage:
    """A stage in the execution plan containing resources that can run in parallel."""

    resources: List[ResourceMetadata]
    stage_number: int


class ExecutionPlan:
    """
    Represents a complete execution plan for the pipeline.
    Handles dependency resolution, stage organization, and configuration parsing.
    """

    def __init__(
        self,
        config: PipelineConfig,
        redis_context: RedisContextManager,
        selection: Optional[Dict[str, Optional[Set[str]]]] = None,
    ):
        """
        Initialize execution plan from pipeline configuration.

        Args:
            config: Pipeline configuration to plan execution for
            selection: Optional selection structure mapping source names to resource name sets.
                       None means all resources, Set[str] means specific resource names.
        """
        self.config = config
        self.selection = selection
        self.stages: List[ExecutionStage] = []
        self._dependency_graph: Dict[str, Set[str]] = {}  # resource -> dependencies
        self._reverse_graph: Dict[str, Set[str]] = defaultdict(set)  # resource -> dependents
        self._resource_metadata: Dict[str, ResourceMetadata] = {}
        self._source_configs: Dict[str, SourceConfig] = {}  # Cache for source configs
        self.required_queries: Dict[str, str] = {}
        self.redis_context = redis_context

        self.logger = get_logger(self.__class__.__name__)
        self._build_plan()

    def _should_include_source(self, source_name: str, source_config: SourceConfig) -> bool:
        """
        Determine if a source should be included in the execution plan.

        Args:
            source_name: Name of the source
            source_config: Source configuration

        Returns:
            bool: True if source should be included
        """
        if self.selection is not None:
            return source_name in self.selection

        return source_config.enabled

    def _get_enabled_dependents(
        self, source_name: str, resource_name: str
    ) -> List[Tuple[str, str]]:
        """
        Get all enabled endpoints that depend on this endpoint.

        Args:
            source_name: Name of the source
            resource_name: Name of the endpoint

        Returns:
            List[Tuple[str, str]]: List of (source_name, resource_name) tuples for enabled dependents
        """
        resource_id = f"{source_name}.{resource_name}"
        dependent_ids = self._reverse_graph.get(resource_id, set())
        enabled_dependents = []

        for dep_id in dependent_ids:
            dep_source, dep_resource = dep_id.split(".")
            dep_config = self.get_resource_config(dep_source, dep_resource)
            if dep_config:
                # Check if dependent is in selection
                is_selected = False
                if self.selection is not None:
                    if dep_source in self.selection:
                        selected_resources = self.selection[dep_source]
                        if selected_resources is None:
                            # All endpoints selected for this source
                            is_selected = True
                        elif dep_resource in selected_resources:
                            # Specific endpoint selected
                            is_selected = True

                if is_selected or dep_config.enabled:
                    enabled_dependents.append((dep_source, dep_resource))

        return enabled_dependents

    def _should_include_resource(
        self, resource_name: str, resource_config: ResourceConfig, source_name: str
    ) -> bool:
        """
        Determine if an endpoint should be included in the execution plan.

        Args:
            resource_name: Name of the endpoint
            resource_config: Resource configuration
            source_name: Name of the parent source

        Returns:
            bool: True if endpoint should be included
        """
        # Check if source is selected
        if self.selection is not None:
            if source_name not in self.selection:
                return False

            # Check if specific endpoints are selected
            selected_resources = self.selection[source_name]
            if selected_resources is not None:
                # Only include if endpoint is in the selected set
                if resource_name not in selected_resources:
                    return False
                else:
                    # Explicitly selected endpoint - include regardless of enabled status (but still check dependencies - if dependencies are disabled, this endpoint will be included but will likely fail at runtime, which is expected since user explicitly selected it)
                    return True

        # Check if endpoint is required by enabled dependents
        enabled_dependents = self._get_enabled_dependents(source_name, resource_name)
        if enabled_dependents and not resource_config.enabled:
            dependent_list = [
                f"{dep_source}.{dep_resource}" for dep_source, dep_resource in enabled_dependents
            ]
            self.logger.warning(
                f"Resource {source_name}.{resource_name} is disabled but required by enabled resources",
                extra_fields={
                    "dependent_resources": dependent_list,
                    "action": "including_disabled_dependency",
                },
            )
            return True

        return resource_config.enabled

    def _build_plan(self) -> None:
        """Build the complete execution plan."""
        self._build_dependency_graphs()
        self._organize_stages()
        self._load_dependency_queries()
        self._resolve_required_queries_value()

        # Warn if selection resulted in no endpoints
        if self.selection is not None:
            total_resources = sum(len(stage.resources) for stage in self.stages)
            if total_resources == 0:
                # Check which sources/endpoints were selected but not found
                missing_items = []
                for source_name, selected_resources in self.selection.items():
                    source_config = self.get_source_config(source_name)
                    if not source_config:
                        missing_items.append(f"source '{source_name}' (not found)")
                    elif selected_resources is not None:
                        # Check which endpoints don't exist
                        available_resources = set(source_config.resources.keys())
                        missing_resources = selected_resources - available_resources
                        if missing_resources:
                            missing_items.append(
                                f"resources {list(missing_resources)} from source '{source_name}'"
                            )
                        # Check if any selected endpoints exist but are disabled
                        found_but_disabled = [
                            ep
                            for ep in selected_resources
                            if ep in available_resources and not source_config.resources[ep].enabled
                        ]
                        if found_but_disabled:
                            missing_items.append(
                                f"resources {found_but_disabled} from source '{source_name}' (disabled)"
                            )
                    else:
                        # All endpoints selected, but none found
                        if source_config:
                            available_resources = [
                                ep
                                for ep, ep_config in source_config.resources.items()
                                if ep_config.enabled
                            ]
                            if not available_resources:
                                missing_items.append(
                                    f"source '{source_name}' (no enabled resources)"
                                )

                if missing_items:
                    self.logger.warning(
                        "Selection resulted in no resources to execute",
                        extra_fields={
                            "selection": {
                                source: list(resources) if resources else None
                                for source, resources in self.selection.items()
                            },
                            "missing_items": missing_items,
                        },
                    )
                else:
                    self.logger.warning(
                        "Selection resulted in no resources to execute",
                        extra_fields={
                            "selection": {
                                source: list(resources) if resources else None
                                for source, resources in self.selection.items()
                            },
                        },
                    )

        self._validate_database_resources_plan_time_request_contexts()

    def _validate_database_resources_plan_time_request_contexts(self) -> None:
        """Reject database resources whose static batch inputs expand to multiple contexts."""
        for meta in self._resource_metadata.values():
            source_config = self.get_source_config(meta.source_name)
            if source_config is None:
                continue
            validate_plan_time_static_database_request_context_expansion(
                meta, source_type=source_config.type
            )

    def _load_dependency_queries(self) -> None:
        """
        Load dependency query files for parameters that source from Databricks.

        Only resources in the execution plan (self.stages) are considered. Queries
        for excluded sources/resources are never loaded, so Databricks is only
        queried for data that will actually be used. Both parameters and
        path_parameters are checked.
        """
        queries_config = self.config.queries
        for stage in self.stages:
            for resource_meta in stage.resources:
                source_config = self.get_source_config(resource_meta.source_name)

                if not source_config:
                    continue

                resource_config = source_config.resources.get(resource_meta.resource_name)

                if not resource_config:
                    continue

                for input_name, input_config in resource_config.request_inputs.items():
                    for query_ref in input_config.get_databricks_query_refs():
                        # Check if the query ref is present in the config queries first
                        if not any(q for q in queries_config if q.name == query_ref):
                            raise PlanningError(
                                message="Missing dependency query reference in configuration",
                                operation="_load_dependency_queries",
                                details={
                                    "source": resource_meta.source_name,
                                    "resource_name": resource_meta.resource_name,
                                    "input": input_name,
                                    "query_ref": query_ref,
                                },
                            )

                        if query_ref not in self.required_queries:
                            query_content = self.config.load_query_file(query_ref)

                            # Raise error if query content could not be loaded
                            if not query_content or not isinstance(query_content, str):
                                raise PlanningError(
                                    message="Failed to load dependency query",
                                    operation="_load_dependency_queries",
                                    details={
                                        "source": resource_meta.source_name,
                                        "resource_name": resource_meta.resource_name,
                                        "input": input_name,
                                        "query_ref": query_ref,
                                    },
                                )

                            # Validate the loaded query content
                            if not validate_query_content(query_content):
                                raise PlanningError(
                                    message="Invalid query content loaded",
                                    operation="_load_dependency_queries",
                                    details={
                                        "source": resource_meta.source_name,
                                        "resource_name": resource_meta.resource_name,
                                        "input": input_name,
                                        "query_ref": query_ref,
                                    },
                                )

                            # Store the validated query content
                            self.required_queries[query_ref] = query_content

    def _resolve_required_queries_value(self) -> None:
        """
        Resolve query values for parameters that source from Databricks.
        """

        if not self.required_queries:
            self.logger.trace("No Databricks queries to resolve.")
            return

        # Setup the Databricks utilities for resolving queries
        databricks_utils = DatabricksUtils()

        # Iterate through all the required queries and resolve their values
        for query_ref, query_content in self.required_queries.items():
            # Process the query using DatabricksUtils
            resolved_query_value = databricks_utils.resolve_databricks_query(query_content)

            # Store the resolved values into the redis_context for usage in resolving parameters
            redis_key = format_query_ref_key(query_ref)
            self.redis_context.store(
                key=redis_key,
                data=resolved_query_value,
                ttl=3600,  # 1 hour TTL
            )

            self.logger.debug(
                f"Resolved Databricks query values for {query_ref}",
                extra_fields={
                    "query_ref": query_ref,
                    "redis_key": redis_key,
                    "result_count": len(resolved_query_value) if resolved_query_value else 0,
                },
            )

    def _build_dependency_graphs(self) -> None:
        """Build forward and reverse dependency graphs."""
        for source_name, source in self.config.sources.items():
            if not self._should_include_source(source_name, source):
                continue

            # Check if the source has configured headers
            has_source_headers = bool(source.headers)

            for resource_name, resource in source.resources.items():
                resource_id = f"{source_name}.{resource_name}"
                dependencies = set()

                # Check request_inputs for source dependencies
                for _input_name, input_config in resource.request_inputs.items():
                    source_config = input_config.get_source_config()
                    if source_config:
                        dep_id = f"{source_name}.{source_config.source}"
                        dependencies.add(dep_id)
                        if resource.enabled or self._should_include_resource(
                            resource_name, resource, source_name
                        ):
                            self._reverse_graph[dep_id].add(resource_id)

                # Check resource headers for source dependencies or source level headers if they exist
                if resource.headers or has_source_headers:
                    headers_to_check = {
                        **(source.headers or {}),
                        **(resource.headers or {}),
                    }
                    for _, header_value in headers_to_check.items():
                        # Check if header has source config
                        if isinstance(header_value, ComplexDynamicValue):
                            if (
                                header_value.type == DynamicValueType.SOURCE
                                and header_value.source_config
                            ):
                                dep_id = f"{source_name}.{header_value.source_config.source}"
                                dependencies.add(dep_id)
                                if resource.enabled or self._should_include_resource(
                                    resource_name, resource, source_name
                                ):
                                    self._reverse_graph[dep_id].add(resource_id)

                # Store initial dependencies
                self._dependency_graph[resource_id] = dependencies

            for resource_name, resource in source.resources.items():
                resource_id = f"{source_name}.{resource_name}"

                for input_name, input_config in resource.request_inputs.items():
                    if input_config.has_source_config() and resource.snapshot is not None:
                        if not hasattr(self, "_snapshot_dependencies"):
                            self._snapshot_dependencies = set()
                        self._snapshot_dependencies.add(resource_id)
                        if not hasattr(self, "_snapshot_triggers"):
                            self._snapshot_triggers = set()
                        source_config = input_config.get_source_config()
                        if source_config:
                            self._snapshot_triggers.add(f"{source_name}.{source_config.source}")

                        if not resource.snapshot.ready_condition:
                            raise PlanningError(
                                message="Invalid snapshot configuration",
                                operation="_build_dependency_graphs",
                                details={
                                    "resource": resource_id,
                                    "input": input_name,
                                    "error": "Missing required snapshot configuration (ready_condition)",
                                },
                            )

        forced_resources = set()

        for source_name, source in self.config.sources.items():
            if not self._should_include_source(source_name, source):
                continue

            for resource_name, resource in source.resources.items():
                resource_id = f"{source_name}.{resource_name}"

                # Use _should_include_resource (respects selection) not resource.enabled alone
                if self._should_include_resource(resource_name, resource, source_name):
                    forced_resources.add(resource_id)
                    stack = list(self._dependency_graph[resource_id])

                    while stack:
                        dep_id = stack.pop()
                        if dep_id not in forced_resources:
                            forced_resources.add(dep_id)
                            stack.extend(self._dependency_graph.get(dep_id, set()))

        for source_name, source in self.config.sources.items():
            if not self._should_include_source(source_name, source):
                continue

            self._source_configs[source_name] = source

            for resource_name, resource in source.resources.items():
                resource_id = f"{source_name}.{resource_name}"

                if resource_id not in forced_resources:
                    continue

                if not resource.enabled and resource_id in forced_resources:
                    direct_dependents = [
                        dep_id
                        for dep_id in self._reverse_graph[resource_id]
                        if dep_id in forced_resources
                    ]
                    dependent_list = [dep_id.replace(".", " -> ") for dep_id in direct_dependents]

                    self.logger.warning(
                        f"Resource {source_name}.{resource_name} is disabled but required by enabled resources",
                        extra_fields={
                            "dependent_resources": dependent_list,
                            "action": "including_disabled_dependency",
                        },
                    )

                batch_input_sizes = {}
                parent_fields = set()
                filter_relationships = {}

                # Get batch inputs from request_inputs (any location)
                batch_inputs_config = resource.get_batch_inputs()

                for input_name, input_config in batch_inputs_config.items():
                    batch_size = (
                        input_config.batch_size if input_config.batch_size is not None else 1
                    )
                    batch_input_sizes[input_name] = batch_size

                    source_config = input_config.get_source_config()
                    if source_config:
                        filter_config = input_config.get_filter_config()
                        if filter_config:
                            filter_relationships[input_name] = filter_config

                        if source_config.field:
                            parent_fields.add(source_config.field)

                # Backfill from body inputs: value may be a dict with "backfill" key
                body_inputs = resource.get_inputs_by_location("body")
                body_dict = {n: c.value for n, c in body_inputs.items()} if body_inputs else None
                backfill_cfg = get_backfill_config(body_dict)
                self._resource_metadata[resource_id] = ResourceMetadata(
                    source_name=source_name,
                    resource_name=resource_name,
                    dependencies=self._dependency_graph[resource_id],
                    batch_inputs=batch_input_sizes,
                    filter_relationships=filter_relationships or None,
                    parent_fields=parent_fields or None,
                    config=resource,
                    backfill_config=backfill_cfg,
                )

    def get_source_config(self, source_name: str) -> Optional[SourceConfig]:
        """
        Get configuration for a specific source.

        Args:
            source_name: Name of the source

        Returns:
            Optional[SourceConfig]: Source configuration if found
        """
        return self._source_configs.get(source_name)

    def get_resource_config(self, source_name: str, resource_name: str) -> Optional[ResourceConfig]:
        """
        Get configuration for a specific endpoint.

        Args:
            source_name: Name of the source
            resource_name: Name of the endpoint

        Returns:
            Optional[ResourceConfig]: Resource configuration if found
        """
        source_config = self.get_source_config(source_name)
        if source_config:
            return source_config.resources.get(resource_name)
        return None

    def get_resource_inputs(
        self, source_name: str, resource_name: str
    ) -> Tuple[Dict[str, RequestInputConfig], Dict[str, RequestInputConfig]]:
        """
        Get regular and batch request inputs for an endpoint (from request_inputs only).

        Returns:
            Tuple of (regular_inputs, batch_inputs)
        """
        resource_config = self.get_resource_config(source_name, resource_name)
        if not resource_config:
            return {}, {}

        batch_inputs = {}
        regular_inputs = {}

        for name, param in resource_config.request_inputs.items():
            if hasattr(param, "batch_size") and param.batch_size:
                batch_inputs[name] = param
            else:
                regular_inputs[name] = param

        return regular_inputs, batch_inputs

    def _is_snapshot_trigger(self, metadata: ResourceMetadata) -> bool:
        """Check if an endpoint generates snapshot IDs."""
        resource_id = f"{metadata.source_name}.{metadata.resource_name}"
        return hasattr(self, "_snapshot_triggers") and resource_id in self._snapshot_triggers

    def _is_snapshot_dependent(self, metadata: ResourceMetadata) -> bool:
        """Check if an endpoint depends on snapshot IDs."""
        resource_id = f"{metadata.source_name}.{metadata.resource_name}"
        return (
            hasattr(self, "_snapshot_dependencies") and resource_id in self._snapshot_dependencies
        )

    def _organize_stages(self) -> None:
        """Organize resources into execution stages based on dependencies."""
        assigned_resources = set()
        stage_number = 0

        def can_schedule(
            metadata: ResourceMetadata,
            current_stage: List[ResourceMetadata],
            allow_snapshot_dependents: bool = False,
        ) -> bool:
            resource_id = f"{metadata.source_name}.{metadata.resource_name}"

            if not allow_snapshot_dependents and self._is_snapshot_dependent(metadata):
                return False

            dependencies = self._dependency_graph.get(resource_id, set())
            return all(dep in assigned_resources for dep in dependencies)

        # Phase 1: Schedule snapshot triggers first
        if hasattr(self, "_snapshot_triggers"):
            trigger_resources = []
            for resource_id in self._snapshot_triggers:
                if resource_id not in self._resource_metadata:
                    continue
                metadata = self._resource_metadata[resource_id]
                if can_schedule(metadata, trigger_resources):
                    trigger_resources.append(metadata)

            if trigger_resources:
                self.stages.append(
                    ExecutionStage(resources=trigger_resources, stage_number=stage_number + 1)
                )
                assigned_resources.update(
                    f"{metadata.source_name}.{metadata.resource_name}"
                    for metadata in trigger_resources
                )
                stage_number += 1

                self.logger.debug(
                    "Created snapshot trigger stage",
                    extra_fields={
                        "stage_number": stage_number,
                        "resources": [
                            f"{ep.source_name}.{ep.resource_name}" for ep in trigger_resources
                        ],
                    },
                )

        # Phase 2: Schedule regular endpoints
        remaining_regular = [
            metadata
            for resource_id, metadata in self._resource_metadata.items()
            if (resource_id not in assigned_resources and not self._is_snapshot_dependent(metadata))
        ]

        while remaining_regular:
            stage_resources = []
            next_remaining = []

            for metadata in remaining_regular:
                if can_schedule(metadata, stage_resources):
                    stage_resources.append(metadata)
                else:
                    next_remaining.append(metadata)

            if not stage_resources:
                if next_remaining:
                    unassigned = {f"{m.source_name}.{m.resource_name}" for m in next_remaining}
                    raise PlanningError(
                        message="Circular dependency detected in execution plan",
                        operation="_organize_stages",
                        details={
                            "unassigned_resources": list(unassigned),
                            "assigned_resources": list(assigned_resources),
                        },
                    )
                break

            self.stages.append(
                ExecutionStage(resources=stage_resources, stage_number=stage_number + 1)
            )
            assigned_resources.update(
                f"{metadata.source_name}.{metadata.resource_name}" for metadata in stage_resources
            )
            stage_number += 1
            remaining_regular = next_remaining

            self.logger.debug(
                "Created regular stage",
                extra_fields={
                    "stage_number": stage_number,
                    "resources": [f"{ep.source_name}.{ep.resource_name}" for ep in stage_resources],
                },
            )

        # Phase 3: Schedule snapshot dependents
        if hasattr(self, "_snapshot_dependencies"):
            snapshot_dependents = [
                self._resource_metadata[resource_id]
                for resource_id in self._snapshot_dependencies
                if resource_id in self._resource_metadata
            ]

            snapshot_dependents.sort(
                key=lambda m: len(self._dependency_graph[f"{m.source_name}.{m.resource_name}"])
            )

            while snapshot_dependents:
                stage_resources = []
                next_remaining = []

                for metadata in snapshot_dependents:
                    if can_schedule(metadata, stage_resources, allow_snapshot_dependents=True):
                        stage_resources.append(metadata)
                    else:
                        next_remaining.append(metadata)

                if not stage_resources:
                    raise PlanningError(
                        message="Snapshot dependent has unmet dependencies",
                        operation="_organize_stages",
                        details={
                            "unassigned_resources": [
                                f"{m.source_name}.{m.resource_name}" for m in next_remaining
                            ],
                            "dependencies": [
                                list(self._dependency_graph[f"{m.source_name}.{m.resource_name}"])
                                for m in next_remaining
                            ],
                        },
                    )

                self.stages.append(
                    ExecutionStage(resources=stage_resources, stage_number=stage_number + 1)
                )
                assigned_resources.update(
                    f"{metadata.source_name}.{metadata.resource_name}"
                    for metadata in stage_resources
                )
                stage_number += 1
                snapshot_dependents = next_remaining

                self.logger.debug(
                    "Created snapshot dependent stage",
                    extra_fields={
                        "stage_number": stage_number,
                        "resources": [
                            f"{ep.source_name}.{ep.resource_name}" for ep in stage_resources
                        ],
                        "dependencies": [
                            list(self._dependency_graph[f"{ep.source_name}.{ep.resource_name}"])
                            for ep in stage_resources
                        ],
                    },
                )

    def get_resource_metadata(
        self, source_name: str, resource_name: str
    ) -> Optional[ResourceMetadata]:
        """
        Get metadata for a specific endpoint.

        Args:
            source_name: Name of the source
            resource_name: Name of the endpoint

        Returns:
            Optional[ResourceMetadata]: Metadata for the endpoint if found
        """
        resource_id = f"{source_name}.{resource_name}"
        return self._resource_metadata.get(resource_id)

    def get_dependent_resources(
        self, source_name: str, resource_name: str
    ) -> List[ResourceMetadata]:
        """
        Get all endpoints that depend on the specified endpoint.

        Args:
            source_name: Name of the source
            resource_name: Name of the endpoint

        Returns:
            List[ResourceMetadata]: List of dependent endpoint metadata
        """
        resource_id = f"{source_name}.{resource_name}"
        dependent_ids = self._reverse_graph.get(resource_id, set())
        # Only return dependents that are in the execution plan (selection may exclude some)
        return [
            self._resource_metadata[dep_id]
            for dep_id in dependent_ids
            if dep_id in self._resource_metadata
        ]

    def get_dependent_loading_configs(
        self, source_name: str, resource_name: str
    ) -> List[Tuple[LoadingConfig, Optional[str]]]:
        """
        For endpoints that depend on this one, return (loading_config, source_type)
        for each that has loading. Used for auto-backfill when parent has no loading.

        Args:
            source_name: Name of the source
            resource_name: Name of the endpoint

        Returns:
            List of (loading_config, source_type) for dependents that have loading
        """
        resource_id = f"{source_name}.{resource_name}"
        dependent_ids = self._reverse_graph.get(resource_id, set())
        result: List[Tuple[LoadingConfig, Optional[str]]] = []
        for dep_id in dependent_ids:
            dep_source, dep_resource = dep_id.split(".", 1)
            dep_config = self.get_resource_config(dep_source, dep_resource)
            dep_source_config = self.get_source_config(dep_source)
            if dep_config and dep_config.loading and dep_source_config:
                result.append((dep_config.loading, dep_source_config.type))
        return result

    def get_execution_order(self) -> List[ResourceMetadata]:
        """
        Get the complete execution order as a flat list.

        Returns:
            List[ResourceMetadata]: Ordered list of endpoint metadata
        """
        return [meta for stage in self.stages for meta in stage.resources]

    def get_stage_for_resource(self, source_name: str, resource_name: str) -> Optional[int]:
        """
        Get the stage number for a specific endpoint.

        Args:
            source_name: Name of the source
            resource_name: Name of the endpoint

        Returns:
            Optional[int]: Stage number (1-based) or None if not found
        """
        for stage in self.stages:
            for meta in stage.resources:
                if meta.source_name == source_name and meta.resource_name == resource_name:
                    return stage.stage_number
        return None

    def summarize(self) -> Dict[str, Any]:
        """
        Generate a summary of the execution plan with request count estimates.

        Returns:
            Dict[str, Any]: Summary of the execution plan
        """
        total_estimated_requests = 0
        has_unknown_requests = False
        stages_summary = []

        for stage in self.stages:
            resources_summary = []
            for ep in stage.resources:
                estimated_requests = ep.estimate_request_count()
                if estimated_requests:
                    total_estimated_requests += estimated_requests
                elif estimated_requests is None:
                    has_unknown_requests = True

                resource_id = f"{ep.source_name}.{ep.resource_name}"
                dependent_ids = self._reverse_graph.get(resource_id, set())
                dependents = sorted(
                    dep_id for dep_id in dependent_ids if dep_id in self._resource_metadata
                )
                resource_info = {
                    "source": ep.source_name,
                    "resource_name": ep.resource_name,
                    "dependencies": list(ep.dependencies),
                    "dependents": dependents,
                    "batch_inputs": ep.batch_inputs,
                }

                if estimated_requests is not None:
                    resource_info["estimated_requests"] = estimated_requests
                else:
                    resource_info["estimated_requests"] = "unknown"

                resources_summary.append(resource_info)

            stages_summary.append(
                {"stage_number": stage.stage_number, "resources": resources_summary}
            )

        # Format total requests based on whether we have unknowns
        total_requests_value = None
        if total_estimated_requests > 0 or has_unknown_requests:
            if has_unknown_requests:
                total_requests_value = (
                    f"at least {total_estimated_requests}"
                    if total_estimated_requests > 0
                    else "unknown"
                )
            else:
                total_requests_value = total_estimated_requests

        summary = {
            "total_stages": len(self.stages),
            "total_resources": len(self._resource_metadata),
            "total_estimated_requests": total_requests_value,
            "stages": stages_summary,
            "timestamp": datetime.now(UTC).isoformat(),
        }

        return summary

    def get_input_filter(
        self, source_name: str, resource_name: str, input_name: str
    ) -> Optional[FilterConfig]:
        """
        Get filter configuration for a request input if it exists.

        Args:
            source_name: Name of the source
            resource_name: Name of the endpoint
            input_name: Name of the request input

        Returns:
            Optional[FilterConfig]: Filter configuration if exists
        """
        resource_meta = self.get_resource_metadata(source_name, resource_name)
        if resource_meta and resource_meta.filter_relationships:
            return resource_meta.filter_relationships.get(input_name)
        return None

    def has_parent_inputs(self, source_name: str, resource_name: str) -> bool:
        """
        Check if an endpoint has inputs sourced from other endpoints.

        Args:
            source_name: Name of the source
            resource_name: Name of the endpoint

        Returns:
            bool: True if the endpoint has inputs from other endpoints
        """
        resource_meta = self.get_resource_metadata(source_name, resource_name)
        if not resource_meta:
            return False

        return bool(resource_meta.dependencies)

    def has_batch_inputs(self, source_name: str, resource_name: str) -> bool:
        """
        Check if an endpoint has batch inputs (request inputs with batch_size).

        Args:
            source_name: Name of the source
            resource_name: Name of the endpoint

        Returns:
            bool: True if the endpoint has at least one batch parameter
        """
        resource_meta = self.get_resource_metadata(source_name, resource_name)
        if not resource_meta:
            return False

        return bool(resource_meta.batch_inputs)

    def get_enabled_sources(self) -> List[str]:
        """
        Get list of sources that should be processed.
        This is a simple version that doesn't consider dependencies.
        Use get_sources_from_plan() for dependency-aware source extraction.

        Returns:
            List[str]: List of source names to process
        """
        if self.selection is not None:
            return list(self.selection.keys())

        return [
            source_name
            for source_name, source_config in self.config.sources.items()
            if source_config.enabled
        ]

    def get_sources_from_plan(self) -> Set[str]:
        """
        Extract all sources that are actually included in the execution plan.
        This includes sources with endpoints in the plan, including dependencies.
        More accurate than get_enabled_sources() as it considers the actual plan.

        Returns:
            Set[str]: Set of source names that are in the execution plan
        """
        sources = set()
        for stage in self.stages:
            for resource_meta in stage.resources:
                sources.add(resource_meta.source_name)
        return sources

    def get_enabled_resources(self, source_name: str) -> List[str]:
        """
        Get list of endpoints that should be processed for a source.

        Args:
            source_name: Name of the source

        Returns:
            List[str]: List of endpoint names to process
        """
        source_config = self.get_source_config(source_name)
        if not source_config:
            return []

        return [
            resource_name
            for resource_name, res_cfg in source_config.resources.items()
            if self._should_include_resource(resource_name, res_cfg, source_name)
        ]
