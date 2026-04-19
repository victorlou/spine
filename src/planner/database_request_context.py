"""
Database-backed sources: request-context expansion vs a single extract per run.

Plan-time checks use ``ResourceMetadata.estimate_request_count()`` so invalid
static batch configuration fails when the execution plan is built. The handler
still calls ``reject_if_database_has_multiple_runtime_request_contexts`` after
resolving dynamic batch values (SOURCE inputs, Redis, etc.) that the planner
cannot count.
"""

from typing import List, Optional, Protocol, runtime_checkable

from src.config.config_models import SourceType, is_database_source_type
from src.utils.exceptions import HandlerError, PlanningError


@runtime_checkable
class _SupportsStaticRequestCountEstimate(Protocol):
    """Minimal shape for plan-time validation without importing circular types."""

    source_name: str
    resource_name: str
    batch_inputs: dict

    def estimate_request_count(self) -> Optional[int]:
        ...


def validate_plan_time_static_database_request_context_expansion(
    meta: _SupportsStaticRequestCountEstimate,
    *,
    source_type: SourceType,
) -> None:
    """
    Fail plan build when a database resource's batch inputs are purely static
    and provably expand to more than one request context.
    """
    if not is_database_source_type(source_type):
        return
    est = meta.estimate_request_count()
    if est is None or est <= 1:
        return
    raise PlanningError(
        message=(
            "A database-backed resource cannot expand to multiple request contexts from static "
            "batch configuration: the extract runs once and transformations only use the first "
            "context. Remove or narrow batch_inputs, split into separate resources, or use "
            "dynamic inputs only if a single resolved context is intended."
        ),
        operation="validate_database_request_contexts",
        details={
            "source": meta.source_name,
            "resource_name": meta.resource_name,
            "estimated_request_context_count": est,
            "batch_input_keys": list(meta.batch_inputs.keys()),
        },
    )


def reject_if_database_has_multiple_runtime_request_contexts(
    *,
    is_db: bool,
    request_context_count: int,
    resource_name: str,
    source_name: str,
    batch_input_keys: List[str],
    use_backfill: bool,
) -> None:
    """
    Last line of defense after dynamic resolution: database extract is not repeated
    per context and transformations only see the first context.
    """
    if not is_db or request_context_count <= 1:
        return
    raise HandlerError(
        "Database resources cannot expand to multiple request contexts: "
        "the table or select query is read once and transformations use only the first context. "
        "Remove batching expansion for this resource, split it into separate resources, "
        "or lower your request limit so a single context remains.",
        operation="process_resource",
        details={
            "resource_name": resource_name,
            "source": source_name,
            "request_context_count": request_context_count,
            "batch_input_keys": batch_input_keys,
            "use_backfill": use_backfill,
        },
    )
