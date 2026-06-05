"""
Main entry point for the data ingestion pipeline.
Provides a configuration-driven pipeline runner for different data sources.
"""

import json
import sys
import time
from typing import Any, Dict, Optional, Set

import click
from opentelemetry import trace

from src.config.settings import get_settings
from src.handler.dynamic_handler import DynamicHandler
from src.utils.env_manager import load_pipeline_dotenv, process_environment_variables
from src.utils.exceptions import GracefulShutdownError, PipelineError
from src.utils.logger import get_logger, set_root_log_level
from src.utils.telemetry_manager import TelemetryManager


def parse_select(select_str: str) -> Dict[str, Optional[Set[str]]]:
    """
    Parse select argument into source/resource selection structure.

    Formats:
    - "source": all resources for source
    - "source:resource": single resource
    - "source::resource": single resource (double colon)

    Args:
        select_str: Comma-separated string of source or source:resource selections

    Returns:
        Dict[str, Optional[Set[str]]]: Selection structure where:
            - Key is source name
            - Value is None (all resources) or Set[str] (specific resource names)
    """
    selections: Dict[str, Optional[Set[str]]] = {}
    for item in select_str.split(","):
        item = item.strip()
        if not item:
            continue

        # Handle both single and double colon
        if "::" in item:
            source, resource = item.split("::", 1)
        elif ":" in item:
            source, resource = item.split(":", 1)
        else:
            source, resource = item, None

        source = source.strip()
        resource = resource.strip() if resource else None

        if source not in selections:
            selections[source] = set() if resource else None

        if resource:
            if selections[source] is None:
                selections[source] = set()
            selections[source].add(resource)

    return selections


def run_pipeline(
    validate_only: bool = False,
    show_plan: bool = False,
    log_level: Optional[str] = None,
    selection: Optional[Dict[str, Optional[Set[str]]]] = None,
    limit: Optional[int] = None,
    backfill: bool = False,
) -> Dict[str, Any]:
    """
    Run the data ingestion pipeline using configuration.

    Args:
        validate_only: If True, only validate configuration without executing
        show_plan: If True, only show the execution plan without validating or executing
        log_level: Optional log level to override environment variable
        selection: Optional selection structure mapping source names to resource name sets.
                   None means all resources, Set[str] means specific resource names.
        limit: Optional limit on fetch operations per resource (for development/testing). When set, data is not written to the configured loading destination.
        backfill: If True, use backfill date ranges for resources that have backfill config (manual backfill).

    Returns:
        Dict[str, Any]: Results of the pipeline execution
    """
    # Set root log level if provided
    if log_level:
        set_root_log_level(log_level)

    logger = get_logger("Pipeline")

    try:
        if show_plan:
            logger.info("Showing execution plan")
        elif validate_only:
            logger.info("Starting pipeline validation")
        else:
            logger.info(
                "Starting data ingestion pipeline",
                extra_fields={"selection": selection if selection else "all"},
            )

        # Get settings and configuration (filter sources early if specified)
        settings = get_settings(selection=selection)

        # Initialize telemetry (no-op unless enabled and an OTLP endpoint is resolvable).
        TelemetryManager().init(settings.pipeline_config.defaults.telemetry)

        # Create handler with settings and selection filter
        handler = DynamicHandler(
            settings,
            selection=selection,
            record_limit=limit,
            backfill_mode=backfill,
        )

        if show_plan:
            # Only show execution plan
            plan = handler.execution_plan.summarize()
            results = {
                "status": "success",
                "message": "Execution plan generated",
                "execution_plan": plan,
            }
            logger.info("Execution plan generated successfully")
        elif validate_only:
            # Only validate configuration
            handler.validate()
            results = {
                "status": "success",
                "message": "Configuration validation successful",
                "sources": {
                    source: {"status": "validated"} for source in settings.pipeline_config.sources
                },
                "execution_plan": handler.execution_plan.summarize(),
            }
            logger.info("Configuration validation successful")
        else:
            # Run full pipeline under a root span so resource spans nest beneath it.
            tracer = trace.get_tracer("spine.pipeline")
            with tracer.start_as_current_span("spine.pipeline.run") as run_span:
                run_span.set_attribute(
                    "spine.selection", str(sorted(selection)) if selection else "all"
                )
                run_span.set_attribute("spine.backfill", backfill)
                if limit is not None:
                    run_span.set_attribute("spine.limit", limit)

                results = handler.handle()

                run_span.set_attribute("spine.status", results.get("status", "unknown"))
                if results.get("status") != "success":
                    run_span.set_status(trace.StatusCode.ERROR)

            # Log results summary
            status = results["status"]

            if status == "success":
                logger.info(
                    "Pipeline completed successfully",
                    extra_fields={"sources": len(results["sources"]), "status": status},
                )

                # Log successful resources
                for source_name, source_details in results["sources"].items():
                    if source_details["status"] == "success":
                        for resource_name, details in source_details["resources"].items():
                            logger.info(
                                f"Successfully processed {resource_name}",
                                extra_fields={
                                    "source": source_name,
                                    "resource_name": resource_name,
                                    "count": details.get("count"),
                                    "location": details.get("location"),
                                },
                            )
            else:
                # Log detailed error information
                logger.error(
                    "Pipeline completed with errors",
                    extra_fields={"sources": len(results["sources"]), "status": status},
                )

                # Log errors for each failed source
                for source_name, source_details in results["sources"].items():
                    if source_details["status"] == "failed":
                        error_info = source_details.get("error", {})
                        logger.error(f"Source '{source_name}' failed", extra_fields=error_info)

        return results

    except KeyboardInterrupt:
        logger.warning("Pipeline interrupted by user")
        return {"status": "interrupted", "message": "Interrupted by user"}
    except GracefulShutdownError as e:
        logger.warning("Pipeline terminated (SIGTERM)", extra_fields={"message": str(e)})
        return {"status": "interrupted", "message": str(e)}
    except Exception as e:
        # Format error details using PipelineError
        if isinstance(e, PipelineError):
            error_details = e.format_error()
        else:
            error_details = PipelineError.format_unknown_error(e)

        logger.error("Pipeline failed", extra_fields=error_details)

        return {"status": "failed", "error": error_details}


@click.command()
@click.option(
    "--validate-only",
    "-v",
    is_flag=True,
    help="Only validate configuration without executing the pipeline",
)
@click.option(
    "--show-plan",
    "-p",
    is_flag=True,
    help="Only show the execution plan without validating or executing",
)
@click.option(
    "--log-level",
    "-ll",
    type=click.Choice(
        ["TRACE", "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], case_sensitive=False
    ),
    help="Set the log level (overrides environment variable)",
)
@click.option(
    "--select",
    "-s",
    help="Comma-separated list of source or source:resource selections. "
    "Examples: 'tiktok_ads' (all resources), 'tiktok_ads:ad_daily_report' (single resource), "
    "'tiktok_ads::ad_daily_report' (same, double colon supported)",
    default=None,
)
@click.option(
    "--limit",
    "-l",
    type=click.IntRange(min=0),
    help=(
        "Limit number of fetch operations per resource (useful for testing, 0=skip resource). "
        "When set, data is NOT written to the configured loading destination."
    ),
    default=None,
)
@click.option(
    "--backfill",
    "-b",
    is_flag=True,
    help="Use backfill date ranges for resources that have backfill config (manual backfill).",
)
def main(
    validate_only: bool,
    show_plan: bool,
    log_level: str,
    select: Optional[str],
    limit: Optional[int],
    backfill: bool,
):
    """
    Main entry point.
    Loads environment variables and executes the pipeline.
    """
    start = time.perf_counter()
    try:
        # Load environment variables from repo-root (and optional legacy) .env files
        load_pipeline_dotenv()

        # Process environment variables for JSON-formatted secrets
        process_environment_variables()

        # Parse select argument if provided
        selection = parse_select(select) if select else None

        # Run pipeline and get results
        results = run_pipeline(
            validate_only=validate_only,
            show_plan=show_plan,
            log_level=log_level,
            selection=selection,
            limit=limit,
            backfill=backfill,
        )

        # Output results as JSON
        print(json.dumps(results, indent=2))

        end = time.perf_counter()

        duration = end - start
        logger = get_logger("Pipeline")
        logger.info("Total execution time", extra_fields={"duration_seconds": duration})

        # Exit with appropriate status code (130 = terminated by signal/Ctrl+C)
        if results.get("status") == "interrupted":
            sys.exit(130)
        sys.exit(0 if results["status"] == "success" else 1)

    except KeyboardInterrupt:
        logger = get_logger("Pipeline")
        logger.warning("Pipeline interrupted by user")
        print(json.dumps({"status": "interrupted", "message": "Interrupted by user"}, indent=2))
        sys.exit(130)
    except GracefulShutdownError as e:
        logger = get_logger("Pipeline")
        logger.warning("Pipeline terminated (SIGTERM)", extra_fields={"message": str(e)})
        # Flush buffered telemetry promptly on SIGTERM (atexit is the normal-path backstop).
        TelemetryManager().shutdown()
        print(json.dumps({"status": "interrupted", "message": str(e)}, indent=2))
        sys.exit(130)
    except Exception as e:
        # Format error details using PipelineError
        if isinstance(e, PipelineError):
            error_details = e.format_error()
        else:
            error_details = PipelineError.format_unknown_error(e)

        end = time.perf_counter()

        duration = end - start
        logger = get_logger("Pipeline")
        logger.info("Total execution time", extra_fields={"duration_seconds": duration})

        print(json.dumps({"error": error_details}, indent=2), file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
