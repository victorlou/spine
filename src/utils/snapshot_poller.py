"""
Utility for handling snapshot-based polling with safe condition evaluation.
"""

import ast
import time
from datetime import datetime, timedelta
from typing import Any, Callable, Dict

from src.config.config_models import SnapshotConfig


class SnapshotError(Exception):
    """Exception raised when snapshot enters error state."""

    def __init__(self, message: str, response: Any):
        super().__init__(message)
        self.response = response


class SnapshotTimeoutError(Exception):
    """Exception raised when snapshot polling exceeds max time."""

    def __init__(self, message: str, last_response: Any):
        super().__init__(message)
        self.last_response = last_response


class SnapshotPoller:
    """Handles polling for snapshot-based resources (REST)."""

    def __init__(
        self,
        config: "SnapshotConfig",  # Forward reference since config models import this
        logger: Any,
        get_snapshot: Callable[[Dict[str, Any]], Any],
    ):
        """
        Initialize the snapshot poller.

        Args:
            config: Snapshot polling configuration
            logger: Logger instance
            get_snapshot: Callback function to execute GET request
        """
        self.config = config
        self.logger = logger
        self.get_snapshot = get_snapshot

    def _evaluate_condition(self, condition: str, response: Any) -> bool:
        """
        Safely evaluate condition against response.

        Args:
            condition: Python expression to evaluate
            response: API response to evaluate against

        Returns:
            bool: Result of condition evaluation

        The condition is evaluated in a restricted context where only the response
        object is available. This prevents arbitrary code execution.
        """
        try:
            # Create a safe evaluation context with just the response
            context = {"response": response}

            # Parse condition into AST and compile
            expr = ast.parse(condition, mode="eval")
            compiled = compile(expr, "<string>", "eval")

            # Log evaluation attempt
            self.logger.trace(
                "Evaluating condition",
                extra_fields={
                    "condition": condition,
                    "response_type": type(response).__name__,
                    "response_content": response,
                    "context": context,
                },
            )

            # Evaluate in restricted context
            result = bool(eval(compiled, {"__builtins__": {}}, context))

            # Log evaluation result
            self.logger.trace(
                "Condition evaluation result",
                extra_fields={"condition": condition, "result": result},
            )

            return result

        except Exception as e:
            self.logger.error(
                "Failed to evaluate condition",
                extra_fields={"condition": condition, "error": str(e), "response": response},
            )
            return False

    def wait_for_completion(self, params: Dict[str, Any]) -> Any:
        """
        Poll until snapshot is ready or timeout occurs.

        Args:
            params: Parameters for the snapshot GET request

        Returns:
            Dict[str, Any]: Final response from the snapshot resource

        Raises:
            SnapshotTimeoutError: If max_time is exceeded
            SnapshotError: If snapshot enters error state
        """
        start_time = datetime.now()
        deadline = start_time + timedelta(seconds=self.config.max_time)
        current_interval = self.config.interval
        last_response = None

        while datetime.now() < deadline:
            try:
                response = self.get_snapshot(params)
                last_response = response

                # Check for error state first if configured
                if self.config.error_condition and self._evaluate_condition(
                    self.config.error_condition, response
                ):
                    raise SnapshotError(
                        f"Snapshot entered error state: {response}", response=response
                    )

                # Check if ready
                if self._evaluate_condition(self.config.ready_condition, response):
                    self.logger.info(
                        "Snapshot completed successfully",
                        extra_fields={"time_taken": (datetime.now() - start_time).total_seconds()},
                    )
                    return response

                # Calculate next interval with exponential backoff
                next_interval = min(
                    current_interval * self.config.backoff_factor, self.config.max_interval
                )

                self.logger.debug(
                    "Snapshot not ready, waiting to retry",
                    extra_fields={
                        "next_check_in": next_interval,
                        "time_elapsed": (datetime.now() - start_time).total_seconds(),
                        "deadline_in": (deadline - datetime.now()).total_seconds(),
                        "response": response,
                    },
                )

                time.sleep(next_interval)
                current_interval = next_interval

            except SnapshotError:
                # Re-raise snapshot errors (error_condition met)
                raise
            except Exception as e:
                # Re-raise non-retryable errors (e.g. 404 SnapshotId not found)
                from src.utils.exceptions import PipelineError

                if isinstance(e, PipelineError) and not e.is_retryable:
                    raise
                # Log retryable errors and continue polling
                self.logger.error(
                    "Error while polling snapshot",
                    extra_fields={
                        "error": str(e),
                        "time_elapsed": (datetime.now() - start_time).total_seconds(),
                    },
                )
                time.sleep(self.config.interval)

        # If we get here, we've exceeded max_time
        raise SnapshotTimeoutError(
            f"Snapshot did not complete within {self.config.max_time} seconds",
            last_response=last_response or {},
        )
