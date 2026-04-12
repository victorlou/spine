"""
Environment variable management utility.
Handles environment variable loading and processing, including JSON-formatted secrets.
"""

import json
import os
import re
from pathlib import Path
from typing import Any

from src.utils.exceptions import ConfigError
from src.utils.logger import get_logger

logger = get_logger(__name__)


def _parse_env_list_if_json(env_value: str) -> Any:
    """
    If the env value looks like a JSON array, parse and return a list; otherwise return as-is.

    This allows list env vars to be written in .env as:
        ENV_LIST=["219880","426051"]
    or comma-separated (unchanged, returned as string):
        ENV_LIST=219880,426051
    """
    if not isinstance(env_value, str):
        return env_value
    s = env_value.strip()
    if s.startswith("[") and s.endswith("]"):
        try:
            parsed = json.loads(env_value)
            return parsed if isinstance(parsed, list) else env_value
        except json.JSONDecodeError:
            pass
    return env_value


def resolve_env_var(value: Any) -> Any:
    """
    Resolve environment variable references in a value.
    Supports both simple references and default values.

    Args:
        value: Value potentially containing environment variable reference
            Formats supported:
            - ${VAR_NAME}           # Simple reference
            - ${VAR_NAME:-default}  # Reference with default

    Returns:
        Any: Resolved value or original value if not an environment variable reference

    Raises:
        ConfigError: If a required environment variable is not found

    Examples:
        >>> resolve_env_var("${API_KEY}")
        "secret123"
        >>> resolve_env_var("/workspaces/${WORKSPACE_ID}/projects")
        "/workspaces/abc123/projects"
        >>> resolve_env_var("${VAR:-default}")
        "default"  # if VAR not set
        >>> resolve_env_var("/api/${VERSION:-v1}/endpoint")
        "/api/v1/endpoint"  # if VERSION not set, uses v1
    """
    if not isinstance(value, str):
        return value

    # Pattern to match ${VAR_NAME} or ${VAR_NAME:-default}
    # Matches: ${VAR}, ${VAR:-default}, ${VAR:-}
    pattern = r"\$\{([^}]+)\}"

    def replace_env_var(match):
        """Replace a single environment variable reference."""
        env_expr = match.group(1)  # Content inside ${...}

        # Handle empty variable name (malformed pattern)
        if not env_expr:
            logger.warning("Empty environment variable reference found, skipping")
            return match.group(0)  # Return original pattern unchanged

        # Handle default value syntax ${VAR:-default} or ${VAR:-}
        if ":-" in env_expr:
            var_name, default_value = env_expr.split(":-", 1)
            var_name = var_name.strip()
            # Empty default value is allowed (becomes empty string)
            return os.environ.get(var_name, default_value)

        # No default value provided - variable is required
        var_name = env_expr.strip()
        env_value = os.environ.get(var_name)
        if env_value is None:
            raise ConfigError(f"Required environment variable not found: {var_name}")
        return env_value

    # Replace all environment variable references in the string
    try:
        resolved = re.sub(pattern, replace_env_var, value)
        # If the whole value was a single env reference and it resolved to a JSON array string, parse to list
        return _parse_env_list_if_json(resolved)
    except ConfigError:
        # Re-raise ConfigError as-is
        raise
    except Exception as e:
        # Log unexpected errors but don't fail silently
        logger.warning(
            "Unexpected error resolving environment variables",
            extra_fields={"value": value, "error": str(e)},
        )
        # Return original value on unexpected errors to avoid breaking the pipeline
        return value


def process_environment_variables() -> None:
    """
    Process environment variables to handle JSON-formatted secrets.
    This is particularly useful for ECS environments where secrets might be
    injected as JSON strings containing multiple environment variables.

    Example JSON format:
    {
        "VARIABLE_1": "xxx",
        "VARIABLE_2": "xxx",
        "VARIABLE_3": "xxx"
    }
    """
    for key, value in os.environ.items():
        if not value:
            continue

        try:
            # Try to parse as JSON
            json_value = json.loads(value)

            # Check if it's a dictionary of environment variables
            if isinstance(json_value, dict):
                # Update environment with all variables from the JSON
                for env_key, env_value in json_value.items():
                    if env_value is not None:  # Skip null values
                        os.environ[env_key] = str(env_value)
                        logger.debug(
                            "Processed environment variable from JSON",
                            extra_fields={"source_var": key, "processed_var": env_key},
                        )

                logger.info(
                    "Successfully processed JSON environment variables",
                    extra_fields={"source_var": key, "processed_count": len(json_value)},
                )

        except json.JSONDecodeError:
            # Not a JSON string, keep original value
            continue
        except Exception as e:
            # Log any other errors but don't fail
            logger.warning(
                "Error processing environment variable",
                extra_fields={"variable": key, "error": str(e)},
            )
            continue


def load_pipeline_dotenv() -> None:
    """
    Load ``.env`` key/value pairs into ``os.environ`` before settings are read.

    Resolution order (later files do not override keys already set, including
    variables injected by the process or container runtime):

    1. ``<repository_root>/.env`` — recommended location; keeps secrets out of ``src/``.
    2. ``<repository_root>/src/.env`` — legacy path for existing setups.

    In production, prefer injecting configuration via the orchestrator (Kubernetes
    secrets, ECS task environment, ``docker run --env-file``, etc.); this helper
    is mainly for local development and Compose-based runs.
    """
    from dotenv import load_dotenv

    repo_root = Path(__file__).resolve().parents[2]
    src_root = Path(__file__).resolve().parents[1]

    load_dotenv(repo_root / ".env", override=False)
    load_dotenv(src_root / ".env", override=False)
