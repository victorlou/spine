"""
Script to generate and print the authentication token for API testing.
"""

import sys

from src.config.settings import get_settings
from src.service.service_factory import ServiceFactory
from src.utils.dynamic_values import resolve_headers_dict
from src.utils.env_manager import load_pipeline_dotenv, process_environment_variables
from src.utils.redis_context import RedisContextManager


def _get_redis_context(settings):
    """Build RedisContextManager from pipeline config. Required for token/signature resolution."""
    context = settings.pipeline_config.defaults.context
    if context.type != "redis" or not context.redis:
        return None
    redis_config = context.redis.model_dump()
    return RedisContextManager(
        redis_config=redis_config,
        prefix=context.prefix,
        default_ttl=context.ttl,
    )


def _is_walmart_style(source_config):
    """True if source uses Walmart-style headers (INTIMESTAMP + AUTH_SIGNATURE)."""
    headers = source_config.headers or {}
    return "WM_SEC.AUTH_SIGNATURE" in headers and "WM_CONSUMER.INTIMESTAMP" in headers


def main():
    # Load environment variables
    load_pipeline_dotenv()

    # Process environment variables for JSON-formatted secrets
    process_environment_variables()

    # Get source name from command line or use default
    source_name = sys.argv[1] if len(sys.argv) > 1 else None
    if not source_name:
        print("Error: Please provide a source name as argument")
        print("Usage: python -m scripts.get_token <source_name>")
        sys.exit(1)

    # Load config for this source only so we don't require env vars for unrelated sources
    settings = get_settings(selection={source_name: None})

    # Validate source exists (should always be present when selection is used)
    if source_name not in settings.pipeline_config.sources:
        print(f"Error: Source '{source_name}' not found in configuration")
        print("Available sources:", list(settings.pipeline_config.sources.keys()))
        sys.exit(1)

    # Get source config from settings
    source_config = settings.pipeline_config.sources[source_name]

    # Redis context required for token and Walmart signature resolution
    redis_context = _get_redis_context(settings)
    if not redis_context:
        print(
            "Error: get_token requires Redis context (defaults.context.type: redis) in pipeline config"
        )
        sys.exit(1)

    # Walmart-style: use shared resolver so timestamp in INTIMESTAMP matches the one in AUTH_SIGNATURE
    if _is_walmart_style(source_config):
        walmart_headers = {
            "WM_CONSUMER.INTIMESTAMP": source_config.headers["WM_CONSUMER.INTIMESTAMP"],
            "WM_SEC.AUTH_SIGNATURE": source_config.headers["WM_SEC.AUTH_SIGNATURE"],
        }
        resolved = resolve_headers_dict(walmart_headers, redis_context)
        timestamp = resolved["WM_CONSUMER.INTIMESTAMP"]
        signature = resolved["WM_SEC.AUTH_SIGNATURE"]
        print("\nWalmart API headers (use same timestamp and signature together):")
        print("WM_CONSUMER.INTIMESTAMP:", timestamp)
        print("WM_SEC.AUTH_SIGNATURE:", signature)
        print("\nBase URL:", source_config.base_url)
        # Optional: other headers useful for Postman (static from config, no resolution)
        for key in ("WM_SEC.KEY_VERSION", "WM_CONSUMER.ID"):
            if source_config.headers and key in source_config.headers:
                val = source_config.headers[key]
                if not isinstance(val, dict) or "type" not in (val or {}):
                    print(f"{key}:", val)
        # Bearer token if present (for Postman)
        if source_config.auth and source_config.auth.type == "bearer_token":
            service = ServiceFactory.create_service(
                settings, source_name, source_config, redis_context=redis_context
            )
            token = service._get_auth_token()
            print("\nAuthorization (Bearer):", token)
        return

    # Token authentication (oauth_jwt / bearer_token)
    if not source_config.auth or source_config.auth.type not in ["oauth_jwt", "bearer_token"]:
        print(f"\nSource '{source_name}' does not use token authentication")
        print(f"Authentication type: {source_config.auth.type if source_config.auth else 'None'}")
        print("No Walmart-style headers (WM_SEC.AUTH_SIGNATURE, WM_CONSUMER.INTIMESTAMP) found.")
        sys.exit(0)

    # Create service using factory
    service = ServiceFactory.create_service(
        settings, source_name, source_config, redis_context=redis_context
    )
    token = service._get_auth_token()

    print("\nAuthorization Token:")
    print("Bearer", token)
    print("\nBase URL:", source_config.base_url)


if __name__ == "__main__":
    main()
