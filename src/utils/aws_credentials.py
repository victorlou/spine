"""
AWS credential management utility.

Loads AWS credentials via boto3's default credential chain (env vars, profiles,
SSO caches, IAM roles) and exposes them so the local-dev S3A path can hand
explicit keys to Spark when the runtime cannot use the JVM's own credential
chain (typically SSO profile caches on a developer machine).

Reachability and permission checks happen in
``src.loader.destination_preflight`` against each effective bucket, not here.
This module is a credential **loader** only.
"""

import os
from typing import Any, Dict

import boto3

from src.utils.exceptions import AWSError
from src.utils.logger import get_logger


class AWSCredentialManager:
    """
    Singleton manager for AWS credentials.
    Handles both environment variables and IAM roles.

    Note: This singleton implementation is not thread-safe. Currently safe
    as initialization occurs sequentially in the main thread. If adding
    multi-threaded initialization, add proper locking (e.g., threading.Lock).
    """

    _instance = None

    def __new__(cls):
        """Ensure singleton instance."""
        if cls._instance is None:
            cls._instance = super(AWSCredentialManager, cls).__new__(cls)
            cls._instance._logger = get_logger(cls.__name__)
            cls._instance._load_credentials()
        return cls._instance

    def _load_credentials(self) -> None:
        """
        Load AWS credentials using boto3's default credential chain.
        Supports AWS profiles, SSO, environment variables, and IAM roles.

        Raises:
            AWSError: If no valid credentials are found
        """
        try:
            self.session = boto3.Session()

            credentials = self.session.get_credentials()
            if credentials is None:
                raise AWSError(
                    message="No AWS credentials found", operation="_load_credentials", service="iam"
                )

            frozen_creds = credentials.get_frozen_credentials()
            self.aws_access_key = frozen_creds.access_key
            self.aws_secret_key = frozen_creds.secret_key
            self.aws_session_token = frozen_creds.token

            # Check for ECS container metadata to determine if running in AWS
            self.use_explicit_credentials = not bool(os.getenv("ECS_CONTAINER_METADATA_URI"))

            self._logger.debug(
                "AWS credentials loaded successfully",
                extra_fields={
                    "has_session_token": bool(self.aws_session_token),
                    "use_explicit_credentials": self.use_explicit_credentials,
                },
            )

        except AWSError:
            raise
        except Exception as e:
            hint = ""
            err_text = str(e)
            if "config profile" in err_text and "could not be found" in err_text:
                hint = (
                    " Hint: AWS_PROFILE is set but profile files are not available in the runtime. "
                    "If running in Docker, mount $HOME/.aws:/root/.aws:ro (and run aws sso login first "
                    "for SSO profiles), or provide AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY directly."
                )
            message = f"Failed to load AWS credentials: {e!s}"
            if hint:
                message = f"{message}. {hint.strip()}"
            raise AWSError(
                message=message,
                operation="_load_credentials",
                service="iam",
                original_error=e,
            ) from e

    def get_credentials(self) -> Dict[str, Any]:
        """
        Get the current AWS credentials configuration.

        Returns:
            Dict containing credential information and configuration
        """
        return {
            "use_explicit_credentials": self.use_explicit_credentials,
            "aws_access_key": self.aws_access_key if self.use_explicit_credentials else None,
            "aws_secret_key": self.aws_secret_key if self.use_explicit_credentials else None,
            "aws_session_token": self.aws_session_token if self.use_explicit_credentials else None,
            "aws_region": self.session.region_name or "us-east-1",
        }

    @property
    def region(self) -> str:
        """Get the configured AWS region."""
        return self.session.region_name or "us-east-1"
