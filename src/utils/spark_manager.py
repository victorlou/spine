"""
Spark session management utility.

Uses :class:`src.config.config_spark.SparkSessionConf` for builder settings and
:class:`src.config.spark_runtime.resolve_spark_runtime` when callers do not inject a resolved runtime.
"""

import atexit
from typing import Optional, Set

from pyspark.sql import SparkSession

from src.config.config_spark import SparkSessionConf
from src.config.settings import get_settings
from src.config.spark_runtime import ResolvedSparkRuntime, resolve_spark_runtime
from src.utils.aws_credentials import AWSCredentialManager
from src.utils.exceptions import AWSError, SparkError
from src.utils.logger import get_logger


class SparkManager:
    """
    Singleton manager for Spark session.
    Ensures only one Spark session is created and properly managed.

    Multi-pipeline or multi-tenant use in one process is not supported without resetting
    ``_instance`` and ``_spark`` (see tests).
    """

    _instance = None
    _spark: Optional[SparkSession] = None

    def __new__(cls):
        """Ensure singleton instance."""
        if cls._instance is None:
            cls._instance = super(SparkManager, cls).__new__(cls)
            cls._instance._logger = get_logger(cls.__name__)
        return cls._instance

    def _load_credentials(self) -> None:
        """
        Load AWS credentials using the AWSCredentialManager.

        Raises:
            SparkError: If credentials cannot be loaded
        """
        try:
            cred_manager = AWSCredentialManager()
            credentials = cred_manager.get_credentials()

            self.aws_access_key = credentials["aws_access_key"]
            self.aws_secret_key = credentials["aws_secret_key"]
            self.aws_session_token = credentials.get("aws_session_token")
            self.aws_region = credentials["aws_region"]
            self.use_explicit_credentials = credentials["use_explicit_credentials"]

            self._logger.debug(
                "AWS credentials loaded successfully",
                extra_fields={
                    "use_explicit_credentials": self.use_explicit_credentials,
                    "has_session_token": bool(self.aws_session_token),
                },
            )

        except AWSError as e:
            error_msg = f"Failed to load AWS credentials: {e!s}"
            self._logger.error(error_msg)
            raise SparkError(
                message=error_msg, operation="_load_credentials", original_error=e
            ) from e

    def _resolve_spark_runtime(
        self, spark_runtime: Optional[ResolvedSparkRuntime]
    ) -> ResolvedSparkRuntime:
        if spark_runtime is not None:
            return spark_runtime
        return resolve_spark_runtime(get_settings().pipeline_config.defaults.spark_runtime)

    def init_session(
        self,
        destinations: Optional[Set[str]] = None,
        spark_runtime: Optional[ResolvedSparkRuntime] = None,
    ) -> SparkSession:
        """
        Initialize or get an existing Spark session.

        Loads AWS credentials only when ``s3`` is in the destination set (for explicit-key paths or
        region hints); otherwise skips credential initialization.

        Returns:
            SparkSession: The initialized or existing Spark session

        Raises:
            SparkError: If session initialization fails
        """
        if self._spark is None:
            try:
                self._logger.debug("Initializing new Spark session")
                destinations = destinations or {"local"}
                resolved = self._resolve_spark_runtime(spark_runtime)
                self._logger.debug(resolved.summary_for_log())

                self.aws_access_key = ""
                self.aws_secret_key = ""
                self.aws_session_token = None
                self.aws_region = ""
                self.use_explicit_credentials = False
                if "s3" in destinations:
                    # Fail fast: a missing AWS credential chain when S3 is a
                    # configured destination must stop the pipeline before
                    # ingestion. The unified destination preflight then probes
                    # the actual bucket reachability for s3/gcs/azure_blob.
                    self._load_credentials()

                SparkSessionConf.get_java_options(destinations, resolved)
                self._logger.info(
                    SparkSessionConf.startup_summary(
                        destinations=destinations,
                        use_explicit_credentials=self.use_explicit_credentials,
                        resolved=resolved,
                    )
                )

                configs = SparkSessionConf.get_configs_for_destinations(
                    destinations=destinations,
                    use_explicit_credentials=self.use_explicit_credentials,
                    aws_access_key=self.aws_access_key,
                    aws_secret_key=self.aws_secret_key,
                    aws_region=self.aws_region,
                    aws_session_token=self.aws_session_token,
                    resolved=resolved,
                )

                builder = SparkSession.builder
                for key, value in configs.items():
                    builder = builder.config(key, value)

                self._spark = builder.getOrCreate()

                atexit.register(self.stop_session)

                self._logger.debug("Spark session initialized successfully")

            except Exception as e:
                error_msg = f"Failed to initialize Spark session: {e!s}"
                self._logger.error(error_msg)
                raise SparkError(
                    message=error_msg, operation="init_session", original_error=e
                ) from e

        return self._spark

    def get_session(self) -> Optional[SparkSession]:
        """
        Get the current Spark session if it exists.

        Returns:
            Optional[SparkSession]: The current session or None
        """
        return self._spark

    def stop_session(self) -> None:
        """Stop the Spark session if it exists."""
        if self._spark is not None:
            self._logger.trace("Stopping Spark session")
            self._spark.stop()
            self._spark = None
            self._logger.debug("Successfully stopped Spark session")

    def get_s3_path(self, bucket: str, key: str) -> str:
        """
        Get the full S3 path for a bucket and key.

        Args:
            bucket: S3 bucket name
            key: S3 key/path

        Returns:
            str: Full S3 path
        """
        return f"s3a://{bucket}/{key}"

    def __del__(self):
        """Ensure Spark session is stopped on deletion."""
        self.stop_session()
