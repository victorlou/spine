"""
Spark configuration settings.
"""

import logging
import os
from typing import Any, Dict, Optional

# Maven coordinates Spark resolves at startup (Ivy). Keep ngdbc pin aligned with smoke testing.
_HADOOP_AWS_PKG = "org.apache.hadoop:hadoop-aws:3.3.4"
_DELTA_PKG = "io.delta:delta-spark_2.12:3.1.0"
_ICEBERG_PKG = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.1"
_SAP_NGDBC_PKG = "com.sap.cloud.db.jdbc:ngdbc:2.23.10"
SPARK_JARS_PACKAGES = ",".join([_HADOOP_AWS_PKG, _DELTA_PKG, _ICEBERG_PKG, _SAP_NGDBC_PKG])

_ICEBERG_EXTENSIONS = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
_DELTA_EXTENSIONS = "io.delta.sql.DeltaSparkSessionExtension"
SPARK_EXTENSIONS = ",".join([_ICEBERG_EXTENSIONS, _DELTA_EXTENSIONS])


class ConfigSpark:
    """Configuration for Spark session."""

    @staticmethod
    def get_java_options() -> None:
        """Set Java options for local Spark."""
        # Set basic logging configuration
        os.environ["SPARK_SUBMIT_OPTS"] = "-Dlog4j.logger.org.apache.spark.repl.Main=ERROR"

        # Set Spark packages (including Delta Lake)
        # PySpark 3.5.x uses Scala 2.12, so we use delta-spark_2.12
        os.environ["PYSPARK_SUBMIT_ARGS"] = (
            f"--packages {SPARK_JARS_PACKAGES} "
            "--conf spark.ui.showConsoleProgress=false "
            "pyspark-shell"
        )

        # Suppress other Java logging
        logging.getLogger("py4j").setLevel(logging.ERROR)

    @staticmethod
    def get_local_configs(
        aws_access_key: str,
        aws_secret_key: str,
        aws_region: str = "ap-southeast-2",
        aws_session_token: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Get Spark configurations for local development.

        Args:
            aws_access_key: AWS access key for S3 access
            aws_secret_key: AWS secret key for S3 access
            aws_region: AWS region for S3 endpoint
            aws_session_token: Optional session token for temporary credentials

        Returns:
            Dict[str, Any]: Spark configuration dictionary
        """
        config = {
            # Local development settings
            "spark.master": "local[*]",
            "spark.app.name": "DataIngestion",
            "spark.driver.bindAddress": "127.0.0.1",
            "spark.driver.host": "127.0.0.1",
            "spark.ui.enabled": "false",
            # Memory configuration
            "spark.driver.memory": "4g",
            "spark.memory.fraction": "0.8",
            "spark.memory.storageFraction": "0.3",
            "spark.sql.parquet.compression.codec": "snappy",
            # Disable metrics
            "spark.metrics.enabled": "false",
            # Delta Lake configuration
            # Add Delta Lake JARs via packages (PySpark 3.5.x uses Scala 2.12)
            "spark.jars.packages": SPARK_JARS_PACKAGES,
            # Enable Delta SQL extensions
            "spark.sql.extensions": SPARK_EXTENSIONS,
            # Configure Delta catalog and Iceberg catalog
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.sql.catalog.iceberg": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.iceberg.type": "hadoop",
            # We will be needing to configure warehouse root for `iceberg` that will happen while loading
            # "spark.sql.catalog.iceberg.warehouse": "<warehouse_path>" -- if s3: s3a://<bucket-name> else if local: file://<storage-root>
            # AWS S3 configurations
            "spark.hadoop.fs.s3a.access.key": aws_access_key,
            "spark.hadoop.fs.s3a.secret.key": aws_secret_key,
            "spark.hadoop.fs.s3a.endpoint": f"s3.{aws_region}.amazonaws.com",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            # Basic logging configuration
            "spark.driver.extraJavaOptions": "-Djava.security.manager=allow",
            "spark.log.level": "ERROR",
        }

        if aws_session_token:
            config["spark.hadoop.fs.s3a.session.token"] = aws_session_token

        return config

    @staticmethod
    def get_production_configs(region: str = "ap-southeast-2") -> Dict[str, Any]:
        """Configs for use inside AWS environment (ECS, EC2, etc)."""
        return {
            "spark.app.name": "DataIngestion",
            "spark.driver.memory": "4g",
            # Delta Lake configuration
            # Add Delta Lake JARs via packages (PySpark 3.5.x uses Scala 2.12)
            "spark.jars.packages": SPARK_JARS_PACKAGES,
            # Enable Delta SQL extensions
            "spark.sql.extensions": SPARK_EXTENSIONS,
            # Configure Delta catalog and Iceberg catalog
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.sql.catalog.iceberg": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.iceberg.type": "hadoop",
            "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
            "spark.hadoop.fs.s3a.endpoint": f"s3.{region}.amazonaws.com",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.log.level": "ERROR",
        }

    @staticmethod
    def get_configs(
        use_explicit_credentials: bool,
        aws_access_key: str = "",
        aws_secret_key: str = "",
        aws_region: str = "ap-southeast-2",
        aws_session_token: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Dynamically return either local or production Spark configs.
        """
        if use_explicit_credentials:
            return ConfigSpark.get_local_configs(
                aws_access_key, aws_secret_key, aws_region, aws_session_token
            )
        else:
            return ConfigSpark.get_production_configs(aws_region)
