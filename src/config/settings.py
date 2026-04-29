"""
Configuration management for the data pipeline.
Uses pydantic-settings for type-safe configuration handling with environment variables.
"""

from pathlib import Path
from typing import Dict, Optional, Set

from databricks.sdk import WorkspaceClient
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from src.config.config_loader import ConfigLoader
from src.config.config_models import PipelineConfig
from src.config.repository_root import repository_root
from src.utils.exceptions import ConfigError
from src.utils.logger import get_logger

# Initialize logger
logger = get_logger(__name__)


def _resolve_pipeline_config_dir(config_path_setting: str) -> Path:
    """
    Resolve the pipeline data directory (defaults.yml, sources/, queries/).

    Absolute paths are used as-is. Relative paths are resolved under
    ``<repo_root>/config/``.
    """
    raw = Path(config_path_setting)
    if raw.is_absolute():
        return raw.resolve()
    return (repository_root() / "config" / raw).resolve()


class SparkSettings(BaseSettings):
    """Spark-specific settings."""

    model_config = SettingsConfigDict(extra="ignore")

    APP_NAME: str = "DataIngestion"
    MASTER: str = "local[*]"
    DRIVER_MEMORY: str = "4g"
    MEMORY_FRACTION: float = 0.8
    MEMORY_STORAGE_FRACTION: float = 0.3


class APISettings(BaseSettings):
    """API-specific settings."""

    model_config = SettingsConfigDict(extra="ignore")

    TIMEOUT: int = Field(default=30, description="Request timeout in seconds")

    # These will be overridden by values from defaults.yml
    MAX_RETRIES: int = Field(default=3, description="Maximum number of retry attempts")
    INITIAL_DELAY: float = Field(
        default=1.0, description="Initial delay between retries in seconds"
    )
    RETRY_BACKOFF: float = Field(default=2.0, description="Multiplicative factor for retry delay")
    HONOR_RETRY_AFTER_HEADER: bool = Field(
        default=True,
        description="Honor Retry-After on urllib3 transport retries (429/503/413).",
    )
    MAX_RETRY_AFTER_SECONDS: int = Field(
        default=21600,
        description="Upper bound for parsed Retry-After header (seconds).",
    )
    MAX_BACKOFF_SECONDS: float = Field(
        default=120.0,
        description="Maximum exponential backoff sleep between transport retries (seconds).",
    )
    BACKOFF_JITTER_SECONDS: float = Field(
        default=0.0,
        description="Maximum random extra seconds on exponential backoff (urllib3 backoff_jitter).",
    )

    def update_from_config(self, config: PipelineConfig) -> None:
        """
        Update settings from pipeline configuration.

        Args:
            config: Pipeline configuration containing retry settings
        """
        if config.defaults and config.defaults.retry:
            r = config.defaults.retry
            self.MAX_RETRIES = r.max_attempts
            self.INITIAL_DELAY = r.initial_delay
            self.RETRY_BACKOFF = r.backoff_factor
            self.HONOR_RETRY_AFTER_HEADER = r.honor_retry_after_header
            self.MAX_RETRY_AFTER_SECONDS = r.max_retry_after_seconds
            self.MAX_BACKOFF_SECONDS = r.max_backoff_seconds
            self.BACKOFF_JITTER_SECONDS = r.backoff_jitter_seconds


class DatabricksSettings(BaseSettings):
    """Databricks-specific settings."""

    model_config = SettingsConfigDict(env_prefix="DATABRICKS_", extra="ignore")

    HOST: Optional[str] = Field(default="", description="Databricks workspace URL")
    CLIENT_ID: Optional[str] = Field(default="", description="Databricks client ID")
    CLIENT_SECRET: Optional[str] = Field(default="", description="Databricks client secret")
    WAREHOUSE_ID: Optional[str] = Field(default="", description="Databricks SQL warehouse ID")

    def initialize_databricks_workspace_client(self) -> WorkspaceClient:
        """Initialize Databricks SQL client."""
        try:
            if not self.HOST or not self.CLIENT_ID or not self.CLIENT_SECRET:
                raise ValueError(
                    "DATABRICKS_HOST, DATABRICKS_CLIENT_ID, and DATABRICKS_CLIENT_SECRET must be provided"
                )

            return WorkspaceClient(
                host=self.HOST,
                client_id=self.CLIENT_ID,
                client_secret=self.CLIENT_SECRET,
            )
        except Exception as e:
            logger.error(
                "Failed to initialize Databricks workspace client",
                extra_fields={"error": str(e)},
            )
            raise ConfigError(
                message="Failed to initialize Databricks workspace client",
                operation="initialize_databricks_workspace_client",
                details={},
                original_error=e,
            ) from e

    def get_warehouse_id(self) -> str:
        """Get the Databricks SQL warehouse ID."""
        if not self.WAREHOUSE_ID:
            raise ValueError("DATABRICKS_WAREHOUSE_ID must be provided")
        return self.WAREHOUSE_ID


class Settings(BaseSettings):
    """Main settings class that combines all other settings."""

    # Pipeline settings
    CONFIG_PATH: str = Field(
        default=".",
        description=(
            "Pipeline config directory: absolute path, or path relative to <repo_root>/config/ "
            "(e.g. '.' for config/, or 'staging' for config/staging/). Repository root is the "
            "directory that contains both `config/` and `src/`."
        ),
    )
    LOG_LEVEL: str = Field(
        default="INFO", description="Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)"
    )

    # Nested settings
    spark: SparkSettings = Field(default_factory=SparkSettings)
    api: APISettings = Field(default_factory=APISettings)
    databricks_settings: DatabricksSettings = Field(default_factory=DatabricksSettings)

    # Pipeline configuration - will be loaded once during initialization
    _pipeline_config: Optional[PipelineConfig] = None

    def __init__(self, selection: Optional[Dict[str, Optional[Set[str]]]] = None, **kwargs):
        """
        Initialize settings and load configuration.

        Args:
            selection: Optional selection structure mapping source names to resource name sets.
                       None means all resources, Set[str] means specific resource names.
            **kwargs: Additional settings
        """
        super().__init__(**kwargs)
        if not self._pipeline_config:
            self._load_config(selection=selection)

            if self._pipeline_config:
                # Update API settings from configuration
                self.api.update_from_config(self._pipeline_config)

    @property
    def pipeline_config(self) -> PipelineConfig:
        """
        Get the pipeline configuration.
        Configuration is loaded and validated only once.

        Returns:
            PipelineConfig: Validated pipeline configuration
        """
        return self._pipeline_config

    @property
    def loading_destinations(self) -> Set[str]:
        """Effective loading destinations used by the loaded/selected pipeline config."""
        if not self._pipeline_config:
            return set()
        return self._pipeline_config.get_effective_loading_destinations()

    def _load_config(self, selection: Optional[Dict[str, Optional[Set[str]]]] = None) -> None:
        """
        Load and validate the pipeline configuration.
        This is called only once during initialization.

        Args:
            selection: Optional selection structure mapping source names to resource name sets.
                       None means all resources, Set[str] means specific resource names.

        Raises:
            ConfigError: If configuration loading or validation fails
        """
        config_path = None
        try:
            config_path = _resolve_pipeline_config_dir(self.CONFIG_PATH)

            config_loader = ConfigLoader()
            self._pipeline_config = config_loader.load_config(config_path, selection=selection)

        except ConfigError as e:
            logger.error("Failed to load pipeline configuration", extra_fields={"error": str(e)})
            raise
        except Exception as e:
            logger.error(
                "Unexpected error loading pipeline configuration", extra_fields={"error": str(e)}
            )
            raise ConfigError(
                message="Failed to load pipeline configuration",
                operation="_load_config",
                details={"config_path": str(config_path)},
                original_error=e,
            ) from e


_settings_cache = {}


def get_settings(selection: Optional[Dict[str, Optional[Set[str]]]] = None) -> Settings:
    """
    Get the settings instance.
    Caches instances based on selection.

    Args:
        selection: Optional selection structure mapping source names to resource name sets.
                   None means all resources, Set[str] means specific resource names.

    Returns:
        Settings: The settings instance
    """
    # Create cache key from selection
    # Convert sets to sorted tuples for hashability
    if selection:
        cache_key = tuple(
            (source, tuple(sorted(names)) if names else None)
            for source, names in sorted(selection.items())
        )
    else:
        cache_key = None

    if cache_key not in _settings_cache:
        _settings_cache[cache_key] = Settings(selection=selection)

    return _settings_cache[cache_key]
