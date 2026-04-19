"""
Configuration loader for data pipeline.
Handles YAML loading, environment variable substitution, and validation.
"""

from pathlib import Path
from typing import Any, Dict, Optional, Set, Union

import yaml
from pydantic import ValidationError

from src.config.config_models import PipelineConfig
from src.utils.env_manager import resolve_env_var
from src.utils.exceptions import ConfigError
from src.utils.logger import get_logger


class ConfigLoader:
    """
    Loads and validates pipeline configuration from YAML files.
    """

    def __init__(self):
        """Initialize the configuration loader."""
        self.logger = get_logger(self.__class__.__name__)

    def load_config(
        self, config_path: Path, selection: Optional[Dict[str, Optional[Set[str]]]] = None
    ) -> PipelineConfig:
        """
        Load and validate configuration from a config directory.

        Expects defaults.yml and sources/*.yml in the given directory.

        Args:
            config_path: Path to the config directory (containing defaults.yml and sources/)
            selection: Optional selection structure mapping source names to resource name sets.
                       If provided, only these sources will be loaded and validated.

        Returns:
            PipelineConfig: Validated configuration object

        Raises:
            ConfigError: If loading or validation fails
        """
        try:
            config_path = Path(config_path)

            if not config_path.exists():
                raise ConfigError(
                    message=f"Configuration directory not found: {config_path}",
                    operation="load_config",
                )

            if not config_path.is_dir():
                raise ConfigError(
                    message=f"Configuration path must be a directory: {config_path}",
                    operation="load_config",
                )

            raw_config, path_for_disk = self._load_config_from_directory(config_path)

            # Filter sources if selection is provided
            if selection:
                sources = raw_config.get("sources", {})
                # Extract source list from selection keys
                selected_source_list = list(selection.keys())
                filtered_sources = {
                    name: config for name, config in sources.items() if name in selected_source_list
                }

                # Warn about missing sources
                missing_sources = set(selected_source_list) - set(sources.keys())
                if missing_sources:
                    self.logger.warning(
                        "Selected sources not found in configuration",
                        extra_fields={
                            "missing_sources": list(missing_sources),
                            "available_sources": list(sources.keys()),
                        },
                    )

                raw_config["sources"] = filtered_sources
                self.logger.debug(
                    "Filtered sources",
                    extra_fields={
                        "selected": selected_source_list,
                        "filtered_count": len(filtered_sources),
                    },
                )

            # Resolve relative paths in disk_config (relative to config file/directory)
            raw_config = self._resolve_disk_config_paths(raw_config, path_for_disk)

            # Resolve relative local loading storage_root against operator config directory
            raw_config = self._resolve_local_loading_storage_paths(
                raw_config, config_path.resolve()
            )

            # Process configuration
            processed_config = self._process_config(raw_config)
            processed_config["config_root"] = config_path.resolve()

            # Validate with Pydantic
            try:
                config = PipelineConfig(**processed_config)
            except ValidationError as e:
                raise ConfigError(f"Invalid configuration: {e!s}") from e

            self.logger.info(
                "Successfully loaded configuration",
                extra_fields={"version": config.version, "sources": list(config.sources.keys())},
            )

            return config

        except yaml.YAMLError as e:
            raise ConfigError(
                message="Failed to parse YAML configuration",
                operation="load_config",
                details={"config_path": str(config_path)},
                original_error=e,
            ) from e
        except ValidationError as e:
            raise ConfigError(
                message="Invalid configuration format",
                operation="load_config",
                details={"config_path": str(config_path), "validation_errors": e.errors()},
                original_error=e,
            ) from e
        except Exception as e:
            if isinstance(e, ConfigError):
                raise
            raise ConfigError(
                message="Unexpected error loading configuration",
                operation="load_config",
                details={"config_path": str(config_path)},
                original_error=e,
            ) from e

    def _load_config_from_directory(self, config_dir: Path) -> tuple[Dict[str, Any], Path]:
        """
        Load configuration from a directory with defaults.yml and sources/*.yml.

        Args:
            config_dir: Path to the config directory (containing defaults.yml and sources/)

        Returns:
            Tuple of (merged raw config dict, path to use for disk_config resolution)
        """
        defaults_path = config_dir / "defaults.yml"
        if not defaults_path.exists():
            raise ConfigError(
                message=f"defaults.yml not found in config directory: {config_dir}",
                operation="load_config",
            )

        with open(defaults_path) as f:
            defaults_config = yaml.safe_load(f)

        if not isinstance(defaults_config, dict):
            raise ConfigError(
                message="defaults.yml must contain a YAML object",
                operation="load_config",
                details={"path": str(defaults_path)},
            )

        self.logger.debug(
            "Loaded defaults.yml",
            extra_fields={
                "path": str(defaults_path.resolve()),
                "config_dir": str(config_dir.resolve()),
            },
        )

        sources_dir = config_dir / "sources"
        sources: Dict[str, Any] = {}
        if sources_dir.exists() and sources_dir.is_dir():
            for source_file in sorted(sources_dir.glob("*.yml")):
                source_name = source_file.stem
                try:
                    with open(source_file) as f:
                        source_content = yaml.safe_load(f)
                except yaml.YAMLError as e:
                    raise ConfigError(
                        message=f"Failed to parse source YAML: {source_file.name}",
                        operation="load_config",
                        details={"path": str(source_file)},
                        original_error=e,
                    ) from e
                self._validate_source_file(source_content, source_file, source_name)
                sources[source_name] = source_content

        self.logger.debug(
            "Scanned sources/*.yml for pipeline source configs",
            extra_fields={
                "config_dir": str(config_dir.resolve()),
                "valid_source_config_count": len(sources),
                "source_names": sorted(sources.keys()),
            },
        )
        if not sources:
            self.logger.warning(
                "No valid source YAML files under sources/*.yml (defaults.yml is present but no loadable sources)",
                extra_fields={
                    "config_dir": str(config_dir.resolve()),
                    "sources_dir": str(sources_dir),
                },
            )

        raw_config = {
            "version": defaults_config.get("version", "1.0"),
            "defaults": defaults_config.get("defaults", {}),
            "queries": defaults_config.get("queries", []),
            "sources": sources,
        }
        return raw_config, defaults_path

    def _validate_source_file(self, content: Any, source_file: Path, source_name: str) -> None:
        """Ensure source file is a single-source dict with only allowed keys and required type/resources."""
        allowed_keys = {
            "enabled",
            "type",
            "base_url",
            "sdk",
            "auth",
            "headers",
            "resources",
            # Relational database sources (postgresql / hana)
            "host",
            "port",
            "username",
            "password",
            "database",
            "connection_params",
        }
        details = {"path": str(source_file), "source": source_name}
        if not isinstance(content, dict) or not content:
            raise ConfigError(
                message=f"Source file must be a non-empty YAML object: {source_file.name}",
                operation="load_config",
                details=details,
            )
        keys = set(content.keys())
        bad_keys = keys - allowed_keys
        if bad_keys:
            raise ConfigError(
                message=f"Source file has invalid top-level keys (only single-source format allowed): {sorted(bad_keys)} in {source_file.name}",
                operation="load_config",
                details={**details, "invalid_keys": list(bad_keys)},
            )
        missing = [k for k in ("type", "resources") if k not in content]
        if missing:
            raise ConfigError(
                message=f"Source file missing required keys: {missing} in {source_file.name}",
                operation="load_config",
                details={**details, "missing": missing},
            )

    def _process_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process configuration with variable substitution.

        Args:
            config: Raw configuration dictionary

        Returns:
            Dict[str, Any]: Processed configuration

        Raises:
            ConfigError: If a required environment variable is not found
        """
        try:
            return self._traverse_dict(config, resolve_env_var)
        except ConfigError as e:
            self.logger.error("Failed to process configuration", extra_fields={"error": str(e)})
            raise

    def _resolve_disk_config_paths(
        self, config: Dict[str, Any], config_path: Path
    ) -> Dict[str, Any]:
        """
        Resolve relative paths in disk_config to be relative to the config file location.

        Args:
            config: Configuration dictionary
            config_path: Path to the configuration file

        Returns:
            Dict[str, Any]: Configuration with resolved disk_config paths
        """
        config_dir = config_path.parent.resolve()

        # Resolve disk_config path in defaults
        if "defaults" in config and isinstance(config["defaults"], dict):
            if "streaming" in config["defaults"] and isinstance(
                config["defaults"]["streaming"], dict
            ):
                if "disk_config" in config["defaults"]["streaming"] and isinstance(
                    config["defaults"]["streaming"]["disk_config"], dict
                ):
                    disk_config = config["defaults"]["streaming"]["disk_config"]
                    if "path" in disk_config and isinstance(disk_config["path"], str):
                        path = disk_config["path"]
                        # Only resolve if it's a relative path
                        if not Path(path).is_absolute():
                            disk_config["path"] = str(config_dir / path)

        return config

    def _resolve_local_loading_storage_paths(
        self, config: Dict[str, Any], config_root: Path
    ) -> Dict[str, Any]:
        """
        Resolve relative ``storage_root`` for ``destination: local`` loading configs.

        Anchors to the operator config directory (same as ``PipelineConfig.config_root`` /
        ``CONFIG_PATH``), not the process working directory.
        """
        root = config_root.resolve()

        def resolve_loading_dict(loading: Dict[str, Any]) -> None:
            if loading.get("destination") != "local":
                return
            sr = loading.get("storage_root")
            if not isinstance(sr, str):
                return
            p = Path(sr).expanduser()
            if p.is_absolute():
                return
            loading["storage_root"] = str((root / p).resolve())

        defaults = config.get("defaults")
        if isinstance(defaults, dict):
            loading = defaults.get("loading")
            if isinstance(loading, dict):
                resolve_loading_dict(loading)

        sources = config.get("sources", {})
        if isinstance(sources, dict):
            for _sn, source in sources.items():
                if not isinstance(source, dict):
                    continue
                resources = source.get("resources", {})
                if not isinstance(resources, dict):
                    continue
                for _rn, resource in resources.items():
                    if not isinstance(resource, dict):
                        continue
                    loading = resource.get("loading")
                    if isinstance(loading, dict):
                        resolve_loading_dict(loading)

        return config

    def _traverse_dict(
        self, obj: Union[Dict, list, Any], callback: callable
    ) -> Union[Dict, list, Any]:
        """
        Recursively traverse dictionary and apply callback to values.

        Args:
            obj: Object to traverse
            callback: Function to apply to values

        Returns:
            Union[Dict, list, Any]: Processed object
        """
        if isinstance(obj, dict):
            return {key: self._traverse_dict(value, callback) for key, value in obj.items()}
        elif isinstance(obj, list):
            return [self._traverse_dict(item, callback) for item in obj]
        else:
            return callback(obj)
