"""
Base loader class providing common loading functionality.
"""

import uuid
from abc import ABC, abstractmethod
from datetime import UTC, datetime
from typing import Any, Dict, List, Optional

from src.utils.datetime_utils import format_datetime
from src.utils.exceptions import LoaderError
from src.utils.logger import get_logger


class BaseLoader(ABC):
    """
    Abstract base class for data loaders.
    Provides common functionality for loading data into destinations.
    """

    def __init__(self):
        """Initialize the loader."""
        self.logger = get_logger(self.__class__.__name__)

    @abstractmethod
    def load(self, data: List[Dict[str, Any]], destination: str, **kwargs) -> None:
        """
        Load data into the specified destination.
        Must be implemented by concrete classes.

        Args:
            data: List of records to load
            destination: Destination to load data into
            **kwargs: Additional arguments for the loader

        Raises:
            LoaderError: If loading fails
        """
        pass

    def _generate_destination_key(
        self,
        prefix: str,
        extension: str = "",
        include_timestamp: bool = True,
        include_uuid: bool = False,
    ) -> str:
        """
        Generate a unique destination key with UTC timestamp and optional UUID.

        Args:
            prefix: The prefix for the key
            extension: Optional file extension (with dot)
            include_timestamp: Whether to include timestamp in the key
            include_uuid: Whether to include a UUID in the key

        Returns:
            str: Generated key in the format prefix/YYYY-MM-DDThh-mm-ssZ[_uuid][.extension]
        """
        # Clean prefix
        clean_prefix = self._clean_path(prefix or "data")

        # Generate components
        components = [clean_prefix]

        if include_timestamp:
            timestamp = datetime.now(UTC).strftime("%Y-%m-%dT%H-%M-%SZ")
            components.append(timestamp)

        if include_uuid:
            unique_id = str(uuid.uuid4())
            components.append(unique_id)

        # Join components
        key = "/".join([components[0], "_".join(components[1:])])

        # Add extension if provided
        if extension:
            if not extension.startswith("."):
                extension = f".{extension}"
            key = f"{key}{extension}"

        return key

    def _clean_path(self, path: str) -> str:
        """
        Clean a path by removing leading/trailing slashes and spaces.

        Args:
            path: Path to clean

        Returns:
            str: Cleaned path
        """
        return path.strip("/").strip()

    def validate_data(
        self, data: List[Dict[str, Any]], required_fields: Optional[List[str]] = None
    ) -> None:
        """
        Validate data before loading.

        Args:
            data: Data to validate
            required_fields: Optional list of required fields

        Raises:
            LoaderError: If validation fails
        """
        if not data:
            raise LoaderError(message="No data provided for loading", operation="validate_data")

        if not all(isinstance(record, dict) for record in data):
            raise LoaderError(
                message="All records must be dictionaries",
                operation="validate_data",
                details={
                    "invalid_records": [
                        i for i, record in enumerate(data) if not isinstance(record, dict)
                    ]
                },
            )

        if required_fields:
            missing_fields = {}
            for i, record in enumerate(data):
                missing = [
                    field
                    for field in required_fields
                    if field not in record or record[field] is None
                ]
                if missing:
                    missing_fields[i] = missing

            if missing_fields:
                raise LoaderError(
                    message="Records missing required fields",
                    operation="validate_data",
                    details={"missing_fields": missing_fields},
                )

    def validate_destination(self, destination: str) -> None:
        """
        Validate the destination exists and is accessible.

        Args:
            destination: Destination to validate

        Raises:
            LoaderError: If destination validation fails
        """
        if not destination:
            raise LoaderError(message="No destination provided", operation="validate_destination")

    def format_timestamp(self, dt: datetime) -> str:
        """
        Format a datetime object for loading.

        Args:
            dt: Datetime to format

        Returns:
            str: Formatted datetime string

        Raises:
            LoaderError: If datetime formatting fails
        """
        try:
            return format_datetime(dt)
        except ValueError as e:
            raise LoaderError(
                message="Failed to format datetime",
                operation="format_timestamp",
                details={"datetime": str(dt)},
                original_error=e,
            ) from e
