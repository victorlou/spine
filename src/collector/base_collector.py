"""
Base collector classes for data accumulation.
"""

from typing import Any, Dict, List, Optional, Union


class RawDataBatch:
    """Container for raw data and its associated context."""

    def __init__(
        self,
        raw_data: Union[List[Dict[str, Any]], Dict[str, Any]],
        parent_context: Optional[Dict[str, Any]] = None,
        request_context: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize raw data batch.

        Args:
            raw_data: Raw fetched records from the source
            parent_context: Context from parent resource (for dependent resources)
            request_context: Context from the request (parameters, body, etc.)
        """
        self.raw_data = raw_data
        self.parent_context = parent_context or {}
        self.request_context = request_context or {}


class RawDataCollector:
    """Collects raw data batches for centralized parsing."""

    def __init__(self):
        self.batches: List[RawDataBatch] = []

    def add_batch(self, batch: RawDataBatch) -> None:
        self.batches.append(batch)

    def is_empty(self) -> bool:
        return len(self.batches) == 0
