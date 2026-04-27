"""
Base loader class providing common loading functionality.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List

from src.utils.logger import get_logger


class BaseLoader(ABC):
    """Abstract base class for data loaders."""

    def __init__(self):
        self.logger = get_logger(self.__class__.__name__)

    @abstractmethod
    def load(self, data: List[Dict[str, Any]], destination: str, **kwargs) -> None:
        """Load data into the specified destination."""
        pass
