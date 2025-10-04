from abc import ABC, abstractmethod
from typing import Any, Dict


class BaseCacheProvider(ABC):
    """
    The abstract contract for all cache providers.
    """

    @abstractmethod
    async def write(self, content: bytes, metadata: Dict[str, Any]) -> str:
        """
        Writes content to the cache.

        Args:
            content: The raw bytes of the content to be cached.
            metadata: A dictionary containing context about the write operation,
                      including the connection, path, and timestamp.

        Returns:
            A string representing the canonical path or URI of the cached object.
        """
        raise NotImplementedError
