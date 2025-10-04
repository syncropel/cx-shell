# ~/repositories/cx-shell/src/cx_shell/management/renderers/base.py
from abc import ABC, abstractmethod
from typing import Any, Dict

from cx_core_schemas.notebook import ContextualPage


class BaseRenderer(ABC):
    """
    The abstract contract for all Publisher renderers.
    """

    renderer_key: str

    @abstractmethod
    async def render(
        self, page: ContextualPage, results: Dict[str, Any]
    ) -> str | bytes:
        """
        Takes a parsed Contextual Page and its execution results and renders
        them into a final, static artifact.

        Args:
            page: The parsed ContextualPage Pydantic model.
            results: A dictionary mapping block IDs to their output data.

        Returns:
            The rendered artifact as a string (for text-based formats) or
            bytes (for binary formats like PDF).
        """
        raise NotImplementedError
