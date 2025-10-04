from abc import ABC, abstractmethod
from typing import Any, List, Dict
import pandas as pd


class BaseTransformEngine(ABC):
    """The contract for all transformation engines."""

    engine_name: str = "base"

    @abstractmethod
    async def transform(
        self,
        data: pd.DataFrame,
        operations: List[Dict[str, Any]],
        context: Dict[str, Any],
    ) -> pd.DataFrame:
        """Applies a list of declarative operations to the input DataFrame."""
        raise NotImplementedError
