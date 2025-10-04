from abc import ABC, abstractmethod
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional, TYPE_CHECKING
from ....data.agent_schemas import DryRunResult

import structlog

if TYPE_CHECKING:
    from cx_core_schemas.connection import Connection
    from cx_core_schemas.vfs import VfsFileContentResponse

logger = structlog.get_logger(__name__)


class BaseConnectorStrategy(ABC):
    """The abstract "contract" for all connection strategies."""

    strategy_key: Optional[str] = None

    def __init__(self, **kwargs):
        pass

    @abstractmethod
    async def test_connection(
        self, connection: "Connection", secrets: Dict[str, Any]
    ) -> bool:
        raise NotImplementedError

    @abstractmethod
    @asynccontextmanager
    async def get_client(self, connection: "Connection", secrets: Dict[str, Any]):
        yield
        raise NotImplementedError

    @abstractmethod
    async def browse_path(
        self, path_parts: List[str], connection: "Connection", secrets: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    async def get_content(
        self, path_parts: List[str], connection: "Connection", secrets: Dict[str, Any]
    ) -> "VfsFileContentResponse":
        raise NotImplementedError

    async def run_declarative_action(
        self,
        connection: "Connection",
        secrets: Dict[str, Any],
        action_params: Dict[str, Any],
        script_input: Dict[str, Any],
        debug_mode: bool = False,
    ) -> Dict[str, Any]:
        """
        (Optional) Executes a generic, templated action defined in a blueprint.

        This method is the cornerstone of the declarative action system, allowing
        strategies to perform complex operations (like sending an email) without
        requiring a new, dedicated strategy class.
        """
        raise NotImplementedError(
            f"The '{self.strategy_key}' strategy does not support 'run_declarative_action'."
        )

    @abstractmethod
    async def dry_run(
        self,
        connection: "Connection",
        secrets: Dict[str, Any],
        action_params: Dict[str, Any],
    ) -> "DryRunResult":
        """
        Simulates the execution of an action to predict its outcome without
        making state-altering or expensive calls.

        Returns:
            A DryRunResult object summarizing the predicted outcome.
        """
        # Default implementation for strategies that don't support dry run yet.
        return DryRunResult(
            indicates_failure=False,
            message="Dry run not implemented for this action type. Assuming success.",
        )
