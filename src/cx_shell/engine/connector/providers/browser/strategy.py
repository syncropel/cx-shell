from contextlib import asynccontextmanager
from typing import Dict, Any, TYPE_CHECKING
import structlog

from ..base import BaseConnectorStrategy
from .providers.browser_manager import BrowserManager
from .agent.agent_session import AgentSession
from .....data.agent_schemas import DryRunResult  # Using cx_shell's schemas now


# Placeholder for ObservabilityConfig, which we will remove later. For now, a stub.
class ObservabilityConfig:
    pass


if TYPE_CHECKING:
    from cx_core_schemas.connection import Connection
    from cx_core_schemas.vfs import VfsFileContentResponse

logger = structlog.get_logger(__name__)


class DeclarativeBrowserStrategy(BaseConnectorStrategy):
    """
    A unique, stateful strategy for managing and interacting with a web browser session.
    """

    strategy_key = "browser-declarative"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.browser_manager = None  # Will be initialized in start_session
        self.agent_session = AgentSession()
        logger.info("DeclarativeBrowserStrategy initialized.")

    async def start_session(
        self, connection: "Connection", secrets: Dict[str, Any]
    ) -> AgentSession:
        """
        Launches a browser, creates a new page, and initializes an AgentSession,
        using configuration from the connection model.
        """
        log = logger.bind(connection_id=connection.id)
        log.info("Starting new browser session...")

        browser_type = connection.details.get("browser_type", "chromium")
        headless_str = connection.details.get("headless", "true")
        headless = str(headless_str).lower() in ("true", "1", "yes")

        self.browser_manager = BrowserManager.get_provider("local")

        try:
            _browser, page = await self.browser_manager.get_browser(
                browser_type=browser_type, headless=headless
            )

            # --- THIS IS THE FIX ---
            # The initialize method now only takes the page.
            await self.agent_session.initialize(page)
            # --- END FIX ---

            log.info("Browser session started and agent is initialized.")
            return self.agent_session
        except Exception as e:
            log.error("Failed to start browser session.", error=str(e), exc_info=True)
            await self.end_session()
            raise

    async def execute_step(
        self, agent_session: AgentSession, command_info: Dict[str, Any], step_index: int
    ):
        """
        Executes a single browser action step using the active AgentSession.
        """
        action = command_info.get("command_type")
        log = logger.bind(step=step_index, action=action)
        log.info("Executing browser step.")

        try:
            await agent_session.execute_action(command_info, step_index)
            log.info("Browser step executed successfully.")
            # For now, we return a simple success message. In the future, we can return data.
            return {"status": "success", "action": action, "step": step_index}
        except Exception as e:
            log.error("Browser step failed.", error=str(e), exc_info=True)
            raise

    async def end_session(self):
        """
        Safely closes the browser and cleans up all associated resources.
        """
        logger.info("Ending browser session...")
        if self.browser_manager:
            await self.browser_manager.close()
        logger.info("Browser session ended and resources cleaned up.")

    # --- Implementation of standard BaseConnectorStrategy abstract methods ---
    async def test_connection(
        self, connection: "Connection", secrets: Dict[str, Any]
    ) -> bool:
        logger.info("Browser connection test is implicitly successful.")
        return True

    @asynccontextmanager
    async def get_client(self, connection: "Connection", secrets: Dict[str, Any]):
        raise NotImplementedError("Use start_session() for the browser strategy.")

    async def browse_path(
        self, path_parts: list[str], connection: "Connection", secrets: Dict[str, Any]
    ) -> list[Dict[str, Any]]:
        return []

    async def get_content(
        self, path_parts: list[str], connection: "Connection", secrets: Dict[str, Any]
    ) -> "VfsFileContentResponse":
        raise NotImplementedError("Browser strategy does not support get_content.")

    async def dry_run(
        self,
        connection: "Connection",
        secrets: Dict[str, Any],
        action_params: Dict[str, Any],
    ) -> "DryRunResult":
        return DryRunResult(
            indicates_failure=False,
            message="Dry run successful by default for browser strategy.",
        )
