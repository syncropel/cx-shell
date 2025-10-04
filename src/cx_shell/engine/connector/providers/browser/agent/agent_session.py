from typing import Literal

from playwright.async_api import Page
import structlog

from .action_executor import ActionExecutor
from .locator_resolver import LocatorResolver
from .models import CommandInfo
from .wait_handler import WaitHandler
from ......utils import get_assets_root
# Removed imports for EventBus and API models

logger = structlog.get_logger(__name__)


class AgentSession:
    """
    Manages the state and actions for a single browser page session,
    orchestrating the locator, executor, and wait handler.
    Observability is now handled via structured logging.
    """

    def __init__(self):
        self.page: Page | None = None
        self.locator_resolver: LocatorResolver | None = None
        self.action_executor: ActionExecutor | None = None
        self.wait_handler: WaitHandler | None = None
        self.annotation_script_content: str | None = None
        self._load_annotation_script()
        # The screenshot_on_failure setting is now a simple attribute
        self.screenshot_on_failure = True

    def _load_annotation_script(self):
        """Loads the content of annotations.js."""
        try:
            # Correctly resolve the path relative to this file
            # Use the robust helper to find the assets directory correctly.
            assets_root = get_assets_root()
            script_path = assets_root / "system-lib/browser/annotations.js"
            if not script_path.exists():
                raise FileNotFoundError(f"Annotation script not found at {script_path}")
            with open(script_path, encoding="utf-8") as f:
                self.annotation_script_content = f.read()
            logger.info("Annotation script loaded into AgentSession.")
        except Exception as e:
            logger.error("Failed to load annotation script!", error=str(e))
            raise RuntimeError("Could not load annotations.js for agent.") from e

    async def initialize(self, page: Page, default_timeout: int = 30000):
        """Receives the active Page and initializes all helpers."""
        self.page = page
        self.locator_resolver = LocatorResolver(page)
        self.action_executor = ActionExecutor(page, default_timeout)
        self.wait_handler = WaitHandler(page, default_timeout)
        await self._inject_js_scripts()
        logger.info("AgentSession initialized with page and all helpers.")

    async def _inject_js_scripts(self):
        """Injects necessary JavaScript into the page."""
        if not self.page or not self.annotation_script_content:
            raise RuntimeError("Cannot inject JS, page or script not available.")
        await self.page.add_init_script(self.annotation_script_content)
        logger.debug("Annotation script injected via add_init_script.")

    async def take_screenshot(
        self, reason: Literal["on_failure"], step_index: int
    ) -> bytes | None:
        """Takes a screenshot and returns the raw bytes."""
        if not self.page or self.page.is_closed():
            return None
        try:
            screenshot_bytes = await self.page.screenshot(timeout=5000)
            logger.info("Screenshot taken.", reason=reason, step=step_index)
            return screenshot_bytes
        except Exception as e:
            logger.warning("Failed to take screenshot.", reason=reason, error=str(e))
            return None

    async def execute_action(self, command_info: CommandInfo, step_index: int):
        """
        A centralized method to perform any browser action. It handles
        pre- and post-action observability tasks like taking screenshots.
        """
        if (
            not self.page
            or not self.locator_resolver
            or not self.action_executor
            or not self.wait_handler
        ):
            raise RuntimeError("AgentSession is not fully initialized.")

        action = command_info.get("command_type")
        target = (
            command_info.get("element_info", {}).get("locators", {}).get("css_selector")
        )

        logger.info(
            "Executing agent action", action=action, target=target, step=step_index
        )

        try:
            if action == "navigate":
                url = command_info.get("text")
                if not url:
                    raise ValueError(
                        "Navigate action requires a URL in the 'text' field."
                    )
                await self.wait_handler.wait_for_navigation(
                    trigger_action=lambda: self.page.goto(url, wait_until="commit"),
                    wait_until="load",
                )
            elif action == "get_local_storage":
                return await self.page.evaluate("() => ({...localStorage})")
            elif action == "get_html":
                return await self.page.content()
            else:
                # All other actions are assumed to be element-based
                locator = await self.locator_resolver.find_locator(
                    command_info, step_index
                )
                if not locator:
                    raise RuntimeError(
                        "Locator resolution failed and could not be healed."
                    )
                await self.action_executor.execute_action(
                    locator, command_info, step_index
                )

            # Successful actions can return a confirmation
            return {"status": "success"}

        except Exception as e:
            if self.screenshot_on_failure:
                await self.take_screenshot("on_failure", step_index)
            # Re-raise the exception to be handled by the ScriptEngine
            raise e
