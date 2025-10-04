from playwright.async_api import (
    Browser,
    BrowserContext,
    Page,
    Playwright,
    async_playwright,
)
import structlog

from .base_provider import BaseBrowserProvider

logger = structlog.get_logger(__name__)


class LocalBrowserProvider(BaseBrowserProvider):
    """Launches and manages a local browser instance using Playwright."""

    def __init__(self):
        self.playwright: Playwright | None = None
        self.browser: Browser | None = None

    async def get_browser(
        self, browser_type: str, headless: bool
    ) -> tuple[Browser, Page]:
        """
        Launches a local browser based on the provided configuration.

        Args:
            browser_type: The type of browser to launch ('chromium', 'firefox', 'webkit').
            headless: Whether to run the browser in headless mode.
        """
        logger.info(
            "Initializing local browser...",
            browser_type=browser_type,
            headless=headless,
        )
        self.playwright = await async_playwright().start()

        try:
            browser_launcher = getattr(self.playwright, browser_type)
        except AttributeError:
            logger.error(f"Invalid browser_type specified: {browser_type}")
            raise ValueError(f"Unsupported browser_type: {browser_type}")

        self.browser = await browser_launcher.launch(headless=headless)
        context: BrowserContext = await self.browser.new_context()
        page: Page = await context.new_page()

        logger.info("Local browser launched successfully.")
        return self.browser, page

    async def close(self):
        """Closes the local browser and Playwright instances."""
        if self.browser and self.browser.is_connected():
            logger.info("Closing local browser...")
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()
        logger.info("Local provider cleaned up.")
