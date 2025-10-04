# Standard Library Imports
import asyncio
import time
from collections.abc import Awaitable, Callable
from re import Pattern
from typing import Any

# Playwright Imports
from playwright.async_api import Locator, Page
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
import structlog

# Local Imports
from .exceptions import BrowserAgentError, NavigationTimeoutError, WaitTimeoutError

# --- Logger Setup ---
logger = structlog.get_logger(__name__)


class WaitHandler:
    """
    Handles various waiting scenarios during browser automation using structured logging.
    """

    # Default timeouts
    DEFAULT_NAV_TIMEOUT = 30000
    DEFAULT_UPDATE_TIMEOUT = 10000
    STABILITY_CHECK_INTERVAL_MS = 300
    STABILITY_CHECK_DURATION_MS = 1500

    def __init__(
        self,
        page: Page,
        default_timeout: int | None = None,
    ):
        """Initialize the wait handler."""
        if not page:
            raise ValueError("Page object is required for WaitHandler.")
        self.page = page

        nav_timeout = (
            default_timeout if default_timeout is not None else self.DEFAULT_NAV_TIMEOUT
        )
        update_timeout = (
            int(default_timeout * 0.5)
            if default_timeout is not None
            else self.DEFAULT_UPDATE_TIMEOUT
        )

        self.default_navigation_timeout = max(10000, nav_timeout)
        self.default_update_timeout = max(5000, update_timeout)
        # No logger instance needed; using module logger

    async def wait_for_navigation(
        self,
        trigger_action: Callable[[], Awaitable[Any]] | None = None,
        expected_url: str | Pattern | None = None,
        wait_until: str | None = "load",
        wait_for_selector: str | None = None,
        check_stability_after: bool = True,
        timeout: int | None = None,
    ) -> bool:
        """Waits for page navigation, optionally checking selector and stability."""
        if self.page.is_closed():
            raise BrowserAgentError("Cannot wait for navigation, page is closed.")

        wait_timeout = (
            timeout if timeout is not None else self.default_navigation_timeout
        )
        valid_wait_until = ["load", "domcontentloaded", "commit", "networkidle"]
        effective_wait_until = wait_until if wait_until in valid_wait_until else "load"

        log_msg = (
            f"--- Waiting for Navigation (timeout: {wait_timeout}ms, "
            f"wait_until: '{effective_wait_until}', "
            f"selector: '{wait_for_selector or 'N/A'}', "
            f"stability: {check_stability_after}) ---"
        )
        logger.info(log_msg)
        start_time = time.time()
        initial_url = self.page.url

        try:
            # --- Perform Navigation Wait ---
            if trigger_action:
                logger.debug(
                    "  - Waiting with trigger action using expect_navigation..."
                )
                trigger_timeout = int(wait_timeout * 0.8)  # Leave time for other checks
                async with self.page.expect_navigation(
                    timeout=trigger_timeout,
                    wait_until=effective_wait_until,
                    url=expected_url,
                ):
                    await trigger_action()
                logger.debug(
                    "  - Trigger action completed and navigation event caught."
                )
            else:
                logger.debug(
                    f"  - Explicitly waiting for state '{effective_wait_until}'..."
                )
                await self.page.wait_for_load_state(
                    state=effective_wait_until, timeout=int(wait_timeout * 0.8)
                )
                current_url = self.page.url
                logger.debug(
                    f"    ✓ State '{effective_wait_until}' reached (URL: {current_url})."
                )
                if initial_url == current_url:
                    logger.debug("    Note: URL did not change during explicit wait.")
                if expected_url:
                    url_matches = False
                    if isinstance(expected_url, Pattern):
                        url_matches = bool(expected_url.search(current_url))
                    elif isinstance(expected_url, str):
                        url_matches = expected_url in current_url
                    if not url_matches:
                        logger.warning(
                            f"    ! URL mismatch after wait. Expected '{expected_url}', got '{current_url}'."
                        )
                        # Consider raising NavigationTimeoutError here if strict matching is needed

            # --- Wait for Selector ---
            if wait_for_selector:
                elapsed_ms = (time.time() - start_time) * 1000
                remaining_timeout = max(2000, wait_timeout - int(elapsed_ms))
                logger.debug(
                    f"  - Waiting for selector '{wait_for_selector}' (timeout {remaining_timeout}ms)..."
                )
                try:
                    await self.page.locator(wait_for_selector).first.wait_for(
                        state="visible", timeout=remaining_timeout
                    )
                    logger.debug(
                        f"    ✓ Required selector '{wait_for_selector}' is visible."
                    )
                except PlaywrightTimeoutError as e:
                    raise NavigationTimeoutError(
                        f"Selector '{wait_for_selector}' not visible after {remaining_timeout}ms."
                    ) from e

            # --- Check Stability ---
            if check_stability_after:
                elapsed_ms = (time.time() - start_time) * 1000
                stability_timeout = min(
                    self.STABILITY_CHECK_DURATION_MS,
                    max(500, wait_timeout - int(elapsed_ms)),
                )
                logger.debug(
                    f"  - Checking DOM stability (duration {stability_timeout}ms)..."
                )
                if not await self._wait_for_stable_dom(stability_timeout):
                    logger.warning(
                        "    ! DOM stability check failed (content might still be changing)."
                    )
                # Note: _wait_for_stable_dom logs success internally at debug level

            await asyncio.sleep(0.3)

            elapsed = (time.time() - start_time) * 1000
            logger.info(f"--- Navigation Wait Successful ({elapsed:.0f}ms) ---")
            return True

        except PlaywrightTimeoutError as e:
            elapsed = (time.time() - start_time) * 1000
            error_message = f"Navigation timed out after {elapsed:.0f}ms. Details: {str(e).replace('\n', ' ')}"
            logger.error(f"--- Navigation Wait FAILED: {error_message} ---")
            raise NavigationTimeoutError(error_message) from e
        except Exception as e:
            elapsed = (time.time() - start_time) * 1000
            error_message = f"Error during navigation wait after {elapsed:.0f}ms: {type(e).__name__}: {str(e)}"
            logger.error(
                f"--- Navigation Wait FAILED: {error_message} ---", exc_info=True
            )
            raise BrowserAgentError(error_message) from e

    async def wait_for_dynamic_update(
        self,
        trigger_action: Callable[[], Awaitable[Any]] | None = None,
        expected_selector: Locator | str | None = None,
        expected_selector_state: str = "visible",
        wait_for_network_idle: bool = False,
        wait_for_stability: bool = True,
        timeout: int | None = None,
    ) -> bool:
        """Waits for potential dynamic updates on the current page to settle."""
        if self.page.is_closed():
            raise BrowserAgentError("Cannot wait for dynamic update, page is closed.")

        wait_timeout = timeout if timeout is not None else self.default_update_timeout
        logger.info(f"--- Waiting for Dynamic Update (timeout: {wait_timeout}ms) ---")
        start_time = time.time()
        remaining_timeout = wait_timeout

        try:
            if trigger_action:
                logger.debug("  - Performing trigger action...")
                await trigger_action()
                logger.debug("  - Trigger action completed.")
                await asyncio.sleep(0.05)

            # --- Wait for Expected Selector ---
            if expected_selector:
                selector_timeout = int(remaining_timeout * 0.8)
                logger.debug(
                    f"  - Waiting for selector state: '{expected_selector_state}' (timeout {selector_timeout}ms)..."
                )
                try:
                    locator = (
                        expected_selector
                        if isinstance(expected_selector, Locator)
                        else self.page.locator(expected_selector)
                    )
                    await locator.wait_for(
                        state=expected_selector_state, timeout=selector_timeout
                    )
                    logger.debug(
                        f"    ✓ Selector reached state '{expected_selector_state}'."
                    )
                    elapsed = (time.time() - start_time) * 1000
                    remaining_timeout = max(500, wait_timeout - int(elapsed))
                except PlaywrightTimeoutError as e:
                    raise WaitTimeoutError(
                        f"Expected selector did not reach state '{expected_selector_state}' within {selector_timeout}ms."
                    ) from e

            # --- Wait for Network Idle ---
            if wait_for_network_idle:
                network_timeout = int(remaining_timeout * 0.7)
                logger.debug(
                    f"  - Waiting for network idle (timeout {network_timeout}ms)..."
                )
                try:
                    await self.page.wait_for_load_state(
                        "networkidle", timeout=network_timeout
                    )
                    logger.debug("    ✓ Network appears idle.")
                    elapsed = (time.time() - start_time) * 1000
                    remaining_timeout = max(500, wait_timeout - int(elapsed))
                except PlaywrightTimeoutError:
                    logger.warning("    ! Network idle timeout (may be acceptable).")

            # --- Wait for DOM Stability ---
            if wait_for_stability:
                stability_timeout = min(
                    self.STABILITY_CHECK_DURATION_MS, remaining_timeout
                )
                logger.debug(
                    f"  - Checking DOM stability (duration {stability_timeout}ms)..."
                )
                if not await self._wait_for_stable_dom(stability_timeout):
                    logger.warning(
                        "    ! DOM stability check failed (content might still be changing)."
                    )
                    # Not raising an error here, just warning

            elapsed = (time.time() - start_time) * 1000
            logger.info(f"--- Dynamic Update Wait Successful ({elapsed:.0f}ms) ---")
            return True

        except (PlaywrightTimeoutError, WaitTimeoutError) as e:
            elapsed = (time.time() - start_time) * 1000
            error_message = f"Dynamic update wait timed out after {elapsed:.0f}ms. Details: {str(e).replace('\n', ' ')}"
            logger.error(f"--- Dynamic Update Wait FAILED: {error_message} ---")
            if isinstance(e, WaitTimeoutError):
                raise e
            raise WaitTimeoutError(error_message) from e
        except Exception as e:
            elapsed = (time.time() - start_time) * 1000
            error_message = f"Error during dynamic update wait after {elapsed:.0f}ms: {type(e).__name__}: {str(e)}"
            logger.error(
                f"--- Dynamic Update Wait FAILED: {error_message} ---", exc_info=True
            )
            raise BrowserAgentError(error_message) from e

    async def _wait_for_stable_dom(self, duration_ms: int) -> bool:
        """Helper to check if DOM innerHTML length remains constant for a duration."""
        start_stability_time = time.time()
        last_html_len = -1
        try:
            last_html_len = await self.page.evaluate(
                "() => document.body?.innerHTML.length ?? 0"
            )
        except Exception as e:
            # Log as warning, assume stable if initial check fails
            logger.warning(
                f"      - Warning: Could not get initial DOM length for stability check: {e}",
                exc_info=False,
            )
            return True

        while (time.time() - start_stability_time) * 1000 < duration_ms:
            await asyncio.sleep(self.STABILITY_CHECK_INTERVAL_MS / 1000)
            try:
                current_html_len = await self.page.evaluate(
                    "() => document.body?.innerHTML.length ?? 0"
                )
                if current_html_len != last_html_len:
                    logger.debug(
                        f"      - DOM changed (length {last_html_len} -> {current_html_len}). Resetting stability timer."
                    )
                    start_stability_time = time.time()  # Reset timer
                    last_html_len = current_html_len
            except Exception as e:
                logger.warning(
                    f"      - Warning: Error during stability check loop: {e}",
                    exc_info=False,
                )
                await asyncio.sleep(0.2)  # Extra pause

        logger.debug("    ✓ DOM appears stable.")
        return True
