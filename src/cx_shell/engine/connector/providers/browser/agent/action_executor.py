import asyncio
import re
import time
from collections.abc import Awaitable, Callable

# Import ElementHandle and other necessary Playwright components
from playwright.async_api import ElementHandle, Locator, Page, expect
from playwright.async_api import Error as PlaywrightError
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
import structlog

from .exceptions import (
    ActionFailedError,
    ElementNotInteractableError,
    VerificationFailedError,
)

# Import relevant models and exceptions
from .models import CommandInfo  # Assuming CommandInfo is sufficient here

logger = structlog.get_logger(__name__)


class ActionExecutor:
    """
    Executes actions (click, type, etc.) on a given Playwright Locator.
    Handles retries, smart timeouts, scrolling (including JS fallback),
    and actionability checks. Includes dummy scroll before retry on viewport error.
    Uses structured logging.
    """

    DEFAULT_MAX_RETRIES = 2  # Total attempts = DEFAULT_MAX_RETRIES + 1
    ACTION_TIMEOUT_RATIO = 0.9
    POST_ACTION_DELAY_MS = 100
    JS_SCROLL_TIMEOUT_MS = 7000  # Timeout for JS scroll attempt + verification
    QUICK_ACTION_TIMEOUT_MS = 7000  # Timeout for the quick first try

    def __init__(self, page: Page, default_timeout: int):  # Added logger parameter
        """Initialize the executor."""
        if not page:
            raise ValueError("Page object is required for ActionExecutor.")
        self.page = page
        self.default_timeout = default_timeout  # Overall default timeout basis

    async def execute_action(
        self,
        locator: Locator,
        command_info: CommandInfo,
        step_index: int,  # Add step_index
        max_retries: int | None = None,
    ) -> bool:
        """
        Executes the specified command action on the locator with retries.
        Uses a quick timeout for the first attempt.
        Attempts JS container scroll if initial action fails due to viewport issues.
        Performs a dummy scroll before retrying after a viewport error.
        """
        retries = max_retries if max_retries is not None else self.DEFAULT_MAX_RETRIES
        command_type = command_info["command_type"]
        command_name = command_info.get("name", command_type)
        last_error: Exception | None = None
        js_scroll_tried = False

        target_selector_str = self._get_selector_string_from_command(command_info)

        for attempt in range(retries + 1):
            start_time_attempt = time.time()
            logger.info(
                f"--- Action Attempt {attempt + 1}/{retries + 1} for Command '{command_name}' ({command_type}) ---"
            )
            viewport_error_occurred_this_attempt = False  # Flag for this attempt

            try:
                # Determine Timeout for This Attempt
                if attempt == 0:
                    current_action_timeout = self.QUICK_ACTION_TIMEOUT_MS
                    logger.debug(
                        f"  Performing action '{command_type}' (QUICK timeout {current_action_timeout}ms)..."
                    )
                else:
                    # Ensure minimum timeout for retries
                    current_action_timeout = max(
                        5000, int(self.default_timeout * self.ACTION_TIMEOUT_RATIO)
                    )
                    logger.debug(
                        f"  Performing action '{command_type}' (Retry timeout {current_action_timeout}ms)..."
                    )

                action_method = self._get_action_method(command_type)
                # Pass kwargs down in case future methods need them
                success = await action_method(
                    locator, command_info, current_action_timeout, force=False
                )

                if success:
                    logger.info(
                        f"  ✓ Action '{command_type}' successful on attempt {attempt + 1}."
                    )
                    await asyncio.sleep(self.POST_ACTION_DELAY_MS / 1000)
                    return True
                else:
                    # This case might occur if a method implementation returns False explicitly
                    last_error = ActionFailedError(
                        f"Action method for '{command_type}' returned False."
                    )
                    logger.warning(
                        f"    ! Action method returned False on attempt {attempt + 1}."
                    )

            except (PlaywrightTimeoutError, PlaywrightError) as e:
                last_error = e
                error_msg = (
                    str(e).lower().replace("\n", " ")
                )  # Clean error message for logging
                logger.warning(
                    f"    ! Attempt {attempt + 1} failed: {type(e).__name__}: {error_msg}"
                )
                is_viewport_error = (
                    "outside of the viewport" in error_msg
                    or "element is not visible" in error_msg
                    or "scroll into view" in error_msg
                )

                if is_viewport_error:
                    viewport_error_occurred_this_attempt = True
                    if not js_scroll_tried:  # Try JS scroll only once
                        js_scroll_tried = True
                        logger.info(
                            "    ! Viewport/Visibility error detected. Attempting JS scroll fallback..."
                        )
                        js_scroll_timeout = self.JS_SCROLL_TIMEOUT_MS
                        try:
                            scrolled_ok = await self._ensure_visible_js_scroll_fallback(
                                locator, js_scroll_timeout, target_selector_str
                            )
                            if scrolled_ok:
                                logger.info("      ✓ JS scroll fallback succeeded.")
                            else:
                                logger.warning(
                                    "      ! JS scroll fallback failed or element still not visible."
                                )
                        except Exception as js_err:
                            logger.warning(
                                f"      ! Error during JS scroll fallback execution: {js_err}",
                                exc_info=True,
                            )
                    elif attempt < retries:  # If JS already tried, just log it
                        logger.warning(
                            f"    ! Viewport error occurred again on attempt {attempt + 1} (JS scroll already tried)."
                        )
                    else:  # Error on final attempt
                        logger.error(
                            "    ! Viewport/Visibility error on final attempt."
                        )

            except (ElementNotInteractableError, ActionFailedError) as e:
                last_error = e
                logger.warning(
                    f"    ! Attempt {attempt + 1} failed: {type(e).__name__}: {str(e)}"
                )
            except Exception as e:
                last_error = e
                logger.error(
                    f"    ! Unexpected error on attempt {attempt + 1}: {type(e).__name__}: {str(e)}",
                    exc_info=True,
                )
                # Re-raise unexpected errors immediately if preferred, or let loop continue
                # raise # Uncomment to stop immediately on unexpected errors

            # If loop continues (action failed), wait before next retry
            if attempt < retries:
                # Perform dummy scroll if a viewport error happened THIS attempt
                if viewport_error_occurred_this_attempt:
                    logger.info(
                        f"    ! Performing dummy page scroll (800px) before retry {attempt + 2}..."
                    )
                    try:
                        await self.page.mouse.wheel(0, 800)  # Use 800px scroll delta
                        await asyncio.sleep(0.3)  # Short pause after dummy scroll
                    except Exception as wheel_err:
                        logger.warning(
                            f"      ! Warning: Dummy scroll failed: {wheel_err}"
                        )

                wait_time = 0.5 + attempt * 0.6  # Exponential backoff delay
                logger.info(f"    Retrying after {wait_time:.1f} seconds...")
                await asyncio.sleep(wait_time)

        # Final failure message after all retries
        final_error_message = f"Action '{command_name}' ({command_type}) failed after {retries + 1} attempts."
        if last_error:
            final_error_message += (
                f" Last error: {type(last_error).__name__}: {str(last_error)}"
            )
        logger.error(f"--- Action Execution FAILED: {final_error_message} ---")
        raise ActionFailedError(final_error_message) from last_error

    def _get_action_method(self, command_type: str) -> Callable[..., Awaitable[bool]]:
        """Maps command type string to the internal method."""
        method_map = {
            "click": self._perform_click,
            "click_text": self._perform_click,
            "type": self._perform_type,
            "fill": self._perform_fill,
            "select": self._perform_select,
            "check": self._perform_check,
            "uncheck": self._perform_uncheck,
            "copy_text": self._perform_copy_text,
            "verify_checked": self._perform_verify_checked,
        }
        method = method_map.get(command_type)
        if not method:
            raise NotImplementedError(
                f"Action method for command type '{command_type}' is not implemented."
            )
        return method

    async def _perform_verify_checked(
        self, locator: Locator, command_info: CommandInfo, timeout: int, **kwargs
    ) -> bool:
        """Verifies if a checkbox/radio element is checked or unchecked."""
        expected_state_str = str(
            command_info.get("text", "checked")
        ).lower()  # Ensure string
        should_be_checked = expected_state_str == "checked"
        # Use a portion of the action timeout for verification, with a minimum
        verification_timeout = max(1000, int(timeout * 0.3))

        logger.debug(
            f"  Verifying checkbox state is '{expected_state_str}' (timeout {verification_timeout}ms)."
        )

        try:
            await expect(locator).to_be_checked(
                timeout=verification_timeout, checked=should_be_checked
            )
            logger.debug(
                f"    ✓ Verification successful: Element is {'checked' if should_be_checked else 'unchecked'}."
            )
            return True
        except PlaywrightTimeoutError as e:
            logger.error(
                f"    ! Verification FAILED: Element was NOT {'checked' if should_be_checked else 'unchecked'}."
            )
            # Raise specific error for verification failures
            raise VerificationFailedError(
                f"Expected element to be {'checked' if should_be_checked else 'unchecked'}, but it was not."
            ) from e
        except Exception as e:
            logger.error(
                f"    ! Error during verification: {type(e).__name__}: {e}",
                exc_info=True,
            )
            raise ActionFailedError(f"Error during checkbox verification: {e}") from e

    # Inside ActionExecutor class in browser/action_executor.py

    async def _perform_click(
        self,
        locator: Locator,
        command_info: CommandInfo,
        timeout: int,
        force: bool = False,
        **kwargs,
    ) -> bool:
        """Performs a click action, handling different element types and positions."""
        tag = ""
        el_type = ""
        try:
            # Check if it's a checkbox/radio first (optional, but keep logic)
            tag = await locator.evaluate("el => el.tagName.toLowerCase()", timeout=500)
            if tag == "input":
                el_type = await locator.evaluate("el => el.type", timeout=500)
        except Exception as tag_err:
            logger.debug(
                f"    Note: Could not determine tag/type before click: {tag_err}"
            )

        if tag == "input" and el_type in ["checkbox", "radio"]:
            logger.debug(
                "    Note: Using locator.click() on a checkbox/radio based on command type."
            )
            # --- MODIFICATION START ---
            await locator.click(timeout=timeout, force=force, no_wait_after=True)
            await asyncio.sleep(0.2)  # Short pause after click
            # --- MODIFICATION END ---
        else:
            # Handle standard click with optional position
            click_location = str(command_info.get("click_location", "center")).lower()
            position: dict[str, float] | None = None
            offset = 2

            if click_location != "center":
                logger.debug(
                    f"    Calculating position for click location: '{click_location}'"
                )
                try:
                    bounding_box = await locator.bounding_box(timeout=1000)
                    if bounding_box:
                        width, height = bounding_box["width"], bounding_box["height"]
                        pos_map: dict[str, dict[str, float]] = {
                            "top_left": {"x": offset, "y": offset},
                            "top_right": {"x": width - offset, "y": offset},
                            "bottom_left": {"x": offset, "y": height - offset},
                            "bottom_right": {"x": width - offset, "y": height - offset},
                            "left_center": {"x": offset, "y": height / 2},
                            "right_center": {"x": width - offset, "y": height / 2},
                            "top_center": {"x": width / 2, "y": offset},
                            "bottom_center": {"x": width / 2, "y": height - offset},
                        }
                        position = pos_map.get(click_location)
                        if position is None:
                            logger.warning(
                                f"    Warning: Unsupported click_location keyword '{click_location}', defaulting to center."
                            )
                            click_location = "center"
                    else:
                        logger.warning(
                            "    Warning: Could not get bounding box. Defaulting to center."
                        )
                        click_location = "center"
                except Exception as bbox_err:
                    logger.warning(
                        f"    Warning: Error getting bounding box ({bbox_err}). Defaulting to center.",
                        # exc_info=True, # Reduce noise maybe
                    )
                    click_location = "center"

            logger.debug(
                f"    Using locator.click(position={position}, force={force}, no_wait_after=True). Location='{click_location}'"
            )
            # --- MODIFICATION START ---
            await locator.click(
                timeout=timeout, position=position, force=force, no_wait_after=True
            )
            await asyncio.sleep(0.2)  # Short pause after click
            # --- MODIFICATION END ---

        return True  # Return True if click action itself doesn't raise error (timeout handled by execute_action)

    async def _perform_type(
        self, locator: Locator, command_info: CommandInfo, timeout: int, **kwargs
    ) -> bool:
        """Performs a type action, clearing the field first."""
        text_to_type = str(command_info.get("text", ""))  # Ensure string
        logger.debug(
            f"    Typing text: '{text_to_type[:30]}{'...' if len(text_to_type) > 30 else ''}'"
        )
        try:
            # Clear field first, allocate less time for clear
            await locator.fill("", timeout=max(1000, int(timeout * 0.3)))
        except Exception as fill_err:
            # Log warning but continue with type attempt
            logger.warning(
                f"    Warning: Clearing field before type failed: {fill_err}"
            )
        # Allocate remaining time for typing
        await locator.type(
            text_to_type, timeout=max(1000, int(timeout * 0.7)), delay=50
        )
        await self._handle_dynamic_content_after_type(locator)
        return True

    async def _perform_fill(
        self, locator: Locator, command_info: CommandInfo, timeout: int, **kwargs
    ) -> bool:
        """Performs a fill action."""
        text_to_fill = str(command_info.get("text", ""))  # Ensure string
        logger.debug(
            f"    Filling text: '{text_to_fill[:30]}{'...' if len(text_to_fill) > 30 else ''}'"
        )
        await locator.fill(text_to_fill, timeout=timeout)
        await self._handle_dynamic_content_after_type(
            locator
        )  # Check for dynamic content after fill too
        return True

    # ~/repositories/functions/browser/action_executor.py
    # Inside the ActionExecutor class

    async def _perform_select(
        self, locator: Locator, command_info: CommandInfo, timeout: int, **kwargs
    ) -> bool:
        """
        Performs a select action.
        - Handles comma-separated preference list in command_info['text'].
        - Selects the first preference found among available options.
        - If only one valid option exists (excluding disabled/placeholders), selects it by default.
        """
        # 1. Parse Preferences from command_info['text']
        preferences_str = str(command_info.get("text", ""))
        # Split by comma, strip whitespace, remove empty strings
        preferences = [p.strip() for p in preferences_str.split(",") if p.strip()]

        if not preferences:
            raise ValueError(
                "Select command requires at least one preferred option text/value in the 'text' field."
            )

        logger.debug(f"   Attempting select action. Preferences: {preferences}")

        # 2. Get Available Options from the <select> element
        option_elements = locator.locator("option")
        options_data = []
        try:
            option_count = await option_elements.count()
            logger.debug(f"      Found {option_count} <option> elements.")
            valid_option_found = False  # Flag to track if we find any selectable option

            for i in range(option_count):
                option_locator = option_elements.nth(i)
                is_disabled = await option_locator.is_disabled(timeout=500)
                text = await option_locator.text_content(timeout=500) or ""
                value = await option_locator.get_attribute("value", timeout=500) or ""
                text = text.strip()  # Clean whitespace

                # Basic check to ignore placeholder/disabled options
                is_placeholder = (
                    "select" in text.lower() and not value
                )  # Simple heuristic
                is_valid_option = (
                    not is_disabled and not is_placeholder and (text or value)
                )

                if is_valid_option:
                    valid_option_found = True

                options_data.append(
                    {
                        "locator": option_locator,
                        "text": text,
                        "value": value,
                        "is_disabled": is_disabled,
                        "is_placeholder": is_placeholder,
                        "is_valid": is_valid_option,
                    }
                )
                logger.debug(
                    f"      Option {i}: Text='{text}', Value='{value}', Valid={is_valid_option}"
                )

            if not valid_option_found:
                raise ActionFailedError(
                    "No valid/selectable options found within the <select> element."
                )

        except Exception as e:
            logger.error(
                f"      Error retrieving options from <select>: {e}", exc_info=True
            )
            raise ActionFailedError(
                f"Failed to get options from select element: {e}"
            ) from e

        # 3. Check Single Valid Option Case
        valid_options = [opt for opt in options_data if opt["is_valid"]]
        target_option_data = None  # Store the option we decide to select

        if len(valid_options) == 1:
            logger.info("   Only one valid option found. Selecting it by default.")
            target_option_data = valid_options[0]
        else:
            # 4. Multiple Options: Match Preferences (in order)
            logger.debug(
                "   Multiple valid options found. Matching against preferences..."
            )
            for pref in preferences:
                logger.debug(f"      Checking preference: '{pref}'")
                # Try matching by text first (case-insensitive, whitespace normalized)
                normalized_pref = " ".join(pref.split()).lower()
                for option in valid_options:
                    normalized_text = " ".join(option["text"].split()).lower()
                    if normalized_pref == normalized_text:
                        logger.debug(
                            f"         Found match by text: '{option['text']}'"
                        )
                        target_option_data = option
                        break  # Found match for this preference

                if target_option_data:
                    break  # Stop checking preferences if match found

                # If no text match for this pref, try matching by value
                if not target_option_data:
                    for option in valid_options:
                        # Compare pref directly against value (values are often case-sensitive)
                        if pref == option["value"]:
                            logger.debug(
                                f"         Found match by value: '{option['value']}' (Text: '{option['text']}')"
                            )
                            target_option_data = option
                            break  # Found match for this preference

                if target_option_data:
                    break  # Stop checking preferences if match found

        # 5. Perform Selection or Raise Error
        if target_option_data:
            option_to_select = (
                target_option_data["value"]
                if target_option_data["value"]
                else {"label": target_option_data["text"]}
            )
            log_selection = (
                f"value='{target_option_data['value']}'"
                if target_option_data["value"]
                else f"label='{target_option_data['text']}'"
            )
            logger.info(f"   Selected option to target: {log_selection}")
            try:
                await locator.select_option(option_to_select, timeout=timeout)
                logger.info(
                    f"      ✓ Successfully selected option via select_option({log_selection})."
                )
                # Optional: Add verification after selection if needed
                # selected_value = await locator.input_value()
                # if selected_value == target_option_data["value"]: ...
                return True
            except Exception as e:
                logger.warning(
                    f"      select_option({log_selection}) failed: {e}. Falling back to heuristic click..."
                )
                # Fallback heuristic (less reliable) - Click the specific option locator
                try:
                    await target_option_data["locator"].click(
                        timeout=max(1000, int(timeout * 0.5))
                    )
                    logger.info("      ✓ Heuristic option click succeeded.")
                    # Verification after click is harder, maybe check parent select's value
                    await asyncio.sleep(0.2)  # Pause after click
                    # selected_value = await locator.input_value() # Check if value updated
                    # if selected_value != target_option_data["value"]:
                    #      logger.warning(f"      Heuristic click verification failed (value not updated to {target_option_data['value']})")
                    #      return False # Consider click failed if value didn't update
                    return True
                except Exception as click_err:
                    logger.error(
                        f"      Heuristic option click also failed: {click_err}",
                        exc_info=True,
                    )
                    raise ActionFailedError(
                        f"Both select_option and heuristic click failed for {log_selection}"
                    ) from click_err
        else:
            # 6. No Match Found
            logger.error(
                f"   No available option matched the provided preferences: {preferences}"
            )
            raise ActionFailedError(
                f"Could not find any of the preferred options {preferences} in the select dropdown."
            )

    async def _perform_check(
        self, locator: Locator, command_info: CommandInfo, timeout: int, **kwargs
    ) -> bool:
        """Performs a check action and verifies the result."""
        logger.debug("    Performing check action.")
        await locator.check(timeout=timeout)
        # --- Verification ---
        try:
            await expect(locator).to_be_checked(
                timeout=1000
            )  # Short timeout for verification
            logger.debug("      ✓ Checkbox state verified as checked.")
            await asyncio.sleep(0.1)
        except PlaywrightTimeoutError:
            logger.error(
                "    ! Checkbox state verification failed (not checked after action)."
            )
            raise ActionFailedError(
                "Checkbox failed verification: Was not checked after action."
            )
        # --- END Verification ---
        return True

    async def _perform_uncheck(
        self, locator: Locator, command_info: CommandInfo, timeout: int, **kwargs
    ) -> bool:
        """Performs an uncheck action and verifies the result."""
        logger.debug("    Performing uncheck action.")
        await locator.uncheck(timeout=timeout)
        # --- Verification ---
        try:
            await expect(locator).to_be_checked(
                timeout=1000, checked=False
            )  # Verify it's unchecked
            logger.debug("      ✓ Checkbox state verified as unchecked.")
            await asyncio.sleep(0.1)
        except PlaywrightTimeoutError:
            logger.error(
                "    ! Checkbox state verification failed (still checked after action)."
            )
            raise ActionFailedError(
                "Checkbox failed verification: Was still checked after action."
            )
        # --- END Verification ---
        return True

    async def _perform_copy_text(
        self, locator: Locator, command_info: CommandInfo, timeout: int, **kwargs
    ) -> bool:
        """Copies text content from the element."""
        logger.debug("    Performing copy_text action.")
        try:
            # Use a reasonable timeout for getting text
            text = await locator.text_content(timeout=max(1000, int(timeout * 0.5)))
            # Store full text in 'data', update 'message' as per original code
            command_info["data"] = text
            command_info["message"] = text  # Store raw copied text as message
            logger.info(
                f"    Copied text: '{str(text)[:50]}{'...' if text and len(text) > 50 else ''}'"
            )
            return True
        except Exception as e:
            logger.error(f"    Failed to get text content: {e}", exc_info=True)
            command_info["message"] = f"Failed to copy text: {e}"
            return False  # Indicate failure

    async def _find_scrollable_ancestor(self, locator: Locator) -> ElementHandle | None:
        """Finds the nearest scrollable ancestor element using JS evaluation."""
        logger.debug("    Searching for scrollable ancestor...")
        handle: ElementHandle | None = None
        try:
            # Prioritize known scrollable container selectors
            known_scroll_selectors = [
                ".flightFliterScroll",
                ".filter-options-scroll",
                ".scrollable-container",
                ".scrollable",
                '[style*="overflow: auto"]',
                '[style*="overflow-y: auto"]',
                '[style*="overflow: scroll"]',
                '[style*="overflow-y: scroll"]',
            ]
            for selector in known_scroll_selectors:
                try:
                    # Use Playwright's :xpath pseudo-class for ancestor search
                    xpath_selector = f"xpath=./ancestor::{selector.replace('.', '*[contains(@class, "') + '")]' if '.' in selector else selector}"
                    logger.debug(
                        f"      Checking known scroll selector via xpath: {selector}"
                    )
                    ancestor = locator.locator(xpath_selector)

                    # Check count efficiently before getting handle
                    count = await ancestor.count()
                    if count > 0:
                        container_handle = await ancestor.first.element_handle(
                            timeout=500
                        )
                        if container_handle:
                            is_scrollable = await container_handle.evaluate(
                                "el => { const style = window.getComputedStyle(el); const overflowY = style.overflowY; return (overflowY === 'auto' || overflowY === 'scroll') && el.scrollHeight > el.clientHeight + 2; }"
                            )  # Add small buffer
                            if is_scrollable:
                                logger.debug(
                                    f"      Found ancestor matching known selector '{selector}' and verified scrollable."
                                )
                                return container_handle  # Found it
                            else:
                                logger.debug(
                                    f"      Ancestor matching '{selector}' found but not scrollable."
                                )
                                await (
                                    container_handle.dispose()
                                )  # Dispose unused handle
                        else:
                            logger.debug(
                                f"      Could not get handle for ancestor matching '{selector}'."
                            )

                except PlaywrightTimeoutError:
                    logger.debug(
                        f"      Timeout checking known scroll selector '{selector}'."
                    )  # Expected if selector not present
                except Exception as e:
                    logger.warning(
                        f"      Error checking known scroll selector '{selector}': {e}",
                        exc_info=False,
                    )  # Log other errors as warning

            # Fallback to generic JS traversal if no known selectors worked
            logger.debug(
                "    Known scroll selectors not found/matched, trying generic JS traversal..."
            )
            handle = await locator.evaluate_handle(
                """el => {
                if (!el) return null;
                let current = el.parentElement;
                while (current && current !== document.body && current !== document.documentElement) {
                    const style = window.getComputedStyle(current);
                    const overflowY = style.overflowY;
                    const isScrollable = overflowY === 'auto' || overflowY === 'scroll';
                    // Check if element is actually scrollable (scrollHeight > clientHeight)
                    if (isScrollable && current.scrollHeight > current.clientHeight + 2) { // Add small buffer
                        return current;
                    }
                    current = current.parentElement;
                }
                return null; // No scrollable ancestor found
            }""",
                timeout=1500,
            )  # Increased timeout slightly

            # Check if the handle is valid (not null)
            if handle and await handle.evaluate("h => h !== null"):
                logger.debug(
                    "    Found potential scrollable ancestor JSHandle via traversal."
                )
                return handle
            else:
                logger.debug("    No scrollable ancestor found via JS traversal.")
                if handle:
                    await handle.dispose()  # Dispose if null handle was returned
                return None
        except Exception as e:
            logger.warning(
                f"    Error finding scrollable ancestor: {type(e).__name__} - {e}",
                exc_info=False,
            )
            if handle:
                try:
                    await handle.dispose()
                except Exception:
                    pass  # Ignore dispose error
            return None

    async def _scroll_element_in_container_js(
        self, target_locator: Locator, container_handle: ElementHandle
    ) -> bool:
        """Uses JS target.scrollIntoView() relative to its parent, executed via page.evaluate."""
        logger.debug(
            "      Attempting JS scroll via page.evaluate using scrollIntoView..."
        )
        target_handle: ElementHandle | None = None
        try:
            target_handle = await target_locator.element_handle(
                timeout=max(1000, int(self.JS_SCROLL_TIMEOUT_MS * 0.5))
            )
            if not target_handle:
                logger.warning(
                    "      Failed to get target element handle for JS scroll."
                )
                return False

            # Evaluate scrollIntoView in the page context, passing handles
            await self.page.evaluate(
                """([targetEl, containerEl]) => {
                if (targetEl && typeof targetEl.scrollIntoView === 'function') {
                    console.log('[JS Scroll] Calling target.scrollIntoView({ block: "center" })');
                    targetEl.scrollIntoView({ behavior: 'auto', block: 'center', inline: 'nearest' });
                } else if (targetEl) {
                     console.warn('[JS Scroll] Target element does not have scrollIntoView method.');
                     // Fallback: scroll container based on target position? (More complex)
                     // Example: containerEl.scrollTop = targetEl.offsetTop - containerEl.clientHeight / 2;
                 } else {
                    console.warn('[JS Scroll] Target element invalid.');
                }
            }""",
                [target_handle, container_handle],
            )

            await asyncio.sleep(0.7)  # Wait for potential scroll animation/rendering
            logger.debug("      JS scrollIntoView evaluation executed.")
            return True
        except Exception as js_err:
            logger.warning(
                f"      JS scroll within container failed: {type(js_err).__name__}: {js_err}",
                exc_info=False,
            )
            return False
        finally:
            if target_handle:
                try:
                    await target_handle.dispose()
                except Exception:
                    pass  # Ignore dispose errors

    async def _ensure_visible_js_scroll_fallback(
        self, locator: Locator, timeout: int, target_selector_str: str | None = None
    ) -> bool:
        """Attempts JS scrollIntoView (container or direct) and verifies visibility."""
        start_time = time.time()
        container_handle: ElementHandle | None = None
        js_scrolled = False
        try:
            container_handle = await self._find_scrollable_ancestor(locator)
            if container_handle:
                logger.debug(
                    "    Found scrollable container, trying JS scroll via page.evaluate."
                )
                js_scrolled = await self._scroll_element_in_container_js(
                    locator, container_handle
                )
            else:
                logger.debug(
                    "    No specific container found, trying direct JS scrollIntoView on target."
                )
                try:
                    # Use wait_for to ensure element exists before evaluating scrollIntoView
                    await locator.wait_for(
                        state="attached", timeout=max(1000, int(timeout * 0.2))
                    )
                    await locator.evaluate(
                        "el => el.scrollIntoView({ behavior: 'auto', block: 'center', inline: 'nearest' })",
                        timeout=int(timeout * 0.6),
                    )
                    await asyncio.sleep(0.5)  # Pause after scroll
                    js_scrolled = True
                except Exception as main_js_err:
                    logger.warning(
                        f"    Direct JS scrollIntoView failed: {main_js_err}",
                        exc_info=False,
                    )
                    js_scrolled = False

            # Verify Visibility AFTER JS Scroll Attempt
            if js_scrolled:
                logger.debug("    JS scroll executed. Verifying visibility...")
                try:
                    elapsed_ms = (time.time() - start_time) * 1000
                    verify_timeout = max(
                        1500, timeout - int(elapsed_ms)
                    )  # Min 1.5s verification time
                    await locator.wait_for(state="visible", timeout=verify_timeout)
                    logger.debug("    Element is visible after JS scroll.")
                    return True
                except Exception as vis_err:
                    logger.warning(
                        f"    Element NOT visible after JS scroll attempt: {vis_err}"
                    )
                    return False
            else:
                logger.debug("    JS scroll attempt failed or did not run.")
                return False
        except Exception as js_fallback_err:
            logger.warning(
                f"    Error during JS scroll fallback logic: {js_fallback_err}",
                exc_info=True,
            )
            return False
        finally:
            if container_handle:
                try:
                    await container_handle.dispose()
                except Exception:
                    pass

    async def _handle_dynamic_content_after_type(self, locator: Locator):
        """Checks for and potentially waits for autocomplete/suggestion lists after typing."""
        try:
            # Check common attributes indicating autocomplete behavior
            has_autocomplete_attrs = await locator.evaluate(
                """el =>
                el.hasAttribute('aria-autocomplete') ||
                el.getAttribute('role') === 'combobox' ||
                (el.form && el.form.outerHTML.includes('autocomplete')) ||
                el.hasAttribute('list')
            """,
                timeout=500,
            )  # Quick check

            if has_autocomplete_attrs:
                logger.debug(
                    "    Input has autocomplete attributes, checking for suggestions..."
                )
                common_selectors = [
                    '[role="listbox"]',
                    ".autocomplete-suggestions",
                    ".dropdown-menu",
                    'ul[id*="typeahead"]',
                    'div[class*="suggestion"]',
                    'div[class*="results"]',
                    # Add other common suggestion container selectors here
                ]
                suggestion_locator = self.page.locator(",".join(common_selectors))
                try:
                    # Wait briefly for suggestions to appear
                    await suggestion_locator.first.wait_for(
                        state="visible", timeout=2000
                    )
                    logger.debug(
                        "    Detected potential dynamic suggestion list after type."
                    )
                    await asyncio.sleep(0.3)  # Short pause if suggestions appear
                except PlaywrightTimeoutError:
                    logger.debug("    No suggestion list detected quickly.")
                    pass  # It's okay if no suggestions appear
        except Exception as e:
            # Log as warning, don't fail the main action
            logger.warning(
                f"    Note: Error checking for dynamic content after type: {str(e)}",
                exc_info=False,
            )

    def _get_selector_string_from_command(
        self, command_info: CommandInfo
    ) -> str | None:
        """Attempts to generate a likely selector string from historical command info."""
        try:
            element_info = command_info.get("element_info")
            if not isinstance(element_info, dict):
                return None
            locators = element_info.get("locators", {})
            if isinstance(locators, dict) and locators.get("css_selector"):
                return locators["css_selector"]
            if isinstance(locators, dict) and locators.get("xpath"):
                return f"xpath={locators['xpath']}"
            # Fallback logic (simplified)
            attrs = element_info.get("attributes", {})
            if isinstance(attrs, dict) and attrs.get("id"):
                pred_id = attrs.get("id")
                if pred_id and not re.match(
                    r"^(ember\d+|gwt-|ext-|jQuery\d+)", pred_id, re.IGNORECASE
                ):
                    return f"#{pred_id}"
        except Exception:
            pass  # Ignore errors during prediction
        return None  # Return None if no good selector found
