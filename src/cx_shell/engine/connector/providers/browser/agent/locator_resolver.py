import json
import re
from typing import Any

from playwright.async_api import Locator, Page
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
import structlog


from .exceptions import LocatorResolutionError

# Import relevant models and exceptions
# Assuming CommandInfo and ElementInfo types/structure are defined in models.py
# We primarily use dictionary access here, so exact model isn't strictly needed for runtime
# but helps with understanding.
from .models import CommandInfo  # ElementInfo structure is accessed via dict

logger = structlog.get_logger(__name__)


# --- CSS Escaping Helper (Add this within LocatorResolver or import) ---
def escape_css_selector_value(value: str) -> str:
    """Escapes characters problematic in CSS selector values, especially within quotes."""
    if not isinstance(value, str):
        return str(value)  # Should already be string, but handle just in case
    # Escape backslashes first, then quotes
    escaped = value.replace("\\", "\\\\").replace('"', '\\"').replace("'", "\\'")
    # You might need to escape other characters depending on usage, but quotes are common
    return escaped


# --- End Helper ---


class LocatorResolver:
    """
    Resolves the best Playwright Locator based on historical CommandInfo.
    Prioritizes user-facing locators and verifies candidates using structured logging.
    """

    MAX_TEXT_MATCH_LENGTH = 150

    def __init__(self, page: Page):
        """Initialize the resolver with page and event bus."""
        if not page:
            raise ValueError("Page object is required for LocatorResolver.")

        self.page = page
        self._strategies_tried: list[str] = []

    async def find_locator(self, command_info: CommandInfo, step_index: int) -> Locator:
        """
        Finds the most reliable Playwright Locator for the given command.
        Uses a prioritized list of strategies, including contextual filtering,
        verifies candidates, and attempts disambiguation if needed.
        """
        self._strategies_tried = []  # Reset for each call
        cmd_name = command_info.get(
            "name", command_info.get("command_type", "UnknownCmd")
        )
        logger.debug("Locator resolution started.", step=step_index, target=cmd_name)

        try:
            # Log truncated command info for context (existing logging code)
            log_info = command_info.copy()
            if "element_info" in log_info and isinstance(
                log_info["element_info"], dict
            ):
                log_info["element_info"] = {
                    k: (
                        f"{str(v)[:100]}..."
                        if isinstance(v, (dict, list, str)) and len(str(v)) > 100
                        else v
                    )
                    for k, v in log_info["element_info"].items()
                }
            logger.debug(
                f"  Input Command Info (Truncated): {json.dumps(log_info, indent=2, default=str)}"
            )
        except Exception as log_err:
            logger.warning(
                f"  Warning: Could not serialize command_info for logging: {log_err}"
            )

        element_info = command_info.get("element_info", {})
        if not isinstance(element_info, dict):
            element_info = {}  # Ensure it's a dict

        # --- Determine if context filters are present ---
        context_filter = command_info.get("context_filter")
        context_text = command_info.get("context_text")
        has_advanced_filter = isinstance(context_filter, dict) and bool(context_filter)
        has_simple_context = (
            bool(context_text) and not has_advanced_filter
        )  # Only run simple if advanced isn't present

        # --- Define the order of strategies with conditions ---
        strategy_methods = [
            # Tuple: (method_function, condition_to_run)
            (
                self._try_contextual_filter_advanced,
                has_advanced_filter,
            ),  # Run first if advanced filter exists
            (
                self._try_contextual_filter,
                has_simple_context,
            ),  # Run second if ONLY simple context exists
            (self._try_role_and_name, True),  # Standard strategies run always
            (self._try_label, True),
            (self._try_placeholder, True),
            (self._try_name_attribute, True),
            (self._try_value_attribute, True),
            (
                self._try_data_attributes,
                True,
            ),  # Specific data-* attributes of the target element
            (self._try_text_content, True),
            (self._try_autocomplete_item, True),
            (self._try_tag_and_text, True),
            (self._try_test_id, True),
            (self._try_relative_locator, True),
            (
                self._try_attribute_selector,
                True,
            ),  # Specific attributes like data-ng-click
            (self._try_stored_css_verified, True),
            (self._try_stored_xpath_verified, True),
            (self._try_id_attribute, True),
            (self._try_simple_tag, True),  # Most generic, run last
        ]

        # --- Iterate through strategies based on conditions ---
        for strategy_method, should_run in strategy_methods:
            strategy_name = strategy_method.__name__.replace("_try_", "")

            # Skip strategy if its condition is not met
            if not should_run:
                # logger.debug(f"  Skipping Strategy: {strategy_name} (Condition not met)") # Optional: reduce log noise
                continue

            # Skip stored selectors if data is missing (existing logic)
            if strategy_name in ["stored_css_verified", "stored_xpath_verified"]:
                locators_data = element_info.get("locators", {})
                selector_key = "css_selector" if "css" in strategy_name else "xpath"
                selector = (
                    locators_data.get(selector_key)
                    if isinstance(locators_data, dict)
                    else None
                )
                if not selector:
                    # logger.debug(f"  Skipping Strategy: {strategy_name} (No {selector_key} found)") # Optional: reduce log noise
                    continue

            # --- Execute the strategy and verify/disambiguate ---
            self._strategies_tried.append(strategy_name)
            logger.info(f"\n  Trying Strategy: {strategy_name}...")
            try:
                # Attempt to get a locator using the current strategy
                # Pass element_info and command_info consistently
                locator = await strategy_method(element_info, command_info)

                # Verification and Disambiguation Logic (remains the same)
                if locator:
                    logger.debug(
                        f"    Strategy {strategy_name} returned a potential locator."
                    )
                    final_locator: Locator | None = None
                    try:
                        count = await locator.count()

                        logger.debug(
                            "Locator strategy attempt",
                            step=step_index,
                            strategy=strategy_name,
                            result="found_multiple"
                            if count > 1
                            else ("found_one" if count == 1 else "no_match"),
                            count=count,
                        )

                        # --- << START REPLACEMENT for 'if count == 1:' block >> ---
                        if count == 1:
                            # --- Check if it's a verification command ---
                            command_type = command_info.get("command_type")
                            is_verification_command = (
                                command_type
                                in [
                                    "verify_element_text_policy",
                                    "verify_checked",  # Add other verification types if needed
                                    "verify_element_visible",  # Example
                                ]
                            )

                            # --- Use less strict verification for verification commands ---
                            # For verification commands, we mainly care if the basic checks (visible/enabled) pass.
                            # We don't need a strict property match against historical data at this stage.
                            verify_props_in_find = not is_verification_command

                            logger.debug(
                                f"      Single candidate found. Performing verification (verify_properties={verify_props_in_find})..."
                            )
                            passed_verification = await self._verify_element_match(
                                locator.first,
                                command_info,
                                element_info,
                                verify_properties=verify_props_in_find,  # Use determined flag
                            )
                            if passed_verification:
                                logger.debug(
                                    f"      Single candidate passed {'basic' if is_verification_command else 'full'} verification."
                                )
                                final_locator = locator  # Assign the found locator
                            else:
                                logger.debug(
                                    f"      Single candidate failed {'basic' if is_verification_command else 'full'} verification."
                                )
                        # --- << END REPLACEMENT for 'if count == 1:' block >> ---
                        elif count > 1:
                            best_match_locator = await self._find_best_verified_match(
                                locator,
                                command_info,
                                element_info,
                                initial_locator_ambiguous=True,
                            )
                            if best_match_locator:
                                logger.debug("      Disambiguation successful.")
                                final_locator = best_match_locator
                            else:
                                logger.debug("      Disambiguation failed.")

                        # Final check: ensure the chosen locator is still attached
                        if final_locator:
                            try:
                                await final_locator.first.wait_for(
                                    state="attached", timeout=500
                                )
                                logger.debug("    Final locator attach check passed.")
                                logger.info(
                                    "Locator resolution successful",
                                    step=step_index,
                                    strategy=strategy_name,
                                    score=1.0,  # Placeholder for now
                                    locator=str(final_locator),
                                )
                                return final_locator  # SUCCESS: Return the verified/disambiguated locator
                            except PlaywrightTimeoutError:
                                logger.warning(
                                    "    Final locator failed verification: Timed out on attach check."
                                )
                            except Exception as attach_err:
                                logger.warning(
                                    f"    Final locator verification attach check error: {attach_err}",
                                    exc_info=False,
                                )
                            # If final check fails, fall through to try next strategy

                    except Exception as verify_err:
                        logger.warning(
                            f"    Error during verification/disambiguation for strategy {strategy_name}: {type(verify_err).__name__}: {str(verify_err)}",
                            exc_info=False,
                        )
                # If locator is None, or count=0, or verification failed, loop continues...

            except Exception as strategy_err:
                # Log errors encountered within the strategy method itself
                logger.warning(
                    f"    Strategy {strategy_name} encountered error: {type(strategy_err).__name__}: {str(strategy_err)}",
                    exc_info=False,
                )
                if "context was destroyed" in str(strategy_err):
                    logger.error(
                        f"      Context destroyed during {strategy_name}, stopping locator resolution."
                    )
                    raise LocatorResolutionError(
                        "Context destroyed during locator resolution"
                    ) from strategy_err

        # --- If loop finishes without returning a locator ---
        error_message = f"Could not resolve locator for command '{cmd_name}' after trying strategies: {', '.join(self._strategies_tried)}"
        logger.error(f"--- Locator Resolution FAILED: {error_message} ---")
        raise LocatorResolutionError(error_message)

    # --- ADDED: Advanced Contextual Filter Strategy ---
    async def _try_contextual_filter_advanced(
        self, element_info: dict, command_info: CommandInfo
    ) -> Locator | None:
        """
        Locates an element within a container identified by potentially multiple
        criteria specified in command_info['context_filter'].
        Prioritizes using a direct css_selector for the container if provided.
        Uses target_selector_override for the target element if provided.
        """
        context_filter = command_info.get("context_filter")
        if not isinstance(context_filter, dict) or not context_filter:
            logger.debug("    Skipping advanced filter: No context_filter dict found.")
            return None

        logger.debug(f"    Applying advanced contextual filter: {context_filter}")

        # --- 2. Determine Container Selector ---
        container_selector_str: str | None = None

        # Prioritize direct css_selector from filter for the container
        if (
            isinstance(context_filter.get("css_selector"), str)
            and context_filter["css_selector"]
        ):
            container_selector_str = context_filter["css_selector"]
            logger.debug(
                f"      Using direct container css_selector from context_filter: '{container_selector_str}'"
            )
        else:
            # Build Container Selector from other context_filter fields (tag_name, text, attributes)
            container_tag = context_filter.get("tag_name", "*")
            if not isinstance(container_tag, str) or not container_tag:
                container_tag = "*"
            container_selector_parts = [container_tag]

            text_filters = context_filter.get("text")
            if isinstance(text_filters, str) and text_filters:
                text_filters = [text_filters]
            if isinstance(text_filters, list):
                for text_val in text_filters:
                    if isinstance(text_val, str) and text_val:
                        escaped_text = escape_css_selector_value(text_val)
                        container_selector_parts.append(f":has-text('{escaped_text}')")
                    else:
                        logger.warning(
                            f"      Ignoring invalid text filter item: {text_val}"
                        )

            attr_filters = context_filter.get("attributes")
            if isinstance(attr_filters, dict):
                for attr_name, attr_value in attr_filters.items():
                    if (
                        isinstance(attr_name, str)
                        and attr_name
                        and attr_value is not None
                    ):
                        escaped_value = escape_css_selector_value(str(attr_value))
                        container_selector_parts.append(
                            f'[{attr_name}="{escaped_value}"]'
                        )
                    else:
                        logger.warning(
                            f"      Ignoring invalid attribute filter item: {attr_name}={attr_value}"
                        )

            if (
                len(container_selector_parts) > 1
            ):  # Only join if there are filters beyond tag_name
                container_selector_str = "".join(container_selector_parts)
                logger.debug(
                    f"      Constructed container selector: '{container_selector_str}'"
                )
            else:
                logger.warning(
                    "      Could not construct a meaningful container selector from filter criteria (tag/text/attrs)."
                )
                return None  # Cannot proceed without a container selector

        # --- Double check selector was determined ---
        if not container_selector_str:
            logger.error(
                "      Internal Error: Container selector string is empty after processing context_filter."
            )
            return None

        # --- 1. Determine Target Selector (within the container) ---
        # <<< MODIFICATION START: Check for target_selector_override first >>>
        target_selector_override = context_filter.get("target_selector_override")
        if target_selector_override and isinstance(target_selector_override, str):
            target_selector_str = target_selector_override
            logger.debug(
                f"      Using target selector override: '{target_selector_str}'"
            )
        else:
            # Fallback to guessing if override is not provided
            logger.debug(
                "      No target_selector_override found, guessing target selector..."
            )
            target_selector_str = await self._get_target_selector_str(
                element_info, command_info
            )
        # <<< MODIFICATION END >>>

        if not target_selector_str:
            logger.warning(
                "      Could not determine a selector for the target element (neither override nor guess worked)."
            )
            return None
        logger.debug(f"      Final target selector determined: '{target_selector_str}'")

        # --- 3. Build and Verify Final Locator ---
        try:
            container_locator = self.page.locator(container_selector_str)
            container_count = await container_locator.count()
            logger.debug(
                f"      Container locator '{container_selector_str}' found {container_count} element(s)."
            )
            if container_count == 0:
                logger.warning("      Container specified by filter not found.")
                return None
            if container_count > 1:
                logger.warning(
                    f"      Multiple containers ({container_count}) found for filter. Targeting within the first one found."
                )

            final_locator = container_locator.locator(
                target_selector_str
            )  # Target within the container(s)

            count = await final_locator.count()
            logger.debug(
                f"      Found {count} final candidate(s) using advanced context filter."
            )

            if count == 1:
                # Minimal verification recommended even if unique
                passed_verification = await self._verify_element_match(
                    final_locator.first,
                    command_info,
                    element_info,
                    verify_properties=False,
                )
                if passed_verification:
                    logger.info(
                        "      ✓ Unique target found and passed basic verification."
                    )
                    return final_locator
                else:
                    logger.warning("      Unique target failed basic verification.")
                    return None  # Failed verification
            elif count > 1:
                logger.warning(
                    f"      Found {count} targets matching filter. Attempting disambiguation..."
                )
                # If multiple targets *within* the container(s), disambiguation is needed
                best_match = await self._find_best_verified_match(
                    final_locator,
                    command_info,
                    element_info,
                    initial_locator_ambiguous=True,
                )
                if best_match:
                    logger.info("      ✓ Disambiguated target found within context.")
                    return best_match
                else:
                    logger.warning(
                        "      Disambiguation failed for targets within context."
                    )
                    return None
            else:  # count == 0
                logger.warning(
                    "      No element found matching the target selector within the specified container."
                )
                return None

        except Exception as e:
            logger.error(
                f"      Error applying advanced context filter (Container: '{container_selector_str}', Target: '{target_selector_str}'): {e}",
                exc_info=True,
            )
            return None

    async def _get_target_selector_str(
        self, element_info: dict, command_info: CommandInfo
    ) -> str | None:
        """
        Helper to determine a plausible selector string for the TARGET element
        based on its historical element_info. Tries a few common strategies.
        Returns the selector string (CSS or XPath).
        """
        logger.debug("        Determining target selector string from element_info...")

        # Priority 1: Role and Name (if available and specific)
        acc = element_info.get("accessibility", {})
        attrs = element_info.get("attributes", {})
        role = acc.get("role") or attrs.get("role")
        acc_name = acc.get("name") or acc.get("aria_label")
        tag_name = element_info.get(
            "type", "*"
        )  # Get tag from element_info if possible

        if role and acc_name:
            name_clean = " ".join(acc_name.split())
            if 0 < len(name_clean) < self.MAX_TEXT_MATCH_LENGTH:
                # Simple attribute selector for role + basic name check
                # Note: Playwright's get_by_role is harder to represent as a simple string here
                escaped_name = escape_css_selector_value(name_clean)
                selector = f"{tag_name}[role='{role}']:has-text('{escaped_name}')"  # Approximate
                logger.debug(
                    f"          Target selector (Role/Name approx): {selector}"
                )
                return selector

        # Priority 2: Text Content (if available and specific)
        text_content = element_info.get("text")
        if text_content:
            text_clean = " ".join(text_content.split())
            if 0 < len(text_clean) < self.MAX_TEXT_MATCH_LENGTH:
                escaped_text = escape_css_selector_value(text_clean)
                selector = f"{tag_name}:has-text('{escaped_text}')"
                logger.debug(f"          Target selector (Tag/Text): {selector}")
                return selector

        # Priority 3: Specific Attributes (e.g., testid, name) - check if they exist in element_info
        if isinstance(attrs, dict):
            data_attrs = attrs.get("data_attributes", {})
            test_id = data_attrs.get("testid") if isinstance(data_attrs, dict) else None
            if test_id:
                selector = f'[data-testid="{escape_css_selector_value(str(test_id))}"]'
                logger.debug(f"          Target selector (Test ID): {selector}")
                return selector

            name_attr = attrs.get("name")
            if name_attr:
                selector = (
                    f'{tag_name}[name="{escape_css_selector_value(str(name_attr))}"]'
                )
                logger.debug(f"          Target selector (Name Attr): {selector}")
                return selector

        # Fallback: Just use the tag name if provided
        if tag_name != "*":
            logger.debug(f"          Target selector (Tag only): {tag_name}")
            return tag_name

        logger.warning("        Could not determine a reliable target selector string.")
        return None

    # --- Keep the original _try_contextual_filter for simple context_text ---
    async def _try_contextual_filter(
        self, element_info: dict, command_info: CommandInfo
    ) -> Locator | None:
        # ... (Implementation from the previous answer remains here) ...
        # This handles the simpler case where only `context_text` is provided.
        context_text = command_info.get("context_text")
        if not context_text:
            logger.debug(
                "    Skipping simple contextual filter: No context_text provided."
            )
            return None

        logger.debug(
            f"    Applying simple contextual filter using text: '{context_text}'"
        )

        # 1. Determine base selector for TARGET (reuse helper)
        target_selector_str = await self._get_target_selector_str(
            element_info, command_info
        )
        if not target_selector_str:
            logger.warning(
                "      Could not determine base selector for target (simple context)."
            )
            return None
        logger.debug(
            f"      Base target selector (simple context): '{target_selector_str}'"
        )

        # 2. Determine CONTAINER selector (simpler version using only :has-text)
        escaped_context = escape_css_selector_value(context_text)
        container_selectors_to_try = [
            f"div:has-text('{escaped_context}')",  # Common containers
            f"li:has-text('{escaped_context}')",
            f"tr:has-text('{escaped_context}')",  # Table rows
            f"*:has-text('{escaped_context}')",  # Any element as last resort
        ]

        container_locator: Locator | None = None
        found_container_selector = None
        for cs in container_selectors_to_try:
            try:
                # logger.debug(f"      Trying simple container selector: {cs}") # Can be noisy
                temp_container_locator = self.page.locator(cs)
                count = await temp_container_locator.count()
                if count > 0:
                    if count > 1:
                        logger.warning(
                            f"      Multiple simple containers ({count}) found using '{cs}'. Using first."
                        )
                    container_locator = temp_container_locator.first
                    found_container_selector = cs
                    # logger.debug(f"      Found simple container using: {cs}")
                    break
            except Exception:
                pass  # Ignore errors trying selectors

        if not container_locator:
            logger.warning(
                f"      Could not find simple container for context: '{context_text}'"
            )
            return None

        # 3. Locate target within container
        try:
            final_locator = container_locator.locator(target_selector_str)
            count = await final_locator.count()
            logger.debug(f"      Found {count} target(s) using simple context filter.")

            if count == 1:
                # Minimal verification check might be good here too
                passed = await self._verify_element_match(
                    final_locator.first,
                    command_info,
                    element_info,
                    verify_properties=False,
                )
                if passed:
                    logger.info(
                        "      ✓ Unique target found and passed basic verification (simple context)."
                    )
                    return final_locator
                else:
                    logger.warning(
                        "      Unique target failed basic verification (simple context)."
                    )
                    return None
            elif count > 1:
                logger.warning(
                    f"      Found {count} targets matching simple context. Returning group."
                )
                return final_locator  # Return group
            else:
                logger.warning(
                    "      Target not found within simple context container."
                )
                return None

        except Exception as e:
            logger.error(
                f"      Error locating target within simple container: {e}",
                exc_info=True,
            )
            return None

    # --- Strategy Implementations ---

    async def _try_role_and_name(
        self, element_info: dict, command_info: CommandInfo
    ) -> Locator | None:
        """Tries Playwright's get_by_role with accessible name."""
        acc = element_info.get("accessibility", {})
        attrs = element_info.get("attributes", {})
        role = acc.get("role") or attrs.get("role")
        tag_name = command_info.get("element_type", "")  # Default to empty string

        # Infer role if not explicitly set
        if not role and tag_name:  # Check if tag_name is not empty
            tag_name_lower = tag_name.lower()
            role_map = {
                "button": "button",
                "a": "link",
                "input": "textbox",
                "select": "combobox",
                "textarea": "textbox",
            }
            role = role_map.get(tag_name_lower)
            if tag_name_lower == "input":
                input_type = attrs.get("type")
                if input_type in ["button", "submit", "reset", "image"]:
                    role = "button"
                elif input_type == "checkbox":
                    role = "checkbox"
                elif input_type == "radio":
                    role = "radio"
                elif input_type == "search":
                    role = "searchbox"

        if not role:
            logger.debug("    No valid role found/inferred for get_by_role.")
            return None

        # Prepare names to try (AccName, TextContent, Title)
        names_to_try: list[str] = []
        acc_name = acc.get("name") or acc.get("aria_label")
        full_text = element_info.get("text")  # This is the historical text content

        # Priority 1: Accessible Name (or aria-label)
        if acc_name:
            cleaned_name = " ".join(acc_name.split())
            if 0 < len(cleaned_name) < self.MAX_TEXT_MATCH_LENGTH:
                names_to_try.append(cleaned_name)
                logger.debug(f"    Adding name from accessibility: '{cleaned_name}'")

        # Priority 2: Historical Text Content (if different from Acc Name)
        if full_text:
            cleaned_full_text = " ".join(full_text.split())
            if 0 < len(cleaned_full_text) < self.MAX_TEXT_MATCH_LENGTH:
                # Add only if different from acc_name to avoid redundant checks
                if not names_to_try or cleaned_full_text != names_to_try[0]:
                    # *** CORRECTED LINE ***
                    names_to_try.append(cleaned_full_text)
                    logger.debug(
                        f"    Adding name from historical text content: '{cleaned_full_text}'"
                    )

        # Fallback to title if no other name found
        if not names_to_try:
            title_name = attrs.get("title")
            if title_name:
                cleaned_title = " ".join(title_name.split())
                if 0 < len(cleaned_title) < self.MAX_TEXT_MATCH_LENGTH:
                    names_to_try.append(cleaned_title)
                    logger.debug(
                        f"    Adding name from title attribute: '{cleaned_title}'"
                    )

        if not names_to_try:
            logger.debug("    No suitable name found for get_by_role.")
            return None

        for name in names_to_try:
            logger.debug(f"    Attempting get_by_role('{role}', name='{name}')")
            try:
                if self.page.is_closed():
                    logger.warning(
                        "    Page closed during locator resolution, stopping strategy."
                    )
                    return None  # Stop if page closes

                # Use regex for case-insensitive matching
                name_pattern = re.compile(re.escape(name), re.IGNORECASE)
                locator = self.page.get_by_role(role, name=name_pattern)
                count = 0  # Initialize count
                try:
                    count = await locator.count()
                except Exception as count_err:
                    logger.warning(
                        f"      Error getting count for role/name: {count_err}",
                        exc_info=False,
                    )
                    continue  # Skip this name if count fails

                logger.debug(f"      Found {count} candidate(s).")

                if count == 1:
                    if await self._verify_element_match(
                        locator.first, command_info, element_info
                    ):
                        return locator
                    else:
                        logger.debug("      Single candidate failed verification.")
                elif count > 1:
                    best_match = await self._find_best_verified_match(
                        locator,
                        command_info,
                        element_info,
                        initial_locator_ambiguous=True,
                    )
                    if best_match:
                        return (
                            best_match  # Return the best verified match among multiples
                        )
            except Exception as e:
                logger.warning(
                    f"      get_by_role attempt failed for role='{role}', name='{name}': {type(e).__name__} - {e}",
                    exc_info=False,
                )
                if "context was destroyed" in str(e):
                    logger.error(
                        "      Context destroyed during get_by_role, stopping strategy."
                    )
                    # If context is destroyed, likely no further strategies will work
                    raise LocatorResolutionError(
                        "Context destroyed during locator resolution"
                    ) from e

        return None  # Strategy failed for all names tried

    async def _try_label(
        self, element_info: dict, command_info: CommandInfo
    ) -> Locator | None:
        """Tries Playwright's get_by_label."""
        locators_data = element_info.get("locators", {})
        label_text = (
            locators_data.get("label_text") if isinstance(locators_data, dict) else None
        )

        if not label_text:
            logger.debug("    No label_text found in command info.")
            return None
        try:
            label_text_clean = " ".join(label_text.split())
            logger.debug(f"    Attempting get_by_label('{label_text_clean}')")
            locator = self.page.get_by_label(label_text_clean)
            count = await locator.count()
            logger.debug(f"      Found {count} candidate(s).")
            if count == 1:
                if await self._verify_element_match(
                    locator.first, command_info, element_info
                ):
                    return locator
                else:
                    logger.debug("      Single candidate failed verification.")
        except Exception as e:
            logger.warning(f"      get_by_label failed: {e}", exc_info=False)
        return None

    async def _try_placeholder(
        self, element_info: dict, command_info: CommandInfo
    ) -> Locator | None:
        """Tries Playwright's get_by_placeholder."""
        attrs = element_info.get("attributes", {})
        placeholder = attrs.get("placeholder") if isinstance(attrs, dict) else None

        if not placeholder:
            logger.debug("    No placeholder found in command info.")
            return None
        try:
            placeholder_clean = " ".join(placeholder.split())
            logger.debug(f"    Attempting get_by_placeholder('{placeholder_clean}')")
            locator = self.page.get_by_placeholder(placeholder_clean)
            count = await locator.count()
            logger.debug(f"      Found {count} candidate(s).")
            if count == 1:
                if await self._verify_element_match(
                    locator.first, command_info, element_info
                ):
                    return locator
                else:
                    logger.debug("      Single candidate failed verification.")
            elif count > 1:
                best_match = await self._find_best_verified_match(
                    locator, command_info, element_info, initial_locator_ambiguous=True
                )
                if best_match:
                    return best_match
        except Exception as e:
            logger.warning(f"      get_by_placeholder failed: {e}", exc_info=False)
        return None

    async def _try_name_attribute(
        self, element_info: dict, command_info: CommandInfo
    ) -> Locator | None:
        """Tries locating using the 'name' HTML attribute."""
        attrs = element_info.get("attributes", {})
        name_attr_val = attrs.get("name") if isinstance(attrs, dict) else None
        tag_name = (
            command_info.get("element_type", "*") or "*"
        )  # Use specific tag if known

        if name_attr_val:
            # Escape double quotes within the attribute value for the CSS selector
            escaped_name_val = name_attr_val.replace('"', '\\"')
            selector = f'{tag_name}[name="{escaped_name_val}"]'
            logger.debug(f"    Attempting attribute selector: {selector}")
            try:
                locator = self.page.locator(selector)
                count = await locator.count()
                logger.debug(f"      Found {count} candidate(s).")
                if count == 1:
                    if await self._verify_element_match(
                        locator.first, command_info, element_info
                    ):
                        return locator  # Return the locator itself, not locator.first
                    else:
                        logger.debug("      Candidate failed verification.")
                elif count > 1:
                    best_match = await self._find_best_verified_match(
                        locator,
                        command_info,
                        element_info,
                        initial_locator_ambiguous=True,
                    )
                    if best_match:
                        return best_match  # Return the verified nth locator
            except Exception as e:
                logger.warning(
                    f"      Error using name attribute selector: {type(e).__name__} - {e}",
                    exc_info=False,
                )
        else:
            logger.debug("    No name attribute found.")
        return None

    async def _try_value_attribute(
        self, element_info: dict, command_info: CommandInfo
    ) -> Locator | None:
        """Tries locating input/select/textarea elements using the 'value' attribute."""
        attrs = element_info.get("attributes", {})
        value_attr = attrs.get("value") if isinstance(attrs, dict) else None
        tag_name = command_info.get("element_type", "")  # Default to empty string

        # Only apply to relevant tags and if value exists and tag_name is not empty
        if (
            not tag_name
            or tag_name.lower()
            not in ["input", "select", "textarea", "option", "button"]  # Added button
            or not value_attr
        ):
            logger.debug(
                f"    Skipping value attribute: Tag '{tag_name}' not relevant or no value found."
            )
            return None

        tag_name_lower = tag_name.lower()  # Safe to call lower

        try:
            # Escape double quotes within the attribute value for the CSS selector
            # Values can be sensitive, compare exact value.
            escaped_value = str(value_attr).replace('"', '\\\\"')
            selector = (
                f'{tag_name_lower}[value="{escaped_value}"]'  # Use lowercased tag
            )
            logger.debug(f"    Attempting value attribute selector: {selector}")

            locator = self.page.locator(selector)
            count = await locator.count()
            logger.debug(f"      Found {count} candidate(s).")

            if count == 1:
                # Verify the match
                if await self._verify_element_match(
                    locator.first, command_info, element_info
                ):
                    return locator  # Return locator itself
                else:
                    logger.debug("      Single value candidate failed verification.")
            elif count > 1:
                # If multiple matches, try disambiguation
                best_match = await self._find_best_verified_match(
                    locator,
                    command_info,
                    element_info,
                    initial_locator_ambiguous=True,
                )
                if best_match:
                    return best_match  # Return specific nth locator
        except Exception as e:
            logger.warning(
                f"      Error using value attribute selector: {type(e).__name__} - {e}",
                exc_info=False,
            )
        return None

    async def _try_text_content(
        self, element_info: dict, command_info: CommandInfo
    ) -> Locator | None:
        """Tries locating using get_by_text with command text, accessible name, or visible text."""
        texts_to_try: list[str] = []

        # --- PRIORITY 1: Text directly provided in the command (for interactive click_text) ---
        cmd_text = command_info.get("text")
        if cmd_text:
            cleaned_cmd_text = " ".join(cmd_text.split())
            if 0 < len(cleaned_cmd_text) < self.MAX_TEXT_MATCH_LENGTH:
                # Add command text first
                texts_to_try.append(cleaned_cmd_text)
                logger.debug(f"    Adding text from command_info: '{cleaned_cmd_text}'")

        # --- PRIORITY 2: Historical accessible name ---
        acc_name = element_info.get("accessibility", {}).get("name")
        if acc_name:
            cleaned_name = " ".join(acc_name.split())
            if 0 < len(cleaned_name) < self.MAX_TEXT_MATCH_LENGTH:
                # Add only if different from command text
                if cleaned_name not in texts_to_try:
                    texts_to_try.append(cleaned_name)
                    logger.debug(
                        f"    Adding text from historical acc_name: '{cleaned_name}'"
                    )

        # --- PRIORITY 3: Historical element text content ---
        full_text = element_info.get("text")  # Historical element text
        if full_text:
            cleaned_full_text = " ".join(full_text.split())
            if 0 < len(cleaned_full_text) < self.MAX_TEXT_MATCH_LENGTH:
                # Add only if different from command text and acc_name
                if cleaned_full_text not in texts_to_try:
                    texts_to_try.append(cleaned_full_text)
                    logger.debug(
                        f"    Adding text from historical text content: '{cleaned_full_text}'"
                    )

        if not texts_to_try:
            logger.debug(
                "    No suitable text found (from command or history) for get_by_text."
            )
            return None

        # --- Loop through texts and attempt location/verification ---
        for text in texts_to_try:
            logger.debug(f"    Attempting get_by_text('{text[:50]}...')")
            try:
                # Try exact match first
                locator_exact = self.page.get_by_text(text, exact=True)
                count_exact = 0  # Initialize count
                try:
                    count_exact = await locator_exact.count()
                except Exception as count_err:
                    logger.warning(
                        f"      Error getting count for exact text match: {count_err}",
                        exc_info=False,
                    )
                    continue  # Skip this text if count fails

                logger.debug(f"      Found {count_exact} via exact match.")
                if count_exact == 1:
                    if await self._verify_element_match(
                        locator_exact.first, command_info, element_info
                    ):
                        logger.info(
                            f"      ✓ Exact match verified for '{text[:50]}...'."
                        )
                        return locator_exact  # Return the locator itself
                    else:
                        logger.debug(
                            "      Single exact candidate failed verification."
                        )
                elif count_exact > 1:
                    logger.debug(
                        "      Multiple exact matches found, attempting disambiguation..."
                    )
                    best_match = await self._find_best_verified_match(
                        locator_exact,
                        command_info,
                        element_info,
                        initial_locator_ambiguous=True,
                    )
                    if best_match:
                        logger.info(
                            f"      ✓ Disambiguated exact match verified for '{text[:50]}...'."
                        )
                        return best_match  # Return the specific Nth locator
                    else:
                        logger.debug("      Disambiguation failed for exact matches.")

                # If exact match failed or wasn't unique and verified, try contains match
                # Only try contains if exact match yielded nothing OR failed verification/disambiguation
                if count_exact == 0 or (
                    count_exact > 0 and not best_match
                ):  # Added condition here
                    logger.debug("      Trying contains match...")
                    locator_contains = self.page.get_by_text(text, exact=False)
                    count_contains = 0  # Initialize count
                    try:
                        count_contains = await locator_contains.count()
                    except Exception as count_err:
                        logger.warning(
                            f"      Error getting count for contains text match: {count_err}",
                            exc_info=False,
                        )
                        continue  # Skip this text if count fails

                    logger.debug(f"      Found {count_contains} via contains match.")
                    if count_contains == 1:
                        if await self._verify_element_match(
                            locator_contains.first, command_info, element_info
                        ):
                            logger.info(
                                f"      ✓ Contains match verified for '{text[:50]}...'."
                            )
                            return locator_contains  # Return the locator itself
                        else:
                            logger.debug(
                                "      Single contains candidate failed verification."
                            )
                    elif count_contains > 1:
                        logger.debug(
                            "      Multiple contains matches found, attempting disambiguation..."
                        )
                        best_match_contains = await self._find_best_verified_match(
                            locator_contains,
                            command_info,
                            element_info,
                            initial_locator_ambiguous=True,
                        )
                        if best_match_contains:
                            logger.info(
                                f"      ✓ Disambiguated contains match verified for '{text[:50]}...'."
                            )
                            return (
                                best_match_contains  # Return the specific Nth locator
                            )
                        else:
                            logger.debug(
                                "      Disambiguation failed for contains matches."
                            )

            except Exception as e:
                # Catch errors specific to get_by_text or its verification steps
                logger.warning(
                    f"      get_by_text attempt failed for '{text[:50]}...': {type(e).__name__}: {e}",
                    exc_info=False,  # Reduce noise unless debugging specific get_by_text errors
                )
                # Check for specific errors that might indicate page context issues
                if "context was destroyed" in str(e):
                    logger.error(
                        "      Context destroyed during get_by_text, stopping strategy."
                    )
                    # If context is destroyed, likely no further strategies will work
                    raise LocatorResolutionError(
                        "Context destroyed during locator resolution"
                    ) from e

        # If the loop finishes without finding a verified match for any text
        logger.debug(
            "    No verified match found using get_by_text strategy for any tried text."
        )
        return None

    async def _try_autocomplete_item(
        self, element_info: dict, command_info: CommandInfo
    ) -> Locator | None:
        """Specifically targets items within likely autocomplete containers."""
        target_tag = command_info.get("element_type", "")  # Default to empty string
        # Broaden target tags slightly
        if not target_tag or target_tag.lower() not in [
            "li",
            "div",
            "a",
            "span",
            "td",
            "th",
            "button",
            "option",
        ]:  # Check tag_name is not empty
            logger.debug(
                f"    Skipping autocomplete: Target tag '{target_tag}' not typical or missing."
            )
            return None

        target_tag_lower = target_tag.lower()  # Now safe to call lower

        text = element_info.get("accessibility", {}).get("name") or element_info.get(
            "text"
        )
        if not text:
            logger.debug("    Skipping autocomplete: No text found.")
            return None
        text_clean = " ".join(text.split())
        if not (0 < len(text_clean) < self.MAX_TEXT_MATCH_LENGTH):
            logger.debug(
                f"    Skipping autocomplete: Text invalid or too long ('{text_clean[:30]}...')."
            )
            return None

        container_selectors = [
            "ul[style*='height']",
            "div.autocomplete-suggestions",
            "ul.ui-autocomplete",
            "[role='listbox']",
            "ul[id*='typeahead']",
            "div[class*='suggestion']",
            "div[class*='results']",
            "div[class*='dropdown-menu']",
        ]
        # Escape text for CSS :has-text selector
        escaped_text = text_clean.replace("'", "\\\\'")

        try:
            for container_selector in container_selectors:
                logger.debug(
                    f"    Checking within container selector: '{container_selector}'"
                )
                container_locator = self.page.locator(container_selector)
                container_count = await container_locator.count()
                logger.debug(f"      Found {container_count} potential container(s).")
                if container_count == 0:
                    continue

                # Check only the first few containers for performance
                for i in range(min(container_count, 3)):
                    specific_container = container_locator.nth(i)
                    try:
                        # Check visibility briefly
                        await specific_container.wait_for(state="visible", timeout=1000)
                        logger.debug(f"      Container {i} is visible.")
                    except PlaywrightTimeoutError:
                        logger.debug(f"      Container {i} not visible quickly.")
                        continue  # Skip non-visible container

                    # Search for the item within this specific visible container
                    item_selector = f"{target_tag_lower}:has-text('{escaped_text}')"  # Use lowercased tag
                    item_locator_in_container = specific_container.locator(
                        item_selector
                    )
                    item_count = await item_locator_in_container.count()

                    if item_count > 0:
                        logger.debug(
                            f"      Found {item_count} candidate item(s) for '{item_selector}' within container {i}."
                        )
                        # Verify candidates within this container
                        best_match = await self._find_best_verified_match(
                            item_locator_in_container,
                            command_info,
                            element_info,
                            initial_locator_ambiguous=True,
                        )
                        if best_match:
                            logger.info(
                                f"      Found verified autocomplete item within container: '{container_selector}'"
                            )
                            return best_match  # Return the first verified match found across containers

        except Exception as e:
            logger.warning(
                f"    Error during autocomplete strategy: {type(e).__name__}: {e}",
                exc_info=False,
            )

        logger.debug("    No verified item found using autocomplete strategy.")
        return None

    async def _try_tag_and_text(
        self, element_info: dict, command_info: CommandInfo
    ) -> Locator | None:
        """Tries a simple 'tag:has-text("text")' selector."""
        tag_name = command_info.get("element_type", "")  # Default to empty string
        text = element_info.get("accessibility", {}).get("name") or element_info.get(
            "text"
        )

        if not (tag_name and text):  # Check both are not empty/None
            logger.debug("    Missing tag or text for tag_and_text strategy.")
            return None

        tag_name_lower = tag_name.lower()  # Safe to call lower now

        text_clean = " ".join(text.split())
        if 0 < len(text_clean) < self.MAX_TEXT_MATCH_LENGTH:
            try:
                escaped_text = text_clean.replace("'", "\\\\'")
                selector = (
                    f"{tag_name_lower}:has-text('{escaped_text}')"  # Use lowercased tag
                )
                logger.debug(f"    Attempting generic tag+text: '{selector}'")
                locator = self.page.locator(selector)
                count = await locator.count()
                logger.debug(f"      Found {count} candidate(s).")
                if count == 1:
                    if await self._verify_element_match(
                        locator.first, command_info, element_info
                    ):
                        return locator
                    else:
                        logger.debug("      Single candidate failed verification.")
                elif count > 1:
                    best_match = await self._find_best_verified_match(
                        locator,
                        command_info,
                        element_info,
                        initial_locator_ambiguous=True,
                    )
                    if best_match:
                        return best_match
            except Exception as e:
                logger.warning(
                    f"      Error in generic tag+text strategy: {e}", exc_info=False
                )
        else:
            logger.debug(
                f"    Text invalid or too long for tag+text: '{text_clean[:50]}...'"
            )
        return None

    async def _try_test_id(
        self, element_info: dict, command_info: CommandInfo
    ) -> Locator | None:
        """Tries Playwright's get_by_test_id."""
        attrs = element_info.get("attributes", {})
        data_attrs = attrs.get("data_attributes", {}) if isinstance(attrs, dict) else {}
        test_id = data_attrs.get("testid") if isinstance(data_attrs, dict) else None
        # Also check for 'data-pw' as a common alternative
        if not test_id and isinstance(data_attrs, dict):
            test_id = data_attrs.get("pw")

        if not test_id:
            logger.debug("    No testid or data-pw found in command info.")
            return None
        try:
            logger.debug(f"    Attempting get_by_test_id('{test_id}')")
            locator = self.page.get_by_test_id(test_id)
            count = await locator.count()
            logger.debug(f"      Found {count} candidate(s).")
            if count == 1:
                # Use minimal verification for test IDs as they should be reliable
                if await self._verify_element_match(
                    locator.first, command_info, element_info, verify_properties=False
                ):
                    return locator
                else:
                    logger.warning(
                        "      Single test ID candidate failed minimal verification."
                    )
            # Don't usually try to find best match for test IDs, assume they are unique
            elif count > 1:
                logger.warning(
                    f"      Found multiple elements ({count}) matching test ID '{test_id}'. Strategy skipped."
                )

        except Exception as e:
            logger.warning(f"      Error in get_by_test_id: {e}", exc_info=False)
        return None

    async def _try_relative_locator(
        self, element_info: dict, command_info: CommandInfo
    ) -> Locator | None:
        """Tries locating +/- buttons relative to 'Adults'/'Children' text."""
        target_tag = command_info.get("element_type", "")  # Default to empty string
        hist_attrs = element_info.get("attributes", {})
        hist_data_attrs = (
            hist_attrs.get("data_attributes", {})
            if isinstance(hist_attrs, dict)
            else {}
        )
        hist_ng_click = (
            hist_data_attrs.get("ngClick")
            if isinstance(hist_data_attrs, dict)
            else None
        )

        # Only apply this specific relative logic to anchor tags with ngClick containing Room counts
        if (
            not target_tag
            or target_tag.lower() != "a"
            or not hist_ng_click
            or "Room" not in hist_ng_click
        ):  # Check tag_name is not empty
            logger.debug(
                f"    Skipping relative: Not a +/- room occupant button based on tag '{target_tag}'/ngClick '{hist_ng_click}'."
            )
            return None

        target_tag_lower = target_tag.lower()  # Safe to call lower

        is_decrease = "decrease" in hist_ng_click
        is_increase = "increase" in hist_ng_click
        icon_class = "fa-minus" if is_decrease else ("fa-plus" if is_increase else None)

        if not icon_class:
            logger.debug("    Skipping relative: Cannot determine icon class.")
            return None

        related_text = None
        if "Adult" in hist_ng_click:
            related_text = "Adults"
        elif "Child" in hist_ng_click:
            related_text = "Children"
        elif "Infant" in hist_ng_click:
            related_text = "Infant"  # Allow singular 'Infant' or 'Infants'
        # Add more room/guest types if necessary

        if not related_text:
            logger.debug(
                "    Skipping relative: Cannot determine related text (Adults/Children/...)."
            )
            return None

        logger.debug(
            f"    Attempting relative: Find '{related_text}', then '{target_tag_lower}' with icon '{icon_class}'"
        )
        try:
            # Find the parent list item containing the label text
            # Use regex for flexibility (Adult/Adults)
            related_text_pattern = re.compile(related_text, re.IGNORECASE)
            parent_li_locator = self.page.locator(
                f"li:has(div.box_lft:has-text({related_text_pattern}))"
            )

            count = await parent_li_locator.count()
            logger.debug(
                f"      Found {count} parent LI(s) containing '{related_text}'."
            )

            if count == 1:
                # Find the specific +/- anchor within that LI
                target_locator = parent_li_locator.locator(
                    f"a.icon_click:has(i.{icon_class})"
                )
                target_count = await target_locator.count()
                logger.debug(
                    f"      Found {target_count} target anchor(s) within the LI."
                )
                if target_count == 1:
                    # Use minimal verification as relative position is the main goal
                    if await self._verify_element_match(
                        target_locator.first,
                        command_info,
                        element_info,
                        verify_properties=False,
                    ):
                        # Return the specific nth locator (which is .first here)
                        # Playwright locators automatically narrow, so return the final specific one
                        return target_locator.first
                    else:
                        logger.warning(
                            "      Relative candidate failed minimal verification."
                        )
                else:
                    logger.warning(f"      Found {target_count} anchors, expected 1.")
            elif count > 1:
                logger.warning(
                    "      Found multiple parent LIs, strategy too ambiguous."
                )
        except Exception as e:
            logger.warning(
                f"      Relative locator strategy error: {e}", exc_info=False
            )
        return None

    async def _try_attribute_selector(
        self, element_info: dict, command_info: CommandInfo
    ) -> Locator | None:
        """Tries to locate using specific data-* attributes like data-ng-click."""
        attrs = element_info.get("attributes", {})
        data_attrs = attrs.get("data_attributes", {}) if isinstance(attrs, dict) else {}
        target_tag = command_info.get("element_type", "*") or "*"  # Default to *
        # Use lower case tag in selector if specific tag is known
        target_tag_selector = target_tag.lower() if target_tag != "*" else "*"

        # Example: Prioritize data-ng-click
        ng_click_val = (
            data_attrs.get("ngClick") if isinstance(data_attrs, dict) else None
        )
        if ng_click_val:
            # Escape double quotes for the selector
            escaped_val = ng_click_val.replace('"', '\\\\"')
            selector = f'{target_tag_selector}[data-ng-click="{escaped_val}"]'  # Use lowercased tag
            logger.debug(f"    Attempting attribute selector: {selector}")
            try:
                locator = self.page.locator(selector)
                count = await locator.count()
                logger.debug(f"      Found {count} candidate(s).")
                if count == 1:
                    # Use minimal verification for specific attributes? Maybe not needed if unique.
                    if await self._verify_element_match(
                        locator.first,
                        command_info,
                        element_info,
                        verify_properties=False,
                    ):
                        return locator  # Return locator itself
                    else:
                        logger.debug("      Candidate failed minimal verification.")
                elif count > 1:
                    # If multiple matches, fall back to stricter verification
                    best_match = await self._find_best_verified_match(
                        locator,
                        command_info,
                        element_info,
                        initial_locator_ambiguous=True,
                    )
                    if best_match:
                        return best_match
            except Exception as e:
                logger.warning(
                    f"      Error using data-ng-click attribute: {e}", exc_info=False
                )
        else:
            logger.debug("    No data-ng-click attribute found.")
        # Add checks for other potentially unique data attributes here if needed
        return None

    async def _try_stored_css_verified(
        self, element_info: dict, command_info: CommandInfo
    ) -> Locator | None:
        """Tries the historically stored CSS selector and verifies the match."""
        locators_data = element_info.get("locators", {})
        css_selector = (
            locators_data.get("css_selector")
            if isinstance(locators_data, dict)
            else None
        )

        if not css_selector:
            logger.debug("    No stored CSS selector found.")
            return None
        try:
            logger.debug(
                f"    Attempting stored CSS: '{css_selector[:100]}{'...' if len(css_selector) > 100 else ''}'"
            )
            locator = self.page.locator(css_selector)
            count = await locator.count()
            logger.debug(f"      Found {count} candidate(s).")
            if count == 1:
                if await self._verify_element_match(
                    locator.first, command_info, element_info
                ):
                    return locator
                else:
                    logger.debug("      Single candidate failed verification.")
            elif count > 1:
                best_match = await self._find_best_verified_match(
                    locator, command_info, element_info, initial_locator_ambiguous=True
                )
                if best_match:
                    return best_match
        except Exception as e:
            logger.warning(f"      Error using stored CSS: {e}", exc_info=False)
        return None

    async def _try_stored_xpath_verified(
        self, element_info: dict, command_info: CommandInfo
    ) -> Locator | None:
        """Tries the historically stored XPath selector and verifies the match."""
        locators_data = element_info.get("locators", {})
        xpath = locators_data.get("xpath") if isinstance(locators_data, dict) else None

        if not xpath:
            logger.debug("    No stored XPath found.")
            return None
        try:
            # Ensure xpath starts with 'xpath=' for Playwright
            xpath_selector = xpath if xpath.startswith("xpath=") else f"xpath={xpath}"
            logger.debug(
                f"    Attempting stored XPath: '{xpath_selector[:100]}{'...' if len(xpath_selector) > 100 else ''}'"
            )
            locator = self.page.locator(xpath_selector)
            count = await locator.count()
            logger.debug(f"      Found {count} candidate(s).")
            if count == 1:
                if await self._verify_element_match(
                    locator.first, command_info, element_info
                ):
                    return locator
                else:
                    logger.debug("      Single candidate failed verification.")
            elif count > 1:
                best_match = await self._find_best_verified_match(
                    locator, command_info, element_info, initial_locator_ambiguous=True
                )
                if best_match:
                    return best_match
        except Exception as e:
            logger.warning(f"      Error using stored XPath: {e}", exc_info=False)
        return None

    async def _try_id_attribute(
        self, element_info: dict, command_info: CommandInfo
    ) -> Locator | None:
        """Tries locating using the 'id' HTML attribute, checked last due to potential dynamic IDs."""
        attrs = element_info.get("attributes", {})
        id_attr = attrs.get("id") if isinstance(attrs, dict) else None

        if not id_attr:
            logger.debug("    No ID attribute found.")
            return None
        # Skip potentially dynamic IDs
        if re.match(r"^(ember\d+|gwt-|ext-|jQuery\d+)", id_attr, re.IGNORECASE):
            logger.debug(f"    Skipping dynamic-like ID: '{id_attr}'")
            return None
        try:
            # CSS selector for ID
            css_id_selector = f"#{id_attr}"
            logger.debug(f"    Attempting ID attribute selector: '{css_id_selector}'")
            locator = self.page.locator(css_id_selector)
            count = await locator.count()
            logger.debug(f"      Found {count} candidate(s).")
            if count == 1:
                # Verify even if ID matches, as IDs are not always unique in practice
                if await self._verify_element_match(
                    locator.first, command_info, element_info
                ):
                    return locator
                else:
                    logger.warning("      Single ID candidate failed verification.")
            elif count > 1:
                logger.warning(
                    f"      Found multiple elements ({count}) for ID '{id_attr}'. ID is not unique."
                )
                # Optionally try verification if multiple elements share an ID
                # best_match = await self._find_best_verified_match(locator, command_info, element_info)
                # if best_match: return best_match

        except Exception as e:
            logger.warning(f"      Error using ID attribute: {e}", exc_info=False)
        return None

    async def _try_simple_tag(
        self, element_info: dict, command_info: CommandInfo
    ) -> Locator | None:
        """Tries locating purely by tag name, relying on verification to disambiguate."""
        tag_name = command_info.get("element_type", "")  # Default to empty string

        if not tag_name:
            logger.debug(
                "    Skipping simple tag: No element_type found in command info."
            )
            return None

        tag_name_lower = tag_name.lower()  # Safe to call lower

        try:
            logger.debug(f"    Attempting simple tag locator: '{tag_name_lower}'")
            locator = self.page.locator(tag_name_lower)

            count = 0  # Initialize count
            try:
                count = await locator.count()
            except Exception as count_err:
                logger.warning(
                    f"      Error getting count for tag '{tag_name_lower}': {count_err}",
                    exc_info=False,
                )
                return None  # Skip if count fails

            logger.debug(
                f"      Found {count} candidate(s) for tag '{tag_name_lower}'."
            )

            if count == 0:
                return None  # No elements found with this tag

            # If candidates found, immediately try to find the best verified match
            # This relies on _verify_element_match comparing properties (like value)
            logger.debug(
                f"      Passing {count} candidates to verification/disambiguation..."
            )
            best_match = await self._find_best_verified_match(
                locator,
                command_info,
                element_info,
                initial_locator_ambiguous=(
                    count > 1
                ),  # Mark as ambiguous if more than one
            )

            if best_match:
                logger.info(
                    f"      ✓ Verified match found using simple tag '{tag_name_lower}' + property verification."
                )
                return best_match  # Return the specific nth locator verified
            else:
                logger.debug(
                    f"      No candidates passed verification for simple tag '{tag_name_lower}'."
                )

        except Exception as e:
            logger.warning(
                f"      Error using simple tag strategy: {type(e).__name__} - {e}",
                exc_info=False,
            )
        return None

    # --- Verification and Helper Methods ---

    async def _get_current_element_properties(self, locator: Locator) -> dict[str, Any]:
        """Gets key properties of the element identified by the locator for verification."""
        props: dict[str, Any] = {
            "tag_name": None,
            "text": None,
            "acc_name": None,
            "attrs": {},
        }
        # Use evaluate to get multiple properties efficiently
        # *** MODIFIED JS CODE TO EXPECT SINGLE ELEMENT 'el' ***
        js_code = """el => {
            if (!el) return null;
            const attrs = {};
            const dataAttrs = {};
            for (const attr of el.attributes) {
                if (attr.name.startsWith('data-')) {
                    // Basic camelCase conversion for dataset keys
                    const key = attr.name.substring(5).replace(/-([a-z])/g, (g) => g[1].toUpperCase());
                    // Ensure value is stringified if it's not already (e.g., boolean/number data attrs)
                    dataAttrs[key] = String(attr.value);
                } else {
                    // Ensure value is stringified
                    attrs[attr.name] = String(attr.value);
                }
            }
            attrs['data_attributes'] = dataAttrs; // Add processed data attributes

            // Attempt to get accessible name (more robustly)
            let accName = el.getAttribute('aria-label') || '';
            if (!accName) {
                const labelledBy = el.getAttribute('aria-labelledby');
                if (labelledBy) {
                    accName = labelledBy.split(' ')
                                    .map(id => document.getElementById(id)?.textContent?.trim())
                                    .filter(Boolean)
                                    .join(' ') || '';
                }
            }
            if (!accName && el.labels && el.labels.length > 0) {
                accName = Array.from(el.labels).map(lbl => lbl.textContent?.trim()).filter(Boolean).join(' ') || '';
            }
            if (!accName) { accName = el.title || ''; }
            if (!accName) { accName = el.textContent || ''; } // Fallback to textContent

            return {
                tag: el.tagName.toLowerCase(),
                // Ensure text content is handled correctly
                txt: (el.textContent || '').trim().replace(/\\s+/g, ' '),
                // Ensure accessible name is handled correctly
                acc: (accName || '').trim().replace(/\\s+/g, ' '),
                attrs: attrs
            };
        } // Removed the .map() and [0] access
        """
        try:
            # Evaluate on the first matching element
            el_props = await locator.first.evaluate(
                js_code, timeout=1500
            )  # Evaluate on first element
            if el_props:
                props["tag_name"] = el_props.get("tag")
                props["text"] = el_props.get("txt", "")
                props["acc_name"] = el_props.get("acc")
                props["attrs"] = el_props.get("attrs", {})
                # Add class string for easier comparison later if needed
                # Ensure 'class' attribute value is a string
                props["attrs"]["class_str"] = str(props["attrs"].get("class", ""))
            else:
                logger.warning(
                    "      _get_current_element_properties: evaluate returned null."
                )

        except Exception as e:
            # ... (rest of the fallback logic remains the same) ...
            logger.warning(
                f"      _get_current_element_properties: evaluate failed ({type(e).__name__}: {e}), falling back to individual gets.",
                exc_info=False,  # Less noise for common eval errors
            )
            # Fallback gets (less efficient)
            try:
                props["tag_name"] = await locator.first.evaluate(
                    "el => el.tagName.toLowerCase()", timeout=300
                )
            except Exception:
                pass
            try:
                props["text"] = await locator.first.text_content(timeout=500) or ""
            except Exception:
                pass
            # Simplified accessible name fallback
            try:
                props["acc_name"] = await locator.first.get_attribute(
                    "aria-label", timeout=300
                )
                if not props["acc_name"]:
                    props["acc_name"] = await locator.first.get_attribute(
                        "title", timeout=300
                    )
            except Exception:
                pass

            key_attrs = [
                "id",
                "name",
                "placeholder",
                "value",
                "type",
                "href",
                "role",
                "class",
            ]
            for attr in key_attrs:
                try:
                    val = await locator.first.get_attribute(attr, timeout=300)
                    # Ensure attribute value is stored as string or None
                    props["attrs"][attr] = str(val) if val is not None else None
                except Exception:
                    props["attrs"][attr] = None
            try:
                # Ensure dataset values are strings
                ds = await locator.first.evaluate("el => el.dataset", timeout=300) or {}
                props["attrs"]["data_attributes"] = {k: str(v) for k, v in ds.items()}
            except Exception:
                props["attrs"]["data_attributes"] = {}
            # Ensure class_str is derived correctly after fallbacks
            props["attrs"]["class_str"] = str(props["attrs"].get("class", ""))

        return props

    async def _verify_element_match(
        self,
        locator: Locator,
        command_info: CommandInfo,
        element_info: dict,
        verify_properties: bool = True,
        similarity_threshold: float = 0.6,
        # NEW: Parameter to control return type
        return_score: bool = False,
    ) -> bool | tuple[float, bool]:  # Return type depends on return_score
        """
        Verifies if the found locator likely corresponds to the original element.
        Handles readonly inputs as potentially interactable for clicks.
        Can optionally return the score and basic check status instead of just True/False.
        """
        logger.debug("      Verifying candidate element...")
        passed_basic_checks = False  # Track if visibility/enabled checks pass
        try:
            # --- Basic Checks: Visibility & Enabled State ---
            is_visible = await locator.is_visible(timeout=1000)
            if not is_visible:
                logger.debug("        Failed Basic Check: Not visible.")
                # If return_score is True, return 0 score but indicate basic check failure
                return (0.0, False) if return_score else False
            else:
                logger.debug("        Visible: OK")

            # Check enabled state only if the command requires interaction
            non_interactive_commands = [
                "goto",
                "scroll",
                "wait_for_navigation",
                "get_page_text",
                "copy_text",  # copy_text doesn't require element to be enabled
                "verify_element_visible",
                "save_state",
                "get_page_html",
                "start_network",
                "stop_network",
                "get_network",
                "wait_network",
                "wait_for_network_idle",
                "wait_for_element",
                "wait_for_next_element",
                "verify_element_text_policy",  # This verification doesn't need enabled state
                "verify_price_summary",  # Page level verification
            ]
            command_type = command_info.get("command_type")

            if command_type not in non_interactive_commands:
                is_currently_enabled = await locator.is_enabled(timeout=1000)

                # Check for readonly attribute specifically for inputs/textareas
                is_currently_readonly = False
                try:
                    tag = await locator.evaluate(
                        "el => el.tagName.toLowerCase()", timeout=200
                    )
                    if tag in ["input", "textarea"]:
                        is_currently_readonly = await locator.evaluate(
                            "el => el.readOnly", timeout=200
                        )
                        if is_currently_readonly:
                            logger.debug(
                                "        Attribute Check: Element is readonly."
                            )
                except Exception as read_err:
                    logger.warning(
                        f"        Could not check readonly status: {read_err}",
                        exc_info=False,
                    )

                # --- MODIFICATION START: Safely access historical state ---
                original_state = element_info.get("state")  # Get potentially None
                was_originally_enabled = True  # Default assumption
                if isinstance(original_state, dict):
                    was_originally_enabled = original_state.get("is_enabled", True)
                # --- MODIFICATION END ---

                if not is_currently_enabled:
                    # Allow match IF:
                    # 1. It was originally disabled too.
                    # 2. OR it's currently readonly (often clickable to open datepickers etc.)
                    if was_originally_enabled and not is_currently_readonly:
                        logger.debug(
                            "        Failed Basic Check: Not enabled and not readonly (originally was enabled)."
                        )
                        return (0.0, False) if return_score else False
                    elif is_currently_readonly:
                        logger.debug(
                            "        Info: Matched element is readonly (treating as interactable for verification)."
                        )
                        passed_basic_checks = True
                    else:  # Was originally disabled, and still is
                        logger.debug(
                            "        Info: Matched disabled element (was originally disabled)."
                        )
                        passed_basic_checks = True
                else:  # Currently enabled
                    logger.debug("        Enabled: OK")
                    passed_basic_checks = True
            else:
                # If no enabled check needed (non-interactive command), consider basic checks passed if visible
                logger.debug(
                    f"        Skipping enabled check (non-interactive command: {command_type})."
                )
                passed_basic_checks = True

            # --- Property Comparison (Only if basic checks passed OR verify_properties is skipped) ---
            if not verify_properties:
                if passed_basic_checks:
                    logger.debug(
                        "        Passed basic checks (property verification skipped)."
                    )
                    return (1.0, True) if return_score else True
                else:
                    logger.debug(
                        "        Failed basic checks (property verification skipped)."
                    )
                    return (0.0, False) if return_score else False

            if not passed_basic_checks:
                logger.debug(
                    "        Skipping property comparison due to failed basic checks."
                )
                return (0.0, False) if return_score else False

            # --- Property Comparison Logic ---
            logger.debug("        Comparing properties...")
            current_props = await self._get_current_element_properties(locator)

            # --- MODIFICATION START: Safely get historical data ---
            hist_attrs = element_info.get("attributes")  # Get potentially None
            hist_acc = element_info.get("accessibility")  # Get potentially None
            # --- MODIFICATION END ---

            hist_type = command_info.get("element_type")
            hist_type_lower = hist_type.lower() if isinstance(hist_type, str) else None
            hist_text = " ".join((element_info.get("text") or "").split())

            match_score = 0.0
            total_checks = 0.0
            WEIGHT_TAG = 1.0
            WEIGHT_ACCNAME = 1.5
            WEIGHT_TEXT = 1.0
            WEIGHT_NAME_ATTR = 2.0
            WEIGHT_PLACEHOLDER = 1.0
            WEIGHT_VALUE = 1.0
            WEIGHT_CLASS = 0.5

            # Tag Name Check
            if hist_type_lower and current_props["tag_name"]:
                total_checks += WEIGHT_TAG
                if hist_type_lower == current_props["tag_name"]:
                    match_score += WEIGHT_TAG
                    logger.debug(f"          Tag: OK ({hist_type_lower})")
                else:
                    logger.debug(
                        f"          Tag: MISMATCH (Expected: {hist_type_lower}, Found: {current_props['tag_name']})"
                    )

            # AccName/Text Check
            hist_acc_name = None
            # --- MODIFICATION START: Check if hist_acc is a dict ---
            if isinstance(hist_acc, dict):
                hist_acc_name = hist_acc.get("name") or hist_acc.get("aria_label")
            # --- MODIFICATION END ---

            curr_acc_name = current_props["acc_name"]

            # Prioritize comparing accessible names if both exist
            if (
                hist_acc_name and curr_acc_name
            ):  # Checks if hist_acc_name was successfully retrieved
                total_checks += WEIGHT_ACCNAME
                hist_acc_name_clean = " ".join(hist_acc_name.split())
                if hist_acc_name_clean.lower() == curr_acc_name.lower():
                    match_score += WEIGHT_ACCNAME
                    logger.debug(f"          AccName: OK ('{hist_acc_name_clean}')")
                else:
                    logger.debug(
                        f"          AccName: MISMATCH (Expected: '{hist_acc_name_clean}', Found: '{curr_acc_name}')"
                    )
            # Fallback to visible text if AccName missing or didn't match strongly
            elif hist_text and current_props["text"] is not None:
                total_checks += WEIGHT_TEXT
                curr_text_clean = current_props["text"]
                if hist_text.lower() in curr_text_clean.lower():
                    match_score += WEIGHT_TEXT
                    logger.debug(
                        f"          Text: OK (Hist:'{hist_text}' found in Curr:'{curr_text_clean[:50]}...') - Used as fallback"
                    )
                else:
                    logger.debug(
                        f"          Text: MISMATCH (Hist:'{hist_text}', Curr:'{curr_text_clean[:50]}...') - Used as fallback"
                    )

            # Key Attributes Check (Placeholder, Name, Value)
            key_hist_attrs = {}
            # --- MODIFICATION START: Check if hist_attrs is a dict ---
            if isinstance(hist_attrs, dict):
                # --- MODIFICATION END ---
                key_hist_attrs["placeholder"] = hist_attrs.get("placeholder")
                key_hist_attrs["name"] = hist_attrs.get("name")
                value_relevant_tags = [
                    "input",
                    "option",
                    "button",
                    "textarea",
                    "select",
                    "meter",
                    "progress",
                    "param",
                ]
                if (
                    hist_type_lower in value_relevant_tags
                    and hist_attrs.get("value") is not None
                ):
                    key_hist_attrs["value"] = hist_attrs.get("value")

            key_curr_attrs = current_props.get("attrs", {})
            # Loop through key_hist_attrs (which might be empty if hist_attrs was None)
            for attr, hist_val in key_hist_attrs.items():
                if hist_val is not None:  # Only compare if historical value exists
                    weight = (
                        WEIGHT_NAME_ATTR
                        if attr == "name"
                        else (WEIGHT_VALUE if attr == "value" else WEIGHT_PLACEHOLDER)
                    )
                    total_checks += weight
                    curr_val = key_curr_attrs.get(attr)

                    hist_val_norm = str(hist_val) if hist_val is not None else ""
                    curr_val_norm = curr_val if curr_val is not None else ""

                    if attr in ["placeholder", "name"]:
                        if hist_val_norm.lower() == curr_val_norm.lower():
                            match_score += weight
                            logger.debug(
                                f"          Attr[{attr}]: OK ('{hist_val_norm}')"
                            )
                        else:
                            logger.debug(
                                f"          Attr[{attr}]: MISMATCH (Expected: '{hist_val_norm}', Found: '{curr_val_norm}')"
                            )
                    else:  # Case-sensitive for value
                        if hist_val_norm == curr_val_norm:
                            match_score += weight
                            logger.debug(
                                f"          Attr[{attr}]: OK ('{hist_val_norm}')"
                            )
                        else:
                            logger.debug(
                                f"          Attr[{attr}]: MISMATCH (Expected: '{hist_val_norm}', Found: '{curr_val_norm}')"
                            )

            # Class List Check (Overlap - lenient check)
            hist_class_list_raw = None
            # --- MODIFICATION START: Check if hist_attrs is a dict ---
            if isinstance(hist_attrs, dict):
                hist_class_list_raw = hist_attrs.get("class_list", [])
            # --- MODIFICATION END ---

            hist_class_list = set(
                hist_class_list_raw if isinstance(hist_class_list_raw, list) else []
            )
            common_dynamic_classes = {
                "selected",
                "active",
                "focus",
                "hover",
                "visited",
                "ng-dirty",
                "ng-touched",
                "ng-valid",
                "ng-invalid",
                "ng-pending",
                "ng-pristine",
                "ng-not-empty",
            }
            hist_class_list_filtered = {
                cls
                for cls in hist_class_list
                if cls
                and not cls.startswith("ng-")
                and cls not in common_dynamic_classes
            }

            if (
                hist_class_list_filtered
            ):  # Only check if there are potentially stable historical classes
                total_checks += WEIGHT_CLASS
                try:
                    current_class_str = key_curr_attrs.get("class_str", "") or ""
                    current_class_list = set(current_class_str.split())
                    intersection = hist_class_list_filtered.intersection(
                        current_class_list
                    )
                    if intersection:
                        match_score += WEIGHT_CLASS
                        logger.debug(
                            f"          Class: OK (Overlap found: {intersection})"
                        )
                    else:
                        logger.debug(
                            f"          Class: MISMATCH (No overlap with hist: {hist_class_list_filtered}, curr: {current_class_list})"
                        )
                except Exception as class_err:
                    logger.warning(
                        f"          Class: WARN (Error checking classes: {class_err})",
                        exc_info=False,
                    )

            # --- Final Score Calculation ---
            if total_checks == 0:
                # If no properties were comparable, score is 1 if basic checks passed, 0 otherwise
                # This handles cases where historical info was completely missing (attributes=None, accessibility=None etc.)
                score_ratio = 1.0 if passed_basic_checks else 0.0
                logger.debug(
                    "        No specific properties to verify (or historical data missing), score based on basic checks."
                )
            else:
                score_ratio = match_score / total_checks

            logger.debug(
                f"        Property Verification Score: {score_ratio:.2f} (Checks:{total_checks:.1f}, Matched:{match_score:.1f})"
            )

            # --- Return based on request ---
            if return_score:
                return score_ratio, passed_basic_checks
            else:
                is_match = score_ratio >= similarity_threshold
                final_decision = is_match and passed_basic_checks
                logger.debug(
                    f"        Overall Verification: {'MATCH' if final_decision else 'NO MATCH'} (Score Ratio: {score_ratio:.2f}, Threshold: {similarity_threshold}, Basic Checks Passed: {passed_basic_checks})"
                )
                return final_decision

        except PlaywrightTimeoutError as pte:
            logger.warning(f"      Verification check timed out: {pte}")
            return (0.0, False) if return_score else False
        except Exception as e:
            logger.error(
                f"      Error during verification: {type(e).__name__}: {str(e)}",
                exc_info=True,  # Log full traceback for unexpected verification errors
            )
            return (0.0, False) if return_score else False

    async def _get_current_element_context(self, locator: Locator) -> dict[str, Any]:
        """Gets parent and ancestor context for the CURRENT element found by the locator."""
        context_info = {"parent": None, "ancestor": None}
        js_code = """el => {
            if (!el) return { parent: null, ancestor: null };

            // Parent Info
            let parentInfo = null;
            if (el.parentElement) {
                const parent = el.parentElement;
                parentInfo = {
                    tagName: parent.tagName.toLowerCase(),
                    id: parent.id || null,
                    role: parent.getAttribute('role'),
                    classList: Array.from(parent.classList)
                                    .filter(cls => !cls.startsWith('ng-') && !['selected', 'active', 'focus'].includes(cls))
                                    .slice(0, 3)
                };
            }

            // Ancestor Info
            let ancestorInfo = null;
            let current = el.parentElement;
            let depth = 0;
            const significantRoles = ['listbox', 'menu', 'menubar', 'dialog', 'form', 'grid', 'table', 'radiogroup', 'tablist', 'toolbar', 'tree', 'combobox'];
            const significantTags = ['form', 'table', 'ul', 'ol', 'nav', 'aside', 'main', 'section', 'article'];

            while (current && current !== document.body && depth < 5) {
                const role = current.getAttribute('role');
                const tagName = current.tagName.toLowerCase();
                if (role && significantRoles.includes(role)) {
                    ancestorInfo = { tagName: tagName, id: current.id || null, role: role };
                    break;
                }
                if (!ancestorInfo && significantTags.includes(tagName)) {
                     ancestorInfo = { tagName: tagName, id: current.id || null, role: role };
                }
                current = current.parentElement;
                depth++;
            }
            return { parent: parentInfo, ancestor: ancestorInfo };
        }"""
        try:
            # Evaluate on the specific candidate locator
            context_info = await locator.evaluate(js_code, timeout=1000)
        except Exception as e:
            logger.warning(
                f"      _get_current_element_context: evaluate failed ({e}), returning empty context.",
                exc_info=False,
            )
            context_info = {
                "parent": None,
                "ancestor": None,
            }  # Ensure dict structure on error

        return (
            context_info
            if isinstance(context_info, dict)
            else {"parent": None, "ancestor": None}
        )

    def _calculate_context_similarity(
        self, hist_ctx: dict | None, curr_ctx: dict | None
    ) -> float:
        """Calculates a similarity score (0-1) between two context dictionaries."""
        if not hist_ctx or not curr_ctx:
            return 0.0  # No similarity if one context is missing

        score = 0.0
        max_score = 0.0

        # Compare Tag Name (Required for basic match)
        max_score += 1.0
        if hist_ctx.get("tagName") == curr_ctx.get("tagName"):
            score += 1.0
        else:
            return 0.0  # Tags must match for any similarity

        # Compare ID (Strong indicator)
        max_score += 1.5
        hist_id = hist_ctx.get("id")
        curr_id = curr_ctx.get("id")
        if hist_id and curr_id and hist_id == curr_id:
            score += 1.5
        elif hist_id and not curr_id:
            score += 0.0  # Penalty if historical had ID but current doesn't
        elif not hist_id and curr_id:
            score += 0.5  # Slight penalty if current has ID but historical didn't
        elif not hist_id and not curr_id:
            score += 1.0  # No penalty if neither had ID

        # Compare Role
        max_score += 1.0
        hist_role = hist_ctx.get("role")
        curr_role = curr_ctx.get("role")
        if hist_role and curr_role and hist_role == curr_role:
            score += 1.0
        elif not hist_role and not curr_role:
            score += 1.0  # No penalty if neither had role
        # Else: Penalty applied by not adding points if roles mismatch or one is missing

        # Compare Class List (Overlap)
        hist_classes = set(hist_ctx.get("classList", []))
        curr_classes = set(curr_ctx.get("classList", []))
        if hist_classes:  # Only compare if historical classes were recorded
            max_score += 0.5
            intersection = len(hist_classes.intersection(curr_classes))
            # Simple overlap score
            overlap_score = intersection / len(hist_classes) if hist_classes else 0
            score += overlap_score * 0.5

        return score / max_score if max_score > 0 else 0.0

    async def _try_data_attributes(
        self, element_info: dict, command_info: CommandInfo
    ) -> Locator | None:
        """Tries locating using specific data-* attributes found during annotation."""
        attrs = element_info.get("attributes", {})
        tag_name = command_info.get("element_type", "*") or "*"

        # --- ADDED DEBUGGING ---
        logger.debug(
            f"    _try_data_attributes: Checking attributes type: {type(attrs)}"
        )
        logger.debug(
            f"    _try_data_attributes: Attributes content (truncated): {str(attrs)[:200]}..."
        )
        # --- END DEBUGGING ---

        # Ensure we are accessing the correct nested structure
        # Check if 'attrs' is a dictionary before trying to get 'data_attributes'
        if not isinstance(attrs, dict):
            logger.warning(
                "    Historical attributes are not a dictionary. Cannot access data attributes."
            )
            return None

        data_attrs = attrs.get("data_attributes")  # Get the nested dict/None

        # --- ADDED DEBUGGING ---
        logger.debug(
            f"    _try_data_attributes: Checking data_attributes type: {type(data_attrs)}"
        )
        logger.debug(f"    _try_data_attributes: Data attributes content: {data_attrs}")
        # --- END DEBUGGING ---

        # Corrected Check: Ensure data_attrs is a dictionary and not empty
        if not isinstance(data_attrs, dict) or not data_attrs:
            # This log message should now be accurate
            logger.debug(
                "    No valid data attributes dictionary found in historical element info."
            )
            return None

        # Prioritize potentially more unique attributes if needed, or just iterate
        # Example: prioritize 'testid', then others
        sorted_keys = sorted(data_attrs.keys(), key=lambda k: 0 if k == "testid" else 1)

        for key in sorted_keys:
            value = data_attrs[key]
            # Convert key from camelCase back to kebab-case for selector
            kebab_key = re.sub(r"(?<!^)(?=[A-Z])", "-", key).lower()
            data_attr_name = f"data-{kebab_key}"  # General case

            # Ensure value is not None and convert to string for selector
            if value is not None:
                value_str = str(value)  # Convert boolean/numbers to string
                try:
                    # Escape double quotes for the CSS selector value
                    escaped_value = value_str.replace('"', '\\"')
                    # Escape single quotes as well for robustness
                    escaped_value = escaped_value.replace("'", "\\'")

                    # Construct selector ensuring attribute name is correct
                    selector = f'{tag_name}[{data_attr_name}="{escaped_value}"]'

                    logger.debug(f"    Attempting data attribute selector: {selector}")

                    locator = self.page.locator(selector)
                    # Add a short explicit wait before count to handle potential delays
                    try:
                        await locator.first.wait_for(state="attached", timeout=500)
                    except PlaywrightTimeoutError:
                        logger.debug(
                            f"      Selector '{selector}' not attached quickly."
                        )
                        continue  # Try next data attribute if not found quickly

                    count = await locator.count()
                    logger.debug(f"      Found {count} candidate(s).")

                    # --- Rest of the verification logic remains the same ---
                    if count == 1:
                        prop_score, basic_ok = await self._verify_element_match(
                            locator.first,
                            command_info,
                            element_info,
                            verify_properties=False,  # Skip detailed prop check initially
                            return_score=True,  # Get score/status back
                        )
                        if (
                            basic_ok and prop_score > 0.5
                        ):  # Check basic pass and minimal score
                            logger.info(
                                f"      Found unique & verified match via {data_attr_name}."
                            )
                            return locator  # Return the main locator
                        else:
                            logger.debug(
                                f"      Single candidate via {data_attr_name} failed minimal verification (Basic OK: {basic_ok}, Score: {prop_score:.2f})."
                            )

                    elif count > 1:
                        logger.debug(
                            f"      Found {count} matches for {data_attr_name}, attempting full verification + context."
                        )
                        best_match = await self._find_best_verified_match(
                            locator,
                            command_info,
                            element_info,
                            initial_locator_ambiguous=True,
                        )
                        if best_match:
                            logger.info(
                                f"      Found disambiguated match via {data_attr_name} and context."
                            )
                            return best_match
                        # Continue to try other data attributes if this one failed verification

                except Exception as e:
                    logger.warning(
                        f"      Error checking data attribute {data_attr_name}: {type(e).__name__} - {e}",
                        exc_info=False,
                    )

        logger.debug("    No verified match found using data attributes strategy.")
        return None

    async def _find_best_verified_match(
        self,
        locator: Locator,
        command_info: CommandInfo,
        element_info: dict,
        # NEW: Flag to indicate if the initial locator found multiple items
        initial_locator_ambiguous: bool = False,
    ) -> Locator | None:
        """
        Iterates through multiple candidates, verifies using properties and context (if ambiguous),
        and returns the best match above a threshold.
        """
        count = await locator.count()
        logger.debug(
            f"      Verifying {count} candidates found by strategy (Ambiguous: {initial_locator_ambiguous})..."
        )
        hist_context = element_info.get("context", {})  # Get historical context
        hist_data_attrs = element_info.get("attributes", {}).get("data_attributes", {})

        best_candidate_locator: Locator | None = None
        highest_score: float = -1.0

        # Determine thresholds based on ambiguity
        # Be stricter if the initial locator was already broad/ambiguous
        property_threshold = 0.75 if initial_locator_ambiguous else 0.6
        final_threshold = 0.7 if initial_locator_ambiguous else 0.6

        for i in range(count):
            candidate_locator = locator.nth(i)
            logger.debug(f"        Checking candidate {i + 1}/{count}...")

            # --- Perform standard property verification ---
            # Get score and basic check status back from the modified function
            (
                property_match_score,
                passed_basic_checks,
            ) = await self._verify_element_match(
                candidate_locator,
                command_info,
                element_info,
                verify_properties=True,  # Always verify properties here
                similarity_threshold=property_threshold,  # Use appropriate threshold
                return_score=True,  # MUST get score back
            )

            if not passed_basic_checks:
                logger.debug(
                    "        Candidate failed basic checks (visibility/enabled). Skipping."
                )
                continue  # Skip this candidate immediately

            # If property score is already too low, skip context checks
            if property_match_score < (
                property_threshold * 0.5
            ):  # Heuristic: If props way off, context unlikely to save it
                logger.debug(
                    f"        Property score {property_match_score:.2f} too low, skipping context check."
                )
                continue

            # --- Perform Contextual Verification (Only if ambiguous AND historical context exists) ---
            contextual_match_score: float = 0.0
            context_checks_total_weight: float = 0.0
            # Apply context check only if the initial search was ambiguous or property match is marginal
            apply_context_check = initial_locator_ambiguous or (
                property_match_score < 0.85
            )

            if apply_context_check and hist_context:
                logger.debug("        Performing contextual verification...")
                try:
                    # Get current context for this candidate
                    current_context = await self._get_current_element_context(
                        candidate_locator
                    )

                    # Define weights for context components
                    WEIGHT_PARENT = 1.5
                    WEIGHT_ANCESTOR = 1.0
                    WEIGHT_DATA_ATTRS = 1.0  # Contextual check of data attributes

                    # Compare Parent Context
                    hist_parent = hist_context.get("parent")
                    curr_parent = current_context.get("parent")
                    if hist_parent and curr_parent:  # Only compare if both exist
                        context_checks_total_weight += WEIGHT_PARENT
                        parent_sim = self._calculate_context_similarity(
                            hist_parent, curr_parent
                        )
                        contextual_match_score += parent_sim * WEIGHT_PARENT
                        logger.debug(
                            f"          Parent Context Score: {parent_sim:.2f}"
                        )

                    # Compare Ancestor Context
                    hist_ancestor = hist_context.get("ancestor")
                    curr_ancestor = current_context.get("ancestor")
                    if hist_ancestor and curr_ancestor:  # Only compare if both exist
                        context_checks_total_weight += WEIGHT_ANCESTOR
                        ancestor_sim = self._calculate_context_similarity(
                            hist_ancestor, curr_ancestor
                        )
                        contextual_match_score += ancestor_sim * WEIGHT_ANCESTOR
                        logger.debug(
                            f"          Ancestor Context Score: {ancestor_sim:.2f}"
                        )

                    # Compare Data Attributes Overlap (Contextual check)
                    if hist_data_attrs:
                        try:
                            curr_data_attrs = (
                                await candidate_locator.evaluate("el => el.dataset")
                                or {}
                            )
                            if isinstance(curr_data_attrs, dict):
                                context_checks_total_weight += WEIGHT_DATA_ATTRS
                                common_keys = set(hist_data_attrs.keys()) & set(
                                    curr_data_attrs.keys()
                                )
                                match_count = sum(
                                    1
                                    for k in common_keys
                                    if str(hist_data_attrs[k])
                                    == str(curr_data_attrs[k])
                                )
                                # Score based on matching values among common keys relative to historical keys
                                overlap_score = (
                                    match_count / len(hist_data_attrs)
                                    if hist_data_attrs
                                    else 0
                                )
                                contextual_match_score += (
                                    overlap_score * WEIGHT_DATA_ATTRS
                                )
                                logger.debug(
                                    f"          Data Attrs Context Score: {overlap_score:.2f} ({match_count}/{len(common_keys)} common keys matched)"
                                )
                            else:
                                logger.warning(
                                    "        Current data attributes (dataset) were not a dictionary."
                                )
                        except Exception as da_err:
                            logger.warning(
                                f"        Error getting/comparing data attributes: {da_err}",
                                exc_info=False,
                            )

                except Exception as ctx_err:
                    logger.warning(
                        f"        Error during contextual verification: {ctx_err}",
                        exc_info=False,
                    )
            elif apply_context_check:
                logger.debug(
                    "        Skipping contextual verification (no historical context found)."
                )

            # --- Combine Scores ---
            final_score: float = (
                property_match_score  # Start with property score as base
            )
            if context_checks_total_weight > 0:
                # Calculate average context score
                avg_context_score = contextual_match_score / context_checks_total_weight
                # Weighted average: context helps more if properties were marginal or initial locator was ambiguous
                context_influence = 0.6 if initial_locator_ambiguous else 0.4
                final_score = (property_match_score * (1 - context_influence)) + (
                    avg_context_score * context_influence
                )
                logger.debug(
                    f"        Combined Score: {final_score:.2f} (Prop: {property_match_score:.2f}, Ctx: {avg_context_score:.2f}, Weight: {context_influence})"
                )
            else:
                # If no context checks done, final score is just property score
                logger.debug(
                    f"        Final Score: {final_score:.2f} (Based on properties only)"
                )

            # --- Select Best Candidate So Far ---
            # Candidate must pass basic checks AND have the highest combined score seen so far
            if passed_basic_checks and final_score > highest_score:
                highest_score = final_score
                best_candidate_locator = candidate_locator
                logger.debug(
                    f"        New best candidate found (Index {i}, Score: {highest_score:.2f})."
                )

        # --- Final Decision After Checking All Candidates ---
        if best_candidate_locator and highest_score >= final_threshold:
            logger.info(
                f"      Found best verified match (Final Score: {highest_score:.2f} >= {final_threshold})."
            )
            return best_candidate_locator  # Return the specific Nth locator
        else:
            if count > 0:  # Only log if there were candidates
                logger.debug(
                    f"      No candidate passed final verification threshold (Highest Score: {highest_score:.2f} < {final_threshold})."
                )
            return None

    # Inside LocatorResolver class in browser/locator_resolver.py

    def _get_best_selector_from_history(self, command_info: CommandInfo) -> str | None:
        """
        Analyzes historical command info to determine the most reliable selector string
        for waiting, without querying the live DOM.
        Prioritizes stable selectors like CSS, XPath, ID, specific data attributes.
        Falls back to name, placeholder, and finally tag + text content if necessary.
        """
        element_info = command_info.get("element_info", {})
        if not isinstance(element_info, dict):
            return None  # Cannot determine selector without element_info

        locators = element_info.get("locators", {})
        attrs = element_info.get("attributes", {})
        acc = element_info.get("accessibility", {})
        tag_name = command_info.get("element_type", "*") or "*"
        # Ensure tag_name is valid for CSS, default to '*' if empty or invalid chars expected
        tag_name_selector = (
            tag_name.lower()
            if tag_name != "*" and re.match(r"^[a-zA-Z0-9_-]+$", tag_name)
            else "*"
        )

        logger.debug("    Attempting to find best historical selector for waiting...")

        # Priority 1: Stored CSS Selector
        if isinstance(locators, dict) and locators.get("css_selector"):
            selector = locators["css_selector"]
            # Basic validation: Check if it contains typical invalid characters if not starting with # or .
            # Allow selectors starting with tag name directly
            if (
                not re.match(r"^[#.\\[a-zA-Z]", selector)  # Allow tag name start
                and re.search(r"[>+\\s~:]", selector) is None
                and len(selector.split()) > 2
            ):
                logger.warning(
                    f"      Stored CSS selector '{selector}' looks suspicious, might be text-based. Skipping."
                )
            else:
                logger.debug(f"      Using stored CSS selector: '{selector}'")
                return selector

        # Priority 2: Stored XPath Selector
        if isinstance(locators, dict) and locators.get("xpath"):
            xpath = locators["xpath"]
            selector = xpath if xpath.startswith("xpath=") else f"xpath={xpath}"
            logger.debug(f"      Using stored XPath selector: '{selector}'")
            return selector

        # Priority 3: Specific Stable Attributes (ID, testid, data-ng-click)
        if isinstance(attrs, dict):
            # Check ID
            id_attr = attrs.get("id")
            if id_attr and not re.match(
                r"^(ember\\d+|gwt-|ext-|jQuery\\d+)", id_attr, re.IGNORECASE
            ):
                # Ensure ID doesn't contain invalid characters for CSS ID selector
                # Allowing more chars based on HTML5 spec, but being slightly stricter
                if re.match(r"^[a-zA-Z][a-zA-Z0-9_.:-]*$", id_attr):  # Looser ID check
                    # Escape problematic characters within the ID for CSS selector robustness
                    # Main ones are spaces, quotes, brackets, colons maybe?
                    # Playwright handles most basic escaping, but being safe:
                    escaped_id = re.sub(
                        r'([!"#$%&\'()*+,./:;<=>?@\[\\\]^`{|}~])', r"\\\1", id_attr
                    )
                    selector = f"#{escaped_id}"
                    logger.debug(f"      Using ID attribute selector: '{selector}'")
                    return selector
                else:
                    logger.warning(
                        f"      Skipping ID '{id_attr}' due to potentially invalid characters for CSS ID selector (or starting with digit)."
                    )

            # Check data attributes
            data_attrs = attrs.get("data_attributes")
            if isinstance(data_attrs, dict):
                # Check testid
                test_id = data_attrs.get("testid") or data_attrs.get("pw")
                if test_id:
                    try:
                        escaped_testid = escape_css_selector_value(str(test_id))
                        selector = f'[data-testid="{escaped_testid}"]'  # Assuming standard data-testid
                        # If using data-pw, adjust accordingly: selector = f"[data-pw=\"{escaped_testid}\"]"
                        logger.debug(f"      Using test ID selector: '{selector}'")
                        return selector
                    except Exception:
                        logger.warning(
                            "      Failed to create selector from testid",
                            exc_info=False,
                        )

                # Check ngClick (as an example of another specific attribute)
                ng_click_val = data_attrs.get("ngClick")
                if ng_click_val:
                    try:
                        escaped_val = escape_css_selector_value(str(ng_click_val))
                        selector = f'{tag_name_selector}[data-ng-click="{escaped_val}"]'
                        logger.debug(
                            f"      Using data-ng-click selector: '{selector}'"
                        )
                        return selector
                    except Exception:
                        logger.warning(
                            "      Failed to create selector from data-ng-click",
                            exc_info=False,
                        )

        # --- NEW: Priority 4: Name Attribute ---
        if isinstance(attrs, dict) and attrs.get("name"):
            name_attr = attrs.get("name")
            try:
                escaped_name = escape_css_selector_value(str(name_attr))
                selector = f'{tag_name_selector}[name="{escaped_name}"]'
                logger.debug(f"      Using Name attribute selector: '{selector}'")
                return selector
            except Exception:
                logger.warning(
                    "      Failed to create selector from name attribute",
                    exc_info=False,
                )

        # --- NEW: Priority 5: Placeholder Attribute ---
        if isinstance(attrs, dict) and attrs.get("placeholder"):
            placeholder_attr = attrs.get("placeholder")
            try:
                escaped_placeholder = escape_css_selector_value(str(placeholder_attr))
                selector = f'{tag_name_selector}[placeholder="{escaped_placeholder}"]'
                logger.debug(
                    f"      Using Placeholder attribute selector: '{selector}'"
                )
                return selector
            except Exception:
                logger.warning(
                    "      Failed to create selector from placeholder attribute",
                    exc_info=False,
                )

        # --- Priority 6 (Was 4): Text Content or Accessible Name ---
        text_to_use = None
        acc_name = None
        if isinstance(acc, dict):  # Check if accessibility info exists
            acc_name = acc.get("name") or acc.get("aria_label")
        element_text = element_info.get("text")

        if acc_name:  # Prioritize accessible name
            cleaned_name = " ".join(str(acc_name).split())
            if 0 < len(cleaned_name) < self.MAX_TEXT_MATCH_LENGTH:
                text_to_use = cleaned_name
                logger.debug("      Selected text from 'accessibility.name/aria_label'")

        if not text_to_use and element_text:  # Fallback to text content
            cleaned_text = " ".join(str(element_text).split())
            if 0 < len(cleaned_text) < self.MAX_TEXT_MATCH_LENGTH:
                text_to_use = cleaned_text
                logger.debug("      Selected text from 'element_info.text'")

        if text_to_use:
            try:
                escaped_text = escape_css_selector_value(text_to_use)
                # Combine tag name with :has-text
                # Use '*' if tag_name_selector is '*' to avoid invalid selectors like ':has-text(...)'
                effective_tag = tag_name_selector if tag_name_selector != "*" else ""
                selector = f"{effective_tag}:has-text('{escaped_text}')"
                logger.debug(f"      Using Tag+Text selector: '{selector}'")
                return selector
            except Exception as e:
                logger.warning(
                    f"      Failed to create selector from text '{text_to_use}': {e}",
                    exc_info=False,
                )

        # --- Priority 7 (Was 5): Role (less specific) ---
        role = None
        if isinstance(acc, dict):
            role = acc.get("role")
        if not role and isinstance(attrs, dict):
            role = attrs.get("role")  # Fallback to attribute role

        if role:
            try:
                # Basic attribute selector for role
                selector = f"{tag_name_selector}[role='{escape_css_selector_value(str(role))}']"
                logger.debug(
                    f"      Using approximate Role selector (less specific): '{selector}'"
                )
                return selector
            except Exception:
                logger.warning(
                    "      Failed to create selector from role", exc_info=False
                )

        logger.warning(
            "    Could not determine a reliable historical selector for waiting."
        )
        return None
