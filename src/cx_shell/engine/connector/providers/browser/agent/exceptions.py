"""
Custom exceptions used by the browser automation agent.
"""


class BrowserAgentError(Exception):
    """Base exception for all agent-related errors."""

    pass


class InitializationError(BrowserAgentError):
    """Error during browser or agent initialization."""

    pass


class CleanupError(BrowserAgentError):
    """Error during browser or agent cleanup."""

    pass


class ElementNotFoundError(BrowserAgentError):
    """Failed to find a specific element."""

    pass


class LocatorResolutionError(ElementNotFoundError):
    """Failed to resolve a usable Playwright Locator from command info."""

    pass


class ElementNotInteractableError(BrowserAgentError):
    """Element was found but is not in an interactable state (e.g., not visible, disabled)."""

    pass


class ActionFailedError(BrowserAgentError):
    """An action (click, type, etc.) failed after retries."""

    pass


class NavigationTimeoutError(BrowserAgentError):
    """A page navigation or explicit wait for navigation timed out."""

    pass


class WaitTimeoutError(BrowserAgentError):
    """A general wait condition (e.g., for dynamic update, element state) timed out."""

    pass


class ScriptExecutionError(BrowserAgentError):
    """Error executing JavaScript within the browser page."""

    pass


class ConfigurationError(BrowserAgentError):
    """Error related to agent configuration."""

    pass


class NetworkError(BrowserAgentError):
    """Error related to network monitoring or response handling."""

    pass


class VerificationFailedError(BrowserAgentError):  # <<< ADDED
    """An explicit verification check (e.g., element visibility, state) failed."""

    pass


# Add this new exception
class PriceVerificationError(VerificationFailedError):
    """Verification failed due to price constraints or calculation mismatch."""

    pass
