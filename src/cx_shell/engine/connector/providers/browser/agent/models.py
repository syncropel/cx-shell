# browser/models.py

"""
Contains Pydantic models, TypedDicts, and simple data classes defining the
data structures used by the web agent and associated components.
"""

import json  # Import json if used within models
from datetime import datetime, timezone
from typing import Any, TypedDict

from dateutil.parser import parse
from pydantic import BaseModel, Field


# --- Pydantic Models ---
# ... (Function, ServiceCredential, Credential, Action, ToolArguments remain the same) ...
class Function(BaseModel):
    """Represents a custom function definition."""

    name: str = Field(..., description="Unique name of the function.")
    id: str | None = Field(None, description="Optional database ID.")
    content: str = Field(
        ..., description="The actual code or definition of the function."
    )


class ServiceCredential(BaseModel):
    """Represents credentials specific to a service (nested within Credential)."""

    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Service-specific metadata (e.g., tokens, keys).",
    )
    name: str | None = Field(None, description="Name of the service credential record.")
    service_name: str | None = Field(
        None, description="Name of the service this credential belongs to."
    )
    id: str | None = Field(None, description="Optional database ID.")
    status: str | None = Field(
        None, description="Status of the credential (e.g., active, inactive)."
    )
    author_id: str | None = Field(
        None, description="ID of the user who created this record."
    )
    created_datetime: str | None = Field(
        None, description="Creation timestamp (ISO format)."
    )
    updated_datetime: str | None = Field(
        None, description="Last update timestamp (ISO format)."
    )
    service_id: str | None = Field(None, description="ID of the associated service.")
    tags: str | None = Field(None, description="Comma-separated tags.")
    description: str | None = Field(
        None, description="Description of the service credential."
    )
    entity_type: str | None = Field(
        "ServiceCredential", description="Type identifier for the entity."
    )


class Credential(BaseModel):
    """Represents a user's credential, potentially linking to service-specific details."""

    metadata: dict[str, Any] | None = Field(
        default_factory=dict,
        description="General metadata for the credential (e.g., usage info).",
    )
    name: str | None = Field(None, description="Name of the user credential record.")
    service_name: str | None = Field(
        None, description="Name of the service this credential relates to."
    )
    id: str | Any | None = Field(
        None, description="Optional database ID."
    )  # Allowing Any for potential flexibility
    status: str | None = Field(None, description="Status of the credential.")
    service_credential: ServiceCredential | None = Field(
        None, description="Nested service-specific credential details."
    )
    author_id: str | None = Field(
        None, description="ID of the user who owns this credential record."
    )
    created_datetime: str | None = Field(
        None, description="Creation timestamp (ISO format)."
    )
    updated_datetime: str | None = Field(
        None, description="Last update timestamp (ISO format)."
    )
    service_id: str | None = Field(None, description="ID of the associated service.")
    service_credential_id: str | None = Field(
        None, description="ID of the linked ServiceCredential record."
    )
    service: str | None = Field(
        None, description="Deprecated? Seems redundant with service_name/service_id."
    )
    tags: str | None = Field(None, description="Comma-separated tags.")
    description: str | None = Field(
        None, description="Description of the user credential."
    )
    entity_type: str | None = Field(
        "Credential", description="Type identifier for the entity."
    )

    model_config = {
        "extra": "allow"  # Allows arbitrary key-value pairs not explicitly defined
    }

    def is_token_expired(self, buffer_seconds: int = 300) -> bool:
        """
        Check if a token (assumed to be in metadata) has expired.

        Checks `metadata['expires_in']` relative to `updated_datetime`.

        Args:
            buffer_seconds: A safety buffer subtracted from the expiry time.

        Returns:
            True if the token is expired or likely expired, False otherwise.

        Raises:
            ValueError: If required metadata fields are missing or invalid.
        """
        if not self.metadata:
            raise ValueError("Credential metadata is missing for expiry check.")

        expires_in = self.metadata.get("expires_in")
        updated_dt_str = self.updated_datetime

        if expires_in is None or updated_dt_str is None:
            raise ValueError(
                "'expires_in' or 'updated_datetime' not found in credential for expiry check."
            )

        try:
            last_update = parse(updated_dt_str)
            if last_update.tzinfo is None:
                last_update = last_update.replace(tzinfo=timezone.utc)

            expiration_timestamp = last_update.timestamp() + float(expires_in)
            current_timestamp = datetime.now(timezone.utc).timestamp()

            return current_timestamp + buffer_seconds >= expiration_timestamp

        except (ValueError, TypeError) as e:
            raise ValueError(f"Error parsing dates or calculating expiration: {str(e)}")


class Action(BaseModel):
    """Represents a single step or action within a larger process or workflow."""

    action_status: str | None = Field(
        None,
        description="Current status of the action (e.g., pending, completed, failed).",
    )
    action_order: int = Field(1, description="Sequential order of the action.")
    name: str = Field(
        ..., description="Unique name or identifier for this action step."
    )
    func_name: str = Field(
        ..., description="The name of the function/method to execute for this action."
    )
    operation: str = Field(
        ..., description="The type of operation (e.g., READ, CREATE, EXECUTE)."
    )
    service: str = Field(
        ...,
        description="The target service or system for the action (e.g., browser, api).",
    )
    query: str | None = Field(
        None, description="Query string or details (e.g., SQL, API endpoint path)."
    )
    entity: str | None = Field(
        None,
        description="Specific entity or resource within the service (e.g., table name, object type).",
    )
    options: dict[str, Any] | None = Field(
        default_factory=dict, description="Configuration options for the action."
    )
    input: dict[str, Any] | None = Field(
        default_factory=dict, description="Input data required by the action."
    )
    data: dict[str, Any] | None = Field(
        default_factory=dict, description="Payload data for CREATE/UPDATE operations."
    )
    steps: list[dict[str, Any]] | None = Field(
        None, description="Nested sub-steps (if the action is complex)."
    )  # Consider making this List['Action'] if nesting is deep
    credential: Credential | None = Field(
        None, description="Single credential object to use."
    )
    credentials: Credential | list[Credential] | None = Field(
        None, description="Alternative for single or multiple credentials."
    )
    func: Function | None = Field(
        None, description="Associated Function object (if executing a custom function)."
    )
    id: str | None = Field(None, description="Optional database ID.")
    description: str | None = Field(
        None, description="User-friendly description of the action."
    )
    cache: dict[str, Any] | None = Field(
        default_factory=dict, description="Caching related options or status."
    )
    view: dict[str, Any] | None = Field(
        default_factory=dict, description="UI/View related information."
    )
    skip: dict[str, Any] | str | bool | None = Field(
        None, description="Condition or flag to skip this action."
    )
    message: dict[str, Any] | None = Field(
        default_factory=dict, description="Result messages or codes."
    )
    success_message_code: str | None = Field(
        None, description="Specific code indicating success."
    )
    func_id: str | None = Field(
        None, description="ID of the associated Function record."
    )
    credential_id: str | None = Field(
        None, description="ID of the associated Credential record."
    )


class ToolArguments(BaseModel):
    """Structure for passing arguments, including an Action, to a tool/agent."""

    action: Action = Field(..., description="The primary action to be performed.")
    input: dict[str, Any] | None = Field(
        default_factory=dict,
        description="Additional input data specific to the tool invocation.",
    )


# --- TypedDicts for Browser Agent ---
# ... (BrowserConfig, BoundingBox, ElementLocators, ElementAttributes, ElementAccessibility, ElementState, ElementInfo, CommandInfo remain the same) ...
class BrowserConfig(TypedDict, total=False):
    """Configuration options specifically for the WebAgent."""

    headless: bool
    browser: str  # 'chromium', 'firefox', 'webkit'
    timeout: int  # milliseconds
    viewport_size: dict[str, int]
    user_agent: str | None
    proxy: dict[str, str] | None
    download_path: str | None
    geolocation: dict[str, float] | None
    timezone: str | None
    locale: str | None


class BoundingBox(TypedDict):
    """Represents the coordinates and dimensions of an element."""

    x: float
    y: float
    width: float
    height: float


class ElementLocators(TypedDict, total=False):
    """Locators extracted for an element."""

    xpath: str | None
    css_selector: str | None
    label_text: str | None
    custom_selectors: dict[str, str] | None


class ElementAttributes(TypedDict, total=False):
    """Common HTML attributes extracted from an element."""

    id: str | None
    class_list: list[str] | None  # Renamed from 'class'
    name: str | None
    value: str | None
    placeholder: str | None
    type: str | None
    title: str | None
    role: str | None
    href: str | None
    src: str | None
    data_attributes: dict[str, str] | None


class ElementAccessibility(TypedDict, total=False):
    """Accessibility-related properties."""

    role: str | None
    name: str | None
    description: str | None
    haspopup: str | None
    current: str | None
    expanded: str | None
    selected: str | None
    level: str | None
    aria_label: str | None


class ElementState(TypedDict, total=False):
    """State information about an element."""

    is_visible: bool
    is_enabled: bool
    is_focused: bool | None
    is_checked: bool | None
    is_required: bool | None
    cursor: str | None


class ElementInfo(TypedDict):
    """Detailed information about an interactive element found on the page."""

    id: int
    type: str
    text: str | None
    bbox: BoundingBox
    attributes: ElementAttributes
    accessibility: ElementAccessibility
    state: ElementState
    locators: ElementLocators


class CommandInfo(TypedDict, total=False):
    """Represents a recorded or replayed command."""

    command_type: str
    name: str | None
    execution_order: int | None
    element_id: int | None
    element_xpath: str | None
    element_selector: str | None
    element_type: str | None
    element_text: str | None
    bbox: BoundingBox | None
    element_info: dict | None
    text: str | None
    click_location: str | None
    url: str | None
    status: str  # 'passed', 'failed', 'pending', 'running', 'warning'
    message: str | None
    timestamp: str | None
    start_datetime: str | None
    end_datetime: str | None
    before_screenshot: str | None
    after_screenshot: str | None
    replay_mode: bool | None
    data: Any | None
    context_text: str | None
    context_filter: dict[str, Any] | None


# --- Other Helper Classes ---


class LocatorStrategy:
    """Defines constants for different element location strategies."""

    XPATH = "xpath"
    BOUNDING_BOX = "bounding_box"
    TEXT = "text"
    ATTRIBUTES = "attributes"
    SMART = "smart"
    CUSTOM = "custom"


# --- ADDED NetworkRequest Class ---
class NetworkRequest:
    """Represents a captured network request's relevant details."""

    def __init__(self, url: str, method: str, response_body: str | None = None):
        self.url = url
        self.method = method
        self.response_body = response_body
        self.timestamp = datetime.now().isoformat()  # Use standard ISO format


# --- End Added ---


# --- Results Class (Moved from original script for better organization) ---
class Results:
    """Simple class to collect results during execution."""

    def __init__(self):
        self.results: list[dict[str, Any]] = []

    def add_result(self, result: dict[str, Any]):
        """Adds a result dictionary to the list."""
        self.results.append(result)

    def get_results(self, **kwargs) -> list[dict[str, Any]]:
        """
        Returns the collected results.

        Args:
            **kwargs: Placeholder for potential future filtering options.

        Returns:
            The list of collected result dictionaries.
        """
        # Currently returns all results, filtering logic could be added here
        # based on kwargs if needed in the future.
        return self.results

    def clear(self):
        """Clears all collected results."""
        self.results = []

    def __str__(self):
        """String representation for printing."""
        return json.dumps(self.results, indent=2)

    def __repr__(self):
        """Representation for debugging."""
        return f"Results(count={len(self.results)})"


# --- End Results Class ---
