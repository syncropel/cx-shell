from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Dict
from uuid import UUID


def safe_serialize(data: Any) -> Any:
    """
    Recursively traverses a data structure and converts common, non-standard
    JSON types into a JSON-serializable format.
    """
    if isinstance(data, list):
        return [safe_serialize(item) for item in data]

    if isinstance(data, dict):
        return {key: safe_serialize(value) for key, value in data.items()}

    # --- THIS IS THE FIX ---
    # Handle datetime objects first, as they are a subclass of date.
    if isinstance(data, datetime):
        if data.tzinfo is None:
            # If the datetime is naive, assume it's UTC.
            data = data.replace(tzinfo=timezone.utc)
        return data.isoformat().replace("+00:00", "Z")

    # Now handle date objects, which do not have tzinfo.
    if isinstance(data, date):
        return data.isoformat()
    # --- END FIX ---

    if isinstance(data, UUID):
        return str(data)

    if isinstance(data, Decimal):
        return float(data)

    # Safely handle RecordID-like objects
    if (
        hasattr(data, "id")
        and isinstance(getattr(data, "id", None), str)
        and ":" in data.id
    ):
        return str(data)

    return data


def get_nested_value(data: Dict, key_path: str, default: Any = None) -> Any:
    """
    Safely retrieves a value from a nested dictionary using dot notation.
    e.g., get_nested_value(data, "track.album.name")
    """
    if not isinstance(data, dict) or not isinstance(key_path, str):
        return default

    keys = key_path.split(".")
    value = data

    for key in keys:
        if isinstance(value, dict):
            value = value.get(key)
            if value is None:
                return default  # Key not found at this level
        else:
            return default  # Cannot traverse further into a non-dict

    return value if value is not None else default


def is_binary_string(content: str) -> bool:
    """
    Heuristically checks if a string contains binary content.

    The presence of a NULL byte ('\0') is a very strong indicator that the
    file is binary and not intended to be read as plain text.

    Args:
        content: The string content to check.

    Returns:
        True if the content is likely binary, False otherwise.
    """
    return "\0" in content
