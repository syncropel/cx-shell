# ~/repositories/cx-shell/src/cx_shell/utils.py
import sys
from pathlib import Path
import os
from typing import Optional

# --- Centralized Path Constant ---
# This is now the single source of truth for the CX_HOME path.
CX_HOME = Path(os.getenv("CX_HOME", Path.home() / ".cx"))


def get_pkg_root() -> Path:
    """
    Gets the root directory of the cx_shell package. This works correctly
    whether running from source or as a frozen PyInstaller executable.
    """
    if getattr(sys, "frozen", False):
        return Path(sys._MEIPASS) / "cx_shell"
    else:
        return Path(__file__).parent


def get_assets_root() -> Path:
    """Gets the root directory of the bundled 'assets'."""
    return get_pkg_root() / "assets"


def resolve_path(path_str: str, current_file_path: Optional[Path] = None) -> Path:
    """
    Resolves a path string by checking against workspace roots and using the
    currently executing file's location for context-aware schemes.

    Args:
        path_str: The path string to resolve (e.g., 'project-asset:scripts/myscript.py').
        current_file_path: The absolute path of the script or notebook that contains the path_str.
                           This is crucial for resolving 'project-asset:' and 'app-asset:'.
    """
    from .management.workspace_manager import (
        WorkspaceManager,
    )  # Local import to avoid circular dependency

    # --- Scheme-based resolution (highest priority) ---
    if path_str.startswith(("project-asset:", "app-asset:")):
        if not current_file_path:
            raise ValueError(
                f"Cannot resolve '{path_str}' path without a current file path context."
            )

        scheme, relative_path = path_str.split(":", 1)
        manager = WorkspaceManager()
        project_root = manager.find_project_root_for_file(current_file_path)

        if not project_root:
            raise FileNotFoundError(
                f"Could not resolve '{scheme}:' because the source file '{current_file_path}' does not belong to a registered workspace root."
            )

        # Both app-asset and project-asset resolve relative to the project root of the current file.
        return (project_root / relative_path).resolve()

    if path_str.startswith("file:"):
        path_part = path_str.split(":", 1)[1]
        clean_path = path_part.lstrip("/")
        # Re-add leading slash for Unix absolute paths if it's not a Windows path
        is_windows_path = Path(clean_path).drive or clean_path.startswith("\\\\")
        absolute_path_str = (
            f"/{clean_path}"
            if not is_windows_path and not Path(clean_path).is_absolute()
            else clean_path
        )
        return Path(absolute_path_str).resolve()

    # --- Workspace-relative path (e.g., "my-project/flows/main.flow.yaml") ---
    if "/" in path_str and not path_str.startswith(("/", "~", ".")):
        try:
            workspace_name, relative_path = path_str.split("/", 1)
            manager = WorkspaceManager()
            for root in manager.get_roots():
                if root.name == workspace_name:
                    return (root / relative_path).resolve()
        except ValueError:
            pass  # Not a workspace path, fall through

    # --- Fallback for all other cases (e.g., `~/...`, `./...`, or a CWD-relative path) ---
    return Path(path_str).expanduser().resolve()
