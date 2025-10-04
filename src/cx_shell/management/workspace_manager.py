import json
from pathlib import Path
from typing import List, Dict, Optional

import structlog
from rich.console import Console
from rich.table import Table

from ..utils import CX_HOME

logger = structlog.get_logger(__name__)
console = Console()


class WorkspaceManager:
    """
    Manages the user's project roots via the workspace.json manifest.
    This class is designed to be fully self-contained and testable.
    """

    def __init__(self, cx_home_path: Optional[Path] = None):
        """
        Initializes the WorkspaceManager.

        Args:
            cx_home_path: An optional path to a custom .cx directory, primarily
                          used for testing. If None, it defaults to the global CX_HOME.
        """
        # --- START OF DEFINITIVE, SELF-CONTAINED PATTERN ---
        # The manager stores its required paths as instance variables.
        # It no longer modifies or relies on unpredictable module-level globals.
        self._cx_home = cx_home_path or CX_HOME
        self._workspace_file = self._cx_home / "workspace.json"
        # --- END OF DEFINITIVE, SELF-CONTAINED PATTERN ---

    def _load_manifest(self) -> Dict:
        """Loads the workspace manifest file from its instance-specific path."""
        if not self._workspace_file.exists():
            return {"roots": []}
        try:
            return json.loads(self._workspace_file.read_text())
        except (json.JSONDecodeError, FileNotFoundError):
            logger.warning(
                "workspace_manager.load_manifest.failed",
                path=str(self._workspace_file),
                exc_info=True,
            )
            return {"roots": []}

    def _save_manifest(self, manifest_data: Dict):
        """Saves the workspace manifest file to its instance-specific path."""
        try:
            self._workspace_file.parent.mkdir(parents=True, exist_ok=True)
            self._workspace_file.write_text(json.dumps(manifest_data, indent=2))
        except IOError as e:
            logger.error(
                "workspace_manager.save_manifest.failed",
                path=str(self._workspace_file),
                error=str(e),
            )

    def get_roots(self) -> List[Path]:
        """
        Gets a list of all active project root paths.
        The system root (the instance-specific _cx_home) is always implicitly included.
        """
        manifest = self._load_manifest()
        # Use the stored _cx_home path, which is context-aware and correct for the instance.
        roots = [self._cx_home]
        for path_str in manifest.get("roots", []):
            roots.append(Path(path_str).expanduser().resolve())
        return roots

    def list_roots(self):
        """Displays a table of registered project roots."""
        manifest = self._load_manifest()
        roots = manifest.get("roots", [])

        table = Table(title="Registered Workspace Project Roots")
        table.add_column("Path", style="cyan")
        table.add_column("Status", style="green")

        if not roots:
            console.print(
                "[dim]No user-defined project roots. Use `workspace add <path>` to register a project.[/dim]"
            )
        else:
            for path_str in roots:
                path = Path(path_str).expanduser()
                status = "✅ Found" if path.is_dir() else "[red]✗ Not Found[/red]"
                table.add_row(str(path), status)
        console.print(table)

        # Correctly display the context-aware system root being used by this instance.
        console.print(f"\n[dim]System root (always included): {self._cx_home}[/dim]")

    def add_root(self, path_str: str):
        """Adds a new project root to the workspace."""
        path_to_add = Path(path_str).expanduser().resolve()
        if not path_to_add.is_dir():
            console.print(
                f"[bold red]Error:[/bold red] Path '{path_to_add}' is not a valid directory."
            )
            return

        manifest = self._load_manifest()
        # Use strings for JSON serialization and to handle ~ correctly
        root_str_to_add = (
            f"~/{path_to_add.relative_to(Path.home())}"
            if path_to_add.is_relative_to(Path.home())
            else str(path_to_add)
        )

        if root_str_to_add not in manifest["roots"]:
            manifest["roots"].append(root_str_to_add)
            manifest["roots"].sort()
            self._save_manifest(manifest)
            console.print(f"✅ Added '[cyan]{path_to_add}[/cyan]' to your workspace.")
        else:
            console.print(
                f"[yellow]Path '[cyan]{path_to_add}[/cyan]' is already in your workspace.[/yellow]"
            )

    def remove_root(self, path_str: str):
        """Removes a project root from the workspace."""
        path_to_remove = Path(path_str).expanduser().resolve()
        manifest = self._load_manifest()

        # Find the matching string representation to remove
        root_str_to_remove = None
        for root in manifest["roots"]:
            if Path(root).expanduser().resolve() == path_to_remove:
                root_str_to_remove = root
                break

        if root_str_to_remove:
            manifest["roots"].remove(root_str_to_remove)
            self._save_manifest(manifest)
            console.print(
                f"✅ Removed '[cyan]{path_to_remove}[/cyan]' from your workspace."
            )
        else:
            console.print(
                f"[bold red]Error:[/bold red] Path '{path_to_remove}' not found in your workspace roots."
            )

    def find_project_root_for_file(self, file_path: Path) -> Optional[Path]:
        """Finds which registered workspace root a given file belongs to."""
        if not file_path:
            return None

        resolved_file_path = file_path.resolve()
        # Sort roots by path length, descending, to find the most specific match first
        # e.g., to match `~/dev/project/sub` before `~/dev/project`
        sorted_roots = sorted(self.get_roots(), key=lambda p: len(str(p)), reverse=True)

        for root in sorted_roots:
            try:
                if resolved_file_path.is_relative_to(root):
                    return root
            except ValueError:
                # This can happen on Windows if paths are on different drives
                continue
        return None  # Not in a registered project
