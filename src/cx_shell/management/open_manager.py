# ~/repositories/cx-shell/src/cx_shell/management/open_manager.py

import sys
import subprocess
from typing import Any
import webbrowser
from pathlib import Path
import os
import shutil

from rich.console import Console

from ..engine.connector.config import CX_HOME
from ..interactive.session import SessionState
from ..engine.connector.service import ConnectorService

console = Console()


class OpenManager:
    """Handles the logic for opening assets in their default or specified applications."""

    def __init__(self):
        self.asset_dirs = {
            "flow": CX_HOME / "flows",
            "query": CX_HOME / "queries",
            "script": CX_HOME / "scripts",
            "connection": CX_HOME / "connections",
        }
        self.asset_exts = {
            "flow": ".flow.yaml",
            "query": ".sql",
            "script": ".py",
            "connection": ".conn.yaml",
        }
        self.handlers = {
            "default": self._handle_default_open,
            "vscode": self._handle_vscode_open,
        }
        # Check for the 'code' command once at startup for efficiency.
        self.is_vscode_installed = shutil.which("code") is not None

    def _handle_vscode_open(self, path_to_open: Path):
        """Handler to specifically open a file or directory in VS Code."""
        if not self.is_vscode_installed:
            console.print(
                "[bold red]Error:[/bold red] The 'code' command is not available in your system's PATH. Cannot open with VS Code."
            )
            return

        console.print(f"Opening [cyan]{path_to_open}[/cyan] in Visual Studio Code...")
        try:
            # Use '-r' to reuse the existing window, preventing new instances.
            subprocess.run(["code", "-r", str(path_to_open)], check=True)
        except subprocess.CalledProcessError as e:
            console.print(f"[bold red]Error executing 'code' command:[/bold red] {e}")

    def _handle_default_open(self, path_to_open: Path):
        """Cross-platform method to open a file or directory with the OS default."""
        console.print(
            f"Opening [cyan]{path_to_open}[/cyan] with default application..."
        )

        # --- THIS IS THE DEFINITIVE, SIMPLIFIED FIX ---
        # Prioritize VS Code if it's available, as it's the best tool for code files.
        # This works seamlessly across WSL, macOS, and native Linux.
        if self.is_vscode_installed:
            console.print(
                "[dim]VS Code detected. Using the 'code' command for the best experience.[/dim]"
            )
            self._handle_vscode_open(path_to_open)
            return
        # --- END FIX ---

        # If VS Code isn't available, fall back to the OS-specific generic commands.
        is_in_wsl = "WSL_DISTRO_NAME" in os.environ
        try:
            if is_in_wsl:
                # wslview is the most robust generic opener for WSL.
                subprocess.run(["wslview", str(path_to_open)], check=True)
            elif sys.platform == "win32":
                os.startfile(str(path_to_open))
            elif sys.platform == "darwin":
                subprocess.run(["open", path_to_open], check=True)
            else:  # Native Linux Desktop
                subprocess.run(["xdg-open", path_to_open], check=True)
        except Exception as e:
            console.print(
                f"[bold red]Error opening file:[/bold red] Could not open '{path_to_open}'. Reason: {e}"
            )

    async def open_asset(
        self,
        state: SessionState,
        service: ConnectorService,
        asset_type: str,
        asset_name: str | None,
        handler_name: str,
        on_alias: str | None,
        piped_input: Any = None,
    ):
        """Finds an asset and dispatches it to the correct handler."""
        if piped_input:
            if isinstance(piped_input, str):
                path_to_open = Path(piped_input)
                if path_to_open.exists():
                    self.handlers.get(handler_name, self._handle_default_open)(
                        path_to_open
                    )
                    return
                else:
                    raise FileNotFoundError(f"Piped path does not exist: {piped_input}")
            else:
                # If piped input isn't a string, it's an error for the open command.
                raise TypeError(
                    f"The 'open' command received piped input of type '{type(piped_input).__name__}' but expected a string file path."
                )
        if on_alias:
            console.print(
                f"[yellow]Note: Remote 'open' on connection '{on_alias}' is a future feature.[/yellow]"
            )
            return

        handler_func = self.handlers.get(handler_name)
        if not handler_func:
            raise ValueError(
                f"Unknown open handler '--in {handler_name}'. Supported handlers: {list(self.handlers.keys())}"
            )

        path_to_open = None
        if asset_type == "config":
            path_to_open = CX_HOME
        elif asset_type.startswith("{{") and asset_type.endswith("}}"):
            from jinja2 import Environment

            url_to_open = Environment().from_string(asset_type).render(state.variables)
            console.print(f"Opening URL [link={url_to_open}]{url_to_open}[/link]...")
            webbrowser.open(url_to_open)
            return
        else:
            asset_dir = self.asset_dirs.get(asset_type)
            asset_ext = self.asset_exts.get(asset_type)
            if not asset_dir or not asset_ext or not asset_name:
                valid_types = list(self.asset_dirs.keys()) + ["config"]
                raise ValueError(
                    f"Invalid asset specification. Usage: `open <type> [name]`. Valid types: {valid_types}"
                )

            asset_file = asset_dir / f"{asset_name}{asset_ext}"
            if not asset_file.exists():
                raise FileNotFoundError(
                    f"Asset '{asset_name}' of type '{asset_type}' not found."
                )
            path_to_open = asset_file

        if path_to_open:
            handler_func(path_to_open)
