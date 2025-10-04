# ~/repositories/cx-shell/src/cx_shell/management/session_manager.py

from pathlib import Path
import pickle
from datetime import datetime
from typing import Optional

from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich import box
from prompt_toolkit.shortcuts import PromptSession
from prompt_toolkit.formatted_text import HTML

from ..engine.connector.config import CX_HOME
from ..interactive.session import SessionState

SESSION_DIR = CX_HOME / "sessions"
console = Console()


class SessionManager:
    """Handles all logic for listing, saving, loading, and deleting session files."""

    def __init__(self, cx_home_path: Optional[Path] = None):
        _cx_home = cx_home_path or CX_HOME
        self.SESSION_DIR = _cx_home / "sessions"
        self.SESSION_DIR.mkdir(exist_ok=True, parents=True)

    def list_sessions(self):
        sessions = list(SESSION_DIR.glob("*.cxsession"))
        if not sessions:
            console.print("No saved sessions found.")
            return

        table = Table(title="Saved Sessions", box=box.ROUNDED)
        table.add_column("Session Name", style="cyan")
        table.add_column("Last Modified", style="magenta")
        table.add_column("Size (KB)", style="green", justify="right")

        for session_file in sorted(sessions):
            try:
                stats = session_file.stat()
                mtime = datetime.fromtimestamp(stats.st_mtime).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
                size_kb = f"{stats.st_size / 1024:.2f}"
                table.add_row(session_file.stem, mtime, size_kb)
            except Exception:
                table.add_row(
                    f"[red]{session_file.stem}[/red]",
                    "[red]Error[/red]",
                    "[red]N/A[/red]",
                )

        console.print(table)

    def save_session(self, state: SessionState, name: str) -> str:
        session_file = SESSION_DIR / f"{name}.cxsession"
        with open(session_file, "wb") as f:
            pickle.dump(state, f)
        return f"Session '{name}' saved."  # <-- RETURN string

    async def delete_session(self, name: str) -> str:
        session_file = SESSION_DIR / f"{name}.cxsession"
        if not session_file.exists():
            raise FileNotFoundError(f"Session '{name}' not found.")

        prompt_session = PromptSession()
        confirmed = await prompt_session.prompt_async(
            HTML(f"Are you sure you want to delete session '<b>{name}</b>'? [y/n]: "),
        )

        if confirmed and confirmed.lower() == "y":
            session_file.unlink()
            return f"Session '{name}' deleted."
        else:
            return "Deletion cancelled."

    def load_session(self, name: str) -> SessionState:
        session_file = SESSION_DIR / f"{name}.cxsession"
        if not session_file.exists():
            raise FileNotFoundError(f"Session '{name}' not found.")
        with open(session_file, "rb") as f:
            loaded_state = pickle.load(f)
        console.print(f"[bold green]✓ Session '{name}' loaded.[/bold green]")
        return loaded_state

    def show_status(self, state: SessionState):
        panel_content = (
            f"[bold]Active Connections:[/bold] {len(state.connections)}\n"
            f"[bold]Session Variables:[/bold]  {len(state.variables)}"
        )
        console.print(
            Panel(panel_content, title="Current Session Status", border_style="blue")
        )
