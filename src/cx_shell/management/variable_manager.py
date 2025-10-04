import sys

from rich.console import Console
from rich.table import Table
from rich import box

from ..interactive.session import SessionState

console = Console()


class VariableManager:
    """Handles all logic for listing and deleting session variables."""

    def list_variables(self, state: SessionState):
        if not state.variables:
            console.print("No variables set in the current session.")
            return

        table = Table(title="Session Variables", box=box.ROUNDED)
        table.add_column("Variable Name", style="cyan")
        table.add_column("Type", style="magenta")
        table.add_column("Size / Length", style="green", justify="right")
        table.add_column("Preview", style="dim", overflow="fold", max_width=60)

        for name, value in sorted(state.variables.items()):
            var_type = type(value).__name__
            size = ""
            preview = ""

            if isinstance(value, (list, tuple, set, dict)):
                size = str(len(value))
                preview = repr(value)[:200] + ("..." if len(repr(value)) > 200 else "")
            else:
                size = f"{sys.getsizeof(value)} bytes"
                preview = repr(value)

            table.add_row(name, var_type, size, preview)

        console.print(table)

    def delete_variable(self, state: SessionState, name: str) -> str:
        if name not in state.variables:
            raise KeyError(f"Variable '{name}' not found.")

        del state.variables[name]
        return f"Variable '{name}' deleted."
