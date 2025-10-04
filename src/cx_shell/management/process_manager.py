import subprocess
from pathlib import Path
from datetime import datetime
from typing import Optional

from rich.console import Console
from rich.table import Table
from rich import box

from ..engine.connector.config import CX_HOME
from ..data.process_schemas import Process

# --- Constants ---
PROCESS_DIR = CX_HOME / "processes"
LOG_DIR = CX_HOME / "logs"
CONSOLE = Console()


class ProcessManager:
    """
    Manages long-running, asynchronous background tasks initiated by the agent or user.
    """

    def __init__(self, cx_home_path: Optional[Path] = None):
        _cx_home = cx_home_path or CX_HOME
        # Update the module-level constants
        global PROCESS_DIR, LOG_DIR
        PROCESS_DIR = _cx_home / "processes"
        LOG_DIR = _cx_home / "logs"

        PROCESS_DIR.mkdir(parents=True, exist_ok=True)
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        self.user = "default_user"

    def _get_process_file(self, process_id: str) -> Path:
        """Returns the path to a process's state file."""
        return PROCESS_DIR / f"{process_id}.json"

    def _read_process(self, process_file: Path) -> Process:
        """Reads and validates a process state file."""
        return Process.model_validate_json(process_file.read_text())

    def _write_process(self, process: Process):
        """Writes a process's state to its file."""
        process_file = self._get_process_file(process.id)
        process_file.write_text(process.model_dump_json(indent=2))

    def start_process(self, goal: str, flow_path: Path) -> Process:
        """
        Creates and starts a new background process to execute a flow.

        Args:
            goal: The original user goal for context.
            flow_path: The path to the .flow.yaml file to execute.

        Returns:
            The created Process object.
        """
        if not flow_path.exists():
            raise FileNotFoundError(f"Flow file not found: {flow_path}")

        timestamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
        process_id = f"proc-{timestamp}"
        log_path = LOG_DIR / f"{process_id}.log"

        process = Process(
            id=process_id,
            goal=goal,
            flow_path=flow_path,
            log_path=log_path,
            owner=self.user,
        )

        try:
            with open(log_path, "wb") as log_file:
                # We execute a new instance of the `cx` CLI in a detached subprocess.
                # This ensures the process is fully independent of the interactive shell.
                # We use `extract run` as it is the non-interactive entrypoint for running flows.
                proc_handle = subprocess.Popen(
                    ["cx", "extract", "run", "--script", str(flow_path)],
                    stdout=log_file,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,  # Ensures it continues running if the parent shell closes
                )

            process.status = "running"
            process.started_at = datetime.utcnow()
            process.pid = proc_handle.pid
            self._write_process(process)

            CONSOLE.print(
                f"[bold green]✓[/bold green] Process '[cyan]{process.id}[/cyan]' started in the background."
            )
            return process

        except Exception as e:
            process.status = "failed"
            process.completed_at = datetime.utcnow()
            self._write_process(process)
            CONSOLE.print(f"[bold red]✗[/bold red] Failed to start process: {e}")
            raise

    def list_processes(self):
        """Displays a table of all managed processes."""
        process_files = list(PROCESS_DIR.glob("*.json"))
        if not process_files:
            CONSOLE.print("No background processes found.")
            return

        table = Table(title="Background Processes", box=box.ROUNDED)
        table.add_column("ID", style="cyan")
        table.add_column("Status", style="yellow")
        table.add_column("Flow", style="magenta")
        table.add_column("Started At (UTC)", style="dim")

        processes = [self._read_process(pf) for pf in process_files]

        # Update status of running processes
        for proc in processes:
            if proc.status == "running" and proc.pid:
                # Check if the OS process is still alive
                try:
                    # A non-existent PID raises ProcessLookupError. A zombie process returns None.
                    if subprocess.os.waitpid(proc.pid, subprocess.os.WNOHANG) != (0, 0):
                        # The process has finished. We assume success unless log indicates failure.
                        # A more robust system would parse the log for a final status.
                        proc.status = "completed"
                        proc.completed_at = datetime.utcnow()
                        self._write_process(proc)
                except (ProcessLookupError, ChildProcessError):
                    proc.status = "completed"  # Or "unknown", if process disappeared
                    proc.completed_at = datetime.utcnow()
                    self._write_process(proc)

        for proc in sorted(processes, key=lambda p: p.created_at, reverse=True):
            table.add_row(
                proc.id,
                proc.status,
                proc.flow_path.name,
                str(proc.started_at) if proc.started_at else "N/A",
            )

        CONSOLE.print(table)

    def get_logs(self, process_id: str, follow: bool = False):
        """
        Displays the logs for a specific process.

        Args:
            process_id: The ID of the process to view.
            follow: If True, tails the log file continuously.
        """
        process_file = self._get_process_file(process_id)
        if not process_file.exists():
            raise FileNotFoundError(f"Process '{process_id}' not found.")

        process = self._read_process(process_file)

        if follow:
            CONSOLE.print(
                f"[dim]Tailing logs for [cyan]{process_id}[/cyan]... (Press Ctrl+C to stop)[/dim]"
            )
            try:
                subprocess.run(["tail", "-f", "-n", "+1", str(process.log_path)])
            except KeyboardInterrupt:
                CONSOLE.print()  # Newline after stopping
        else:
            if process.log_path.exists():
                CONSOLE.print(f"--- Logs for [cyan]{process_id}[/cyan] ---")
                CONSOLE.print(process.log_path.read_text())
                CONSOLE.print("--- End of Logs ---")
            else:
                CONSOLE.print(
                    "Log file not found or process has not yet produced output."
                )
