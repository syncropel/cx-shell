import asyncio
import functools
import importlib.metadata
import json
import logging
import os
from pathlib import Path
import shlex
import shutil
import sys
from typing import Optional

import structlog
import typer
import uvicorn
import yaml
from rich.console import Console
from rich.traceback import Traceback
from ._bootstrap import bootstrap_models

from cx_shell.interactive.executor import CommandExecutor
from cx_shell.interactive.main import start_repl
from cx_shell.interactive.session import SessionState
from cx_shell.management.upgrade_manager import UpgradeManager
from cx_shell.state import APP_STATE
from .utils import get_assets_root
from .management.shell_manager import ShellManager

# --- THIS IS THE NEW, CRITICAL BLOCK ---
# We perform the late imports needed for Pydantic's model_rebuild here,
# after all modules have been loaded by the initial import chain.

from .interactive.session import SessionState

# Call the bootstrap function once, at the top level of the application's entry point.
# This ensures all Pydantic models are ready before any commands are executed.
bootstrap_models()
# --- END NEW BLOCK ---

console = Console()
logger = structlog.get_logger(__name__)


def version_callback(value: bool):
    """Prints the application version and exits."""
    if value:
        try:
            version = importlib.metadata.version("cx-shell")
            console.print(f"cx version: {version}")
        except importlib.metadata.PackageNotFoundError:
            console.print("cx version: unknown (package not installed)")
        raise typer.Exit()


def setup_logging(verbose: bool):
    log_level = logging.DEBUG if verbose else logging.INFO
    shared_processors = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.format_exc_info,
    ]
    structlog.configure(
        processors=shared_processors
        + [structlog.stdlib.ProcessorFormatter.wrap_for_formatter],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=shared_processors,
        processor=structlog.dev.ConsoleRenderer(),
    )
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.setLevel(log_level)
    if verbose:
        logging.getLogger("httpx").setLevel(logging.DEBUG)


def handle_exceptions(func):
    """A decorator to catch and format exceptions for all CLI commands."""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        error_console = Console(stderr=True)
        try:
            return func(*args, **kwargs)
        except typer.Exit:
            raise
        except Exception as e:
            error_console.print(f"[bold red]Error:[/bold red] {e}")
            if APP_STATE.verbose_mode:
                error_console.print(
                    Traceback.from_exception(
                        type(e), e, e.__traceback__, show_locals=True
                    )
                )
            raise typer.Exit(code=1)

    return wrapper


def _reconstruct_command_string(command_name: str, args: list[str]) -> str:
    """
    Safely reconstructs a command string from a list of arguments,
    quoting any arguments that contain spaces or special characters to
    ensure they are parsed correctly by the internal Lark parser.
    """
    quoted_args = [shlex.quote(arg) for arg in args]
    return f"{command_name} {' '.join(quoted_args)}"


# def _run_command_string(command: str):
#     """Instantiates a temporary executor and runs a single command string."""
#     logger.info(
#         "CLI wrapper constructing command string for executor", command_string=command
#     )
#     temp_state = SessionState(is_interactive=False)

#     # --- START OF DEFINITIVE FIX ---
#     # Import the handler and instantiate it for non-interactive runs.
#     from cx_shell.interactive.output_handler import RichConsoleHandler

#     output_handler = RichConsoleHandler()
#     executor = CommandExecutor(temp_state, output_handler)
#     # --- END OF DEFINITIVE FIX ---

#     piped_input = None
#     if not sys.stdin.isatty():
#         content = sys.stdin.read()
#         if content:
#             try:
#                 piped_input = json.loads(content)
#             except json.JSONDecodeError:
#                 piped_input = content
#     asyncio.run(executor.execute(command, piped_input=piped_input))


# The new, correct function for src/cx_shell/cli.py
def _run_command_string(command: str):
    """Instantiates a temporary executor and runs a single command string."""
    logger.info(
        "CLI wrapper constructing command string for executor", command_string=command
    )
    temp_state = SessionState(is_interactive=False)

    # --- START OF DEFINITIVE, CORRECTED FIX ---
    from cx_shell.interactive.output_handler import RichConsoleHandler

    # 1. Instantiate the CommandExecutor first, passing a temporary `None` for the handler.
    executor = CommandExecutor(temp_state, output_handler=None)

    # 2. Now, instantiate the RichConsoleHandler and pass the fully built executor to it.
    output_handler = RichConsoleHandler(executor=executor)

    # 3. Finally, assign the handler back to the executor to complete the loop.
    executor.output_handler = output_handler
    # --- END OF DEFINITIVE, CORRECTED FIX ---

    piped_input = None
    if not sys.stdin.isatty():
        content = sys.stdin.read()
        if content:
            try:
                piped_input = json.loads(content)
            except json.JSONDecodeError:
                piped_input = content

    asyncio.run(executor.execute(command, piped_input=piped_input))


app = typer.Typer(
    name="cx",
    help="The Contextual Shell: A declarative, multi-stage automation platform.",
    invoke_without_command=True,
    rich_markup_mode="markdown",
)


@app.callback()
def main_callback(
    ctx: typer.Context,
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Enable verbose DEBUG logging."
    ),
    version: Optional[bool] = typer.Option(
        None,
        "--version",
        callback=version_callback,
        is_eager=True,
        help="Show the application's version and exit.",
    ),
):
    """Main entry point. Handles global options and starts the REPL."""
    # --- START OF FIX ---
    # Set up the shared VFS root as an environment variable.
    # This ensures all components (connector, transformer, cache) use the same base path.
    from .utils import CX_HOME

    vfs_root = CX_HOME / "data"
    vfs_root.mkdir(parents=True, exist_ok=True)
    os.environ["VFS_ROOT"] = str(vfs_root)
    # --- END OF FIX ---

    APP_STATE.verbose_mode = verbose
    setup_logging(verbose)
    if ctx.invoked_subcommand is None:
        start_repl()


@app.command()
@handle_exceptions
def init(
    project_name: Optional[str] = typer.Option(
        None,
        "--project",
        "-p",
        help="Initialize a new project directory with the recommended structure.",
    ),
):
    """Initializes the `~/.cx` workspace or a new project directory."""
    # --- START OF DEFINITIVE FIX ---
    # We import the path constants INSIDE the function. This is a standard pattern
    # for making CLI functions testable. It ensures that when this function is
    # called from a test, it will see the values that have been monkeypatched
    # by the test fixture *before* the function was called. If the imports were
    # at the top of the file, they would cache the un-patched, real paths.
    from cx_shell.utils import CX_HOME
    from cx_shell.engine.connector.config import BLUEPRINTS_BASE_PATH
    # --- END OF DEFINITIVE FIX ---

    if project_name:
        # --- PROJECT INITIALIZATION LOGIC (Unchanged) ---
        project_dir = Path.cwd() / project_name
        if project_dir.exists():
            console.print(
                f"[bold red]Error:[/bold red] Directory '{project_name}' already exists."
            )
            raise typer.Exit(code=1)

        console.print(
            f"🚀 Initializing new cx project in [bold cyan]{project_dir}[/bold cyan]..."
        )

        project_subdirs = [
            "flows",
            "queries",
            "scripts",
            "templates",
            "outputs",
            "examples",
            "notebooks",  # Added notebooks directory as a best practice
        ]
        for subdir in project_subdirs:
            (project_dir / subdir).mkdir(parents=True, exist_ok=True)
            console.print(f"✅ Created directory: [dim]{subdir}/[/dim]")

        # Create a default .gitignore
        gitignore_content = "*.old\n*.bak\n\n# Local outputs\noutputs/\ndata/\n"
        (project_dir / ".gitignore").write_text(gitignore_content)
        console.print("✅ Created [dim].gitignore[/dim]")

        # Scaffold a README.md with a placeholder title
        readme_content = f"# {project_name.replace('-', ' ').title()} Project\n\nDescribe the purpose of this project here.\n"
        (project_dir / "README.md").write_text(readme_content)
        console.print("✅ Created [dim]README.md[/dim]")

        console.print("\n[bold green]Project initialization complete![/bold green]")
        console.print(f"Navigate to your new project: [bold]cd {project_name}[/bold]")

    else:
        # --- GLOBAL WORKSPACE INITIALIZATION LOGIC ---
        console.print(
            "[bold green]Initializing global `~/.cx` workspace...[/bold green]"
        )
        # This now correctly uses the (potentially patched) CX_HOME constant.
        connections_dir = CX_HOME / "connections"

        # This list now correctly uses the (potentially patched) constants.
        dirs_to_create = [
            connections_dir,
            CX_HOME / "secrets",
            BLUEPRINTS_BASE_PATH / "user",
            CX_HOME / "flows",
            CX_HOME / "queries",
            CX_HOME / "scripts",
            CX_HOME / "notebooks",  # Added notebooks directory
        ]
        for d in dirs_to_create:
            d.mkdir(parents=True, exist_ok=True)
            console.print(f"✅ Ensured directory exists: [dim]{d}[/dim]")

        try:
            assets_root = get_assets_root()
            source_connections_dir = assets_root / "connections"
            if source_connections_dir.is_dir():
                for conn_asset in source_connections_dir.glob("*.conn.yaml"):
                    target_path = connections_dir / conn_asset.name
                    if not target_path.exists():
                        shutil.copy(conn_asset, target_path)
                        console.print(
                            f"✅ Created sample connection: [dim]{target_path}[/dim]"
                        )
                    else:
                        console.print(
                            f"☑️  Connection already exists, skipping: [dim]{target_path}[/dim]"
                        )
        except Exception as e:
            console.print(f"[bold red]Error copying sample connections:[/bold red] {e}")

        try:
            assets_root = get_assets_root()
            bundled_blueprints_root = assets_root / "blueprints" / "community"
            if bundled_blueprints_root.is_dir():
                for blueprint_source_dir in bundled_blueprints_root.iterdir():
                    if blueprint_source_dir.is_dir():
                        blueprint_name = blueprint_source_dir.name
                        manifest_path = blueprint_source_dir / "blueprint.cx.yaml"
                        if not manifest_path.is_file():
                            continue
                        with open(manifest_path, "r") as f:
                            version = (
                                yaml.safe_load(f).get("version", "0.0.0").lstrip("v")
                            )
                        # This target_dir now correctly uses the patched BLUEPRINTS_BASE_PATH
                        target_dir = (
                            BLUEPRINTS_BASE_PATH
                            / "community"
                            / blueprint_name
                            / version
                        )
                        if target_dir.exists():
                            shutil.rmtree(target_dir)
                        shutil.copytree(blueprint_source_dir, target_dir)
                        console.print(
                            f"✅ Copied sample blueprint '{blueprint_name}' to: [dim]{target_dir}[/dim]"
                        )
        except Exception as e:
            console.print(f"[bold red]Error copying sample blueprints:[/bold red] {e}")

        console.print(
            "\n[bold green]Global workspace initialization complete![/bold green]"
        )
        console.print("Run `cx` to start the interactive shell.")


@app.command()
@handle_exceptions
def upgrade():
    """Checks for and installs the latest version of the cx shell."""
    manager = UpgradeManager()
    manager.run_upgrade()


@app.command()
@handle_exceptions
def serve(
    host: str = typer.Option("0.0.0.0", help="The host to bind the server to."),
    port: int = typer.Option(8888, help="The port to run the server on."),
):
    """Launches the cx-server API for programmatic access and UIs."""
    console.print(
        f"[bold green]🚀 Launching cx-server on http://{host}:{port}[/bold green]"
    )
    console.print("Press Ctrl+C to shut down.")

    # We point uvicorn to the 'app' instance inside our server.main module
    uvicorn.run("cx_shell.server.main:app", host=host, port=port, log_level="info")


# --- Pass-Through Command Groups ---


@app.command(
    "connection",
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
)
@handle_exceptions
def connection_cmd(ctx: typer.Context):
    """Manage local connections (e.g., list, create)."""
    command_string = _reconstruct_command_string(ctx.command.name, ctx.args)
    _run_command_string(command_string)


@app.command(
    "app", context_settings={"allow_extra_args": True, "ignore_unknown_options": True}
)
@handle_exceptions
def app_cmd(ctx: typer.Context):
    """Discover, install, and manage Syncropel Applications."""
    command_string = _reconstruct_command_string(ctx.command.name, ctx.args)
    _run_command_string(command_string)


@app.command(
    "flow", context_settings={"allow_extra_args": True, "ignore_unknown_options": True}
)
@handle_exceptions
def flow_cmd(ctx: typer.Context):
    """List and run reusable .flow.yaml workflows."""
    command_string = _reconstruct_command_string(ctx.command.name, ctx.args)
    _run_command_string(command_string)


@app.command(
    "query", context_settings={"allow_extra_args": True, "ignore_unknown_options": True}
)
@handle_exceptions
def query_cmd(ctx: typer.Context):
    """List and run reusable .sql queries."""
    command_string = _reconstruct_command_string(ctx.command.name, ctx.args)
    _run_command_string(command_string)


@app.command(
    "script",
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
)
@handle_exceptions
def script_cmd(ctx: typer.Context):
    """List and run reusable .py scripts."""
    command_string = _reconstruct_command_string(ctx.command.name, ctx.args)
    _run_command_string(command_string)


@app.command(
    "process",
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
)
@handle_exceptions
def process_cmd(ctx: typer.Context):
    """Manage long-running background processes."""
    command_string = _reconstruct_command_string(ctx.command.name, ctx.args)
    _run_command_string(command_string)


@app.command(
    "compile",
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
)
@handle_exceptions
def compile_cmd(ctx: typer.Context):
    """Compiles an API specification into a `cx` blueprint."""
    command_string = _reconstruct_command_string(ctx.command.name, ctx.args)
    _run_command_string(command_string)


@app.command(
    "extract",
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
)
@handle_exceptions
def extract_cmd(ctx: typer.Context):
    """(For scripting) Execute an extraction workflow. Designed for piping."""
    command_string = _reconstruct_command_string(ctx.command.name, ctx.args)
    _run_command_string(command_string)


@app.command(
    "transform",
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
)
@handle_exceptions
def transform_cmd(ctx: typer.Context):
    """(For scripting) Execute a transformation workflow. Designed for piping."""
    command_string = _reconstruct_command_string(ctx.command.name, ctx.args)
    _run_command_string(command_string)


@app.command(
    "workspace",
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
)
@handle_exceptions
def workspace_cmd(ctx: typer.Context):
    """Manage multi-root workspace paths (e.g., add, list, remove, index)."""
    command_string = _reconstruct_command_string(ctx.command.name, ctx.args)
    _run_command_string(command_string)


@app.command()
@handle_exceptions
def install():
    """
    Creates a virtual environment and installs dependencies for the current project.
    Reads configuration from the 'cx.project.yaml' file in the current directory.
    """
    from cx_shell.management.install_manager import InstallManager

    manager = InstallManager()
    # Assume the command is run from the project root
    project_root = Path.cwd()
    manager.install_project_dependencies(project_root)


@app.command()
@handle_exceptions
def shell():
    """
    Launches the interactive Syncropel shell for the current project.

    If a cx.project.yaml with an `environment` is found, it will activate a
    hermetic shell using Nix. Otherwise, it starts a standard session.
    """
    # The command is always run in the context of the current working directory.
    project_root = Path.cwd()

    # We create a new instance of the manager for this single operation.
    shell_manager = ShellManager()

    # Delegate the entire activation logic to the manager.
    shell_manager.activate_shell(project_root)
