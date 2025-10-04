import asyncio
import io
import json
import logging
import sys
from contextlib import redirect_stdout
from pathlib import Path
from typing import Any

from .utils import safe_serialize
import structlog
import typer
from rich.console import Console

from .service import ConnectorService

# --- CLI Setup ---
app = typer.Typer(
    name="connector-cli",
    help="🤖 A standalone CLI for testing and interacting with the Connector Fabric.",
    no_args_is_help=True,
    rich_markup_mode="markdown",
)

console = Console(stderr=True)


def setup_logging(verbose: bool):
    """
    Configures structlog to capture ALL logs (including from the standard library)
    and route them ONLY to stderr, correctly handling extra context.
    """
    log_level = logging.DEBUG if verbose else logging.INFO

    # --- THIS IS THE DEFINITIVE, CORRECT CONFIGURATION ---

    # 1. Define the processors for structlog-aware loggers.
    shared_processors = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.ExtraAdder(),  # Handles extra kwargs
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
    ]

    # 2. Configure structlog itself.
    structlog.configure(
        processors=shared_processors
        + [
            # This processor prepares the log record for the standard library.
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    # 3. Create a formatter that will render the processed log records.
    formatter = structlog.stdlib.ProcessorFormatter(
        # The foreign_pre_chain is for logs that DON'T originate from structlog.
        foreign_pre_chain=shared_processors,
        processor=structlog.dev.ConsoleRenderer(colors=True, force_colors=True),
    )

    # 4. Get the root logger and configure its handler.
    root_logger = logging.getLogger()
    # Remove any handlers added by other libraries to ensure we have full control.
    for handler in root_logger.handlers:
        root_logger.removeHandler(handler)

    # Add our stderr-only handler.
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(formatter)
    root_logger.addHandler(handler)
    root_logger.setLevel(log_level)

    # --- END FIX ---

    logger = structlog.get_logger("connector_cli")
    logger.info("CLI logging configured to route all output to stderr.")


@app.callback()
def main_callback(
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Enable verbose DEBUG logging."
    ),
):
    """Main Typer callback to process global options before any command runs."""
    setup_logging(verbose)


@app.command()
def run(
    script_path: Path = typer.Option(
        ...,
        "--script",
        "-s",
        help="Path to the .connector.yaml script to execute.",
        exists=True,
    ),
    debug_action: bool = typer.Option(
        False,
        "--debug-action",
        help="Print rendered action payloads to stderr before sending.",
    ),
):
    """Executes a *.connector.yaml script, consuming optional stdin and printing results to stdout."""
    try:
        connector_service = ConnectorService()

        piped_input = {}
        if not sys.stdin.isatty():
            content = sys.stdin.read()
            if content:
                piped_input = json.loads(content)

        results = asyncio.run(
            connector_service.run_script(script_path, piped_input, debug_action)
        )

        serializable_result = safe_serialize(results)
        print(json.dumps(serializable_result, default=str))

        console.print(
            "--- ✅ [bold green]Script Finished Successfully[/bold green] ---"
        )
    except Exception as e:
        console.print("\n--- ❌ [bold red]Script Failed[/bold red] ---")
        console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(code=1)


def _print_data_to_stdout(result: Any):
    serializable_result = safe_serialize(result)
    print(json.dumps(serializable_result, default=str))


@app.command()
def test(
    connection_source: str = typer.Argument(
        ..., help="Connection source (e.g., 'file:./connections/zendesk-dev')"
    ),
):
    """Tests a single connection source. Does not print to stdout."""
    try:
        connector_service = ConnectorService()

        async def _test():
            connection, secrets = await connector_service.resolver.resolve(
                connection_source
            )
            strategy = connector_service._get_strategy_for_connection_model(connection)
            is_ok = await strategy.test_connection(connection, secrets)
            status_text = (
                "✅ [bold green]SUCCESS[/bold green]"
                if is_ok
                else "❌ [bold red]FAILURE[/bold red]"
            )
            console.print(
                f"Connection test for '[cyan]{connection.name}[/cyan]': {status_text}"
            )
            if not is_ok:
                raise typer.Exit(code=1)

        asyncio.run(_test())
    except Exception as e:
        console.print("\n--- ❌ [bold red]Test Failed[/bold red] ---")
        console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(code=1)


@app.command()
def browse(
    connection_source: str = typer.Argument(..., help="Connection source to browse."),
    path: str = typer.Option("/", "--path", "-p", help="Virtual path to browse."),
):
    """Browses a connection's virtual filesystem and prints the listing to stdout."""
    f = io.StringIO()
    try:
        connector_service = ConnectorService()
        with redirect_stdout(f):

            async def _browse():
                connection, secrets = await connector_service.resolver.resolve(
                    connection_source
                )
                strategy = connector_service._get_strategy_for_connection_model(
                    connection
                )
                path_parts = [part for part in path.strip("/").split("/") if part]
                items = await strategy.browse_path(path_parts, connection, secrets)
                console.print(
                    f"\n--- 📂 Contents of '[yellow]{path}[/yellow]' in '[cyan]{connection.name}[/cyan]' ---"
                )
                return items

            results = asyncio.run(_browse())
        _print_data_to_stdout(results)
    except Exception as e:
        console.print("\n--- ❌ [bold red]Browse Failed[/bold red] ---")
        console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(code=1)


@app.command()
def read(
    connection_source: str = typer.Argument(
        ..., help="Connection source containing the file."
    ),
    path: str = typer.Argument(..., help="Virtual path of the file to read."),
):
    """Reads the content of a virtual file and prints it to stdout."""
    f = io.StringIO()
    try:
        connector_service = ConnectorService()
        with redirect_stdout(f):

            async def _read():
                connection, secrets = await connector_service.resolver.resolve(
                    connection_source
                )
                strategy = connector_service._get_strategy_for_connection_model(
                    connection
                )
                path_parts = [part for part in path.strip("/").split("/") if part]
                content_response = await strategy.get_content(
                    path_parts, connection, secrets
                )
                console.print(
                    f"\n--- 📄 Content of '[yellow]{path}[/yellow]' in '[cyan]{connection.name}[/cyan]' ---"
                )
                return content_response.content

            results = asyncio.run(_read())

        # The content from strategies is already a string, so we just print it.
        print(results)
    except Exception as e:
        console.print("\n--- ❌ [bold red]Read Failed[/bold red] ---")
        console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(code=1)
