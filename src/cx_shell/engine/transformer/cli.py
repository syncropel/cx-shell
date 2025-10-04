import asyncio
import json
import logging
import sys
from pathlib import Path

import structlog
import typer
import yaml
from rich.console import Console

from .service import TransformerService

# --- CLI Setup ---
app = typer.Typer(
    name="transformer-cli",
    help="🤖 A standalone CLI for running declarative data transformation scripts.",
    no_args_is_help=True,
    rich_markup_mode="markdown",
)

console = Console(stderr=True)


def setup_logging(verbose: bool):
    """Configures structlog to route all logs to stderr."""
    log_level = logging.DEBUG if verbose else logging.INFO
    # A simplified logger for the CLI, routing everything to stderr.
    structlog.configure(
        processors=[
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.dev.ConsoleRenderer(colors=True),
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    # Configure the root logger
    root_logger = logging.getLogger()
    handler = logging.StreamHandler(sys.stderr)
    # This simple formatter is fine since we don't have non-structlog libraries here
    handler.setFormatter(logging.Formatter("%(message)s"))
    # Clear existing handlers
    if root_logger.hasHandlers():
        root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.setLevel(log_level)

    logger = structlog.get_logger("transformer_cli")
    logger.info("CLI logging configured.")


# --- THIS IS THE FIX ---
# We add the main callback function, just like in connector-cli.
# This establishes the multi-command structure correctly.
@app.callback()
def main_callback(
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Enable verbose DEBUG logging."
    ),
):
    """Main Typer callback to process global options before any command runs."""
    setup_logging(verbose)


# --- END FIX ---


@app.command()
def run(
    script_path: Path = typer.Option(
        ...,
        "--script",
        "-s",
        help="Path to the .transformer.yaml script to execute.",
        exists=True,
        file_okay=True,
        dir_okay=False,
    ),
):
    """
    Reads JSON data from stdin, transforms it according to the script,
    and prints the final JSON result to stdout (unless saved to a file).
    """
    logger = structlog.get_logger("transformer_cli.run")
    try:
        logger.info("Loading transform script...", path=str(script_path))
        with open(script_path, "r", encoding="utf-8") as f:
            script_data = yaml.safe_load(f)

        logger.info("Reading initial data from stdin...")
        piped_content = sys.stdin.read()
        if not piped_content:
            console.print("[bold red]Error:[/bold red] No data received from stdin.")
            raise typer.Exit(code=1)
        initial_data = json.loads(piped_content)

        service = TransformerService()
        run_context = {"initial_input": initial_data}

        final_result = asyncio.run(service.run(script_data, run_context))

        if final_result is not None:
            print(json.dumps(final_result, indent=2))

        console.print(
            "--- ✅ [bold green]Script Finished Successfully[/bold green] ---",
        )

    except Exception as e:
        console.print("\n--- ❌ [bold red]Script Failed[/bold red] ---")
        console.print(f"[red]Error:[/red] {e}")
        logger.error("script.failed", error=str(e), exc_info=True)
        raise typer.Exit(code=1)
