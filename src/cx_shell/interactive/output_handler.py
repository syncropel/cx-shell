# ~/repositories/cx-shell/src/cx_shell/interactive/output_handler.py

import json
from abc import ABC, abstractmethod
from typing import Any, Optional, Dict, TYPE_CHECKING

import jmespath
import structlog
from rich import box
from rich.console import Console
from rich.markdown import Markdown
from rich.panel import Panel
from rich.pretty import Pretty
from rich.syntax import Syntax
from rich.table import Table

from .commands import Command, InspectCommand
from ..management.notebook_parser import NotebookParser

if TYPE_CHECKING:
    from ..engine.context import RunContext
    from .executor import CommandExecutor


# A single, shared console instance for all rich output in the REPL
console = Console()
logger = structlog.get_logger(__name__)


class IOutputHandler(ABC):
    """
    An abstract interface for handling the output of executed commands.
    This architecture decouples the CommandExecutor from the presentation layer.
    """

    def __init__(self, executor: "CommandExecutor"):
        self.executor = executor

    @abstractmethod
    async def handle_result(
        self,
        result: Any,
        executable: Optional[Command],
        options: Optional[Dict] = None,
        run_context: Optional["RunContext"] = None,
    ):
        """
        Processes and displays the final result of a command.
        """
        pass


class RichConsoleHandler(IOutputHandler):
    """
    An implementation of the output handler that renders results to the
    terminal using the rich library. This class contains all the presentation
    logic for the interactive REPL.
    """

    async def handle_result(
        self,
        result: Any,
        executable: Optional[Command],
        options: Optional[Dict] = None,
        run_context: Optional["RunContext"] = None,
    ):
        options = options or {}

        logger.debug(
            "output_handler.handle_result.received_raw",
            raw_result=result,
            options=options,
            run_type=run_context.run_type if run_context else "unknown",
        )

        # --- DEFINITIVE FIX: Dispatch to the correct renderer ---
        if run_context and run_context.run_type == "notebook":
            self._render_notebook_output(result, run_context, options)
            return
        # --- END FIX ---

        # Fallback to single output rendering for flows and interactive commands
        data_to_render = self._apply_formatters(result, options)
        self._render_single_output(data_to_render, executable, options)

    def _render_notebook_output(
        self, result: Dict[str, Any], context: "RunContext", options: Dict
    ):
        """
        Renders the output of a notebook execution sequentially, block-by-block,
        respecting the original document structure and applying formatters.
        """
        if not context.current_flow_path:
            logger.warning(
                "notebook_render.missing_path", "Falling back to single output render."
            )
            self._render_single_output(result, None, options)
            return

        try:
            # 1. Load the original notebook structure to get the correct order and content.
            parser = NotebookParser()
            page = parser.parse(context.current_flow_path)

            console.rule(
                f"[bold green]Page Output: {page.name}[/bold green]", style="green"
            )

            # 2. Iterate through the blocks of the original page to render in order.
            for block in page.blocks:
                if block.engine == "markdown":
                    # Render the markdown, now with Jinja templating to display results inline!
                    template = self.executor.script_engine.jinja_env.from_string(
                        block.content
                    )
                    render_context = {
                        "steps": context.steps,
                        "inputs": context.script_input,
                    }
                    rendered_markdown = template.render(render_context)
                    console.print(Markdown(rendered_markdown))

                else:  # It's an executable block
                    block_result = result.get(block.id)

                    # Render a simple header for the code block
                    status_icon = "✓" if block_result is not None else "⚪"
                    console.print(
                        f"\n[dim]--- {status_icon} Block: [bold cyan]{block.id}[/bold cyan] ({block.engine}) ---[/dim]"
                    )

                    # Render the output of the block
                    if block_result is not None:
                        # Apply universal formatters (--cx-*) to the block's individual output
                        formatted_block_result = self._apply_formatters(
                            block_result, options
                        )
                        self._render_single_output(
                            formatted_block_result, None, {}
                        )  # Use default options for block output
                    else:
                        console.print("[dim](Skipped or no output)[/dim]")

                    console.print()  # Add a blank line for spacing

            console.rule(style="green")

        except Exception as e:
            logger.error("notebook_render.failed", error=str(e), exc_info=True)
            console.print(f"[bold red]Error rendering notebook output:[/bold red] {e}")
            # Fallback to simple print if rendering fails
            self._render_single_output(result, None, options)

    def _render_single_output(
        self, data_to_render: Any, executable: Optional[Command], options: Dict
    ):
        """
        Handles the rendering of a single, final data payload.

        This method intelligently detects the data type and applies the best-suited
        renderer. The default for structured data (lists or dictionaries) is
        pretty-printed JSON. Table view is only used when explicitly requested
        with the `--cx-output table` flag.
        """
        logger.debug(
            "output_handler.render_single_output.final_data",
            data_to_render=data_to_render,
            options=options,
        )

        # Handle null output
        if data_to_render is None:
            console.print(Pretty(None))
            return

        # Handle string data, attempting to parse it as JSON first.
        if isinstance(data_to_render, str):
            try:
                # If it's a valid JSON string, treat it as structured data for rendering.
                data_to_render = json.loads(data_to_render)
                logger.debug("output_handler.string_render.json_detected")
            except (json.JSONDecodeError, TypeError):
                # If it's not JSON, treat it as a simple status message.
                if any(
                    word in data_to_render.lower()
                    for word in [
                        "saved",
                        "deleted",
                        "cancelled",
                        "restored",
                        "successful",
                        "variable",
                    ]
                ):
                    console.print(f"[yellow]✓ {data_to_render}[/yellow]")
                else:
                    console.print(f"[bold green]✓[/bold green] {data_to_render}")
                return

        # Handle structured error messages from the engine.
        if isinstance(data_to_render, dict) and "error" in data_to_render:
            console.print(
                f"[bold red]Runtime Error:[/bold red] {data_to_render['error']}"
            )
            return

        # Handle structured success messages from the engine.
        if (
            isinstance(data_to_render, dict)
            and "message" in data_to_render
            and "status" in data_to_render
        ):
            console.print(f"[bold green]✓[/bold green] {data_to_render['message']}")
            return

        # Handle special rendering for the `inspect` command.
        if isinstance(executable, InspectCommand):
            summary = data_to_render
            panel_content = (
                f"[bold]Variable:[/bold] [cyan]{summary['var_name']}[/cyan]\n"
                f"[bold]Type:[/bold] [green]{summary['type']}[/green]\n"
            )
            if "length" in summary:
                panel_content += f"[bold]Length:[/bold] {summary['length']}\n"
            if "keys" in summary:
                panel_content += f"[bold]Keys:[/bold] {summary['keys']}"
            if "item_zero_keys" in summary:
                panel_content += (
                    f"[bold]Item[0] Keys:[/bold] {summary['item_zero_keys']}"
                )
            console.print(
                Panel(panel_content, title="Object Inspector", border_style="yellow")
            )
            return

        # --- Definitive Output Mode Dispatcher ---
        output_mode = options.get("output_mode", "default")

        # PATH 1: Render as a table ONLY if explicitly requested.
        if output_mode == "table":
            is_list_of_dicts = (
                isinstance(data_to_render, list)
                and bool(data_to_render)
                and all(isinstance(i, dict) for i in data_to_render)
            )
            if is_list_of_dicts:
                try:
                    table = Table(
                        title="[bold]Data View[/bold]", box=box.ROUNDED, show_lines=True
                    )
                    headers = options.get("columns") or list(data_to_render[0].keys())
                    for header in headers:
                        table.add_column(str(header), style="cyan", overflow="fold")
                    for row in data_to_render:
                        table.add_row(*(str(row.get(h, "")) for h in headers))
                    console.print(table)
                    return
                except Exception:
                    logger.warning("output_handler.table_render.failed", exc_info=True)
                    console.print(
                        "[yellow]Warning: Could not render data as a table. Falling back to JSON.[/yellow]"
                    )
            else:
                console.print(
                    "[yellow]Warning: --cx-output table can only be used on a list of objects. Displaying as JSON.[/yellow]"
                )

        # PATH 2 (Default): Render as pretty-printed JSON for all other structured data.
        try:
            if not isinstance(data_to_render, (dict, list)) or not data_to_render:
                console.print(Pretty(data_to_render))
                return
            formatted_json = json.dumps(data_to_render, indent=2, default=str)
            syntax = Syntax(formatted_json, "json", theme="monokai", line_numbers=True)
            console.print(syntax)
        except (TypeError, OverflowError):
            # Final fallback for any non-serializable objects.
            console.print(Pretty(data_to_render))

    #     return processed_result
    def _apply_formatters(self, raw_result: Any, formatter_options: Dict) -> Any:
        processed_result = raw_result

        # The unwrapping logic is now handled by the ScriptEngine.
        # This formatter is now only responsible for explicit JMESPath queries.
        if "query" in formatter_options:
            processed_result = jmespath.search(
                formatter_options["query"], processed_result
            )

        return processed_result
