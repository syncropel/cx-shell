from pathlib import Path
from rich.console import Console
import typer

from ..engine.connector.service import ConnectorService
from ..engine.connector.config import BLUEPRINTS_BASE_PATH

console = Console()


class CompileManager:
    """Handles the logic for compiling API specifications into blueprints."""

    async def run_compile(
        self,
        spec_source: str,
        name: str,
        version: str,
        namespace: str = "user",
        output_dir: Path = BLUEPRINTS_BASE_PATH,
    ):
        """
        Executes the blueprint compilation workflow. This logic is moved
        from the old `cli.compile` function.
        """
        console.print(
            f"🚀 Starting compilation for service '[bold cyan]{name}[/bold cyan]'..."
        )

        # The version in the path should not have a 'v'
        version = version.lstrip("v")
        full_output_dir = output_dir / namespace / name / version
        full_output_dir.mkdir(parents=True, exist_ok=True)
        console.print(f"Target directory prepared: [dim]{full_output_dir}[/dim]")

        script_input = {
            "spec_source": spec_source,
            "output_dir": str(full_output_dir),
        }

        # This assumes assets are bundled relative to this file's location
        # A utility like get_asset_path might be better long-term.
        compile_script_path = (
            Path(__file__).parent.parent / "assets/system-tasks/compile.connector.yaml"
        )

        try:
            service = ConnectorService()
            result = await service.run_script(compile_script_path, script_input)

            for step_name, step_result in result.items():
                if isinstance(step_result, dict) and "error" in step_result:
                    console.print(
                        "\n--- [bold red]❌ Compilation Workflow Failed[/bold red] ---"
                    )
                    console.print(
                        f"Error in step '[bold yellow]{step_name}[/bold yellow]':"
                    )
                    console.print(f"[red]{step_result['error']}[/red]")
                    raise typer.Exit(code=1)

            console.print(
                "\n--- [bold green]✅ Compilation Successful[/bold green] ---"
            )
            console.print(
                f"Blueprint package for '[bold cyan]{name}@{version}[/bold cyan]' created at:"
            )
            console.print(f"[cyan]{full_output_dir}[/cyan]")

        except Exception as e:
            if not isinstance(e, typer.Exit):
                console.print(
                    "\n--- [bold red]❌ Compilation Command Failed[/bold red] ---"
                )
                console.print(f"[red]An unexpected error occurred:[/red] {e}")
            raise typer.Exit(code=1)
