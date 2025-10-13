# /src/cx_shell/management/compile_manager.py

import json
import subprocess
from pathlib import Path
from typing import TYPE_CHECKING

import yaml
from rich.console import Console
from rich.panel import Panel
import structlog

# Use the environment provider abstraction
from ..environments.venv_provider import VenvEnvironment

# Use the robust asset path resolver
from ..utils import get_assets_root as get_asset_path

if TYPE_CHECKING:
    from ..engine.context import RunContext

# Use a shared console and logger for consistent output
console = Console()
logger = structlog.get_logger(__name__)


class CompileManager:
    """
    Handles the logic for compiling API specifications into Syncropel Blueprints
    by orchestrating an external Python compiler script via an Environment Provider.
    """

    async def run_compile(self, run_context: "RunContext", **named_args):
        """
        Executes the full blueprint compilation workflow.
        """
        spec_source = named_args.get("spec-source")
        name = named_args.get("name")
        version = named_args.get("version")
        namespace = named_args.get("namespace", "user")

        if not all([spec_source, name, version]):
            raise ValueError(
                "`compile` requires --spec-source, --name, and --version arguments."
            )

        log = logger.bind(service_name=name, version=version)
        console.print(
            f"🚀 Starting compilation for service '[bold cyan]{name}@{version}[/bold cyan]'..."
        )

        try:
            # 1. Resolve the path to our universal compiler script. This is a bundled asset.
            compiler_script_path = get_asset_path(
                "system-lib/compilers/spec_compiler.py"
            )
            if not compiler_script_path.exists():
                raise FileNotFoundError(
                    "Fatal: Universal spec compiler script not found in application assets."
                )

            # 2. Fetch the specification content using the Smart Fetcher.
            # This is a powerful pattern that leverages our existing engine to handle
            # both local file paths and remote URLs seamlessly.
            console.print(f"Fetching specification from: [dim]{spec_source}[/dim]")

            # We use a helper method on the connector service to run a single, simple action.
            spec_content_result = (
                await run_context.services.connector_service.run_action_by_name(
                    run_context=run_context,
                    connection_source="user:system_smart_fetcher",
                    action_name="read_content",
                    action_params={"path": spec_source},
                )
            )
            spec_content_str = spec_content_result

            # 3. Determine the execution environment.
            # For a core system task like this, we'll use the current working directory
            # as the project root to ensure a consistent execution context.
            project_root = Path.cwd()
            env_provider = VenvEnvironment(project_root)

            # 4. Delegate the execution of the compiler script to the Environment Provider.
            console.print("Compiling specification...")
            command_to_run = [str(compiler_script_path)]

            # The 'check=True' in the provider will raise CalledProcessError on failure.
            process = env_provider.execute(
                command=command_to_run, stdin_data=spec_content_str
            )

            # 5. Process the structured JSON output from the compiler script.
            compiler_output = json.loads(process.stdout)
            blueprint_yaml = compiler_output["blueprint_yaml"]
            schemas_py = compiler_output["schemas_py"]

            # 6. Write the final artifacts to the correct location in the workspace.
            version_clean = version.lstrip("v")
            # We get the base path from the resolver service in the RunContext.
            output_dir = (
                run_context.services.resolver.BLUEPRINTS_BASE_PATH
                / namespace
                / name
                / version_clean
            )
            output_dir.mkdir(parents=True, exist_ok=True)

            blueprint_path = output_dir / "blueprint.cx.yaml"
            schemas_path = output_dir / "schemas.py"
            source_spec_path = output_dir / "source_spec.json"

            blueprint_path.write_text(blueprint_yaml, encoding="utf-8")
            schemas_path.write_text(schemas_py, encoding="utf-8")

            # Store a consistently formatted version of the original spec for auditability.
            source_spec_path.write_text(
                json.dumps(yaml.safe_load(spec_content_str), indent=2), encoding="utf-8"
            )

            log.info("compile.success", output_path=str(output_dir))
            console.print(
                "\n--- [bold green]✅ Compilation Successful[/bold green] ---"
            )
            console.print(
                f"Blueprint package for '[bold cyan]{name}@{version}[/bold cyan]' created at:"
            )
            console.print(f"[cyan]{output_dir}[/cyan]")

        except subprocess.CalledProcessError as e:
            log.error("compile.failed.script_error", stderr=e.stderr)
            console.print("\n--- [bold red]❌ Compilation Failed[/bold red] ---")
            console.print(
                "The compiler script encountered a fatal error. See details below:"
            )
            # The compiler script is designed to print detailed logs to stderr.
            console.print(Panel(e.stderr, border_style="dim"))
            # Re-raise to ensure the shell knows the command failed.
            raise
        except Exception as e:
            log.error("compile.failed.unexpected_error", error=str(e), exc_info=True)
            console.print("\n--- [bold red]❌ Compilation Failed[/bold red] ---")
            console.print(f"[red]An unexpected error occurred:[/red] {e}")
            raise
