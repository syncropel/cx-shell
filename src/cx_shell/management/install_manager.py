# ~/repositories/cx-shell/src/cx_shell/management/install_manager.py
# [REASON]: This new manager encapsulates all logic for creating project-specific virtual
# environments and installing dependencies via `uv`. It's a key part of making cx
# applications self-contained and reproducible.

import subprocess
import sys
from pathlib import Path

import yaml
from rich.console import Console

console = Console()


class InstallManager:
    """Handles logic for setting up project-scoped environments."""

    def install_project_dependencies(self, project_root: Path):
        """
        Creates a virtual environment and installs dependencies for a project
        based on its `cx.project.yaml` manifest.
        """
        manifest_path = project_root / "cx.project.yaml"
        if not manifest_path.exists():
            console.print(
                f"[bold red]Error:[/bold red] Manifest 'cx.project.yaml' not found in [cyan]{project_root}[/cyan]."
            )
            return

        with open(manifest_path, "r") as f:
            manifest = yaml.safe_load(f)

        project_name = manifest.get("project_name", project_root.name)
        env_config = manifest.get("environment", {})
        requirements_file = env_config.get("requirements")

        if not requirements_file:
            console.print(
                f"[yellow]No 'requirements' file specified in the manifest for project '{project_name}'. Nothing to install.[/yellow]"
            )
            return

        requirements_path = project_root / requirements_file
        if not requirements_path.exists():
            console.print(
                f"[bold red]Error:[/bold red] Requirements file '{requirements_path}' not found."
            )
            return

        venv_path = project_root / ".venv"

        with console.status(
            f"Setting up project environment for '[bold cyan]{project_name}[/bold cyan]'..."
        ) as status:
            try:
                # Step 1: Create virtual environment using the global 'uv' command.
                status.update("Creating virtual environment...")
                subprocess.run(
                    ["uv", "venv", str(venv_path), "--python", sys.executable],
                    check=True,
                    capture_output=True,
                    cwd=project_root,
                )
                console.print(
                    f"  [green]✓[/green] Virtual environment created at [dim]{venv_path}[/dim]"
                )

                # Step 2: Install dependencies into the new venv using the global 'uv'.
                status.update(f"Installing dependencies from {requirements_file}...")
                subprocess.run(
                    [
                        "uv",
                        "pip",
                        "install",
                        "-r",
                        str(requirements_path),
                    ],
                    check=True,
                    capture_output=True,
                    cwd=project_root,
                )
                console.print("  [green]✓[/green] Dependencies installed successfully.")

            except subprocess.CalledProcessError as e:
                console.print(
                    "\n[bold red]Error during project installation:[/bold red]"
                )
                console.print(f"Command: {' '.join(e.cmd)}")
                console.print(f"[dim]Stderr:\n{e.stderr.decode()}[/dim]")
                return
            except Exception as e:
                console.print(
                    f"\n[bold red]An unexpected error occurred:[/bold red] {e}"
                )
                return

        console.print(
            f"\n[bold green]✅ Project '{project_name}' is ready.[/bold green]"
        )
