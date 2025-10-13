# /src/cx_shell/management/shell_manager.py

from pathlib import Path
import yaml
import structlog

from cx_core_schemas.project import ProjectManifest
from ..environments.nix_provider import NixEnvironment
# We won't need the VenvProvider for the interactive `cx shell` command.
# The standard REPL will run in the user's current activated venv by default.

logger = structlog.get_logger(__name__)


class ShellManager:
    """
    Manages the activation of project-specific, hermetic shell environments.
    """

    def activate_shell(self, project_root: Path):
        """
        Determines the required environment for a project and activates it.
        This is the main entry point for the `cx shell` command.
        """
        manifest_path = project_root / "cx.project.yaml"
        manifest = ProjectManifest()  # Default to an empty manifest

        if manifest_path.exists():
            try:
                manifest_data = yaml.safe_load(manifest_path.read_text())
                if manifest_data:  # Ensure file is not empty
                    manifest = ProjectManifest.model_validate(manifest_data)
            except Exception as e:
                logger.warn(
                    "shell_manager.manifest_load_failed",
                    path=str(manifest_path),
                    error=str(e),
                )
                # Proceed with a default environment if manifest is invalid

        # --- The Definitive Environment Selection Logic ---
        if manifest.environment and manifest.environment.packages:
            # If the project explicitly defines system packages, we MUST use Nix
            # to provide a hermetic environment.
            log = logger.bind(project=project_root.name, provider="nix")
            log.info("Project requires a hermetic environment. Activating Nix shell.")
            try:
                provider = NixEnvironment(project_root, manifest)
                # The activate() method takes over the current process and does not return.
                provider.activate()
            except Exception as e:
                log.error("nix_shell.activation.failed", error=str(e))
                print(
                    "\n[bold red]Error:[/bold red] Failed to activate the Nix environment for this project."
                )
                print(f"[dim]Details: {e}[/dim]")
                # Exit with an error code to prevent falling back to a non-hermetic shell
                exit(1)
        else:
            # If no system environment is defined, we don't need Nix.
            # We will simply launch the standard interactive REPL.
            # The REPL will operate within the user's current activated environment.
            logger.info("No hermetic environment required. Launching standard REPL.")
            # We import here to avoid circular dependencies
            from ..interactive.main import start_repl

            start_repl()
