# /src/cx_shell/environments/nix_provider.py

import subprocess
import os
from typing import List, Dict, Optional
from pathlib import Path
import structlog

from .base import BaseEnvironment
from cx_core_schemas.project import (
    ProjectManifest,
)  # We'll need this to parse the manifest

logger = structlog.get_logger(__name__)


class NixEnvironment(BaseEnvironment):
    """
    An environment provider that uses Nix to create hermetic, reproducible
    shell environments.
    """

    def __init__(self, project_root: Path, manifest: ProjectManifest):
        super().__init__(project_root)
        self.manifest = manifest

    def _construct_nix_shell_command(self) -> List[str]:
        """Builds the `nix-shell` command arguments from the project manifest."""
        packages = (
            self.manifest.environment.packages if self.manifest.environment else []
        )
        if not packages:
            raise ValueError(
                "Nix environment selected, but no packages are defined in cx.project.yaml."
            )

        # The '-p' flag is equivalent to '--packages'
        command = ["nix-shell", "-p"] + packages
        return command

    def activate(self) -> None:
        """
        Activates a long-lived, interactive Nix shell.
        This method replaces the current process with the new Nix shell.
        """
        command = self._construct_nix_shell_command()

        logger.info("Activating Nix interactive shell...", command=" ".join(command))
        print(f"Entering hermetic environment for project: {self.project_root.name}...")

        # os.execvp replaces the current Python process with the nix-shell process.
        # When the user exits the nix-shell, they will exit the program.
        os.execvp(command[0], command)

    def execute(
        self,
        command: List[str],
        stdin_data: Optional[str] = None,
        env_vars: Optional[Dict[str, str]] = None,
    ) -> subprocess.CompletedProcess:
        """
        Executes a single, non-interactive command within a 'jailed' Nix environment.
        This is used by the cx-server for secure, multi-tenant execution.
        """
        nix_command = self._construct_nix_shell_command()
        # Add the flags for pure, sandboxed execution.
        nix_command.extend(["--pure", "--run"])

        # The command to run inside the shell must be a single string argument.
        full_command_str = " ".join(command)
        nix_command.append(full_command_str)

        # The rest of the logic is identical to our VenvEnvironment, but
        # it's now running inside a perfectly reproducible container.
        process_env = os.environ.copy()
        process_env["PYTHONPATH"] = str(self.project_root)
        if env_vars:
            process_env.update(env_vars)

        logger.info(
            "nix_provider.execute",
            command=" ".join(nix_command),
            project_root=str(self.project_root),
        )

        result = subprocess.run(
            nix_command,
            input=stdin_data,
            capture_output=True,
            text=True,
            check=True,
            timeout=600,
            env=process_env,
            cwd=self.project_root,
        )

        return result
