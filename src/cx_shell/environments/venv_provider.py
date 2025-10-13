import subprocess
import sys
from typing import List, Dict, Optional
import os
import structlog

from .base import BaseEnvironment

logger = structlog.get_logger(__name__)


class VenvEnvironment(BaseEnvironment):
    """
    An environment provider that uses the parent cx-shell's virtual environment.
    This represents the legacy, non-hermetic mode of operation.
    """

    def activate(self) -> None:
        """Activating a shared venv is not supported. This is for hermetic envs."""
        raise NotImplementedError(
            "The legacy VenvEnvironment does not support interactive activation."
        )

    def execute(
        self,
        command: List[str],
        stdin_data: Optional[str] = None,
        env_vars: Optional[Dict[str, str]] = None,
    ) -> subprocess.CompletedProcess:
        """
        Executes a command using the current Python environment, but with the
        PYTHONPATH set to the project root.
        """
        # Always use the Python executable that is running the cx-shell itself.
        # This is the core of the "unified runtime environment" principle.
        executable = [sys.executable]
        full_command = executable + command

        # Set up the environment
        process_env = os.environ.copy()
        process_env["PYTHONPATH"] = str(self.project_root)
        if env_vars:
            process_env.update(env_vars)

        logger.info(
            "venv_provider.execute",
            command=" ".join(full_command),
            project_root=str(self.project_root),
        )

        # We are simply wrapping the standard subprocess.run call here.
        # The 'check=True' flag will cause it to raise CalledProcessError on failure.
        result = subprocess.run(
            full_command,
            input=stdin_data,
            capture_output=True,
            text=True,
            check=True,
            timeout=600,  # 10-minute timeout
            env=process_env,
            cwd=self.project_root,
        )

        return result
