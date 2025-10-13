from abc import ABC, abstractmethod
from typing import List, Dict, Optional
from pathlib import Path
import subprocess


class BaseEnvironment(ABC):
    """
    The abstract contract for all environment providers.

    An environment provider is responsible for activating an environment
    and executing commands within it.
    """

    def __init__(self, project_root: Path):
        """
        Initializes the provider with the root of the project it will manage.
        """
        self.project_root = project_root

    @abstractmethod
    def activate(self) -> None:
        """
        Activates a long-lived, interactive shell environment.
        This method will typically take over the current process.
        """
        raise NotImplementedError

    @abstractmethod
    def execute(
        self,
        command: List[str],
        stdin_data: Optional[str] = None,
        env_vars: Optional[Dict[str, str]] = None,
    ) -> subprocess.CompletedProcess:
        """
        Executes a single, non-interactive command within the environment.

        Args:
            command: The command and its arguments as a list of strings.
            stdin_data: Optional string data to be passed to the command's stdin.
            env_vars: Optional dictionary of environment variables to set.

        Returns:
            A CompletedProcess object containing the result of the execution.
        """
        raise NotImplementedError
