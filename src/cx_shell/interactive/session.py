from typing import Any, Dict, Optional
from ..engine.connector.config import ConnectionResolver


class SessionState:
    """
    A simple class to hold the state of an interactive cx shell session.
    """

    def __init__(self, is_interactive: bool = True):
        """
        Initializes the session state.

        Args:
            is_interactive: If False, suppresses the welcome message for non-interactive runs.
        """
        self.connections: Dict[str, Any] = {}
        self.variables: Dict[str, Any] = {}
        self.is_running: bool = True
        self._resolver = (
            ConnectionResolver()
        )  # It's good practice to have this available

        if is_interactive:
            print("Welcome to the Contextual Shell (Interactive Mode)!")
            print("Type 'exit' or press Ctrl+D to quit.")

    def get_alias_for_source(self, connection_id: str) -> Optional[str]:
        """
        Performs a reverse lookup to find the active session alias for a given
        connection source ID (e.g., 'user:my-db').
        """
        for alias, source_id in self.connections.items():
            if source_id == connection_id:
                return alias
        return None

    def get_secrets_for_alias(self, alias: str) -> Dict[str, Any]:
        """
        Securely loads the secrets for a given active connection alias on-demand.

        This prevents secrets from being stored directly in the session state object,
        which might be pickled or logged.

        Args:
            alias: The active session alias (e.g., 'cx_openai').

        Returns:
            A dictionary of secrets for that connection.

        Raises:
            ValueError: If the alias is not active in the current session.
        """
        if alias not in self.connections:
            raise ValueError(
                f"Connection alias '{alias}' is not active in the current session."
            )

        source = self.connections[alias]

        # We don't need the full async resolve here, as the resolver has a synchronous
        # method to load secrets from files, which is what we need.
        # The resolve() method returns a tuple of (Connection, secrets).
        # We only need the secrets part.
        _conn, secrets = self._resolver._resolve_from_file(
            self._resolver.user_connections_dir / f"{source.split(':')[1]}.conn.yaml"
        )
        return secrets
