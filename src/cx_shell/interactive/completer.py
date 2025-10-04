from typing import Dict, Iterable

from prompt_toolkit.completion import Completer, Completion
from prompt_toolkit.document import Document

from .session import SessionState
from ..engine.connector.config import ConnectionResolver
from cx_core_schemas.api_catalog import ApiCatalog


class CxCompleter(Completer):
    """
    A dynamic, context-aware completer for the Flow Syncropel Shell.

    This completer provides intelligent suggestions for:
    1. Built-in shell commands (e.g., 'connect', 'help').
    2. Active connection aliases (e.g., 'api', 'db').
    3. Blueprint-driven actions available on an alias (e.g., 'api.getPetById').
    """

    def __init__(self, state: SessionState):
        """
        Initializes the completer with the current session state.

        Args:
            state: The SessionState object holding active connections.
        """
        self.state = state
        self.builtin_commands = ["connect", "connections", "help", "exit", "quit"]

        # The resolver is used to load blueprint files from disk.
        self.resolver = ConnectionResolver()
        # A simple in-memory cache to store loaded blueprints for the duration of the session.
        # This prevents repeated file I/O on every keystroke, making completion fast.
        self.blueprint_cache: Dict[str, ApiCatalog] = {}

    def _get_blueprint_for_alias(self, alias: str) -> ApiCatalog | None:
        """
        Loads and caches the blueprint for a given connection alias.

        If the blueprint for an alias is already in the cache, it's returned
        immediately. Otherwise, it uses the ConnectionResolver to load the
        connection file and its associated blueprint, then caches the result.

        Args:
            alias: The connection alias (e.g., 'api').

        Returns:
            An ApiCatalog object if a blueprint is successfully loaded, otherwise None.
        """
        if alias in self.blueprint_cache:
            return self.blueprint_cache[alias]

        if alias in self.state.connections:
            source = self.state.connections[alias]
            try:
                # The connection source is in the format 'user:petstore'. We need the name part.
                connection_name = source.split(":")[1]
                conn_file_path = (
                    self.resolver.user_connections_dir / f"{connection_name}.conn.yaml"
                )

                # Use the resolver to perform a full load, which includes merging the blueprint.
                connection, _ = self.resolver._resolve_from_file(conn_file_path)

                if connection and connection.catalog:
                    self.blueprint_cache[alias] = connection.catalog
                    return connection.catalog
            except Exception:
                # If anything goes wrong (file not found, parse error), we fail silently.
                # This ensures that a broken blueprint doesn't crash the entire shell.
                # The user will simply not get completions for that alias.
                return None
        return None

    def get_completions(
        self, document: Document, complete_event
    ) -> Iterable[Completion]:
        """
        The main generator function called by prompt_toolkit to get suggestions.
        """
        text_before_cursor = document.text_before_cursor
        word_before_cursor = document.get_word_before_cursor()

        # --- CONTEXT 1: Dot-Notation Action Completion ---
        # Active when the user has typed an alias and a dot (e.g., `api.get`).
        if "." in text_before_cursor and " " not in text_before_cursor:
            try:
                alias, action_prefix = text_before_cursor.split(".", 1)

                # Attempt to load the blueprint for the given alias.
                blueprint = self._get_blueprint_for_alias(alias)
                if (
                    blueprint
                    and blueprint.browse_config
                    and "action_templates" in blueprint.browse_config
                ):
                    actions = blueprint.browse_config["action_templates"].keys()
                    for action in actions:
                        if action.startswith(action_prefix):
                            yield Completion(
                                text=action,
                                start_position=-len(action_prefix),
                                display_meta="blueprint action",
                            )
                    return
            except ValueError:
                # Ignore invalid formats like `alias.action.something`.
                pass

        # --- CONTEXT 2: First-Word Command/Alias Completion ---
        # Active when the user is typing the first word on the line.
        elif " " not in text_before_cursor:
            # Suggest built-in shell commands
            for command in self.builtin_commands:
                if command.startswith(word_before_cursor):
                    yield Completion(
                        text=command, start_position=-len(word_before_cursor)
                    )

            # Suggest active connection aliases
            for alias in self.state.connections:
                if alias.startswith(word_before_cursor):
                    yield Completion(
                        text=alias,
                        start_position=-len(word_before_cursor),
                        display_meta="connection alias",
                    )
