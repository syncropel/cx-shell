from abc import ABC
from pathlib import Path
from typing import Any, List, Dict, Optional, TYPE_CHECKING

# Rich imports are only for type hinting, not for direct use.
from rich.status import Status

# Use TYPE_CHECKING to prevent circular imports at runtime,
# as the executor will import this file.
if TYPE_CHECKING:
    from ..engine.context import RunContext


import structlog
import yaml

# Local application imports
from .session import SessionState
from ..engine.connector.service import ConnectorService
from ..engine.transformer.service import TransformerService
from ..utils import CX_HOME

logger = structlog.get_logger(__name__)


from cx_core_schemas.connector_script import (
    ConnectorScript,
    ConnectorStep,
    RunDeclarativeAction,
    RunSqlQueryAction,
    BrowsePathAction,
    ReadContentAction,
)


SESSION_DIR = CX_HOME / "sessions"


def create_script_for_step(step: ConnectorStep) -> ConnectorScript:
    """Helper function to wrap a single step in a script object."""
    return ConnectorScript(name="Interactive Script", steps=[step])


class Command(ABC):
    """Abstract base class for all executable REPL commands."""

    async def execute(
        self,
        state: SessionState,
        service: ConnectorService,
        status: Status,
        piped_input: Any = None,
    ) -> Any:
        """Executes the command and returns a result."""
        # Piped input is added to the signature for commands that can receive it.
        # Most commands will ignore it.
        raise NotImplementedError


# --- REPLACE YOUR EXISTING DotNotationCommand WITH THIS ---
class DotNotationCommand(Command):
    """Represents a blueprint-driven command with named arguments, e.g., `gh.getUser(username="torvalds")`."""

    def __init__(self, alias: str, action_name: str, kwargs: Dict[str, Any]):
        self.alias = alias
        self.action_name = action_name
        self.kwargs = kwargs

    def to_step(self, state: SessionState) -> ConnectorStep:
        """Converts the command into a declarative ConnectorStep model for the engine."""
        if self.alias not in state.connections:
            raise ValueError(f"Unknown connection alias '{self.alias}'.")

        connection_source = state.connections[self.alias]

        return ConnectorStep(
            id=f"interactive_{self.action_name}",
            name=f"Interactive {self.action_name}",
            connection_source=connection_source,
            run=RunDeclarativeAction(
                action="run_declarative_action",
                template_key=self.action_name,
                context=self.kwargs,
            ),
        )

    async def execute(self, run_context: "RunContext", status: Status) -> Any:
        """
        Executes the command by creating a temporary, single-step script, running
        it via the ScriptEngine, and returning the result.
        """
        status.update(
            f"Executing [cyan]{self.alias}[/cyan].[yellow]{self.action_name}[/yellow]([magenta]{self.kwargs or ''}[/magenta])..."
        )

        step = self.to_step(run_context.session)
        script = create_script_for_step(step)

        results = await run_context.services.script_engine.run_script_model(
            context=run_context, script_data=script.model_dump()
        )

        logger.debug(
            "command.dot_notation.result_from_engine",
            alias=self.alias,
            action=self.action_name,
            engine_result=results,
        )

        return results


# --- REPLACE YOUR EXISTING PositionalArgActionCommand WITH THIS ---
class PositionalArgActionCommand(Command):
    """Represents a command with a single positional argument, e.g., `db.query("SELECT *...")`."""

    def __init__(self, alias: str, action_name: str, arg: Any):
        self.alias = alias
        self.action_name = action_name
        self.arg = arg

    def to_step(self, state: SessionState) -> ConnectorStep:
        """Converts the command into a declarative ConnectorStep model for the engine."""
        if self.alias not in state.connections:
            raise ValueError(f"Unknown connection alias '{self.alias}'.")

        connection_source = state.connections[self.alias]
        run_action = None

        if self.action_name == "query":
            run_action = RunSqlQueryAction(
                action="run_sql_query", query=self.arg, parameters={}
            )
        elif self.action_name == "browse":
            run_action = BrowsePathAction(action="browse_path", path=self.arg)
        elif self.action_name == "read":
            run_action = ReadContentAction(action="read_content", path=self.arg)
        else:
            raise NotImplementedError(
                f"Positional argument action '{self.action_name}' is not supported."
            )

        return ConnectorStep(
            id=f"interactive_{self.action_name}",
            name=f"Interactive {self.action_name}",
            connection_source=connection_source,
            run=run_action,
        )

    async def execute(self, run_context: "RunContext", status: Status) -> Any:
        """
        Executes the command by creating a temporary script and running it.
        """
        status.update(
            f"Executing [cyan]{self.alias}[/cyan].[yellow]{self.action_name}[/yellow]([magenta]'{str(self.arg)[:50]}...'[/magenta])..."
        )

        step = self.to_step(run_context.session)
        script = create_script_for_step(step)

        results = await run_context.services.script_engine.run_script_model(
            context=run_context, script_data=script.model_dump()
        )

        logger.debug(
            "command.positional_arg.result_from_engine",
            alias=self.alias,
            action=self.action_name,
            engine_result=results,
        )

        return results


class BuiltinCommand(Command):
    """Represents a built-in command like `connect` or `help`."""

    def __init__(self, parts: List[str]):
        self.command = parts[0].lower() if parts else ""
        self.args = parts[1:] if len(parts) > 1 else []

    async def execute(
        self,
        state: SessionState,
        service: ConnectorService,
        status: Status,
        piped_input: Any = None,
    ) -> Any:
        raise NotImplementedError(
            "BuiltinCommand execution is handled by CommandExecutor."
        )


class AssignmentCommand(Command):
    """Represents a variable assignment."""

    def __init__(self, var_name: str, command_to_run: Command):
        self.var_name = var_name
        self.command_to_run = command_to_run

    async def execute(
        self,
        state: SessionState,
        service: ConnectorService,
        status: Status,
        piped_input: Any = None,
    ) -> Any:
        # The executor is responsible for calling the RHS command and assigning the variable.
        # This method is a placeholder to satisfy the ABC.
        raise NotImplementedError(
            "AssignmentCommand execution is handled by CommandExecutor."
        )


class InspectCommand(Command):
    """Represents a variable inspection, e.g., `my_var?`."""

    def __init__(self, var_name: str):
        self.var_name = var_name

    async def execute(
        self,
        state: SessionState,
        service: ConnectorService,
        status: Status,
        piped_input: Any = None,
    ) -> Any:
        if self.var_name not in state.variables:
            raise ValueError(
                f"Variable '{self.var_name}' not found in current session."
            )
        obj = state.variables[self.var_name]
        summary = {"var_name": self.var_name, "type": type(obj).__name__}
        if isinstance(obj, (list, tuple, set)):
            summary["length"] = len(obj)
            if obj:
                first_item = next(iter(obj))
                if isinstance(first_item, dict):
                    summary["item_zero_keys"] = list(first_item.keys())
                else:
                    summary["item_zero_preview"] = repr(first_item)
        elif isinstance(obj, dict):
            summary["length"] = len(obj)
            summary["keys"] = list(obj.keys())
        else:
            summary["value_preview"] = repr(obj)
        return summary


class PipelineCommand(Command):
    """Represents a series of commands chained by pipes. Acts as a data container."""

    def __init__(self, commands: List[Command]):
        self.commands = commands

    async def execute(
        self,
        state: SessionState,
        service: ConnectorService,
        status: Status,
        piped_input: Any = None,
    ) -> Any:
        raise NotImplementedError(
            "PipelineCommand execution is handled by CommandExecutor."
        )


class ScriptedCommand(Command):
    """Represents a command that runs a YAML script, like `transform run`."""

    def __init__(self, command_type: str, script_path: str):
        self.command_type = command_type
        self.script_path = script_path

    async def execute(
        self,
        state: SessionState,
        service: ConnectorService,
        status: Status,
        piped_input: Any = None,
    ) -> Any:
        expanded_path = Path(self.script_path).expanduser().resolve()
        if not expanded_path.exists():
            raise FileNotFoundError(f"Script not found at: {expanded_path}")
        with open(expanded_path, "r", encoding="utf-8") as f:
            script_data = yaml.safe_load(f)

        if self.command_type == "transform":
            transformer = TransformerService()
            run_context = {"initial_input": piped_input, **state.variables}
            return await transformer.run(script_data, run_context)

        raise NotImplementedError(
            f"Scripted command '{self.command_type}' not implemented."
        )


class SessionCommand(Command):
    """Represents a session management command, e.g., `session save my-session`."""

    def __init__(self, subcommand: str, arg: str | None = None):
        self.subcommand = subcommand
        self.arg = arg

    async def execute(
        self,
        state: SessionState,
        service: ConnectorService,
        status: Status,
        piped_input: Any = None,
    ) -> Any:
        # This command is special and will be handled directly by the executor,
        # which will instantiate and use the SessionManager.
        raise NotImplementedError(
            "SessionCommand execution is handled by CommandExecutor."
        )


class VariableCommand(Command):
    """Represents a variable management command, e.g., `var list` or `var rm my_var`."""

    def __init__(self, subcommand: str, arg: str | None = None):
        self.subcommand = subcommand
        self.arg = arg

    async def execute(
        self,
        state: SessionState,
        service: ConnectorService,
        status: Status,
        piped_input: Any = None,
    ) -> Any:
        # This is a synchronous, built-in style command.
        # The executor will handle it directly.
        raise NotImplementedError(
            "VariableCommand execution is handled by CommandExecutor."
        )


class FlowCommand(Command):
    """Represents a flow management command, e.g., `flow list` or `flow run`."""

    def __init__(self, subcommand: str, named_args: Dict[str, Any]):
        self.subcommand = subcommand
        self.named_args = named_args

    async def execute(
        self,
        state: SessionState,
        service: ConnectorService,
        status: Status,
        piped_input: Any = None,
    ) -> Any:
        raise NotImplementedError(
            "FlowCommand execution is handled by CommandExecutor."
        )


class QueryCommand(Command):
    """Represents a query management command, e.g., `query run --on db --name my-query`."""

    def __init__(self, subcommand: str, named_args: Dict[str, Any]):
        self.subcommand = subcommand
        self.named_args = named_args

    async def execute(
        self,
        state: SessionState,
        service: ConnectorService,
        status: Status,
        piped_input: Any = None,
    ) -> Any:
        raise NotImplementedError(
            "QueryCommand execution is handled by CommandExecutor."
        )


class ScriptCommand(Command):
    """Represents a script management command, e.g., `script run --name my-script`."""

    def __init__(self, subcommand: str, named_args: Dict[str, Any]):
        self.subcommand = subcommand
        self.named_args = named_args

    async def execute(
        self,
        state: SessionState,
        service: ConnectorService,
        status: Status,
        piped_input: Any = None,
    ) -> Any:
        raise NotImplementedError(
            "ScriptCommand execution is handled by CommandExecutor."
        )


class ConnectionCommand(Command):
    """Represents a connection management command, e.g., `connection list` or `connection create`."""

    def __init__(self, subcommand: str, named_args: Dict[str, Any] | None = None):
        self.subcommand = subcommand
        self.named_args = named_args or {}

    async def execute(
        self,
        state: SessionState,
        service: ConnectorService,
        status: Status,
        piped_input: Any = None,
    ) -> Any:
        """
        This method is a placeholder. The actual logic is handled synchronously
        by the CommandExecutor, which delegates to the ConnectionManager.
        """
        raise NotImplementedError(
            "ConnectionCommand execution is handled by the CommandExecutor's dispatch logic."
        )


class OpenCommand(Command):
    """Represents the `open` command for assets, now with support for named arguments."""

    def __init__(
        self, asset_type: str, asset_name: str | None, named_args: Dict[str, Any]
    ):
        self.asset_type = asset_type
        self.asset_name = asset_name
        self.named_args = named_args

    async def execute(
        self,
        state: SessionState,
        service: ConnectorService,
        status: Status,
        piped_input: Any = None,
    ) -> Any:
        """
        This method is a placeholder. The actual logic is handled by the
        CommandExecutor, which delegates to the OpenManager.
        """
        raise NotImplementedError(
            "OpenCommand execution is handled by the CommandExecutor's dispatch logic."
        )


class AppCommand(Command):
    """Represents an application management command."""

    def __init__(self, subcommand: str, args: Dict[str, Any]):
        self.subcommand = subcommand
        self.args = args

    async def execute(
        self,
        state: SessionState,
        service: ConnectorService,
        status: Status,
        piped_input: Any = None,
    ) -> Any:
        # This will be handled directly by the CommandExecutor's dispatch logic
        raise NotImplementedError


class AgentCommand(Command):
    """Represents an agent invocation command, e.g., `agent 'do something'`."""

    def __init__(self, goal: str):
        self.goal = goal

    async def execute(
        self,
        state: SessionState,
        service: ConnectorService,
        status: Status,
        piped_input: Any = None,
    ) -> Any:
        raise NotImplementedError


class ProcessCommand(Command):
    """Represents a background process management command, e.g., `process list`."""

    def __init__(self, subcommand: str, arg: str | None = None, follow: bool = False):
        self.subcommand = subcommand
        self.arg = arg
        self.follow = follow

    async def execute(
        self,
        state: SessionState,
        service: ConnectorService,
        status: Status,
        piped_input: Any = None,
    ) -> Any:
        raise NotImplementedError


class CompileCommand(Command):
    """Represents a `compile` command with its named arguments."""

    def __init__(self, named_args: Dict[str, Any]):
        self.named_args = named_args

    async def execute(
        self,
        state: SessionState,
        service: ConnectorService,
        status: Status,
        piped_input: Any = None,
    ) -> Any:
        # This will be handled by the executor's dispatch logic
        raise NotImplementedError


class WorkspaceCommand(Command):
    """Represents a workspace management command."""

    def __init__(self, subcommand: str, args: Dict[str, Any]):
        self.subcommand = subcommand
        self.args = args

    async def execute(
        self,
        state: SessionState,
        service: ConnectorService,
        status: Status,
        piped_input: Any = None,
    ) -> Any:
        raise NotImplementedError(
            "WorkspaceCommand execution is handled by CommandExecutor."
        )


class FindCommand(Command):
    """Represents a VFS find command."""

    def __init__(self, query: Optional[str], args: Dict[str, Any]):
        self.query = query
        self.args = args

    async def execute(
        self,
        state: SessionState,
        service: ConnectorService,
        status: Status,
        piped_input: Any = None,
    ) -> Any:
        raise NotImplementedError


# Add this class definition to the file, for example, near the end.
class InstallCommand(Command):
    """
    Represents the built-in 'install' command.
    This command is argument-less and operates on the current working directory,
    triggering the installation of project-scoped dependencies based on a
    'cx.project.yaml' manifest.
    """

    def __init__(self):
        """Initializes the InstallCommand."""
        pass

    async def execute(self, run_context: "RunContext", status: "Status") -> Any:
        """
        This method is a placeholder. The actual logic is handled synchronously by
        the CommandExecutor's dispatch logic, which delegates to the InstallManager.
        """
        raise NotImplementedError(
            "InstallCommand execution is handled directly by the CommandExecutor's dispatch logic."
        )


class SliceCommand(Command):
    """Represents a JMESPath query on a session variable, e.g., `my_var["key.sub_key"]`."""

    def __init__(self, var_name: str, query: str):
        self.var_name = var_name
        self.query = query

    async def execute(self, run_context: "RunContext", status: Status) -> Any:
        """Executes the query against the variable in the session state."""
        if self.var_name not in run_context.session.variables:
            raise ValueError(
                f"Variable '{self.var_name}' not found in current session."
            )

        source_data = run_context.session.variables[self.var_name]

        # We can now use the jmespath library directly
        import jmespath

        try:
            result = jmespath.search(self.query, source_data)
            return result
        except Exception as e:
            raise ValueError(
                f"JMESPath query failed for variable '{self.var_name}': {e}"
            )


class PublishCommand(Command):
    """Represents a `publish` command with its arguments."""

    def __init__(self, named_args: Dict[str, Any]):
        self.named_args = named_args

    async def execute(self, run_context: "RunContext", status: "Status") -> Any:
        raise NotImplementedError(
            "PublishCommand execution is handled by CommandExecutor."
        )
