import asyncio
import inspect
import json
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from ast import literal_eval
import jmespath
import structlog

from lark import Lark, Transformer, v_args
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich import box

from ..engine.context import RunContext, ServiceRegistry
from ..engine.connector.service import ConnectorService
from ..engine.connector.config import ConnectionResolver
from ..engine.connector.engine import ScriptEngine
from ..management.session_manager import SessionManager
from ..management.variable_manager import VariableManager
from ..management.flow_manager import FlowManager
from ..management.query_manager import QueryManager
from ..management.script_manager import ScriptManager
from ..management.connection_manager import ConnectionManager
from ..management.open_manager import OpenManager
from ..management.app_manager import AppManager
from ..management.process_manager import ProcessManager
from .agent_orchestrator import AgentOrchestrator
from ..management.compile_manager import CompileManager
from ..management.workspace_manager import WorkspaceManager
from ..management.index_manager import IndexManager
from ..management.find_manager import FindManager
from ..management.install_manager import InstallManager
from ..engine.transformer.service import TransformerService
from ..management.publisher import Publisher
from ..history_logger import HistoryLogger


from .commands import (
    Command,
    DotNotationCommand,
    BuiltinCommand,
    PositionalArgActionCommand,
    AssignmentCommand,
    InspectCommand,
    PipelineCommand,
    SessionCommand,
    VariableCommand,
    FlowCommand,
    QueryCommand,
    ScriptCommand,
    ConnectionCommand,
    OpenCommand,
    AppCommand,
    AgentCommand,
    ProcessCommand,
    CompileCommand,
    WorkspaceCommand,
    FindCommand,
    InstallCommand,
    SliceCommand,
    PublishCommand,
)
from .commands import create_script_for_step
from .session import SessionState
from ..data.agent_schemas import DryRunResult
from ..utils import get_pkg_root
from .output_handler import IOutputHandler

from cx_core_schemas.connector_script import (
    ConnectorStep,
    ReadContentAction,
)

console = Console()
logger = structlog.get_logger(__name__)


@dataclass
class VariableLookup:
    var_name: str


@v_args(inline=True)
class CommandTransformer(Transformer):
    """Transforms the Lark parse tree into our executable Command objects."""

    def expression(self, pipeline):
        return pipeline

    def pipeline(self, *items):
        clean_commands = [item for item in items if isinstance(item, tuple)]
        return PipelineCommand(clean_commands)

    def command_unit(self, executable, formatter=None):
        return (executable, dict(formatter or []))

    def executable(self, exec_obj):
        return exec_obj

    def single_executable(self, exec_obj):
        return exec_obj

    def assignment(self, var_name, expression):
        return AssignmentCommand(var_name.value, expression)

    def single_command(self, command):
        return command

    def builtin_command(self, cmd):
        return cmd

    def variable_lookup(self, var_name):
        return VariableLookup(var_name.value)

    def formatter(self, *options):
        return options

    def formatter_option(self, option):
        return option

    def output_option(self, mode):
        return ("output_mode", mode.value)

    def columns_option(self, columns):
        return ("columns", columns)

    def query_option(self, query_str):
        return ("query", literal_eval(query_str.value))

    def column_list(self, *cols):
        return [c.value for c in cols]

    def dot_notation_command(self, alias, action_name, arg_block=None):
        if isinstance(arg_block, dict) or arg_block is None:
            return DotNotationCommand(alias.value, action_name.value, arg_block or {})
        else:
            return PositionalArgActionCommand(alias.value, action_name.value, arg_block)

    def connect_command(self, source, alias):
        return BuiltinCommand(["connect", source.value, "--as", alias.value])

    def connections_command(self):
        return BuiltinCommand(["connections"])

    def help_command(self):
        return BuiltinCommand(["help"])

    def install(self, _):
        """
        Transforms the 'install' token from the parse tree into an InstallCommand object.
        The command itself is argument-less.
        """
        return InstallCommand()

    def inspect_command(self, var_name):
        return InspectCommand(var_name.value)

    def agent_command(self, goal):
        return AgentCommand(literal_eval(goal.value))

    def session_command(self, cmd_obj):
        return cmd_obj

    def session_subcommand(self, cmd_obj):
        return cmd_obj

    def variable_command(self, cmd_obj):
        return cmd_obj

    def variable_subcommand(self, cmd_obj):
        return cmd_obj

    def flow_command(self, cmd_obj):
        return cmd_obj

    def query_command(self, cmd_obj):
        return cmd_obj

    def script_command(self, cmd_obj):
        return cmd_obj

    def connection_command(self, cmd_obj):
        return cmd_obj

    def open_command(self, cmd_obj):
        return cmd_obj

    def app_command(self, cmd_obj):
        return cmd_obj

    def app_subcommand(self, cmd_obj):
        return cmd_obj

    def compile_command(self, cmd_obj):
        return cmd_obj

    def process_command(self, cmd_obj):
        return cmd_obj

    def process_subcommand(self, cmd_obj):
        return cmd_obj

    def open_args(self, *args):
        return list(args)

    def open_command_handler(self, open_args=None):
        args = open_args or []
        positional_args = [
            arg.value
            for arg in args
            if hasattr(arg, "type") and arg.type in ("ARG", "JINJA_BLOCK")
        ]
        named_args_list = [arg for arg in args if isinstance(arg, tuple)]
        asset_type = positional_args[0] if positional_args else None
        asset_name = positional_args[1] if len(positional_args) > 1 else None
        args_dict = {key.lstrip("-"): value for key, value in named_args_list}
        return OpenCommand(asset_type, asset_name, args_dict)

    def connection_create(self, *named_args):
        return ConnectionCommand("create", named_args=dict(named_args))

    def compile_command_with_args(self, *named_args):
        return CompileCommand(named_args=dict(named_args))

    def app_install(self, *named_args):
        return AppCommand("install", args=dict(named_args))

    def session_list(self):
        return SessionCommand("list")

    def session_save(self, name):
        return SessionCommand("save", name.value)

    def session_load(self, name):
        return SessionCommand("load", name.value)

    def session_rm(self, name):
        return SessionCommand("rm", name.value)

    def session_status(self):
        return SessionCommand("status")

    def variable_list(self):
        return VariableCommand("list")

    def variable_rm(self, var_name):
        return VariableCommand("rm", var_name.value)

    def flow_list(self):
        return FlowCommand("list", named_args={})

    def query_list(self):
        return QueryCommand("list", named_args={})

    def script_list(self):
        return ScriptCommand("list", named_args={})

    def connection_list(self):
        return ConnectionCommand("list")

    def app_list(self):
        return AppCommand("list", args={})

    def app_uninstall(self, arg):
        return AppCommand("uninstall", args={"id": arg.value})

    def app_sync(self):
        return AppCommand("sync", args={})

    def app_package(self, arg):
        return AppCommand("package", args={"path": arg.value})

    def app_search(self, query=None):
        return AppCommand("search", args={"query": query.value if query else None})

    def process_list(self):
        return ProcessCommand("list")

    def process_logs(self, arg, follow=None):
        return ProcessCommand("logs", arg.value, follow is not None)

    def process_stop(self, arg):
        return ProcessCommand("stop", arg.value)

    def workspace_command(self, cmd_obj):
        return cmd_obj

    def workspace_subcommand(self, cmd_obj):
        return cmd_obj

    def workspace_list(self):
        return WorkspaceCommand("list", args={})  # <-- Pass an empty dict

    def workspace_add(self, path):
        return WorkspaceCommand("add", args={"path": path.value})  # <-- Create a dict

    def workspace_remove(self, path):
        return WorkspaceCommand("remove", args={"path": path.value})

    def workspace_index(self, *named_args):
        return WorkspaceCommand("index", args={k.lstrip("-"): v for k, v in named_args})

    def find_command(self, *args):
        # The *args from Lark will contain all matched items (Tokens and Tuples).
        logger.debug("transformer.find_command.received_args", args=args)

        items = list(args)

        query = next(
            (
                item.value
                for item in items
                if hasattr(item, "type") and item.type == "STRING"
            ),
            None,
        )
        if query:
            query = literal_eval(query)

        # This comprehension correctly handles named arguments (which are tuples)
        named_args = {
            item[0].lstrip("-"): (
                item[1].value if hasattr(item[1], "value") else item[1]
            )
            for item in items
            if isinstance(item, tuple)
        }

        return FindCommand(query=query, args=named_args)

    def params_block(self, *params):
        # *params is a list of the child results, which are (key, value) tuples.
        # dict() correctly converts a list of tuples into a dictionary.
        # The issue was in how this was being called by the run methods.
        return dict(params)

    def flow_run(self, name, params=None):
        # --- START OF DEFINITIVE FIX ---
        params_dict = {}
        if params:
            # 'params' will be a list of (key, value) tuples from the 'params_block' rule
            params_dict = dict(params)

        # Create a single, flat dictionary.
        # The 'name' is a required identifier.
        # All other key=value pairs are spread into this dictionary.
        named_args = {"name": name, **params_dict}
        logger.debug("transformer.flow_run.constructed_args", final_args=named_args)
        return FlowCommand("run", named_args=named_args)

    def query_run(self, on_alias, name, params=None):
        # --- START OF DEFINITIVE FIX ---
        params_dict = {}
        if params:
            params_dict = dict(params)

        # Create a single, flat dictionary.
        named_args = {"on": on_alias, "name": name, **params_dict}
        logger.debug("transformer.query_run.constructed_args", final_args=named_args)
        return QueryCommand("run", named_args=named_args)
        # --- END OF DEFINITIVE FIX ---

    def script_run(self, name, params=None):
        # --- START OF DEFINITIVE FIX ---
        params_dict = {}
        if params:
            params_dict = dict(params)

        # Create a single, flat dictionary.
        named_args = {"name": name, **params_dict}
        logger.debug("transformer.script_run.constructed_args", final_args=named_args)
        return ScriptCommand("run", named_args=named_args)
        # --- END OF DEFINITIVE FIX ---

    def arguments(self, *args):
        return dict(args)

    def kv_pair(self, key, value):
        return key.value, value

    def kw_argument(self, key, value):
        return key.value, value

    def named_argument(self, flag, value=None):
        """
        Processes a flag and its optional value.
        Critically, it evaluates STRING tokens to strip quotes.
        """
        final_value = value
        if value is not None:
            # Check if the value is a Lark Token and if its type is STRING
            if hasattr(value, "type") and value.type == "STRING":
                final_value = literal_eval(value.value)
            # Handle other token types that have a .value attribute
            elif hasattr(value, "value"):
                final_value = value.value
        else:
            final_value = True  # Handle boolean flags like --rebuild

        return (flag.value, final_value)

    def slice_operation(self, var_name, query_string):
        # The query_string is a STRING token, so we need to evaluate it to get the raw string content
        from ast import literal_eval

        query = literal_eval(query_string.value)
        return SliceCommand(var_name.value, query)

    def data_access(self, item):
        # Because of @v_args(inline=True), 'item' is the actual child object
        # (either a VariableLookup or a SliceCommand), not a list.
        # We simply return it directly.
        return item

    def read_command(self, path=None):
        # This method handles the `read_command` rule from the grammar.
        # It creates a standard BuiltinCommand. If a path argument was provided,
        # it will be in the `args` list. If not, the list will be empty.
        args = [path] if path is not None else []
        return BuiltinCommand(["read", *args])

    def publish_command_handler(self, *args):
        # The grammar now provides a simple, flat list of all arguments (flags and k=v pairs).
        # First, convert this list of tuples into a clean dictionary.
        args_dict = {}
        for item in args:
            if isinstance(item, tuple) and len(item) == 2:
                key, value = item
                # Strip the leading '--' from flags to normalize the keys
                clean_key = key.lstrip("-")
                args_dict[clean_key] = value

        # --- VALIDATION LOGIC MOVED TO PYTHON ---
        # Now, we validate the required arguments here in the code.
        required_flags = ["name", "to"]
        missing_flags = [flag for flag in required_flags if flag not in args_dict]
        if missing_flags:
            raise ValueError(
                f"Missing required arguments for 'publish' command: --{', --'.join(missing_flags)}"
            )

        return PublishCommand(named_args=args_dict)

    def value(self, v):
        if hasattr(v, "type"):
            if v.type == "JINJA_BLOCK":
                return v.value
            if v.type in ("STRING", "NUMBER"):
                return literal_eval(v.value)
            if v.type == "ARG":
                return v.value
            if v.type == "CNAME":
                return v.value
        return v

    def true(self, _):
        return True

    def false(self, _):
        return False

    def null(self, _):
        return None


class CommandExecutor:
    def __init__(
        self,
        state: SessionState,
        output_handler: IOutputHandler,
        cx_home_path: Optional[Path] = None,
    ):
        self.state = state
        self.output_handler = output_handler

        # --- Definitive Fix: Explicit Dependency Injection for Testability ---
        # All services that depend on the CX_HOME path are now explicitly
        # initialized with the correct path. In production, `cx_home_path` is None,
        # and they fall back to the default from `utils.py`. In tests, the
        # temporary path is injected, guaranteeing isolation.

        # 1. Instantiate the WorkspaceManager with the correct path.
        self.workspace_manager = WorkspaceManager(cx_home_path=cx_home_path)

        # 2. Instantiate all other managers, passing the cx_home_path or the
        #    correctly configured WorkspaceManager where needed.
        self.resolver = ConnectionResolver(cx_home_path=cx_home_path)
        self.connector_service = ConnectorService(executor=self)
        self.script_engine = ScriptEngine(self.resolver, self.connector_service)
        self.session_manager = SessionManager(cx_home_path=cx_home_path)
        self.variable_manager = VariableManager()
        self.flow_manager = FlowManager(self.workspace_manager)
        self.query_manager = QueryManager(self.workspace_manager)
        self.script_manager = ScriptManager(self.workspace_manager)
        self.connection_manager = ConnectionManager(cx_home_path=cx_home_path)
        self.open_manager = OpenManager()
        self.app_manager = AppManager(executor=self, cx_home_path=cx_home_path)
        self.process_manager = ProcessManager(cx_home_path=cx_home_path)
        self.compile_manager = CompileManager()
        self.index_manager = IndexManager(cx_home_path=cx_home_path)
        self.find_manager = FindManager(cx_home_path=cx_home_path)
        self.transformer_service = TransformerService()
        self.install_manager = InstallManager()
        self.publisher = Publisher()
        self.history_logger = HistoryLogger(cx_home_path=cx_home_path)

        # Create the central service registry with the correctly initialized services.
        self.registry = ServiceRegistry(
            workspace_manager=self.workspace_manager,
            resolver=self.resolver,
            connector_service=self.connector_service,
            script_engine=self.script_engine,
            flow_manager=self.flow_manager,
            query_manager=self.query_manager,
            script_manager=self.script_manager,
            connection_manager=self.connection_manager,
            open_manager=self.open_manager,
            app_manager=self.app_manager,
            process_manager=self.process_manager,
            compile_manager=self.compile_manager,
            index_manager=self.index_manager,
            find_manager=self.find_manager,
            install_manager=self.install_manager,
            transformer_service=self.transformer_service,
            publisher=self.publisher,
        )

        self.builtin_commands = {
            "connect": self.execute_connect,
            "connections": self.execute_list_connections,
            "help": self.execute_help,
            "read": self.execute_read,
        }
        self._orchestrator: Optional[AgentOrchestrator] = None
        pkg_root = get_pkg_root()
        grammar_path = pkg_root / "interactive" / "grammar" / "cx.lark"
        self.parser = Lark(
            grammar_path.read_text(encoding="utf-8"), start="start", parser="lalr"
        )
        self.transformer = CommandTransformer()

    @property
    def orchestrator(self) -> AgentOrchestrator:
        if self._orchestrator is None:
            logger.debug("executor.lazy_load", component="AgentOrchestrator")
            self._orchestrator = AgentOrchestrator(self.state, self)
        return self._orchestrator

    async def execute(
        self, command_text: str, piped_input: Any = None
    ) -> Optional[SessionState]:
        """
        Top-level entry point. Parses, executes, and handles output for a command string.
        """
        if not command_text.strip():
            return None

        run_context = RunContext(
            services=self.registry,
            session=self.state,
            piped_input=piped_input,
        )

        try:
            final_result, last_executable, last_options = await self._execute_pipeline(
                run_context, command_text
            )

            if isinstance(final_result, SessionState):
                return final_result

            if self.output_handler:
                await self.output_handler.handle_result(
                    final_result, last_executable, last_options, run_context
                )

        except Exception as e:
            original_exc = getattr(e, "orig_exc", e)
            logger.error(
                "executor.execute.failed", error=str(original_exc), exc_info=True
            )
            error_result = {"error": f"{type(original_exc).__name__}: {original_exc}"}
            if self.output_handler:
                await self.output_handler.handle_result(
                    error_result, None, None, run_context
                )

        return None

    async def _execute_pipeline(
        self, run_context: RunContext, command_text: str
    ) -> Tuple[Any, Optional[Command], Dict]:
        """
        Parses and executes a command string pipeline, returning the final RAW result
        and the formatter options from the last command.
        """
        tree = self.parser.parse(command_text)
        pipeline_command = self.transformer.transform(tree)

        first_executable, _ = pipeline_command.commands[0]
        is_assignment = isinstance(first_executable, AssignmentCommand)

        final_raw_result: Any = None
        last_executable: Optional[Command] = None
        last_options: Dict = {}

        if is_assignment:
            command_to_run, formatter_options = (
                first_executable.command_to_run.commands[0]
            )

            # --- START OF DEFINITIVE FIX for Assignment ---
            raw_result = await self._execute_executable(run_context, command_to_run)
            # In a test environment, a mocked async method might return a coroutine
            # that we need to explicitly await. This check handles that.
            if inspect.isawaitable(raw_result):
                raw_result = await raw_result
            # --- END OF DEFINITIVE FIX ---

            processed_for_assignment = self.output_handler._apply_formatters(
                raw_result, formatter_options
            )

            if not (
                isinstance(processed_for_assignment, dict)
                and "error" in processed_for_assignment
            ):
                run_context.session.variables[first_executable.var_name] = (
                    processed_for_assignment
                )

            final_raw_result = f"✓ Variable '{first_executable.var_name}' set."
            last_executable = first_executable
            last_options = {}
        else:
            current_data = run_context.piped_input
            for i, (command_to_run, formatter_options) in enumerate(
                pipeline_command.commands
            ):
                run_context.piped_input = current_data

                # --- This block was already correct, but is included for completeness ---
                current_data = await self._execute_executable(
                    run_context, command_to_run
                )
                if inspect.isawaitable(current_data):
                    current_data = await current_data
                # --- End of block ---

                logger.debug(
                    "executor.pipeline.step_raw_result",
                    step_index=i,
                    raw_result_from_engine=current_data,
                )

                if isinstance(current_data, dict) and "error" in current_data:
                    break

            final_raw_result = current_data
            last_executable, last_options = pipeline_command.commands[-1]

        return final_raw_result, last_executable, last_options

    def _apply_formatters(self, raw_result: Any, formatter_options: Dict) -> Any:
        # If the result is from a notebook (which is a dict of block results),
        # do not apply any default unwrapping. Let the handler deal with it.
        if isinstance(raw_result, dict) and any(
            k.startswith("md_") for k in raw_result.keys()
        ):
            # This is a heuristic to detect a notebook result.
            # A better way is to check context.run_type, but this works for now.
            if not formatter_options:
                return raw_result

        if not formatter_options:
            if isinstance(raw_result, dict):
                for key in ["results", "data", "content"]:
                    if key in raw_result:
                        val = raw_result[key]
                        if key == "content":
                            try:
                                val = json.loads(val)
                            except (json.JSONDecodeError, TypeError):
                                pass
                        return val
            return raw_result

        processed_result = raw_result
        if "query" in formatter_options:
            processed_result = jmespath.search(
                formatter_options["query"], processed_result
            )

        return processed_result

    async def _execute_executable(
        self, run_context: "RunContext", executable: Any
    ) -> Any:
        """
        The central dispatcher for all executable command objects. It routes commands
        to the correct manager or execution logic.
        """
        logger.debug(
            "executor.dispatch.begin",
            executable_type=type(executable).__name__,
            has_piped_input=run_context.piped_input is not None,
        )

        # --- Handle direct data access commands first ---
        if isinstance(executable, VariableLookup):
            if executable.var_name not in run_context.session.variables:
                raise ValueError(f"Variable '{executable.var_name}' not found.")
            if run_context.piped_input is not None:
                raise ValueError("Cannot pipe data into a variable lookup.")
            return run_context.session.variables[executable.var_name]

        # --- DEFINITIVE FIX & NEW FEATURE ---
        # The 'status' object is created here inside a `with` block to ensure
        # it's always available for any command that needs it, and it's
        # automatically stopped when the block finishes.
        with console.status("Executing command...", spinner="dots") as status:
            if isinstance(executable, SliceCommand):
                # SliceCommand is fast, so we don't update the status, just pass it.
                return await executable.execute(run_context, status)

            if isinstance(executable, Command):
                # Check if the command is a data-producing "run" command that needs the spinner.
                is_data_producing_command = getattr(
                    executable, "subcommand", None
                ) == "run" or isinstance(
                    executable, (DotNotationCommand, PositionalArgActionCommand)
                )

                if is_data_producing_command:
                    logger.debug(
                        "executor.run_command.begin",
                        command_type=type(executable).__name__,
                    )
                    if isinstance(executable, FlowCommand):
                        status.update(
                            f"Running flow '{executable.named_args.get('name')}'..."
                        )
                        return await self.flow_manager.run_flow(
                            run_context, executable.named_args
                        )
                    if isinstance(executable, QueryCommand):
                        status.update(
                            f"Running query '{executable.named_args.get('name')}' on '{executable.named_args.get('on')}'..."
                        )
                        return await self.query_manager.run_query(
                            run_context, executable.named_args
                        )
                    if isinstance(executable, ScriptCommand):
                        status.update(
                            f"Running script '{executable.named_args.get('name')}'..."
                        )
                        return await self.script_manager.run_script(
                            run_context, executable.named_args
                        )
                    if isinstance(
                        executable, (DotNotationCommand, PositionalArgActionCommand)
                    ):
                        # These commands update the status message themselves.
                        return await executable.execute(run_context, status)
                else:
                    # This path is for non-data-producing management commands.
                    # We stop the spinner immediately as these are typically instant.
                    status.stop()
                    return await self._dispatch_management_command(
                        run_context, executable
                    )

        raise TypeError(f"Cannot execute object of type: {type(executable).__name__}")

    async def _dispatch_management_command(
        self, run_context: RunContext, command: Command
    ) -> Any:
        """
        Dispatches non-data-producing, built-in, and management commands to their respective handlers.
        This method operates on the provided RunContext.
        """
        # Special handling for 'read', as it's a data-producing built-in.
        if isinstance(command, BuiltinCommand) and command.command == "read":
            # We call its handler and explicitly return the result.
            return await self.execute_read(run_context, command.args)

        command_prints_own_output = False
        simple_confirmation_message = None

        # --- Subcommand: list ---
        if hasattr(command, "subcommand") and command.subcommand == "list":
            if isinstance(command, ConnectionCommand):
                return self.connection_manager.list_connections()
            if isinstance(command, FlowCommand):
                return self.flow_manager.list_flows()
            if isinstance(command, QueryCommand):
                return self.query_manager.list_queries()
            if isinstance(command, ScriptCommand):
                return self.script_manager.list_scripts()
            if isinstance(command, SessionCommand):
                return self.session_manager.list_sessions()
            if isinstance(command, VariableCommand):
                return self.variable_manager.list_variables(run_context.session)
            if isinstance(command, AppCommand):
                return await self.app_manager.list_installed_apps()
            if isinstance(command, ProcessCommand):
                return self.process_manager.list_processes()
            if isinstance(command, WorkspaceCommand):
                return self.workspace_manager.list_roots()

        # --- Other Top-Level Commands ---
        if isinstance(command, AppCommand) and command.subcommand == "search":
            return await self.app_manager.search(command.args.get("query"))

        if isinstance(command, InspectCommand):
            # The execute method for InspectCommand now needs the context.
            return await command.execute(run_context)

        if isinstance(command, BuiltinCommand) and command.command == "connections":
            return [
                {"Alias": alias, "Source": source}
                for alias, source in run_context.session.connections.items()
            ]

        if isinstance(command, BuiltinCommand):
            # This block now only handles NON-data-producing built-ins like connect, help, etc.
            command_prints_own_output = True
            handler = self.builtin_commands.get(command.command)
            if handler:
                if asyncio.iscoroutinefunction(handler):
                    await handler(run_context, command.args)
                else:
                    handler(run_context, command.args)

        elif isinstance(command, ConnectionCommand) and command.subcommand == "create":
            await self.connection_manager.create_interactive(
                command.named_args.get("blueprint")
            )
            command_prints_own_output = True

        elif isinstance(command, AppCommand):
            command_prints_own_output = True
            if command.subcommand == "install":
                await self.app_manager.install(command.args)
            elif command.subcommand == "uninstall":
                await self.app_manager.uninstall(command.args["id"])
            elif command.subcommand == "package":
                await self.app_manager.package(command.args["path"])

        elif isinstance(command, SessionCommand):
            if command.subcommand == "status":
                self.session_manager.show_status(run_context.session)
                command_prints_own_output = True
            elif command.subcommand == "save":
                simple_confirmation_message = self.session_manager.save_session(
                    run_context.session, command.arg
                )
            elif command.subcommand == "rm":
                simple_confirmation_message = await self.session_manager.delete_session(
                    command.arg
                )
            elif command.subcommand == "load":
                return self.session_manager.load_session(
                    command.arg
                )  # Returns new state

        elif isinstance(command, VariableCommand) and command.subcommand == "rm":
            simple_confirmation_message = self.variable_manager.delete_variable(
                run_context.session, command.arg
            )

        elif isinstance(command, OpenCommand):
            command_prints_own_output = True
            await self.open_manager.open_asset(run_context, command)

        elif isinstance(command, ProcessCommand):
            command_prints_own_output = True
            if command.subcommand == "logs":
                self.process_manager.get_logs(command.arg, command.follow)
            elif command.subcommand == "stop":
                self.process_manager.stop_process(command.arg)

        elif isinstance(command, CompileCommand):
            command_prints_own_output = True
            await self.compile_manager.run_compile(**command.named_args)

        elif isinstance(command, AgentCommand):
            command_prints_own_output = True
            await self.orchestrator.start_session(command.goal)

        elif isinstance(command, WorkspaceCommand):
            command_prints_own_output = True
            if command.subcommand == "list":
                self.workspace_manager.list_roots()
            elif command.subcommand == "add":
                self.workspace_manager.add_root(command.args["path"])
            elif command.subcommand == "remove":
                self.workspace_manager.remove_root(command.args["path"])
            elif command.subcommand == "index":
                self.index_manager.rebuild_index()
                console.print("✅ VFS Index rebuild complete.")

        elif isinstance(command, InstallCommand):
            self.install_manager.install_project_dependencies(Path.cwd())
            command_prints_own_output = True

        elif isinstance(command, PublishCommand):
            return await self.publisher.publish(run_context, command.named_args)

        if simple_confirmation_message:
            return simple_confirmation_message

        if command_prints_own_output:
            return None  # Suppress further output handling

        return {
            "status": "success",
            "message": f"Management command '{type(command).__name__}' executed successfully.",
        }

    def execute_help(self, run_context: RunContext, args: List[str]):
        """Displays the comprehensive help message for the shell."""
        console.print()
        title = Panel(
            "[bold yellow]Welcome to the Contextual Shell (`cx`) v0.7.0[/bold yellow]",
            expand=False,
            border_style="yellow",
        )
        console.print(title)
        console.print(
            "\n`cx` is an interactive, compositional shell for orchestrating data workflows."
        )

        builtins_table = Table(
            title="[bold cyan]Core & Session Commands[/bold cyan]",
            box=box.MINIMAL,
            padding=(0, 1),
        )
        builtins_table.add_column("Command", style="yellow", no_wrap=True)
        builtins_table.add_column("Description")
        builtins_table.add_row(
            "connect <source> --as <alias>",
            "Activate a connection for the current session.",
        )
        builtins_table.add_row(
            "connections", "List all active connections in the current session."
        )
        builtins_table.add_row(
            "session [list|save|load|rm|status]",
            "Manage persistent workspace sessions.",
        )
        builtins_table.add_row("var [list|rm]", "Manage in-memory session variables.")
        builtins_table.add_row("exit | quit", "Exit the interactive shell.")
        builtins_table.add_row("help", "Show this help message.")
        console.print(builtins_table)

        assets_table = Table(
            title="[bold cyan]Workspace & Asset Management[/bold cyan]",
            box=box.MINIMAL,
            padding=(0, 1),
        )
        assets_table.add_column("Command", style="yellow", no_wrap=True)
        assets_table.add_column("Description")
        assets_table.add_row(
            "flow [list|run]", "Manage and run `.flow.yaml` or `.cx.md` workflows."
        )
        assets_table.add_row(
            "query [list|run]", "Manage and run reusable `.sql` queries."
        )
        assets_table.add_row(
            "script [list|run]", "Manage and run reusable `.py` scripts."
        )
        assets_table.add_row(
            "connection [list|create]", "Manage connection configuration files on disk."
        )
        assets_table.add_row(
            "workspace [list|add|remove|index]",
            "Manage multi-root project directories.",
        )
        assets_table.add_row(
            "find [query]",
            "Perform a semantic search of your workspace history and assets.",
        )
        assets_table.add_row(
            "app [list|install|uninstall|package|search]",
            "Manage self-contained `cx` applications.",
        )
        assets_table.add_row(
            "compile",
            "Compile an API specification (e.g., OpenAPI) into a `cx` Blueprint.",
        )
        assets_table.add_row(
            "install",
            "Install Python dependencies for the current project (`cx.project.yaml`).",
        )
        console.print(assets_table)

        execution_table = Table(
            title="[bold cyan]Execution, Composition & Agentic Features[/bold cyan]",
            box=box.MINIMAL,
            padding=(0, 1),
        )
        execution_table.add_column("Syntax", style="yellow", no_wrap=True)
        execution_table.add_column("Description")
        execution_table.add_row(
            "<alias>.<action>(...)",
            "Execute a blueprint-defined action on an active connection.",
        )
        execution_table.add_row(
            "<command> | <command>",
            "Pipe the output of one command to the input of the next.",
        )
        execution_table.add_row(
            "<variable> = <command>",
            "Assign the result of a command to a session variable.",
        )
        execution_table.add_row(
            "inspect <variable>", "Display a detailed summary of a session variable."
        )
        execution_table.add_row(
            "// <natural language>",
            "Get a one-shot AI translation of your intent into a `cx` command.",
        )
        execution_table.add_row(
            'agent "<goal>"',
            "Start a multi-step AI agent session to accomplish a complex goal.",
        )
        console.print(execution_table)

        formatter_table = Table(
            title="[bold cyan]Universal Output Formatters[/bold cyan]",
            box=box.MINIMAL,
            padding=(0, 1),
        )
        formatter_table.add_column("Flag", style="yellow", no_wrap=True)
        formatter_table.add_column("Description")
        formatter_table.add_row(
            "--cx-output table", "Render list-based results as a formatted table."
        )
        formatter_table.add_row(
            "--cx-columns <col1,col2>", "Select specific columns for table output."
        )
        formatter_table.add_row(
            "--cx-query <jmespath>",
            "Filter or reshape JSON output using a JMESPath query.",
        )
        console.print(formatter_table)
        console.print()

    def execute_list_connections(self, run_context: RunContext, args: List[str]):
        """Displays a table of connections active in the current session."""
        if not run_context.session.connections:
            console.print("No active connections in this session.")
            return
        table = Table(title="[bold green]Active Session Connections[/bold green]")
        table.add_column("Alias", style="cyan", no_wrap=True)
        table.add_column("Source", style="magenta")
        for alias, source in run_context.session.connections.items():
            table.add_row(str(alias), str(source))
        console.print(table)

    async def execute_connect(self, run_context: RunContext, args: List[str]):
        """Tests a connection and, on success, adds it to the current session state."""
        if len(args) < 3 or args[1].lower() != "--as":
            console.print(
                "[bold red]Invalid syntax.[/bold red] Use: `connect <connection_source> --as <alias>`"
            )
            return
        source, alias = args[0], args[2]
        with console.status(
            f"Attempting to connect to '[yellow]{source}[/yellow]'...", spinner="dots"
        ):
            # Use the ConnectorService from the provided RunContext
            result = await run_context.services.connector_service.test_connection(
                run_context=run_context, connection_source=source
            )

        if result.get("status") == "success":
            # Update the session state within the RunContext
            run_context.session.connections[alias] = source
            console.print(
                f"[bold green]✅ Connection successful.[/bold green] Alias '[cyan]{alias}[/cyan]' is now active."
            )
        else:
            error_message = result.get("message", "An unknown error occurred.")
            console.print(f"[bold red]❌ Connection failed:[/bold red] {error_message}")

    async def execute_read(self, run_context: "RunContext", args: List[str]) -> Any:
        """
        Executes the 'read' command. It takes a path from its arguments first,
        or falls back to using the piped input if no argument is provided.
        It then uses the system_smart_fetcher to get the content.
        """
        # Determine the path: either the first argument or the piped input.
        path = args[0] if args else run_context.piped_input

        if not path or not isinstance(path, str):
            raise ValueError(
                "The 'read' command requires a path as an argument or as piped input."
            )

        logger.debug("executor.read.resolved_path", path=path)

        # We create a temporary, single-step script to run the read action,
        # leveraging our existing powerful ScriptEngine.
        step = ConnectorStep(
            id="interactive_read",
            name="Interactive Read",
            connection_source="user:system_smart_fetcher",
            run=ReadContentAction(action="read_content", path=path),
        )
        script = create_script_for_step(step)

        # The engine will execute the step and return the result dictionary.
        results = await self.script_engine.run_script_model(
            context=run_context, script_data=script.model_dump()
        )
        # print("executor.read.results")
        # print(results)

        # We return the raw result from the engine. The executor's pipeline
        # will handle unwrapping and formatting.
        return results

    async def dry_run(self, command_text: str) -> DryRunResult:
        """
        Parses a command and simulates its execution to predict the outcome
        without making state-altering or expensive calls.
        """
        # A dry run needs its own context to resolve connections and strategies.
        run_context = RunContext(services=self.registry, session=self.state)

        try:
            tree = self.parser.parse(command_text)
            pipeline_command = self.transformer.transform(tree)
            executable, _ = pipeline_command.commands[0]

            # Handle assignments by looking at the inner command
            if isinstance(executable, AssignmentCommand):
                executable, _ = executable.command_to_run.commands[0]

            if isinstance(executable, DotNotationCommand):
                step = executable.to_step(run_context.session)
                if step.run.action == "run_declarative_action":
                    conn, secrets = await run_context.services.resolver.resolve(
                        step.connection_source
                    )
                    strategy = run_context.services.connector_service._get_strategy_for_connection_model(
                        conn
                    )
                    if hasattr(strategy, "dry_run"):
                        return await strategy.dry_run(
                            conn, secrets, step.run.model_dump()
                        )

            # For other command types, a successful parse is a successful dry run.
            return DryRunResult(
                indicates_failure=False,
                message="Command is syntactically valid and ready for execution.",
            )
        except Exception as e:
            return DryRunResult(
                indicates_failure=True,
                message=f"Command is invalid and would fail. Error: {e}",
            )
