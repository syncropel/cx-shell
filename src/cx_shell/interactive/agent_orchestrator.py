# ~/repositories/cx-shell/src/cx_shell/interactive/agent_orchestrator.py

import asyncio
import traceback
from typing import Any, Dict, List, Optional, cast
import structlog
import yaml

from rich.console import Console
from rich.panel import Panel
from rich import box
from prompt_toolkit.shortcuts import PromptSession
from prompt_toolkit.formatted_text import HTML

from ..agent.llm_client import LLMClient
from ..interactive.session import SessionState
from ..interactive.context_engine import DynamicContextEngine
from ..management.belief_manager import BeliefManager
from ..history_logger import HistoryLogger
from ..agent.planner_agent import PlannerAgent
from ..agent.tool_specialist_agent import ToolSpecialistAgent
from ..agent.analyst_agent import AnalystAgent
from ..data.agent_schemas import AgentBeliefs, CommandOption, PlanStep, AnalystResponse
from .commands import BuiltinCommand

logger = structlog.get_logger(__name__)


# Forward declaration for type hinting to avoid circular import
class CommandExecutor:
    pass


CONSOLE = Console()


class AgentOrchestrator:
    """
    The core orchestrator for the CARE agent. Manages the reasoning loop,
    and the translate feature.
    """

    def __init__(self, state: SessionState, executor: "CommandExecutor"):
        self.state = state
        self.executor = executor
        self.context_engine = DynamicContextEngine(state)
        self.belief_manager = BeliefManager()
        self.history_logger = HistoryLogger()
        self.prompt_session = PromptSession()

        llm_client = LLMClient(state)
        self.planner = PlannerAgent(state, llm_client)
        self.tool_specialist = ToolSpecialistAgent(state, llm_client)
        self.analyst = AnalystAgent(state, llm_client)

    async def _ensure_agent_connection(self, role_name: str) -> bool:
        """
        Checks for a required agent connection, moving all blocking I/O to a
        separate thread to prevent deadlocking the main event loop.
        """
        await self.tool_specialist.load_config_if_needed()

        if not self.tool_specialist.agent_config:
            CONSOLE.print(
                "[bold red]Error:[/bold red] Agent configuration not found or invalid."
            )
            return False

        profile = self.tool_specialist.agent_config.profiles[
            self.tool_specialist.agent_config.default_profile
        ]
        role_config = getattr(profile, role_name)
        alias = role_config.connection_alias

        if alias in self.state.connections:
            return True

        def find_compatible_connections_sync() -> List[str]:
            provider_name = alias.replace("cx_", "")
            blueprint_id_pattern = f"community/{provider_name}@"
            compatible = []
            for conn_file in self.executor.connection_manager.connections_dir.glob(
                "*.conn.yaml"
            ):
                try:
                    data = yaml.safe_load(conn_file.read_text())
                    if data.get("api_catalog_id", "").startswith(blueprint_id_pattern):
                        compatible.append(data.get("id", "").replace("user:", ""))
                except Exception:
                    continue
            return compatible

        compatible_conns = await asyncio.to_thread(find_compatible_connections_sync)

        if compatible_conns:
            from prompt_toolkit.completion import WordCompleter

            completer = WordCompleter(compatible_conns, ignore_case=True)
            chosen_conn_id = await self.prompt_session.prompt_async(
                HTML(
                    f"Press <b>Enter</b> to activate '<b>{compatible_conns[0]}</b>' or choose another: "
                ),
                completer=completer,
                default=compatible_conns[0],
            )
            if chosen_conn_id in compatible_conns:
                await self.executor.execute_connect(
                    [f"user:{chosen_conn_id}", "--as", alias]
                )
                return alias in self.state.connections

        feature_name = (
            "the 'Translate' feature (`//`)"
            if role_name == "co_pilot"
            else "the Agent (`agent ...`)"
        )
        CONSOLE.print(
            f"\n[agent] No suitable connection is active. Let's set up a new one for the {feature_name}."
        )
        blueprint_id = f"community/{alias.replace('cx_', '')}@1.0.0"

        created_conn_id = await self.executor.connection_manager.create_interactive(
            preselected_blueprint_id=blueprint_id
        )

        if created_conn_id:
            await self.executor.execute_connect(
                [f"user:{created_conn_id}", "--as", alias]
            )
        else:
            CONSOLE.print("[yellow]Setup cancelled. Agent cannot proceed.[/yellow]")
            return False

        return alias in self.state.connections

    async def prepare_and_run_translate(self, prompt: str) -> Optional[str]:
        """Orchestrates the 'Translate' (`//`) feature, now managing its own spinner."""
        log = logger.bind(feature="translate", user_prompt=prompt)
        log.info("translate.begin")

        try:
            is_ready = await self._ensure_agent_connection("co_pilot")
            if not is_ready:
                return None

            with CONSOLE.status("Translating intent to command..."):
                log.info("translate.gathering_context")
                tactical_context = []
                for alias in self.state.connections:
                    if not alias.startswith("cx_"):
                        tactical_context.extend(
                            self.context_engine.get_tactical_context(alias)
                        )

                log.info("translate.invoking_agent")
                temp_beliefs = AgentBeliefs(original_goal=prompt)

                llm_response = await self.tool_specialist.generate_command(
                    beliefs=temp_beliefs,
                    active_step_index=0,
                    tactical_context=tactical_context,
                    is_translate=True,
                )

            if llm_response and llm_response.command_options:
                return llm_response.command_options[0].cx_command
            else:
                log.warn(
                    "translate.failed", reason="LLM returned no valid command options."
                )
                return ""
        except Exception as e:
            CONSOLE.print(f"[bold red]Translate Error:[/bold red] {e}")
            return None

    async def start_session(self, goal: str):
        """
        Initiates and manages a full, stateful, multi-step reasoning session with the user,
        powered by the CARE-ToT architecture.
        """
        if not await self._ensure_agent_connection("planner"):
            return

        CONSOLE.print(
            Panel(
                f"[bold]Goal:[/bold] {goal}",
                title="Agent Session Started",
                border_style="blue",
            )
        )
        session_ended_gracefully = False

        # --- Definitive Fix: Create the RunContext once at the beginning ---
        # This single context object will be passed down to all execution calls,
        # ensuring consistency and correct state propagation.
        from ..engine.context import RunContext

        run_context = RunContext(services=self.executor.registry, session=self.state)

        try:
            # === PHASE 1: INITIAL PLANNING ===
            beliefs = self.belief_manager.initialize_beliefs(self.state, goal)
            with CONSOLE.status(
                "[bold yellow]Planner Agent is formulating a strategy...[/bold yellow]"
            ):
                strategic_context = self.context_engine.get_strategic_context(
                    goal, beliefs
                )
                new_plan = await self.planner.generate_plan(goal, strategic_context)

            # Gate 1: Plan Validation
            validation_error = self._validate_plan(new_plan)
            if (
                validation_error
                or not new_plan
                or (new_plan and getattr(new_plan[0], "status", "pending") == "failed")
            ):
                error_msg = validation_error or "Planner failed to create a valid plan."
                CONSOLE.print(f"[bold red]{error_msg} Ending session.[/bold red]")
                session_ended_gracefully = True
                return

            self.belief_manager.update_beliefs(
                self.state,
                [
                    {
                        "op": "replace",
                        "path": "/plan",
                        "value": [p.model_dump() for p in new_plan],
                    }
                ],
            )
            beliefs = cast(AgentBeliefs, self.belief_manager.get_beliefs(self.state))
            self._pretty_print_plan(beliefs)
            CONSOLE.print(
                "[dim]You can inspect the full state at any time with `inspect _agent_beliefs`[/dim]"
            )

            # === PHASE 2: HIERARCHICAL EXECUTION LOOP ===
            for step_count in range(10):  # Overall safety break
                beliefs = cast(
                    AgentBeliefs, self.belief_manager.get_beliefs(self.state)
                )
                next_step_index, next_step = next(
                    (
                        (i, s)
                        for i, s in enumerate(beliefs.plan)
                        if s.status == "pending"
                    ),
                    (-1, None),
                )

                if not next_step:
                    CONSOLE.print(
                        Panel(
                            "[bold green]Mission Accomplished.[/bold green] All plan steps have been executed.",
                            border_style="green",
                        )
                    )
                    session_ended_gracefully = True
                    break

                CONSOLE.print(
                    f"\n[bold]Executing Step {next_step_index + 1}:[/bold] [italic]{next_step.step}[/italic]"
                )
                self.belief_manager.update_beliefs(
                    self.state,
                    [
                        {
                            "op": "replace",
                            "path": f"/plan/{next_step_index}/status",
                            "value": "in_progress",
                        }
                    ],
                )

                # Tactical Loop: Generate, validate, and confirm a command
                final_command_to_run_str = await self._find_viable_command_for_step(
                    beliefs, next_step, next_step_index
                )

                if final_command_to_run_str == "CANCEL":
                    session_ended_gracefully = True
                    break
                if not final_command_to_run_str:
                    CONSOLE.print(
                        "[bold red]Tool Specialist failed after multiple attempts. Re-planning...[/bold red]"
                    )
                    self.belief_manager.update_beliefs(
                        self.state,
                        [
                            {
                                "op": "replace",
                                "path": f"/plan/{next_step_index}/status",
                                "value": "failed",
                            }
                        ],
                    )
                    continue

                # Execution: Parse the command string and execute the resulting object.
                with CONSOLE.status(
                    f"Executing `[bold cyan]{final_command_to_run_str}[/bold cyan]`..."
                ):
                    # --- START OF DEFINITIVE FIX ---
                    # 1. Parse and transform the command string to get the executable object.
                    parsed_tree = self.executor.parser.parse(final_command_to_run_str)
                    pipeline_command = self.executor.transformer.transform(parsed_tree)
                    executable_obj, _ = pipeline_command.commands[0]

                    # 2. Correctly call the executor's method with BOTH the context and the executable.
                    observation = await self.executor._execute_executable(
                        run_context, executable_obj
                    )
                    # --- END OF DEFINITIVE FIX ---

                # Analysis: Interpret the result
                analyst_response: AnalystResponse
                with CONSOLE.status(
                    "[yellow]Analyst Agent is interpreting the results...[/yellow]"
                ):
                    try:
                        analyst_response = await self.analyst.analyze_observation(
                            next_step.step, observation
                        )
                    except Exception as e:
                        logger.error(
                            "Analyst agent failed to produce a valid response.",
                            error=str(e),
                        )
                        analyst_response = AnalystResponse(
                            belief_update={
                                "op": "add",
                                "path": "/discovered_facts/analyst_error",
                                "value": f"The Analyst agent failed to process the observation: {e}",
                            },
                            summary_text="The Analyst agent failed, which is considered a strategic failure.",
                            indicates_strategic_failure=True,
                        )

                # Strategic Loop: Decide whether to continue or re-plan.
                if analyst_response.indicates_strategic_failure:
                    CONSOLE.print(
                        "[bold yellow]Analyst detected a strategic failure. Re-planning...[/bold yellow]"
                    )
                    self.belief_manager.update_beliefs(
                        self.state,
                        [
                            {
                                "op": "replace",
                                "path": f"/plan/{next_step_index}/status",
                                "value": "failed",
                            }
                        ],
                    )
                    continue

                self._update_beliefs_after_turn(
                    next_step_index, analyst_response, observation
                )
                self._pretty_print_plan(
                    cast(AgentBeliefs, self.belief_manager.get_beliefs(self.state))
                )
            else:
                CONSOLE.print(
                    "[bold yellow]Maximum agent steps reached. Ending session.[/bold yellow]"
                )
        except Exception as e:
            CONSOLE.print(
                f"[bold red]Agentic session failed unexpectedly:[/bold red] {e}"
            )
            traceback.print_exc()
        finally:
            if session_ended_gracefully:
                self.belief_manager.end_session(self.state)
                CONSOLE.print(Panel("Agent Session Ended", border_style="blue"))
            else:
                CONSOLE.print(
                    "[dim]Session ended abruptly. Beliefs are preserved for inspection with `inspect _agent_beliefs`.[/dim]"
                )

    async def _find_viable_command_for_step(
        self, beliefs: AgentBeliefs, step: PlanStep, step_index: int
    ) -> Optional[str]:
        """The tactical retry loop for generating and validating a command."""
        for attempt in range(3):
            with CONSOLE.status(
                f"[yellow]Tool Specialist is generating command (Attempt {attempt + 1}/3)...[/yellow]"
            ):
                tactical_context = await self._get_tactical_context_for_step(step)
                llm_response = await self.tool_specialist.generate_command(
                    beliefs=beliefs,
                    active_step_index=step_index,
                    tactical_context=tactical_context,
                )

            statically_valid_options = await self._statically_validate_options(
                llm_response.command_options
            )
            if not statically_valid_options:
                logger.warn("All command options failed static validation. Retrying.")
                continue

            with CONSOLE.status(
                "[yellow]Simulating command outcomes (dry run)...[/yellow]"
            ):
                viable_options = await self._simulate_command_options(
                    statically_valid_options
                )
            if not viable_options:
                logger.warn("All command options failed dry run simulation. Retrying.")
                continue

            best_option = max(viable_options, key=lambda opt: opt.confidence)
            confirmed, user_command = await self.present_and_confirm_with_preview(
                best_option, [opt for opt in viable_options if opt != best_option]
            )

            if not confirmed and not user_command:
                CONSOLE.print("[yellow]Action cancelled by user.[/yellow]")
                return "CANCEL"

            final_command = user_command or best_option.cx_command
            if user_command:
                self.history_logger.log_user_correction(
                    step.step, best_option.cx_command, user_command
                )

            return final_command
        return None

    def _update_beliefs_after_turn(
        self, step_index: int, analyst_response, observation
    ):
        """Applies the analyst's findings to the belief state."""
        final_patch = [analyst_response.belief_update]
        step_status = "completed"
        if (
            "error" in str(observation).lower()
            or "failed" in analyst_response.summary_text.lower()
            or analyst_response.indicates_strategic_failure
        ):
            step_status = "failed"

        final_patch.extend(
            [
                {
                    "op": "replace",
                    "path": f"/plan/{step_index}/status",
                    "value": step_status,
                },
                {
                    "op": "add",
                    "path": f"/plan/{step_index}/result_summary",
                    "value": analyst_response.summary_text,
                },
            ]
        )

        self.belief_manager.update_beliefs(self.state, final_patch)
        self.history_logger.log_agent_turn(
            analyst_response.summary_text, status=step_status.upper()
        )

    async def present_and_confirm_with_preview(
        self, best_option: CommandOption, alternatives: List[CommandOption]
    ) -> (bool, Optional[str]):
        """Presents the agent's best option, its dry run preview, and awaits user confirmation."""
        from rich.text import Text

        panel_content = Text()
        panel_content.append("Reasoning: ", style="dim")
        panel_content.append(f"{best_option.reasoning}\n\n")
        panel_content.append("Next Command:\n", style="bold")
        panel_content.append(f"> {best_option.cx_command}\n\n")

        if hasattr(best_option, "preview") and best_option.preview:
            panel_content.append("🦾 Dry Run Preview:\n", style="bold")
            preview = best_option.preview
            preview_style = "green" if not preview.indicates_failure else "red"
            preview_icon = "✓" if not preview.indicates_failure else "✗"
            panel_content.append(f"   {preview_icon} ", style=preview_style)
            panel_content.append(preview.message, style=f"italic {preview_style}")

        CONSOLE.print(Panel(panel_content, title="Agent Plan", border_style="yellow"))
        response = await self.prompt_session.prompt_async(
            HTML("<b>Execute?</b> [<b>Y</b>es/<b>n</b>o/<b>e</b>dit]: ")
        )
        response = response.lower().strip()

        if response in ("n", "no"):
            return False, None
        if response in ("e", "edit"):
            edited_command = await self.prompt_session.prompt_async(
                "> ", default=best_option.cx_command
            )
            return False, edited_command.strip()
        return True, None

    def _pretty_print_plan(self, beliefs: AgentBeliefs):
        """Renders the agent's current plan to the console using a rich Table."""
        from rich.table import Table

        table = Table(
            title="[bold]Agent Plan[/bold]",
            box=box.ROUNDED,
            show_header=True,
            header_style="bold cyan",
        )
        table.add_column("Status", justify="center", width=8)
        table.add_column("Step", justify="left", overflow="fold")
        status_map = {
            "completed": "[bold green]✓ Done[/bold green]",
            "failed": "[bold red]✗ Failed[/bold red]",
            "in_progress": "[bold yellow]In Prog…[/bold yellow]",
            "pending": "[dim]Pending[/dim]",
        }
        for i, step in enumerate(beliefs.plan):
            status_text = status_map.get(step.status, "[dim]Pending[/dim]")
            step_description = f"[bold]{i + 1}. {step.step}[/bold]"
            if step.result_summary:
                step_description += (
                    f"\n   [italic dim]└── {step.result_summary}[/italic dim]"
                )
            table.add_row(status_text, step_description)
        CONSOLE.print(table)

    def _validate_plan(self, plan: List[PlanStep]) -> Optional[str]:
        """Performs a sanity check on a newly generated plan (Gate 1)."""
        if not plan:
            return "Planner returned an empty plan."
        known_tools = {"compile", "connection", "connect", "app"} | set(
            self.state.connections.keys()
        )
        actionable_steps = sum(
            1 for step in plan if any(tool in step.step.lower() for tool in known_tools)
        )
        if actionable_steps < len(plan) / 2:
            logger.warn(
                "Plan may have low actionability",
                plan=[p.step for p in plan],
                known_tools=list(known_tools),
            )
        return None

    async def _statically_validate_options(
        self, options: List[CommandOption]
    ) -> List[CommandOption]:
        """Performs Gate 2 checks (Lark syntax, Pydantic parameters) on command options."""
        valid_options = []
        for option in options:
            try:
                parsed_tree = self.executor.parser.parse(option.cx_command)
                # The transformer.transform returns a PipelineCommand, which is not a tuple.
                # We need to get the first executable command *from* the pipeline.
                pipeline_command = self.executor.transformer.transform(parsed_tree)
                executable_obj, _ = pipeline_command.commands[0]
                if (
                    isinstance(executable_obj, BuiltinCommand)
                    and executable_obj.command == "connect"
                ):
                    if executable_obj.args[2] in self.state.connections:
                        logger.warn("Pruning redundant connect command", option=option)
                        continue
                valid_options.append(option)
            except Exception as e:
                logger.warn(
                    "Pruning invalid command option due to static validation failure",
                    command=option.cx_command,
                    error=str(e),
                )
                continue
        return valid_options

    async def _simulate_command_options(
        self, options: List[CommandOption]
    ) -> List[CommandOption]:
        """Performs Gate 3 (Dry Run) on a list of command options."""
        viable_options = []
        tasks = [self.executor.dry_run(option.cx_command) for option in options]
        results = await asyncio.gather(*tasks)
        for option, result in zip(options, results):
            option.preview = result
            if not result.indicates_failure:
                viable_options.append(option)
            else:
                logger.warn(
                    "Pruning option due to dry run failure",
                    command=option.cx_command,
                    reason=result.message,
                )
        return viable_options

    async def _get_tactical_context_for_step(
        self, step: PlanStep
    ) -> List[Dict[str, Any]]:
        """Gathers tool schemas for a specific plan step."""
        tactical_context = []
        core_cx_commands = [
            {
                "type": "function",
                "function": {
                    "name": "cx.compile",
                    "description": "Compiles an API specification (e.g., OpenAPI) from a URL into a new `cx` blueprint.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "spec_url": {"type": "string"},
                            "name": {"type": "string"},
                            "version": {"type": "string"},
                        },
                        "required": ["spec_url", "name", "version"],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "cx.connection.create",
                    "description": "Starts an interactive wizard to create a new connection for a specified blueprint.",
                    "parameters": {
                        "type": "object",
                        "properties": {"blueprint": {"type": "string"}},
                        "required": ["blueprint"],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "cx.connect",
                    "description": "Activates a saved connection, assigning it a temporary alias.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "source": {"type": "string"},
                            "alias": {"type": "string"},
                        },
                        "required": ["source", "alias"],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "cx.flow.run",
                    "description": "Executes a pre-existing `.flow.yaml` workflow.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "name": {"type": "string"},
                            "parameters": {"type": "object"},
                        },
                        "required": ["name"],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "cx.fs.write_file",
                    "description": "Writes or overwrites a file with specified content.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "path": {"type": "string"},
                            "content": {"type": "string"},
                        },
                        "required": ["path", "content"],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "cx.inspect",
                    "description": "Displays a detailed summary of a session variable.",
                    "parameters": {
                        "type": "object",
                        "properties": {"variable_name": {"type": "string"}},
                        "required": ["variable_name"],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "cx.app.package",
                    "description": "Packages a local application directory into a distributable archive.",
                    "parameters": {
                        "type": "object",
                        "properties": {"path": {"type": "string"}},
                        "required": ["path"],
                    },
                },
            },
        ]
        tactical_context.extend(core_cx_commands)
        for alias in self.state.connections:
            if not alias.startswith("cx_"):
                tactical_context.extend(self.context_engine.get_tactical_context(alias))
        return tactical_context
