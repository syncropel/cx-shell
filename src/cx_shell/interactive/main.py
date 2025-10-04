import asyncio
from pathlib import Path

from prompt_toolkit import PromptSession
from prompt_toolkit.filters import Condition
from prompt_toolkit.history import FileHistory
from prompt_toolkit.key_binding import KeyBindings

from .completer import CxCompleter
from .executor import CommandExecutor
from .session import SessionState

# --- THIS IS THE NEW IMPORT ---
from .output_handler import RichConsoleHandler


def start_repl():
    """Starts the main Read-Eval-Print-Loop (REPL) for the interactive shell."""
    history_file = Path.home() / ".cx_history"
    state = SessionState()
    completer = CxCompleter(state)

    # 1. Instantiate the CommandExecutor first, passing a temporary `None` for the handler.
    executor = CommandExecutor(state, output_handler=None)

    # 2. Now, instantiate the RichConsoleHandler and pass the executor to it.
    output_handler = RichConsoleHandler(executor=executor)

    # 3. Finally, assign the fully initialized handler back to the executor.
    executor.output_handler = output_handler

    bindings = KeyBindings()
    prompt_session = PromptSession(
        history=FileHistory(str(history_file)),
        completer=completer,
        complete_while_typing=True,
    )

    @bindings.add(
        "enter",
        filter=Condition(
            lambda: prompt_session.default_buffer.complete_state is not None
        ),
    )
    def _(event):
        """Applies the current completion instead of submitting."""
        event.current_buffer.complete_state.current_completion.apply_completion(
            event.current_buffer
        )

    prompt_session.key_bindings = bindings

    async def repl_main():
        nonlocal state, completer, executor
        next_prompt_default = ""

        while state.is_running:
            try:
                command_text = await prompt_session.prompt_async(
                    "cx> ", default=next_prompt_default
                )
                next_prompt_default = ""

                if not command_text or not command_text.strip():
                    continue
                if command_text.strip().lower() in ["exit", "quit"]:
                    state.is_running = False
                    continue

                if command_text.strip().startswith("//"):
                    goal = command_text.strip().lstrip("//").strip()
                    suggestion = await executor.orchestrator.prepare_and_run_translate(
                        goal
                    )
                    if suggestion is not None:
                        next_prompt_default = suggestion
                    continue

                # The executor.execute() method now handles all output via the RichConsoleHandler
                new_state = await executor.execute(command_text)

                if isinstance(new_state, SessionState):
                    # Handle session loading
                    state = new_state
                    executor.state = state
                    completer.state = state
                    # The handler will print the confirmation message
                    await output_handler.handle_result(
                        "[bold yellow]Session restored.[/bold yellow]", None, None
                    )

            except KeyboardInterrupt:
                print()
                continue
            except EOFError:
                print()
                state.is_running = False

    asyncio.run(repl_main())
    print("Exiting Contextual Shell. Goodbye!")
