# ~/repositories/cx-shell/src/cx_shell/server/main.py
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from typing import Any, Optional, Dict
import structlog
import uuid
import yaml

from ..interactive.session import SessionState
from ..interactive.executor import CommandExecutor
from ..interactive.output_handler import IOutputHandler
from ..engine.connector.utils import safe_serialize
from ..interactive.commands import Command
from ..engine.context import RunContext
from ..management.notebook_parser import NotebookParser
from cx_core_schemas.connector_script import ConnectorScript
from cx_core_schemas.notebook import ContextualPage

logger = structlog.get_logger(__name__)
app = FastAPI()

# In-memory store for session-specific data. In production, this might move to Redis.
SESSION_DATA = {}


class WebSocketHandler(IOutputHandler):
    """
    An IOutputHandler implementation that sends structured JSON messages
    over a WebSocket connection. This is the bridge between the cx-engine
    and the web UI.
    """

    def __init__(
        self, websocket: WebSocket, command_id: str, executor: CommandExecutor
    ):
        self.websocket = websocket
        self.command_id = command_id
        self.executor = executor  # Store executor to access session state

    async def handle_result(
        self,
        result: Any,
        executable: Optional[Command],
        options: Optional[Dict] = None,
        run_context: Optional["RunContext"] = None,  # <-- ADD THE MISSING ARGUMENT
    ):
        # --- END OF DEFINITIVE FIX ---
        """Processes the final result and sends it as a success or error message."""
        try:
            if isinstance(result, dict) and "error" in result:
                await self.send_error(result["error"])
            else:
                # Pass the session state from the executor
                await self.send_success(result, executable, self.executor.state)
        except Exception as e:
            logger.error("Error in WebSocketHandler", exc_info=True)
            await self.send_error(f"Error in WebSocketHandler: {e}")

    async def send_message(self, msg_type: str, payload: Any):
        """Utility to send a structured message over the WebSocket."""
        await self.websocket.send_json(
            {
                "type": msg_type,
                "command_id": self.command_id,
                "payload": safe_serialize(payload),
            }
        )

    async def send_error(self, error_message: str):
        await self.send_message("RESULT_ERROR", {"error": error_message})

    async def send_success(
        self, data: Any, executable: Optional[Command], session_state: SessionState
    ):
        """
        Sends a success message, packaging the command's result and the
        latest session state for UI synchronization.
        """
        is_list_command = (
            hasattr(executable, "subcommand") and executable.subcommand == "list"
        ) or (hasattr(executable, "command") and executable.command == "connections")

        # For list commands, the data IS the payload. For others, it's nested.
        result_payload = data if is_list_command else {"result": data}

        # Always include the latest session state for the UI to sync.
        connections = [
            {"alias": a, "source": s} for a, s in session_state.connections.items()
        ]
        variables = [
            {"name": n, "type": type(v).__name__, "preview": repr(v)[:100]}
            for n, v in session_state.variables.items()
        ]

        full_payload = {
            "result": result_payload,
            "new_session_state": {
                "connections": connections,
                "variables": variables,
            },
        }
        await self.send_message("RESULT_SUCCESS", full_payload)


@app.get("/health")
async def health_check():
    """A simple endpoint to confirm the server is running."""
    return {"status": "ok"}


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    session_id = str(uuid.uuid4())
    SESSION_DATA[session_id] = {"current_page": None, "block_results": {}}
    log = logger.bind(session_id=session_id)
    log.info("WebSocket client connected.")

    session_state = SessionState(is_interactive=False)
    # We need to instantiate the FlowConverter and add it to the registry.
    # This was a missing piece from the previous step.
    from ..management.flow_converter import FlowConverter

    executor = CommandExecutor(session_state, output_handler=None)
    executor.registry.flow_converter = FlowConverter()

    try:
        while True:
            data = await websocket.receive_json()
            command_id = data.get("command_id", str(uuid.uuid4()))
            msg_type = data.get("type")
            payload = data.get("payload", {})

            output_handler = WebSocketHandler(websocket, command_id, executor)
            executor.output_handler = output_handler

            log.info(
                "Received message from client.",
                msg_type=msg_type,
                command_id=command_id,
            )

            # --- START OF DEFINITIVE FIX: The Message Dispatcher ---
            if msg_type == "LOAD_PAGE":
                page_name = payload.get("page_name")
                log.info("Handling page load request.", page_name=page_name)
                try:
                    page_path = executor.flow_manager._find_flow(page_name)

                    if page_path.name.endswith((".cx.md")):
                        parser = NotebookParser()
                        page_model = parser.parse(page_path)
                    elif page_path.name.endswith((".flow.yaml", ".flow.yml")):
                        script_content = yaml.safe_load(page_path.read_text())
                        connector_script = ConnectorScript(**script_content)
                        converter = executor.registry.flow_converter
                        page_model = converter.convert(connector_script)
                    else:
                        raise ValueError(
                            f"Unsupported file type for viewing: {page_path.name}"
                        )

                    page_model.id = page_name

                    SESSION_DATA[session_id]["current_page"] = page_model
                    SESSION_DATA[session_id][
                        "block_results"
                    ] = {}  # Reset results on page load
                    await output_handler.send_message(
                        "PAGE_LOADED", page_model.model_dump()
                    )
                except Exception as e:
                    log.error("page_load.failed", page_name=page_name, error=str(e))
                    await output_handler.send_error(
                        f"Error loading page '{page_name}': {e}"
                    )
            elif msg_type == "RUN_PAGE":
                page_id = payload.get("page_id")
                parameters = payload.get("parameters", {})
                page: Optional[ContextualPage] = SESSION_DATA[session_id].get(
                    "current_page"
                )
                log.info(
                    "Handling run page request.", page_id=page_id, parameters=parameters
                )

                if not page or page.id != page_id:
                    await output_handler.send_error(
                        "Cannot run page: Page is not loaded or mismatch."
                    )
                    continue

                try:
                    # Define the callback function that will be passed to the engine
                    async def status_update_callback(
                        block_id: str, status: str, result: Any
                    ):
                        log.info(
                            "Sending block status update to client.",
                            block_id=block_id,
                            status=status,
                        )
                        await output_handler.send_message(
                            "BLOCK_STATUS_UPDATE",
                            {
                                "block_id": block_id,
                                "status": status,
                                "error": result if status == "error" else None,
                            },
                        )
                        if status == "success":
                            await output_handler.send_message(
                                "BLOCK_RESULT", {"block_id": block_id, "result": result}
                            )

                    # Prepare the context for the full page run
                    run_context = RunContext(
                        services=executor.registry,
                        session=executor.state,
                        current_flow_path=executor.flow_manager._find_flow(page.id),
                        script_input=parameters,
                    )

                    # Execute the entire page, passing our callback
                    await executor.script_engine.run_script(
                        context=run_context, status_callback=status_update_callback
                    )

                except Exception as e:
                    log.error(
                        "Page execution failed.",
                        page_id=page.id,
                        error=str(e),
                        exc_info=True,
                    )
                    await output_handler.send_error(f"Failed to run page: {e}")
            elif msg_type == "RUN_BLOCK":
                block_id = payload.get("block_id")
                page_id_from_payload = payload.get("page_id")
                parameters = payload.get("parameters", {})
                page: Optional[ContextualPage] = SESSION_DATA[session_id].get(
                    "current_page"
                )

                log.info(
                    "Handling run block request.",
                    block_id=block_id,
                    page_id=page_id_from_payload,
                    parameters=parameters,
                )

                await output_handler.send_message(
                    "BLOCK_STATUS_UPDATE",
                    {"block_id": block_id, "status": "running", "payload": None},
                )

                try:
                    if not page or not block_id or page.id != page_id_from_payload:
                        raise ValueError(
                            "Mismatch between requested page and loaded page."
                        )

                    original_block_to_run = next(
                        (b for b in page.blocks if b.id == block_id), None
                    )
                    if not original_block_to_run:
                        raise ValueError(
                            f"Block '{block_id}' not found in current page."
                        )

                    # --- START OF DEFINITIVE FIX ---
                    updated_block = original_block_to_run.model_copy(deep=True)

                    # The client sends the code from the editor in 'block_content'.
                    # For `run` blocks, this content is YAML. We only parse it if it's a non-empty string.
                    block_content_from_client = payload.get("block_content")
                    if (
                        isinstance(block_content_from_client, str)
                        and block_content_from_client.strip()
                    ):
                        log.debug(
                            "Received updated block content from client.",
                            block_id=block_id,
                        )
                        if updated_block.engine == "run" or updated_block.run:
                            try:
                                run_payload = yaml.safe_load(block_content_from_client)
                                updated_block.run = run_payload
                                updated_block.content = (
                                    None  # Clear content field for `run` blocks
                                )
                            except Exception as e:
                                raise ValueError(
                                    f"Invalid YAML in run block: {e}"
                                ) from e
                        else:
                            updated_block.content = block_content_from_client
                    else:
                        # If no content is sent (or it's null), trust the original block or the block_run payload
                        log.debug(
                            "No updated block content from client, using original/parsed block.",
                            block_id=block_id,
                        )
                        if "block_run" in payload:
                            updated_block.run = payload["block_run"]
                            updated_block.content = None
                    # --- END OF DEFINITIVE FIX ---

                    script = ConnectorScript(
                        name=f"Run block {block_id}", steps=[updated_block]
                    )

                    run_context = RunContext(
                        services=executor.registry,
                        session=executor.state,
                        current_flow_path=executor.flow_manager._find_flow(page.id),
                        script_input=parameters,
                    )

                    existing_results = SESSION_DATA[session_id]["block_results"]
                    run_context.steps.update(existing_results)

                    results = await executor.script_engine.run_script_model(
                        context=run_context, script_data=script.model_dump()
                    )

                    if isinstance(results, dict) and "error" in results:
                        raise Exception(results["error"])

                    block_result_data = results.get(block_id)

                    SESSION_DATA[session_id]["block_results"][block_id] = {
                        "outputs": {
                            (
                                updated_block.outputs[0]
                                if updated_block.outputs
                                else "data"
                            ): block_result_data
                        }
                    }

                    await output_handler.send_message(
                        "BLOCK_RESULT",
                        {"block_id": block_id, "result": block_result_data},
                    )

                except Exception as e:
                    error_message = str(e)
                    log.error(
                        "Block execution failed.",
                        block_id=block_id,
                        error=error_message,
                        exc_info=True,
                    )
                    await output_handler.send_message(
                        "BLOCK_STATUS_UPDATE",
                        {
                            "block_id": block_id,
                            "status": "error",
                            "error": error_message,
                        },
                    )
            elif msg_type == "SAVE_PAGE":
                page_data = payload.get("page")
                log.info("Handling save page request.", page_name=page_data.get("id"))

                if not page_data or not page_data.get("id"):
                    await output_handler.send_error(
                        "Invalid page data provided for saving."
                    )
                    continue

                try:
                    page_to_save = ContextualPage(**page_data)
                    target_path = executor.flow_manager._find_flow(page_to_save.id)

                    # --- START OF DEFINITIVE, FORMATTING-AWARE SERIALIZATION ---
                    output_parts = []

                    # 1. Serialize Front Matter
                    front_matter_data = page_to_save.model_dump(
                        exclude={
                            "blocks",
                            "id",
                        },  # Exclude fields not needed in front matter
                        exclude_none=True,
                        by_alias=True,
                    )
                    output_parts.append("---")
                    output_parts.append(
                        yaml.dump(front_matter_data, sort_keys=False).strip()
                    )
                    output_parts.append("---")

                    # 2. Serialize Blocks
                    for block in page_to_save.blocks:
                        # Add a separator for clarity between blocks
                        output_parts.append("\n")

                        if block.engine == "markdown":
                            output_parts.append(block.content)
                            output_parts.append("\n")  # Ensure a newline after markdown
                        else:
                            # Metadata Block
                            metadata_dict = block.model_dump(
                                exclude={"content", "run"},
                                exclude_none=True,
                                by_alias=True,
                            )
                            metadata_dict["cx_block"] = True

                            output_parts.append("```yaml")
                            output_parts.append(
                                yaml.dump(metadata_dict, sort_keys=False).strip()
                            )
                            output_parts.append("```\n")  # Crucial newline

                            # Code Block
                            code_lang = (
                                "yaml"
                                if block.engine == "run"
                                else (block.engine or "text")
                            )

                            # Determine the content to write in the code block
                            content_to_write = ""
                            # The UI updates the 'content' field during edits
                            if block.content is not None:
                                if isinstance(block.content, (dict, list)):
                                    # If content is structured, dump it as YAML
                                    content_to_write = yaml.dump(
                                        block.content, sort_keys=False
                                    ).strip()
                                else:
                                    content_to_write = str(block.content).strip()
                            elif block.run:  # Fallback for non-edited blocks
                                content_to_write = yaml.dump(
                                    block.run.model_dump(exclude_unset=True),
                                    sort_keys=False,
                                ).strip()

                            output_parts.append(f"```{code_lang}")
                            output_parts.append(content_to_write)
                            output_parts.append("```\n")

                    final_content = "\n".join(output_parts)
                    # --- END OF DEFINITIVE SERIALIZATION ---

                    target_path.write_text(final_content, encoding="utf-8")

                    log.info("Page saved successfully.", path=str(target_path))
                    await output_handler.send_message(
                        "PAGE_SAVED", {"path": str(target_path)}
                    )

                except Exception as e:
                    log.error("save_page.failed", error=str(e), exc_info=True)
                    await output_handler.send_error(f"Failed to save page: {e}")
            elif msg_type == "EXECUTE_COMMAND":
                command_text = payload.get("command_text")
                if command_text:
                    log.info(
                        "Handling execute command request.", command_text=command_text
                    )
                    await executor.execute(command_text)

            else:
                log.warning("Received unknown message type.", msg_type=msg_type)
            # --- END OF DEFINITIVE FIX ---

    except WebSocketDisconnect:
        log.info("WebSocket client disconnected.")
    finally:
        if session_id in SESSION_DATA:
            del SESSION_DATA[session_id]
        log.info("Closing WebSocket session and cleaning up state.")
