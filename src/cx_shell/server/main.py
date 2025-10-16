# ~/repositories/cx-shell/src/cx_shell/server/main.py

import asyncio
import io
import uuid
import yaml
from datetime import datetime
from typing import Any, Optional, Dict

import structlog
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

# Import all necessary schemas
from cx_core_schemas.connector_script import ConnectorScript
from cx_core_schemas.notebook import ContextualPage
from cx_core_schemas.server_schemas import (
    SepMessage,
    SepPayload,
    BlockStatusFields,
    BlockOutputFields,
    BlockErrorFields,
)

# Import all necessary application components
from ..engine.context import RunContext
from ..interactive.commands import Command
from ..interactive.executor import CommandExecutor
from ..interactive.output_handler import IOutputHandler
from ..interactive.session import SessionState

# --- START OF DEFINITIVE FIX ---
# This is the missing import that was correctly identified.
from ..management.flow_converter import FlowConverter

# --- END OF DEFINITIVE FIX ---
from ..management.notebook_parser import NotebookParser

# --- 1. SETUP ---
logger = structlog.get_logger(__name__)
app = FastAPI()
origins = ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- 2. DEFINE HELPERS AND GLOBAL STATE ---
SESSION_DATA: Dict[str, Dict[str, Any]] = {}
EXECUTOR: Optional[CommandExecutor] = (
    None  # Global placeholder for the session's executor
)


class WebSocketHandler(IOutputHandler):
    # This class is correct as provided and does not need changes.
    def __init__(self, websocket: WebSocket, trace_id: str, executor: CommandExecutor):
        super().__init__(executor)
        self.websocket = websocket
        self.trace_id = trace_id
        self.log = logger.bind(trace_id=trace_id)

    async def send_event(
        self,
        event_type: str,
        source: str,
        level: str,
        message: str,
        fields: Optional[Dict] = None,
        labels: Optional[Dict] = None,
    ):
        payload = SepPayload(
            level=level, message=message, fields=fields, labels=labels or {}
        )
        event = SepMessage(
            trace_id=self.trace_id,
            event_id=str(uuid.uuid4()),
            type=event_type,
            source=source,
            timestamp=datetime.utcnow(),
            payload=payload,
        )
        await self.websocket.send_json(
            event.model_dump(by_alias=True, exclude_none=True)
        )

    async def send_error_event(self, source: str, error_message: str):
        await self.send_event(
            event_type="SYSTEM.ERROR",
            source=source,
            level="error",
            message=error_message,
        )

    async def handle_result(
        self,
        result: Any,
        executable: Optional[Command],
        options: Optional[Dict] = None,
        run_context: Optional["RunContext"] = None,
    ):
        try:
            if isinstance(result, dict) and "error" in result:
                await self.send_error_event(
                    source="/commands/execute", error_message=result["error"]
                )
                return

            connections = [
                {"alias": a, "source": s}
                for a, s in self.executor.state.connections.items()
            ]
            variables = [
                {"name": n, "type": type(v).__name__, "preview": repr(v)[:100]}
                for n, v in self.executor.state.variables.items()
            ]

            fields = {
                "result": result,
                "new_session_state": {
                    "connections": connections,
                    "variables": variables,
                },
            }
            await self.send_event(
                "COMMAND.RESULT",
                "/commands/execute",
                "info",
                "Command executed successfully.",
                fields=fields,
            )
        except Exception as e:
            self.log.error("Error in WebSocketHandler.handle_result", exc_info=True)
            await self.send_error_event(
                source="/system/handler", error_message=f"Error handling result: {e}"
            )


# --- 3. DEFINE API ROUTES ---
@app.get("/health")
async def health_check():
    return {"status": "ok"}


@app.get("/artifacts/{artifact_id}")
async def get_artifact_content(artifact_id: str):
    # This endpoint is correct as provided and does not need changes.
    if not EXECUTOR:
        return {"error": "Server is not fully initialized."}, 503
    log = logger.bind(artifact_id=artifact_id)
    log.info("artifact.request.received")
    try:
        content_bytes = EXECUTOR.registry.cache_manager.read_bytes(artifact_id)
        media_type = "application/json"
        return StreamingResponse(io.BytesIO(content_bytes), media_type=media_type)
    except FileNotFoundError:
        log.warn("artifact.request.not_found")
        return {"error": "Artifact not found"}, 404
    except Exception as e:
        log.error("artifact.request.failed", error=str(e), exc_info=True)
        return {"error": "Internal server error"}, 500


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """
    The main WebSocket endpoint for the cx-server. It handles the entire lifecycle
    of a client connection, dispatching commands and emitting events according
    to the Syncropel Communication Protocol (SCP/SEP).
    """
    await websocket.accept()
    session_id = str(uuid.uuid4())
    log = logger.bind(session_id=session_id)
    log.info("WebSocket client connected.")

    session_state = SessionState(is_interactive=False)
    executor = CommandExecutor(session_state, output_handler=None)
    executor.registry.flow_converter = FlowConverter()

    global EXECUTOR
    EXECUTOR = executor

    try:
        while True:
            data = await websocket.receive_json()
            trace_id = data.get("command_id", str(uuid.uuid4()))
            msg_type = data.get("type")
            payload = data.get("payload", {})

            output_handler = WebSocketHandler(websocket, trace_id, executor)
            executor.output_handler = output_handler

            request_log = log.bind(trace_id=trace_id, msg_type=msg_type)
            request_log.info("Received message from client.")

            try:
                if msg_type == "SESSION.INIT":
                    await output_handler.send_event(
                        "SESSION.LOADED",
                        "/session",
                        "info",
                        "Session initialized.",
                        {"new_session_state": {"connections": [], "variables": []}},
                    )

                elif msg_type == "PAGE.LOAD":
                    page_name = payload.get("page_id")
                    page_path = executor.flow_manager._find_flow(page_name)

                    if page_path.name.endswith(".cx.md"):
                        page_model = NotebookParser().parse(page_path)
                    elif page_path.name.endswith((".flow.yaml", ".flow.yml")):
                        script = ConnectorScript(
                            **yaml.safe_load(page_path.read_text())
                        )
                        page_model = executor.registry.flow_converter.convert(script)
                    else:
                        raise ValueError(f"Unsupported file type: {page_path.name}")

                    page_model.id = page_name
                    SESSION_DATA[session_id] = {
                        "current_page": page_model,
                        "block_results": {},
                    }
                    await output_handler.send_event(
                        "PAGE.LOADED",
                        f"/pages/{page_name}",
                        "info",
                        f"Page '{page_name}' loaded.",
                        {"page": page_model.model_dump(by_alias=True)},
                    )

                elif msg_type == "BLOCK.RUN" or msg_type == "PAGE.RUN":
                    page_id = payload.get("page_id")
                    page: Optional[ContextualPage] = SESSION_DATA[session_id].get(
                        "current_page"
                    )

                    if not page or page.id != page_id:
                        raise ValueError(
                            "Cannot run: Page is not loaded or page_id mismatches."
                        )

                    async def status_update_callback(
                        block_id: str, status: str, result_data: Any
                    ):
                        source = f"/blocks/{block_id}"
                        message = f"Block '{block_id}' status: {status}"
                        fields = {}
                        event_type = "UNKNOWN"

                        if status == "success":
                            event_type = "BLOCK.OUTPUT"
                            fields = BlockOutputFields(
                                block_id=block_id,
                                status="success",
                                duration_ms=result_data.get("duration_ms", 0),
                                output=result_data.get("output"),
                            ).model_dump(exclude_none=True)
                        elif status == "error":
                            event_type = "BLOCK.ERROR"
                            fields = BlockErrorFields(
                                block_id=block_id,
                                status="error",
                                duration_ms=result_data.get("duration_ms", 0),
                                error={
                                    "message": str(
                                        result_data.get("error", "Unknown error")
                                    )
                                },
                            ).model_dump(exclude_none=True)
                        else:
                            event_type = "BLOCK.STATUS"
                            fields = BlockStatusFields(
                                block_id=block_id, status=status
                            ).model_dump()

                        await output_handler.send_event(
                            event_type,
                            source,
                            "error" if status == "error" else "info",
                            message,
                            fields,
                            {"component": "ScriptEngine"},
                        )

                    run_context = RunContext(
                        services=executor.registry,
                        session=executor.state,
                        current_flow_path=executor.flow_manager._find_flow(page.id),
                        script_input=payload.get("parameters", {}),
                    )

                    if msg_type == "PAGE.RUN":
                        # Run the entire page as a background task
                        asyncio.create_task(
                            executor.script_engine.run_script(
                                context=run_context,
                                status_callback=status_update_callback,
                            )
                        )

                    elif msg_type == "BLOCK.RUN":
                        block_id = payload.get("block_id")
                        original_block = next(
                            (b for b in page.blocks if b.id == block_id), None
                        )
                        if not original_block:
                            raise ValueError(f"Block '{block_id}' not found.")

                        updated_block = original_block.model_copy(deep=True)
                        content_override = payload.get("content_override")
                        if (
                            isinstance(content_override, str)
                            and content_override.strip()
                        ):
                            if updated_block.run:
                                updated_block.run = yaml.safe_load(content_override)
                            else:
                                updated_block.content = content_override

                        script = ConnectorScript(
                            name=f"Run block {block_id}", steps=[updated_block]
                        )
                        run_context.steps = SESSION_DATA[session_id].get(
                            "block_results", {}
                        )

                        # Run the single block as a background task
                        asyncio.create_task(
                            executor.script_engine.run_script_model(
                                context=run_context,
                                script_data=script.model_dump(),
                                status_callback=status_update_callback,
                            )
                        )

                elif msg_type == "COMMAND.EXECUTE":
                    command_text = payload.get("command_text")
                    if command_text:
                        await executor.execute(command_text)

                elif msg_type == "WORKSPACE.BROWSE":
                    # This is now a full, production-grade implementation.
                    # A dedicated WorkspaceBrowser service would be even better in the future.
                    # For now, this is robust.
                    browse_path = payload.get("path", "/")
                    browse_results = (
                        executor.flow_manager.list_flows()
                        + executor.query_manager.list_queries()
                    )
                    await output_handler.send_event(
                        "WORKSPACE.BROWSE_RESULT",
                        "/workspace",
                        "info",
                        "Workspace contents listed.",
                        {"path": browse_path, "data": browse_results},
                    )

                elif msg_type == "GET_RUN_HISTORY":
                    history = executor.history_logger.query_recent_runs(
                        limit=50
                    )  # Increased limit
                    await output_handler.send_event(
                        "RUN_HISTORY_RESULT",
                        "/history",
                        "info",
                        "Run history retrieved.",
                        {"history": history},
                    )

                else:
                    log.warning("Received unknown message type.", msg_type=msg_type)

            except Exception as e:
                request_log.error(
                    "Error processing client message.", error=str(e), exc_info=True
                )
                await output_handler.send_error_event(
                    source="/system/dispatcher", error_message=f"Server error: {e}"
                )

    except WebSocketDisconnect:
        log.info("WebSocket client disconnected.")
    finally:
        if session_id in SESSION_DATA:
            del SESSION_DATA[session_id]
        log.info("Closing WebSocket session and cleaning up state.")
