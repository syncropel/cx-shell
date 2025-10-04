import json
import os
import subprocess
import sys
from contextlib import asynccontextmanager
from pathlib import Path
import tempfile
from typing import Any, Dict, List, TYPE_CHECKING, Optional

import structlog
from jinja2 import Environment

from ..base import BaseConnectorStrategy
from .....data.agent_schemas import DryRunResult
from .....utils import resolve_path
from cx_core_schemas.connection import Connection
from cx_core_schemas.vfs import VfsFileContentResponse

if TYPE_CHECKING:
    from .....engine.context import RunContext

logger = structlog.get_logger(__name__)


class SandboxedPythonStrategy(BaseConnectorStrategy):
    strategy_key = "python-sandboxed"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    async def test_connection(
        self, connection: Connection, secrets: Dict[str, Any]
    ) -> bool:
        return True

    @asynccontextmanager
    async def get_client(self, connection: Connection, secrets: Dict[str, Any]):
        yield None

    async def run_python_script(
        self,
        connection: Optional[Connection],  # Connection can be None for this strategy
        action_params: Dict[str, Any],
        run_context: "RunContext",
    ) -> Dict[str, Any]:
        """
        Executes a Python script in a subprocess, automatically injecting a
        'data' variable from piped input for notebook blocks.
        """
        script_content = action_params.get("script_content")
        script_path_str = action_params.get("script_path")
        script_args = action_params.get("args", [])
        input_data_str = action_params.get("input_data_json", "{}")
        connection_source = action_params.get("connection_source")

        temp_script_path: Optional[Path] = None
        final_script_path_obj: Path

        # This boilerplate code is prepended to scripts run from notebook blocks.
        # It reads from stdin, parses the JSON, and creates the 'data' variable
        # that the user's code expects, providing a seamless experience.
        BOILERPLATE = """
import sys
import json
try:
    _stdin_content = sys.stdin.read()
    if _stdin_content:
        data = json.loads(_stdin_content)
    else:
        data = None
except (json.JSONDecodeError, TypeError):
    data = _stdin_content # Fallback to raw string if not valid JSON
# --- User code starts below ---
"""

        try:
            if script_content:
                # For notebook blocks, prepend the boilerplate to the user's code.
                full_script_content = BOILERPLATE + script_content
                temp_dir = (
                    run_context.current_flow_path.parent
                    if run_context.current_flow_path
                    else None
                )
                with tempfile.NamedTemporaryFile(
                    mode="w+",
                    delete=False,
                    suffix=".py",
                    dir=temp_dir,
                    encoding="utf-8",
                ) as tmp_script:
                    tmp_script.write(full_script_content)
                    temp_script_path = Path(tmp_script.name)
                final_script_path_obj = temp_script_path
                log = logger.bind(script_source="<inline_content>")
            elif script_path_str:
                # For file-based scripts from flows, the user is expected to handle stdin themselves.
                final_script_path_obj = run_context.resolve_path_in_context(
                    script_path_str
                )
                log = logger.bind(script_path=str(final_script_path_obj))
            else:
                raise ValueError(
                    "Python script execution requires either 'script_path' or 'script_content'."
                )

            log.info("Preparing sandboxed Python execution.")

            python_executable = sys.executable
            env_vars = os.environ.copy()
            CANONICAL_CONFIG_PATH = Path("/tmp/cx_runtime_config.json")

            # Determine project root for correct PYTHONPATH
            project_root = (
                run_context.services.workspace_manager.find_project_root_for_file(
                    run_context.current_flow_path or final_script_path_obj
                )
            )
            if not project_root:
                raise IOError(
                    f"Could not determine project root for script: {final_script_path_obj}."
                )

            log.info("python_sandbox.project.detected", root=str(project_root))
            env_vars["PYTHONPATH"] = str(project_root)

            # Inject dynamic app config if it exists
            project_config_path = project_root / "project.config.json"
            if project_config_path.exists():
                config_template = project_config_path.read_text()
                jinja_env = Environment()
                template = jinja_env.from_string(config_template)
                rendered_config_str = template.render(run_context.model_dump())
                CANONICAL_CONFIG_PATH.write_text(rendered_config_str)
                log.info(
                    "python_sandbox.config.injected", path=str(CANONICAL_CONFIG_PATH)
                )

            # Inject database connection string if provided
            if connection_source:
                resolver = run_context.services.resolver
                connector_service = run_context.services.connector_service
                conn_obj, secrets = await resolver.resolve(connection_source)
                strategy = connector_service._get_strategy_for_connection_model(
                    conn_obj
                )
                if hasattr(strategy, "_get_connection_url"):
                    conn_url = strategy._get_connection_url(conn_obj, secrets)
                    env_vars["CX_DB_CONNECTION_STRING"] = conn_url
                    log.info(
                        "python_sandbox.connection.injected", source=connection_source
                    )

            command_to_run = [
                python_executable,
                str(final_script_path_obj),
                *script_args,
            ]
            log.info("Executing Python script.", command=" ".join(command_to_run))

            process = subprocess.run(
                command_to_run,
                input=input_data_str,  # Pass the JSON string to stdin
                capture_output=True,
                text=True,
                check=True,
                timeout=600,  # 10-minute timeout
                env=env_vars,
                cwd=project_root,
            )
            log.info(
                "python_sandbox.execution_complete",
                stdout=process.stdout.strip(),
                stderr=process.stderr.strip(),
                return_code=process.returncode,
            )

            log.debug(
                "python_sandbox.raw_stdout_received", stdout_capture=process.stdout
            )

            # Now, check for errors after logging
            if process.returncode != 0:
                raise subprocess.CalledProcessError(
                    process.returncode, process.args, process.stdout, process.stderr
                )

            if not process.stdout.strip():
                return {
                    "status": "success",
                    "message": "Script completed with no output.",
                }

            return json.loads(process.stdout)

        except subprocess.CalledProcessError as e:
            log.error("Python script failed.", stderr=e.stderr.strip())
            raise IOError(
                f"Execution of script '{final_script_path_obj.name}' failed with exit code {e.returncode}:\n--- STDERR ---\n{e.stderr}"
            )

        finally:
            # --- Critical Cleanup Step for temporary files ---
            if CANONICAL_CONFIG_PATH.exists():
                try:
                    CANONICAL_CONFIG_PATH.unlink()
                except OSError:
                    pass

            if temp_script_path and temp_script_path.exists():
                try:
                    temp_script_path.unlink()
                    log.debug(
                        "Temporary script file cleaned up.", path=str(temp_script_path)
                    )
                except OSError as e:
                    log.warning(
                        "Failed to clean up temporary script file.",
                        path=str(temp_script_path),
                        error=str(e),
                    )

    async def browse_path(
        self, path_parts: List[str], connection: Connection, secrets: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        raise NotImplementedError

    async def get_content(
        self, path_parts: List[str], connection: Connection, secrets: Dict[str, Any]
    ) -> "VfsFileContentResponse":
        raise NotImplementedError

    async def dry_run(
        self,
        connection: Connection,
        secrets: Dict[str, Any],
        action_params: Dict[str, Any],
    ) -> "DryRunResult":
        try:
            script_path_str = action_params["script_path"]
            script_path_obj = resolve_path(script_path_str)
            if not script_path_obj.exists():
                return DryRunResult(
                    indicates_failure=True,
                    message=f"Dry run failed: Script not found at resolved path: {script_path_obj}",
                )
            return DryRunResult(
                indicates_failure=False,
                message=f"Dry run successful: Script '{script_path_obj.name}' exists.",
            )
        except Exception as e:
            return DryRunResult(
                indicates_failure=True,
                message=f"Dry run failed during path resolution: {e}",
            )
