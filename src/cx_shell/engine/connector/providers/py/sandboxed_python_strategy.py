import json
import subprocess
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
from .....environments.venv_provider import VenvEnvironment


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
        connection: Optional[Connection],
        action_params: Dict[str, Any],
        run_context: "RunContext",
    ) -> Dict[str, Any]:
        """
        Executes a Python script by determining the project context and
        dispatching to the appropriate environment provider.
        """
        script_content = action_params.get("script_content")
        script_path_str = action_params.get("script_path")
        script_args = action_params.get("args", [])
        input_data_str = action_params.get("input_data_json", "{}")
        connection_source = action_params.get("connection_source")

        temp_script_path: Optional[Path] = None
        # Use a temporary file path for the dynamic config to ensure cleanup
        temp_config_path: Optional[Path] = None
        final_script_path_obj: Path
        log = logger  # Initialize logger

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
            # --- Script Resolution (Unchanged) ---
            if script_content:
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
                final_script_path_obj = run_context.resolve_path_in_context(
                    script_path_str
                )
                log = logger.bind(script_path=str(final_script_path_obj))
            else:
                raise ValueError(
                    "Python script execution requires either 'script_path' or 'script_content'."
                )

            # --- START OF NEW, REFACTORED LOGIC ---

            log.info("Preparing sandboxed Python execution.")

            project_root = (
                run_context.services.workspace_manager.find_project_root_for_file(
                    run_context.current_flow_path or final_script_path_obj
                )
            )
            if not project_root:
                raise IOError(
                    f"Could not determine project root for script: {final_script_path_obj}."
                )

            # 1. Instantiate the Environment Provider for this project.
            #    (Currently hardcoded to our legacy provider; will be dynamic later).
            env_provider = VenvEnvironment(project_root)

            # 2. Prepare environment variables for injection.
            injected_env_vars = {}
            project_config_path = project_root / "project.config.json"
            if project_config_path.exists():
                config_template = project_config_path.read_text()
                jinja_env = Environment()
                template = jinja_env.from_string(config_template)
                rendered_config_str = template.render(run_context.model_dump())

                # Create a temporary file for the rendered config
                with tempfile.NamedTemporaryFile(
                    mode="w+", delete=False, suffix=".json", encoding="utf-8"
                ) as tmp_config:
                    tmp_config.write(rendered_config_str)
                    temp_config_path = Path(tmp_config.name)

                injected_env_vars["CX_APP_CONFIG_PATH"] = str(temp_config_path)
                log.info("python_sandbox.config.injected", path=str(temp_config_path))

            if connection_source:
                resolver = run_context.services.resolver
                connector_service = run_context.services.connector_service
                conn_obj, secrets = await resolver.resolve(connection_source)
                strategy = connector_service._get_strategy_for_connection_model(
                    conn_obj
                )
                if hasattr(strategy, "_get_connection_url"):
                    conn_url = strategy._get_connection_url(conn_obj, secrets)
                    injected_env_vars["CX_DB_CONNECTION_STRING"] = conn_url
                    log.info(
                        "python_sandbox.connection.injected", source=connection_source
                    )

            # 3. Construct the command to be executed.
            command_to_run = [str(final_script_path_obj)] + script_args

            # 4. Delegate execution to the provider. This is the core change.
            process = env_provider.execute(
                command=command_to_run,
                stdin_data=input_data_str,
                env_vars=injected_env_vars,
            )

            # --- END OF NEW, REFACTORED LOGIC ---

            log.info(
                "python_sandbox.execution_complete",
                stdout=process.stdout.strip(),
                stderr=process.stderr.strip(),
                return_code=process.returncode,
            )

            # --- Result Processing (Unchanged) ---
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
            # --- Cleanup (Now includes the temp config file) ---
            if temp_config_path and temp_config_path.exists():
                try:
                    temp_config_path.unlink()
                except OSError:
                    pass

            if temp_script_path and temp_script_path.exists():
                try:
                    temp_script_path.unlink()
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
