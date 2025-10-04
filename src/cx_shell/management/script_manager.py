import json
from pathlib import Path
from typing import Dict, Any, List, Tuple

import structlog
from .workspace_manager import WorkspaceManager
from ..engine.context import RunContext

from ..engine.connector.utils import safe_serialize
from cx_core_schemas.connector_script import (
    ConnectorScript,
    ConnectorStep,
    RunPythonScriptAction,
)

logger = structlog.get_logger(__name__)


class ScriptManager:
    """Handles logic for listing and running .py files from the multi-rooted workspace."""

    def __init__(self, workspace_manager: WorkspaceManager):
        self.workspace_manager = workspace_manager

    def _get_search_paths(self) -> List[Tuple[str, Path]]:
        """Defines the prioritized search paths for scripts."""
        search_paths = []
        all_roots = self.workspace_manager.get_roots()

        for root_path in all_roots:
            namespace = "system" if ".cx" in str(root_path) else root_path.name
            script_dir = root_path / "scripts"
            search_paths.append((namespace, script_dir))

        return search_paths

    def list_scripts(self) -> List[Dict[str, str]]:
        """Lists all available scripts from all registered workspace roots."""
        scripts_data = []
        found_names = set()

        for namespace, search_path in self._get_search_paths():
            if not search_path.is_dir():
                continue

            for script_file in sorted(search_path.glob("*.py")):
                script_name = script_file.stem
                namespaced_id = f"{namespace}/{script_name}"
                if namespaced_id in found_names:
                    continue
                found_names.add(namespaced_id)

                scripts_data.append(
                    {
                        "Name": namespaced_id,
                        "Description": "Python script.",
                        "Source": namespace,
                    }
                )
        return scripts_data

    def _find_script(self, name: str) -> Path:
        """Finds a script by its potentially namespaced name across all workspace roots."""
        if "/" in name:
            namespace, script_name = name.split("/", 1)
            for ns, search_path in self._get_search_paths():
                if ns == namespace:
                    script_path = search_path / f"{script_name}.py"
                    if script_path.exists():
                        return script_path
        else:
            for _, search_path in self._get_search_paths():
                script_path = search_path / f"{name}.py"
                if script_path.exists():
                    return script_path

        raise FileNotFoundError(
            f"Script '{name}' not found in any registered workspace root."
        )

    async def run_script(
        self,
        run_context: "RunContext",
        named_args: Dict[str, Any],
    ) -> Any:
        """Executes a Python script by name, finding it in the multi-rooted workspace."""
        logger.debug(
            "script_manager.run_script.received",
            named_args=named_args,
            has_piped_input=run_context.piped_input is not None,
        )

        name = named_args.pop("name", None)
        params = named_args.get("params", {})

        if not name:
            raise ValueError("`script run` requires a '--name <script_name>' argument.")

        script_file = self._find_script(name)
        logger.info("script_manager.run_script.resolved", script_path=str(script_file))

        script_args_list = [json.dumps(params)] if params else []
        serializable_input = safe_serialize(run_context.piped_input)

        step = ConnectorStep(
            id=f"run_script_{name.replace('/', '_')}",
            name=f"Run Python Script: {name}",
            connection_source="user:system_python_sandbox",
            run=RunPythonScriptAction(
                action="run_python_script",
                script_path=str(script_file),
                input_data_json=json.dumps(serializable_input),
                args=script_args_list,
            ),
        )
        script = ConnectorScript(
            name=f"Interactive Script run for {name}.py", steps=[step]
        )

        # Create a new context for this ad-hoc script run
        script_run_context = RunContext(
            services=run_context.services,
            session=run_context.session,
            script_input=params,
            piped_input=run_context.piped_input,
            current_flow_path=script_file,  # Set the script itself as the path context
        )

        results = await run_context.services.script_engine.run_script_model(
            context=script_run_context, script_data=script.model_dump()
        )
        return results.get(step.name)
