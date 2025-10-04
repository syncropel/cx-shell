from pathlib import Path
import re
from typing import List, Dict, Any, Tuple, TYPE_CHECKING

import structlog
import yaml
from .workspace_manager import WorkspaceManager

if TYPE_CHECKING:
    from ..engine.context import RunContext

logger = structlog.get_logger(__name__)


class FlowManager:
    """
    Handles logic for listing and running all runnable assets (.flow.yaml, .cx.md)
    from the multi-rooted workspace.
    """

    def __init__(self, workspace_manager: WorkspaceManager):
        """
        Initializes the FlowManager.

        Args:
            workspace_manager: An instance of WorkspaceManager, which is the single
                               source of truth for registered project directories.
        """
        self.workspace_manager = workspace_manager
        # Define the standard subdirectories to search for runnable assets.
        self.asset_subdirs = ["flows", "notebooks"]
        # Define the file patterns to look for.
        self.asset_glob_patterns = ["*.flow.yaml", "*.flow.yml", "*.cx.md"]

    def _get_search_paths(self) -> List[Tuple[str, Path]]:
        """
        Defines the prioritized search paths for all runnable assets by querying the WorkspaceManager.
        This now includes multiple subdirectories per root.
        Returns a list of (namespace, path_object) tuples.
        """
        search_paths = []
        all_roots = self.workspace_manager.get_roots()

        # --- START OF STRATEGIC DEBUG LOG ---
        logger.debug(
            "flow_manager.get_search_paths.discovered_roots",
            roots=[str(p) for p in all_roots],
        )
        # --- END OF STRATEGIC DEBUG LOG ---

        for root_path in all_roots:
            namespace = "system" if ".cx" in str(root_path) else root_path.name
            for subdir in self.asset_subdirs:
                asset_dir = root_path / subdir
                if asset_dir.is_dir():
                    search_paths.append((namespace, asset_dir))

        return search_paths

    def list_flows(self) -> List[Dict[str, str]]:
        """
        Lists all available runnable assets (.flow.yaml and .cx.md) from all
        registered workspace roots.
        """
        assets_data = []
        found_names = set()

        for namespace, search_path in self._get_search_paths():
            for pattern in self.asset_glob_patterns:
                for asset_file in sorted(search_path.glob(pattern)):
                    # Normalize the name by finding the first dot and taking everything before it.
                    asset_name = asset_file.name.split(".")[0]
                    namespaced_id = f"{namespace}/{asset_name}"

                    if namespaced_id in found_names:
                        continue
                    found_names.add(namespaced_id)

                    description = "No description available."
                    try:
                        content = asset_file.read_text(
                            encoding="utf-8", errors="ignore"
                        )
                        # Handle YAML and Markdown front matter by looking for --- delimiters
                        front_matter_match = re.search(
                            r"^\s*---(.*?)---", content, re.DOTALL
                        )
                        if front_matter_match:
                            data = yaml.safe_load(front_matter_match.group(1))
                            if isinstance(data, dict):
                                description = data.get(
                                    "description", "No description available."
                                )
                    except Exception as e:
                        logger.warning(
                            "flow_manager.list.read_error",
                            file=str(asset_file),
                            error=str(e),
                        )
                        description = "[red]Error reading file metadata[/red]"

                    assets_data.append(
                        {
                            "Name": namespaced_id,
                            "Description": description,
                            "Source": namespace,
                        }
                    )

        # Sort the final combined list alphabetically by name
        return sorted(assets_data, key=lambda x: x["Name"])

    def _find_flow(self, name: str) -> Path:
        """
        Finds a runnable asset by its potentially namespaced name across all workspace roots
        and standard subdirectories (`flows/`, `notebooks/`).
        """
        search_name = name
        namespace_filter = None
        if "/" in name:
            namespace_filter, search_name = name.split("/", 1)

        for namespace, search_path in self._get_search_paths():
            if namespace_filter and namespace != namespace_filter:
                continue

            # Check all possible file extensions for the given asset name.
            for pattern in self.asset_glob_patterns:
                file_name = pattern.replace("*", search_name)
                asset_path = search_path / file_name
                if asset_path.exists():
                    logger.debug(
                        "flow_manager.find.success", found_path=str(asset_path)
                    )
                    return asset_path

        # If we get here, the asset was not found.
        raise FileNotFoundError(
            f"Runnable asset '{name}' not found in any registered workspace root under the directories: {self.asset_subdirs}"
        )

    async def run_flow(
        self,
        run_context: "RunContext",
        named_args: Dict[str, Any],
    ) -> Any:
        """
        Finds and executes a runnable asset by its namespaced name, correctly
        parsing parameters and orchestrating the ScriptEngine.
        """
        logger.debug(
            "flow_manager.run_flow.entry",
            incoming_named_args=named_args,
            has_piped_input=run_context.piped_input is not None,
        )

        args_copy = named_args.copy()
        name = args_copy.pop("name", None)
        if not name:
            raise ValueError("`flow run` requires a 'name' argument.")

        logger.debug("flow_manager.run_flow.args_parsed", flow_name_resolved=name)

        params = args_copy
        logger.debug("flow_manager.run_flow.params_isolated", flow_parameters=params)

        no_cache_val = str(params.pop("no_cache", "false")).lower()
        no_cache = no_cache_val in ("true", "yes", "1")

        flow_path = self._find_flow(name)
        logger.info(
            "flow_manager.run_flow.path_resolved",
            flow_name=name,
            resolved_path=str(flow_path),
        )

        # Create a new, scoped RunContext for this specific flow execution.
        from ..engine.context import (
            RunContext,
        )  # Local import to avoid circular dependency

        flow_run_context = RunContext(
            services=run_context.services,
            session=run_context.session,
            script_input=params,
            piped_input=run_context.piped_input,
            current_flow_path=flow_path,
        )

        logger.debug(
            "flow_manager.run_flow.context_created",
            context_script_path=str(flow_run_context.current_flow_path),
            context_script_input=flow_run_context.script_input,
        )

        logger.info(
            "flow_manager.run_flow.delegating_to_engine",
            flow_name=name,
            no_cache=no_cache,
        )
        # Delegate execution to the ScriptEngine with the fully prepared context.
        return await run_context.services.script_engine.run_script(
            context=flow_run_context,
            no_cache=no_cache,
        )
