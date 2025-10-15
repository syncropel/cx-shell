# /src/cx_shell/management/flow_manager.py

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
        self.asset_subdirs = ["flows", "notebooks"]
        self.asset_glob_patterns = ["*.flow.yaml", "*.flow.yml", "*.cx.md"]

    def _get_search_paths(self) -> List[Tuple[str, Path]]:
        """
        Defines the prioritized search paths for all runnable assets by querying the
        WorkspaceManager for all registered roots and their installed dependencies.
        This logic is now stateless and independent of the current working directory.
        """
        search_paths = []
        all_roots = self.workspace_manager.get_roots()

        # 1. Iterate through every registered workspace root.
        for root_path in all_roots:
            # A. Add the root's own asset directories.
            # The namespace is the directory name, or 'system' for the special ~/.cx root.
            namespace = "system" if ".cx" in str(root_path) else root_path.name
            for subdir in self.asset_subdirs:
                asset_dir = root_path / subdir
                if asset_dir.is_dir():
                    search_paths.append((namespace, asset_dir))

            # B. For EACH root, also check if it's a project with installed app dependencies.
            project_assets_dir = root_path / ".cx" / "store"
            if project_assets_dir.is_dir():
                # Scan for installed apps (e.g., system/toolkit)
                for namespace_dir in project_assets_dir.iterdir():
                    if not namespace_dir.is_dir():
                        continue
                    for app_dir in namespace_dir.iterdir():
                        # The namespace is the app's full ID (e.g., system/toolkit)
                        app_namespace = f"{namespace_dir.name}/{app_dir.name}"
                        # Scan for asset types within the app (flows, notebooks)
                        for subdir in self.asset_subdirs:
                            asset_dir_in_app = app_dir / subdir
                            if asset_dir_in_app.is_dir():
                                search_paths.append((app_namespace, asset_dir_in_app))

        # Remove duplicate paths while preserving order and log the final list.
        unique_paths = list(dict.fromkeys(search_paths))
        logger.debug(
            "flow_manager.search_paths.final",
            paths=[(p[0], str(p[1])) for p in unique_paths],
        )
        return unique_paths

    def list_flows(self) -> List[Dict[str, str]]:
        """
        Lists all available runnable assets from all discovered search paths.
        """
        assets_data = []
        found_names = set()
        desc_pattern = re.compile(r"^\s*---(.*?)---", re.DOTALL)

        for namespace, search_path in self._get_search_paths():
            for pattern in self.asset_glob_patterns:
                for asset_file in sorted(search_path.glob(pattern)):
                    asset_name = asset_file.stem
                    # The logical ID is the namespace from the search path + the asset name
                    namespaced_id = f"{namespace}/{asset_name}"

                    if namespaced_id in found_names:
                        continue
                    found_names.add(namespaced_id)

                    description = "No description available."
                    try:
                        content = asset_file.read_text(
                            encoding="utf-8", errors="ignore"
                        )
                        match = desc_pattern.search(content)
                        if match:
                            data = yaml.safe_load(match.group(1))
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
        return sorted(assets_data, key=lambda x: x["Name"])

    def _find_flow(self, name: str) -> Path:
        """
        Finds a runnable asset by its unique, multi-part namespaced Logical ID.
        """
        parts = name.split("/")
        if len(parts) > 1:
            search_name = parts[-1]
            namespace_filter = "/".join(parts[:-1])
        else:
            search_name = name
            namespace_filter = None

        for namespace, search_path in self._get_search_paths():
            if namespace_filter and namespace != namespace_filter:
                continue

            for pattern in self.asset_glob_patterns:
                file_name = pattern.replace("*", search_name)
                asset_path = search_path / file_name
                if asset_path.exists():
                    logger.debug(
                        "flow_manager.find.success",
                        searched_name=name,
                        found_path=str(asset_path),
                    )
                    return asset_path

        raise FileNotFoundError(
            f"Runnable asset '{name}' not found in any registered workspace."
        )

    async def run_flow(
        self, run_context: "RunContext", named_args: Dict[str, Any]
    ) -> Any:
        """
        Finds and executes a runnable asset by name, preparing a scoped RunContext
        and delegating to the ScriptEngine.
        """
        args_copy = named_args.copy()
        name = args_copy.pop("name", None)
        if not name:
            raise ValueError("`flow run` requires a '--name <flow_name>' argument.")

        params = args_copy
        no_cache_val = str(params.pop("no_cache", "false")).lower()
        no_cache = no_cache_val in ("true", "yes", "1")

        flow_path = self._find_flow(name)
        logger.info(
            "flow_manager.run_flow.path_resolved",
            flow_name=name,
            resolved_path=str(flow_path),
        )

        from ..engine.context import RunContext

        flow_run_context = RunContext(
            services=run_context.services,
            session=run_context.session,
            script_input=params,
            piped_input=run_context.piped_input,
            current_flow_path=flow_path,
        )

        logger.info(
            "flow_manager.run_flow.delegating_to_engine",
            flow_name=name,
            no_cache=no_cache,
        )
        return await run_context.services.script_engine.run_script(
            context=flow_run_context, no_cache=no_cache
        )
