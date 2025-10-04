from pathlib import Path
import re
from typing import List, Dict, Any, Tuple

import structlog
from .workspace_manager import WorkspaceManager
from ..engine.context import RunContext
from cx_core_schemas.connector_script import (
    ConnectorScript,
    ConnectorStep,
    RunSqlQueryAction,
)

logger = structlog.get_logger(__name__)


class QueryManager:
    """Handles logic for listing and running .sql files from the multi-rooted workspace."""

    def __init__(self, workspace_manager: WorkspaceManager):
        self.workspace_manager = workspace_manager

    def _get_search_paths(self) -> List[Tuple[str, Path]]:
        """
        Defines the prioritized search paths for queries by querying the WorkspaceManager.
        Returns a list of (namespace, path_object) tuples.
        """
        search_paths = []
        all_roots = self.workspace_manager.get_roots()

        for root_path in all_roots:
            namespace = "system" if ".cx" in str(root_path) else root_path.name
            query_dir = root_path / "queries"
            search_paths.append((namespace, query_dir))

        return search_paths

    def list_queries(self) -> List[Dict[str, str]]:
        """Lists all available queries from all registered workspace roots."""
        queries_data = []
        found_names = set()
        desc_pattern = re.compile(r"^\s*--\s*Description:\s*(.*)", re.IGNORECASE)

        for namespace, search_path in self._get_search_paths():
            if not search_path.is_dir():
                continue

            for q_file in sorted(search_path.glob("*.sql")):
                query_name = q_file.stem
                namespaced_id = f"{namespace}/{query_name}"
                if namespaced_id in found_names:
                    continue
                found_names.add(namespaced_id)

                description = "No description."
                try:
                    with open(q_file, "r") as f:
                        first_line = f.readline()
                        match = desc_pattern.match(first_line)
                        if match:
                            description = match.group(1).strip()
                except Exception:
                    description = "[red]Error reading file[/red]"

                queries_data.append(
                    {
                        "Name": namespaced_id,
                        "Description": description,
                        "Source": namespace,
                    }
                )
        return queries_data

    def _find_query(self, name: str) -> Path:
        """Finds a query by its potentially namespaced name across all workspace roots."""
        if "/" in name:
            namespace, query_name = name.split("/", 1)
            for ns, search_path in self._get_search_paths():
                if ns == namespace:
                    query_path = search_path / f"{query_name}.sql"
                    if query_path.exists():
                        return query_path
        else:
            for _, search_path in self._get_search_paths():
                query_path = search_path / f"{name}.sql"
                if query_path.exists():
                    return query_path

        raise FileNotFoundError(
            f"Query '{name}' not found in any registered workspace root."
        )

    async def run_query(
        self,
        run_context: "RunContext",
        named_args: Dict[str, Any],
    ) -> Any:
        """Executes a query by name, finding it in the multi-rooted workspace."""
        logger.debug("query_manager.run_query.received", named_args=named_args)

        name = named_args.pop("name", None)
        on_alias = named_args.pop("on", None)
        params = named_args.get("params", {})

        if not name or not on_alias:
            raise ValueError(
                "`query run` requires both '--name <query_name>' and '--on <connection_alias>' arguments."
            )

        query_file = self._find_query(name)
        logger.info("query_manager.run_query.resolved", query_path=str(query_file))

        if on_alias not in run_context.session.connections:
            raise ValueError(f"Connection alias '{on_alias}' is not active.")

        connection_source = run_context.session.connections[on_alias]
        query_content = query_file.read_text()

        step = ConnectorStep(
            id=f"interactive_query_{name}",
            name=f"Interactive query {name}",
            connection_source=connection_source,
            run=RunSqlQueryAction(
                action="run_sql_query", query=query_content, parameters=params
            ),
        )
        script = ConnectorScript(name=f"Interactive script for {name}", steps=[step])

        # Create a new context for this ad-hoc script run
        query_run_context = RunContext(
            services=run_context.services,
            session=run_context.session,
            script_input=params,  # Pass SQL params as script_input
        )

        results = await run_context.services.script_engine.run_script_model(
            context=query_run_context, script_data=script.model_dump()
        )
        return results.get(step.name)
