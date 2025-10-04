# ~/repositories/cx-shell/src/cx_shell/interactive/context_engine.py

import sqlite3
import importlib.util
import yaml
import structlog
from pathlib import Path
from typing import List, Dict, Any, Optional

from pydantic import BaseModel
from fastembed import TextEmbedding
import lancedb
from lancedb.pydantic import LanceModel, Vector

from ..engine.connector.config import CX_HOME, ConnectionResolver
from ..interactive.session import SessionState
from ..data.agent_schemas import AgentBeliefs

# --- Constants ---
logger = structlog.get_logger(__name__)
CONTEXT_DIR = CX_HOME / "context"
HISTORY_DB_FILE = CONTEXT_DIR / "history.sqlite"
VECTOR_STORE_DIR = CONTEXT_DIR / "vector.lance"
EMBEDDING_CACHE_DIR = CONTEXT_DIR / "embedding_models"


# --- LanceDB Schema ---
class AssetSchema(LanceModel):
    text: str  # The content to be searched (e.g., description)
    source: str  # The path or ID of the asset (e.g., "flows/my-flow.yaml")
    type: str  # The type of asset (e.g., "flow", "query", "application")
    vector: Vector(384)  # Vector size for BAAI/bge-small-en-v1.5


class DynamicContextEngine:
    """
    Constructs intelligent, minimal, and relevant context for the CARE agents.
    It combines vector search (for semantic similarity) and structured queries
    (for precise history) to provide a rich understanding of the user's workspace.
    """

    def __init__(self, state: SessionState):
        """
        Initializes the context engine. This constructor is lightweight and
        performs NO expensive I/O or model loading.
        """
        CONTEXT_DIR.mkdir(parents=True, exist_ok=True)
        self.state = state
        self.resolver = ConnectionResolver()
        self._schema_cache: Dict[str, Dict[str, Any]] = {}

        # --- LAZY LOADING IMPLEMENTATION ---
        # Initialize expensive components to None. They will be loaded on first access
        # via the @property methods defined below.
        self._embedding_model: Optional[TextEmbedding] = None
        self._asset_table: Optional[Any] = None
        # --- END LAZY LOADING IMPLEMENTATION ---

    @property
    def embedding_model(self) -> Optional[TextEmbedding]:
        """Lazily loads the embedding model on first access."""
        if self._embedding_model is None:
            try:
                logger.debug(
                    "context_engine.lazy_load.begin", component="fastembed_model"
                )
                self._embedding_model = TextEmbedding(
                    model_name="BAAI/bge-small-en-v1.5",
                    cache_dir=str(EMBEDDING_CACHE_DIR),
                )
                logger.info("ContextEngine: fastembed model loaded successfully.")
            except Exception as e:
                logger.warn(
                    "ContextEngine: Failed to initialize fastembed model.", error=str(e)
                )
                # Set to a "failed" state to prevent repeated load attempts
                self._embedding_model = None
        return self._embedding_model

    @property
    def asset_table(self) -> Optional[Any]:
        """Lazily loads the LanceDB vector table on first access."""
        if self._asset_table is None:
            try:
                logger.debug(
                    "context_engine.lazy_load.begin", component="lancedb_table"
                )
                db = lancedb.connect(VECTOR_STORE_DIR)
                if "assets" in db.table_names():
                    self._asset_table = db.open_table("assets")
                else:
                    self._asset_table = db.create_table("assets", schema=AssetSchema)
                logger.info("ContextEngine: LanceDB vector table loaded successfully.")
            except Exception as e:
                logger.warn(
                    "ContextEngine: Failed to initialize LanceDB table.", error=str(e)
                )
                # Set to a "failed" state
                self._asset_table = None
        return self._asset_table

    def index_workspace_assets(self):
        """Scans the user's workspace (~/.cx) and indexes all assets in the vector store."""
        # Accessing the properties will trigger the lazy loading if needed.
        if not self.asset_table or not self.embedding_model:
            logger.warn(
                "Cannot index workspace assets: RAG components not initialized or failed to load."
            )
            return

        assets_to_index = []
        asset_dirs = {
            "flow": CX_HOME / "flows",
            "query": CX_HOME / "queries",
            "script": CX_HOME / "scripts",
        }

        for asset_type, asset_dir in asset_dirs.items():
            if not asset_dir.is_dir():
                continue
            for asset_file in asset_dir.iterdir():
                try:
                    content = asset_file.read_text()
                    description = f"A {asset_type} named '{asset_file.stem}'."
                    if asset_file.suffix in [".yaml", ".yml"]:
                        data = yaml.safe_load(content)
                        description = data.get("description", description)
                    assets_to_index.append(
                        {
                            "text": description,
                            "source": f"{asset_type}s/{asset_file.name}",
                            "type": asset_type,
                        }
                    )
                except Exception:
                    continue

        if not assets_to_index:
            return

        self.asset_table.add(assets_to_index)
        logger.info("Workspace asset indexing complete.", count=len(assets_to_index))

    def get_strategic_context(self, goal: str, beliefs: AgentBeliefs) -> str:
        """Builds a high-level context for the PlannerAgent using hybrid retrieval."""
        context_parts = ["## Current Situation", f'- User\'s goal: "{goal}"']
        if beliefs.plan:
            context_parts.append("- The current plan is:")
            for i, step in enumerate(beliefs.plan):
                status_icon = {"completed": "✓", "failed": "✗"}.get(step.status, "…")
                context_parts.append(f"  {status_icon} {i + 1}. {step.step}")

        # 1. Retrieve similar assets from Vector Store (will trigger lazy loading)
        if self.asset_table and self.embedding_model:
            try:
                goal_vector = list(self.embedding_model.embed([goal]))[0].tolist()
                results = self.asset_table.search(goal_vector).limit(3).to_list()
                if results:
                    context_parts.append("\n## Relevant Assets in Your Workspace")
                    for res in results:
                        context_parts.append(
                            f"- Asset: `{res['source']}` (Description: {res['text']})"
                        )
            except Exception as e:
                logger.warn("Vector search for strategic context failed.", error=str(e))

        # 2. Retrieve recent commands from SQLite
        try:
            with sqlite3.connect(HISTORY_DB_FILE) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT content FROM events WHERE event_type = 'COMMAND' AND status = 'SUCCESS' ORDER BY timestamp DESC LIMIT 3"
                )
                rows = cursor.fetchall()
                if rows:
                    context_parts.append("\n## Recent Successful Commands")
                    for row in rows:
                        context_parts.append(f"- `{row[0]}`")
        except Exception as e:
            logger.warn("SQLite search for strategic context failed.", error=str(e))

        return "\n".join(context_parts)

    def get_tactical_context(self, connection_alias: str) -> List[Dict[str, Any]]:
        """Builds a detailed, structured context (tool schemas) for the ToolSpecialistAgent."""
        if connection_alias not in self.state.connections:
            raise ValueError(f"Connection alias '{connection_alias}' is not active.")

        source = self.state.connections[connection_alias]
        try:
            conn_model, _ = self.resolver.resolve(source)
            if not conn_model.catalog or not conn_model.catalog.browse_config:
                return []
            action_templates = conn_model.catalog.browse_config.get(
                "action_templates", {}
            )
            tools = []
            for action_name, config in action_templates.items():
                func_def = {
                    "name": f"{connection_alias}.{action_name}",
                    "description": config.get(
                        "description", f"Execute the {action_name} action."
                    ),
                    "parameters": {"type": "object", "properties": {}, "required": []},
                }
                model_name_str = config.get("parameters_model")
                if model_name_str and conn_model.catalog.schemas_module_path:
                    schema = self._get_schema_for_model(
                        conn_model.catalog.schemas_module_path, model_name_str
                    )
                    if schema:
                        func_def["parameters"]["properties"] = schema.get(
                            "properties", {}
                        )
                        func_def["parameters"]["required"] = schema.get("required", [])
                tools.append({"type": "function", "function": func_def})
            return tools
        except Exception as e:
            logger.error(
                "Failed to generate tactical context.",
                alias=connection_alias,
                error=str(e),
            )
            return []

    def _get_schema_for_model(
        self, schemas_py_file: str, model_path_str: str
    ) -> Optional[Dict[str, Any]]:
        """Dynamically loads a Pydantic model and converts it to a JSON Schema, with caching."""
        cache_key = f"{schemas_py_file}:{model_path_str}"
        if cache_key in self._schema_cache:
            return self._schema_cache[cache_key]

        if not model_path_str.startswith("schemas."):
            return None
        class_name = model_path_str.split(".", 1)[1]
        try:
            module_name = f"blueprint_schemas_{Path(schemas_py_file).stem}_{class_name}"
            spec = importlib.util.spec_from_file_location(module_name, schemas_py_file)
            if not spec or not spec.loader:
                return None
            schemas_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(schemas_module)
            ParamModel = getattr(schemas_module, class_name)
            if issubclass(ParamModel, BaseModel):
                schema = ParamModel.model_json_schema()
                self._schema_cache[cache_key] = schema
                return schema
        except Exception as e:
            logger.warn(
                "Failed to load or convert Pydantic model to schema.",
                model=model_path_str,
                error=str(e),
            )
        return None
