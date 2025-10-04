import json
from pathlib import Path
from typing import Optional

import lancedb
import structlog
from fastembed import TextEmbedding
from lancedb.pydantic import LanceModel, Vector
from rich.progress import track

from ..utils import CX_HOME

logger = structlog.get_logger(__name__)
CONTEXT_DIR = CX_HOME / "context"
RUNS_DIR = CX_HOME / "runs"
VECTOR_STORE_DIR = CONTEXT_DIR / "vector.lance"
EMBEDDING_CACHE_DIR = CONTEXT_DIR / "embedding_models"


# Define the schema for our searchable assets in the vector store
class AssetSchema(LanceModel):
    text: str  # The content to be searched (e.g., description, name)
    source: str  # The unique identifier (e.g., vfs://... path or flow name)
    type: str  # e.g., "flow", "query", "script", "artifact"
    vector: Vector(384)  # Vector size for BAAI/bge-small-en-v1.5


class IndexManager:
    """Manages the creation and maintenance of the VFS search index."""

    def __init__(self, cx_home_path: Optional[Path] = None):
        _cx_home = cx_home_path or CX_HOME
        # Update the module-level constants
        global CONTEXT_DIR, VECTOR_STORE_DIR, EMBEDDING_CACHE_DIR, RUNS_DIR
        CONTEXT_DIR = _cx_home / "context"
        RUNS_DIR = _cx_home / "runs"
        VECTOR_STORE_DIR = CONTEXT_DIR / "vector.lance"
        EMBEDDING_CACHE_DIR = CONTEXT_DIR / "embedding_models"

        CONTEXT_DIR.mkdir(exist_ok=True, parents=True)
        try:
            self._db = lancedb.connect(VECTOR_STORE_DIR)
            self._embedding_model = TextEmbedding(
                model_name="BAAI/bge-small-en-v1.5", cache_dir=str(EMBEDDING_CACHE_DIR)
            )
            if "assets" in self._db.table_names():
                self._tbl = self._db.open_table("assets")
            else:
                self._tbl = None
        except Exception as e:
            logger.error("index_manager.init.failed", error=str(e))
            self._tbl = None

    def rebuild_index(self):
        """
        Performs a full rebuild of the search index by scanning all RunManifests.
        This is a destructive and rebuilding operation.
        """
        logger.info("index_manager.rebuild.begin")

        # Drop the old table if it exists for a clean rebuild
        if "assets" in self._db.table_names():
            self._db.drop_table("assets")

        tbl = self._db.create_table("assets", schema=AssetSchema)

        manifest_files = list(RUNS_DIR.glob("**/manifest.json"))
        if not manifest_files:
            logger.info("index_manager.rebuild.no_manifests_found")
            return

        docs_to_index = []
        for manifest_file in track(manifest_files, description="Indexing runs..."):
            try:
                manifest = json.loads(manifest_file.read_text())

                # Index the flow itself
                docs_to_index.append(
                    {
                        "text": f"Flow named {manifest.get('flow_id')}. Parameters: {json.dumps(manifest.get('parameters', {}))}",
                        "source": f"vfs://runs/{manifest.get('run_id')}",
                        "type": "flow_run",
                    }
                )

                # Index each artifact from the run
                for name, artifact in manifest.get("artifacts", {}).items():
                    docs_to_index.append(
                        {
                            "text": f"Artifact named {name}",
                            "source": f"vfs://runs/{manifest.get('run_id')}/{name}",
                            "type": "artifact",
                        }
                    )
            except Exception as e:
                logger.warn(
                    "index_manager.rebuild.parse_error",
                    file=str(manifest_file),
                    error=str(e),
                )

        if not docs_to_index:
            logger.info("index_manager.rebuild.no_docs_to_index")
            return

        logger.info("index_manager.rebuild.embedding_docs", count=len(docs_to_index))
        tbl.add(docs_to_index)
        logger.info("index_manager.rebuild.complete")
