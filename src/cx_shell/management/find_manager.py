from pathlib import Path
from typing import List, Dict, Optional

import lancedb
import structlog
from fastembed import TextEmbedding

from ..utils import CX_HOME

logger = structlog.get_logger(__name__)
CONTEXT_DIR = CX_HOME / "context"
VECTOR_STORE_DIR = CONTEXT_DIR / "vector.lance"
EMBEDDING_CACHE_DIR = CONTEXT_DIR / "embedding_models"


class FindManager:
    """Handles logic for searching the VFS index."""

    def __init__(self, cx_home_path: Optional[Path] = None):
        _cx_home = cx_home_path or CX_HOME
        # Update the module-level constants
        global CONTEXT_DIR, VECTOR_STORE_DIR, EMBEDDING_CACHE_DIR
        CONTEXT_DIR = _cx_home / "context"
        VECTOR_STORE_DIR = CONTEXT_DIR / "vector.lance"
        EMBEDDING_CACHE_DIR = CONTEXT_DIR / "embedding_models"

        try:
            CONTEXT_DIR.mkdir(exist_ok=True, parents=True)
            self._db = lancedb.connect(VECTOR_STORE_DIR)
            self._embedding_model = TextEmbedding(
                model_name="BAAI/bge-small-en-v1.5", cache_dir=str(EMBEDDING_CACHE_DIR)
            )
            if "assets" in self._db.table_names():
                self._tbl = self._db.open_table("assets")
            else:
                self._tbl = None
        except Exception as e:
            logger.error("find_manager.init.failed", error=str(e))
            self._tbl = None

    def find_assets(
        self,
        query: Optional[str] = None,
        asset_type: Optional[str] = None,
        limit: int = 10,
    ) -> List[Dict[str, str]]:
        """
        Performs a hybrid search (vector + keyword) on the asset index.
        """
        if self._tbl is None:
            raise RuntimeError(
                "VFS Index not found or failed to load. Run `cx workspace index --rebuild`."
            )

        logger.debug(
            "find_manager.search.begin", query=query, type=asset_type, limit=limit
        )

        search_query = self._tbl.search(query if query else None)

        if asset_type:
            search_query = search_query.where(f"type = '{asset_type}'", prefilter=True)

        results = search_query.limit(limit).to_df()

        if results.empty:
            return []

        # Format for display
        return [
            {
                "Type": row["type"],
                "Source": row["source"],
                "Relevance": f"{row['_distance']:.4f}" if "_distance" in row else "N/A",
                "Content": row["text"],
            }
            for _, row in results.iterrows()
        ]
