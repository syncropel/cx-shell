import os
from pathlib import Path
from datetime import datetime
from typing import Dict, Any

import structlog

from .base import BaseCacheProvider

logger = structlog.get_logger(__name__)


class FilesystemCacheProvider(BaseCacheProvider):
    """Caches raw API responses to the local filesystem."""

    def __init__(self, config: Dict[str, Any]):
        # --- THIS IS THE FIX ---
        # 1. Read the shared VFS_ROOT from the environment to build a default path.
        base_vfs_root_str = os.getenv("VFS_ROOT", "/tmp/cgi_data")
        default_cache_root = Path(base_vfs_root_str).resolve() / "connector"

        # 2. Allow the script's 'cache_config' to override this default.
        #    This provides flexibility for specific scripts if needed.
        self.root_path = Path(config.get("root_path", default_cache_root)).resolve()
        # --- END FIX ---

        self.root_path.mkdir(parents=True, exist_ok=True)
        logger.info(
            "FilesystemCacheProvider initialized.", root_path=str(self.root_path)
        )

    async def write(self, content: bytes, metadata: Dict) -> str:
        connection_name = metadata.get("connection", {}).name.replace(" ", "_").lower()
        vfs_path_parts = metadata.get("vfs_path", "").strip("/").split("/")
        timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H%M%SZ")

        # Construct a structured, human-readable path
        # e.g., /path/to/vfs_root/connector/spotify/my-saved-tracks/2025-08-27T103000Z.json
        target_dir = self.root_path / connection_name / Path(*vfs_path_parts)
        target_dir.mkdir(parents=True, exist_ok=True)

        # For now, we assume JSON content from REST APIs
        file_path = target_dir / f"{timestamp}.json"

        with open(file_path, "wb") as f:
            f.write(content)

        logger.info("Successfully wrote to filesystem cache.", path=str(file_path))
        return f"file://{file_path}"
