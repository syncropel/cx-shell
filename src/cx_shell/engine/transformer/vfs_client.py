import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict

import structlog

logger = structlog.get_logger(__name__)


class AbstractVfsClient(ABC):
    """Defines the abstract contract for a client that can write to a VFS."""

    @abstractmethod
    async def write(self, path: str, content: bytes, context: Dict) -> str:
        """
        Writes content to the specified VFS path.
        Returns the canonical path of the written file.
        """
        raise NotImplementedError


class LocalVfsClient(AbstractVfsClient):
    """
    A concrete VFS client for standalone mode that writes directly to the
    local filesystem relative to a configurable root directory.
    """

    def __init__(self):
        # --- THIS IS THE FIX ---
        # 1. Read the shared VFS_ROOT from the environment.
        # 2. Use a common, safe default if it's not set.
        base_vfs_root_str = os.getenv("VFS_ROOT", "/tmp/cgi_data")

        # 3. Define the specific subdirectory for this service.
        self.root_path = Path(base_vfs_root_str).resolve() / "transformer"
        self.root_path.mkdir(parents=True, exist_ok=True)

        logger.info(
            "LocalVfsClient initialized.",
            service_root_path=str(self.root_path),
            source_variable="VFS_ROOT" if "VFS_ROOT" in os.environ else "default",
        )
        # --- END FIX ---

    async def write(self, path: str, content: bytes, context: Dict) -> str:
        log = logger.bind(vfs_path=path)

        if path.startswith("vfs://"):
            raise ValueError(
                f"LocalVfsClient cannot write to '{path}'. Only 'file://' or local paths are supported."
            )

        file_path_str = path.removeprefix("file://")

        try:
            # Treat the path from the script as relative to our configured service root.
            relative_path = file_path_str.lstrip("./").lstrip("/")
            target_path = self.root_path / relative_path

            log.info("local_vfs.writing_file", target_path=str(target_path))
            target_path.parent.mkdir(parents=True, exist_ok=True)
            target_path.write_bytes(content)

            final_path = f"file://{target_path}"
            log.info("local_vfs.write_success", final_path=final_path)
            return final_path
        except Exception as e:
            log.error("local_vfs.write_failed", error=str(e), exc_info=True)
            raise IOError(f"Failed to write to local file '{path}': {e}") from e
