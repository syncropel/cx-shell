import os
from abc import ABC, abstractmethod
from pathlib import Path

import structlog

logger = structlog.get_logger(__name__)


class AbstractVfsReader(ABC):
    """Defines a simple contract for reading file bytes from a VFS."""

    @abstractmethod
    def read_bytes(self, path: str) -> bytes:
        """Reads the raw bytes of a file at the given path."""
        raise NotImplementedError


class LocalVfsReader(AbstractVfsReader):
    """
    Reads files from the local filesystem. It correctly resolves full canonical
    paths and paths relative to the VFS_ROOT.
    """

    def __init__(self):
        self.vfs_root = Path(os.getenv("VFS_ROOT", "/tmp/cgi_data")).resolve()
        logger.info("LocalVfsReader initialized.", vfs_root=str(self.vfs_root))

    def read_bytes(self, path: str) -> bytes:
        """
        Reads a file's bytes. Handles both absolute canonical paths (file://...)
        and paths relative to a service's subdirectory within VFS_ROOT.
        """
        # The path we receive will often be the full canonical path from transformer-logic.
        file_path = Path(path.removeprefix("file://"))

        # If the path is not absolute, assume it's relative to the transformer output dir.
        if not file_path.is_absolute():
            file_path = (self.vfs_root / "transformer" / path.lstrip("./")).resolve()

        if not file_path.exists():
            logger.error("vfs_reader.file_not_found", path=str(file_path))
            raise FileNotFoundError(f"VFS reader could not find file: {file_path}")

        logger.info("vfs_reader.reading_file", path=str(file_path))
        return file_path.read_bytes()
