import hashlib
from pathlib import Path
import json

import structlog

from ..utils import CX_HOME
from ..engine.connector.utils import safe_serialize

logger = structlog.get_logger(__name__)
CACHE_DIR = CX_HOME / "cache"


class CacheManager:
    """
    Manages the content-addressable storage (CAS) for the Data Fabric.

    This service is responsible for writing data to the cache based on its
    content hash, and providing a way to read it back.
    """

    def __init__(self):
        """Initializes the manager and ensures the cache directory exists."""
        self.cache_root = CACHE_DIR
        self.cache_root.mkdir(exist_ok=True, parents=True)

    def write(self, content: bytes) -> str:
        """
        Writes raw byte content to the cache.

        Args:
            content: The raw bytes of the artifact to be stored.

        Returns:
            The content hash identifier (e.g., "sha256:a1b2c3d4...").
        """
        hasher = hashlib.sha256()
        hasher.update(content)
        content_hash = hasher.hexdigest()

        hash_id = f"sha256:{content_hash}"

        # Create a subdirectory based on the first two characters of the hash
        # to prevent having too many files in one directory.
        cache_subdir = self.cache_root / content_hash[:2]
        cache_subdir.mkdir(exist_ok=True)

        file_path = cache_subdir / content_hash[2:]

        # This operation is idempotent. If the file exists, we don't need to write it again.
        if not file_path.exists():
            file_path.write_bytes(content)
            logger.debug(
                "cache.write.new_object", content_hash=hash_id, path=str(file_path)
            )
        else:
            logger.debug("cache.write.object_exists", content_hash=hash_id)

        return hash_id

    def write_json(self, data: any) -> str:
        """
        A convenience method to serialize a Python object to JSON
        and write it to the cache.
        """
        # Use safe_serialize to handle complex types like datetime
        json_string = json.dumps(safe_serialize(data), sort_keys=True)
        return self.write(json_string.encode("utf-8"))

    def get_path(self, content_hash_id: str) -> Path:
        """
        Resolves a content hash ID to its physical path on disk.

        Args:
            content_hash_id: The hash identifier (e.g., "sha256:a1b2c3d4...").

        Returns:
            A Path object pointing to the cached file.

        Raises:
            FileNotFoundError: If the object does not exist in the cache.
            ValueError: If the hash ID format is invalid.
        """
        if not content_hash_id or ":" not in content_hash_id:
            raise ValueError(f"Invalid content hash ID format: {content_hash_id}")

        algo, hash_val = content_hash_id.split(":", 1)
        if algo != "sha256":
            raise ValueError(f"Unsupported hash algorithm: {algo}")

        path = self.cache_root / hash_val[:2] / hash_val[2:]
        if not path.exists():
            raise FileNotFoundError(
                f"Cache object not found for hash: {content_hash_id}"
            )

        return path

    def read_bytes(self, content_hash_id: str) -> bytes:
        """Reads the raw bytes of a cached object."""
        path = self.get_path(content_hash_id)
        return path.read_bytes()
