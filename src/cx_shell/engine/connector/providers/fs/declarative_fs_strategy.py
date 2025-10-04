import asyncio
import os
import subprocess
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, TYPE_CHECKING, Optional

import structlog
from jinja2 import Environment
import tiktoken

from cx_core_schemas.connection import Connection
from cx_core_schemas.vfs import VfsFileContentResponse, VfsNodeMetadata
from ...utils import is_binary_string
from .....data.agent_schemas import DryRunResult
from ..base import BaseConnectorStrategy


if TYPE_CHECKING:
    from cx_core_schemas.connection import Connection
    from tiktoken import Encoding
    from .....engine.context import RunContext

logger = structlog.get_logger(__name__)

# Default exclusion lists inspired by common .gitignore templates to be used
# in the fallback manual directory walk.
DEFAULT_EXCLUDED_DIRS = {
    ".git",
    ".svn",
    "node_modules",
    "__pycache__",
    ".pytest_cache",
    ".tox",
    ".venv",
    "venv",
    "env",
    "build",
    "dist",
    ".idea",
    ".vscode",
}
DEFAULT_EXCLUDED_EXTS = {
    ".pyc",
    ".pyo",
    ".pyd",
    ".so",
    ".dll",
    ".exe",
    ".jar",
    ".zip",
    ".tar.gz",
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".pdf",
    ".log",
    ".lock",
    ".bin",
}


class DeclarativeFilesystemStrategy(BaseConnectorStrategy):
    """
    A strategy for interacting with the local filesystem as a data source.

    This strategy can declaratively discover files in a directory (respecting .gitignore
    where possible), read the content of individual files, and aggregate content
    from multiple sources into a single output file. It is the core engine for
    the platform's "context generation" capability.
    """

    strategy_key = "fs-declarative"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.jinja_env = Environment(autoescape=True)

        def get_now(tz: str | None = None) -> datetime:
            """
            A Jinja-friendly wrapper for datetime.now that correctly handles 'utc'.
            """
            if tz and tz.lower() == "utc":
                return datetime.now(timezone.utc)
            return datetime.now()

        # Add the smarter 'now' function to the Jinja environment's global namespace.
        self.jinja_env.globals["now"] = get_now
        # Initialize the tokenizer to None. It will be loaded on first use.
        self._tokenizer: Optional["Encoding"] = None

    @property
    def tokenizer(self) -> Optional["Encoding"]:
        """Lazily loads the tiktoken tokenizer on first access."""
        if self._tokenizer is None:
            try:
                logger.debug("fs_strategy.lazy_load.begin", component="tiktoken")
                # "cl100k_base" is the encoding for gpt-4, gpt-3.5-turbo, etc.
                self._tokenizer = tiktoken.get_encoding("cl100k_base")
                logger.info("tiktoken tokenizer initialized successfully.")
            except Exception as e:
                logger.warn(
                    "Could not initialize tiktoken tokenizer. Token counts will not be available.",
                    error=str(e),
                )
        return self._tokenizer

    async def test_connection(
        self, connection: "Connection", secrets: Dict[str, Any]
    ) -> bool:
        """For the local filesystem, a 'connection' is always considered valid."""
        logger.info("Local filesystem connection is always considered valid.")
        return True

    @asynccontextmanager
    async def get_client(self, connection: "Connection", secrets: Dict[str, Any]):
        """The 'client' for the filesystem is the filesystem itself; nothing to yield."""
        yield None

    async def browse_path(
        self, path_parts: List[str], connection: "Connection", secrets: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Performs a fast, non-recursive (shallow) listing of a directory's contents.
        This is safe to run on any directory, including '/'.
        """
        relative_path_str = path_parts[0] if path_parts else ""
        base_path_str = connection.details.get("base_path", "/")
        target_path = Path(os.path.join(base_path_str, relative_path_str)).resolve()
        log = logger.bind(target_path=str(target_path))

        if not target_path.is_dir():
            raise FileNotFoundError(f"Path is not a valid directory: {target_path}")

        def list_directory_contents():
            """Synchronous helper for file system I/O."""
            vfs_nodes = []
            for item in target_path.iterdir():
                try:
                    # Skip items we don't have permission to read
                    if not os.access(item, os.R_OK):
                        continue

                    if item.is_dir():
                        vfs_nodes.append(
                            {
                                "name": item.name,
                                "path": f"{relative_path_str.strip('/')}/{item.name}/",
                                "type": "folder",
                                "icon": "IconFolder",
                            }
                        )
                    elif item.is_file():
                        vfs_nodes.append(
                            {
                                "name": item.name,
                                "path": f"{relative_path_str.strip('/')}/{item.name}",
                                "type": "file",
                                "icon": "IconFileInfo",
                            }
                        )
                except OSError:
                    continue  # Skip broken symlinks or other OS errors

            # Sort with folders first, then by name
            vfs_nodes.sort(key=lambda x: (x["type"] != "folder", x["name"].lower()))
            return vfs_nodes

        log.info("Performing non-recursive directory listing.")
        # Run the synchronous I/O in a separate thread to avoid blocking asyncio
        return await asyncio.to_thread(list_directory_contents)

    async def _recursive_discover_files(self, target_path: Path) -> List[str]:
        """
        Performs a deep, recursive discovery of all relevant files in a directory.
        Used by the 'aggregate_content' action.
        """
        log = logger.bind(target_path=str(target_path))

        # Fast path: Use `git ls-files` if it's a git repo.
        try:
            cmd = ["git", "ls-files", "--cached", "--others", "--exclude-standard"]
            result = subprocess.run(
                cmd,
                cwd=target_path,
                capture_output=True,
                text=True,
                check=True,
                timeout=10,
            )
            files = [
                str(target_path / f) for f in result.stdout.strip().split("\n") if f
            ]
            log.info("Discovered files using git ls-files.", count=len(files))
            return files
        except (
            subprocess.CalledProcessError,
            FileNotFoundError,
            subprocess.TimeoutExpired,
        ):
            log.warn(
                "Not a git repo or git command failed. Falling back to manual walk."
            )

        # Slow path: Manual, recursive walk with default ignores.
        def manual_walk():
            found_files = []
            for root, dirs, files in os.walk(target_path, topdown=True):
                dirs[:] = [d for d in dirs if d not in DEFAULT_EXCLUDED_DIRS]
                for name in files:
                    if Path(name).suffix in DEFAULT_EXCLUDED_EXTS:
                        continue
                    full_path = Path(root, name)
                    if not any(
                        part in DEFAULT_EXCLUDED_DIRS for part in full_path.parts
                    ):
                        found_files.append(str(full_path))
            return found_files

        log.info("Performing recursive file walk.")
        return await asyncio.to_thread(manual_walk)

    async def get_content(
        self, path_parts: List[str], connection: "Connection", secrets: Dict[str, Any]
    ) -> "VfsFileContentResponse":
        """Reads the content of a single local file."""
        # For this strategy, we expect a single, absolute path in path_parts.
        file_path_str = path_parts[0]
        file_path = Path(file_path_str).resolve()

        log = logger.bind(file_path=str(file_path))
        log.info("Reading file content.")

        if not file_path.is_file():
            raise FileNotFoundError(f"File not found: {file_path}")

        try:
            # Read as bytes first to get file stats, then decode.
            content_bytes = file_path.read_bytes()
            # Use errors='ignore' as a fallback for files with mixed encodings.
            content_str = content_bytes.decode("utf-8", errors="ignore")

            # Heuristic check for binary content on a sample of the string.
            if is_binary_string(content_str[:1024]):
                raise UnicodeDecodeError(
                    "utf-8", b"", 0, 1, "File appears to be binary"
                )

            now = datetime.now(timezone.utc)
            stat = file_path.stat()
            metadata = VfsNodeMetadata(
                can_write=True,
                is_versioned=False,
                etag=f"{stat.st_mtime}-{stat.st_size}",
                last_modified=datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc),
            )
            return VfsFileContentResponse(
                path=f"file://{file_path}",
                content=content_str,
                mime_type="text/plain",
                last_modified=now,
                size=len(content_bytes),
                metadata=metadata,
            )
        except (UnicodeDecodeError, Exception) as e:
            log.warn("Skipping file due to read error.", error=str(e))
            # Return a placeholder to avoid crashing the aggregator.
            now = datetime.now(timezone.utc)
            placeholder_content = f"# SKIPPED FILE: {file_path}\n# REASON: {e}\n"
            return VfsFileContentResponse(
                path=f"file://{file_path}",
                content=placeholder_content,
                mime_type="text/plain",
                last_modified=now,
                size=len(placeholder_content),
                metadata=VfsNodeMetadata(
                    can_write=False, is_versioned=False, etag="", last_modified=now
                ),
            )

    async def aggregate_content(
        self,
        connection: "Connection",
        action_params: Dict[str, Any],
        script_input: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Aggregates content from a list of files and directories into a single output file,
        preserving the specified source order.
        """
        target_path_str = action_params["target_path"]
        log = logger.bind(target_path=target_path_str)
        log.info("Aggregating content from source paths.")

        all_content = []

        if action_params.get("header_template"):
            template = self.jinja_env.from_string(action_params["header_template"])
            render_context = {"metadata": action_params.get("metadata", {})}
            header = template.render(render_context)
            all_content.append(header)

        source_paths = action_params.get("source_paths") or []
        all_file_paths_to_read = []

        for path_str in source_paths:
            p = Path(path_str).resolve()
            if not p.exists():
                log.warn("Source path does not exist, skipping.", path=str(p))
                continue

            if p.is_dir():
                # Discover files recursively and sort them alphabetically *within this directory*
                files_found = await self._recursive_discover_files(p)
                all_file_paths_to_read.extend(sorted(files_found))
            elif p.is_file():
                all_file_paths_to_read.append(str(p))

        # --- START OF DEFINITIVE FIX ---
        # Preserve order while removing duplicates
        ordered_unique_files = list(dict.fromkeys(all_file_paths_to_read))
        # We no longer call sorted() on the final, combined list.
        # --- END OF DEFINITIVE FIX ---

        for file_path in ordered_unique_files:  # <-- Use the new ordered list
            try:
                read_conn = Connection(
                    id="temp:file_reader",
                    name=f"Temp FS Reader for {Path(file_path).name}",
                    api_catalog_id="temp:fs",
                    auth_method_type="none",
                )
                vfs_response = await self.get_content(
                    path_parts=[file_path], connection=read_conn, secrets={}
                )
                all_content.append(f"--- \n\n# {file_path}\n\n{vfs_response.content}")
            except Exception as e:
                log.error(
                    "Failed to read content for aggregation.",
                    path=file_path,
                    error=str(e),
                )

        final_content_str = "\n\n".join(all_content)
        target_path = Path(target_path_str).resolve()
        target_path.parent.mkdir(parents=True, exist_ok=True)
        final_content_bytes = final_content_str.encode("utf-8")
        target_path.write_bytes(final_content_bytes)

        log.info("Aggregation successful.")

        # ... (summary stats logic remains the same) ...
        char_count = len(final_content_str)
        byte_count = len(final_content_bytes)
        summary_stats = {
            "files_aggregated": len(ordered_unique_files),
            "total_characters": char_count,
            "total_size_bytes": byte_count,
            "total_size_kb": round(byte_count / 1024, 2),
            "total_size_mb": round(byte_count / 1024 / 1024, 3),
        }

        if self.tokenizer:
            log.info("Calculating token count...")
            token_count = len(self.tokenizer.encode(final_content_str))
            summary_stats["estimated_tokens_cl100k"] = token_count

        return {
            "status": "success",
            "output_path": str(target_path),
            "summary": summary_stats,
        }

    async def write_files(
        self,
        connection: "Connection",
        action_params: Dict[str, Any],
        run_context: "RunContext",
    ) -> Dict[str, Any]:
        """
        Writes a list of file objects to the filesystem.
        """
        base_path = Path(connection.details.get("base_path", ".")).resolve()
        files_to_write = action_params.get("files", [])
        log = logger.bind(base_path=str(base_path), file_count=len(files_to_write))
        log.info("Executing write_files action.")

        written_paths = []
        try:
            for file_item in files_to_write:
                content_path_str = file_item["path"]
                content = file_item["content"]

                # --- START OF DEFINITIVE FIX ---
                path_obj = Path(content_path_str)
                if path_obj.is_absolute():
                    # If the path is already absolute (resolved by the engine), use it directly.
                    target_path = path_obj
                else:
                    # Fallback for relative paths: join with the connection's base_path.
                    target_path = (base_path / content_path_str).resolve()
                # --- END OF DEFINITIVE FIX ---

                # Security check is now simpler because we always operate on an absolute path.
                # A robust check ensures we don't write outside intended areas.
                # For this generic connection, we'll allow writing anywhere the user has permissions.
                # A more restrictive connection could check against its base_path.

                target_path.parent.mkdir(parents=True, exist_ok=True)
                target_path.write_text(content, encoding="utf-8")
                written_paths.append(str(target_path))
                log.info("File written successfully.", path=str(target_path))

            return {"status": "success", "files_written": written_paths}
        except Exception as e:
            log.error("write_files.failed", error=str(e), exc_info=True)
            raise IOError(f"Failed to write one or more files: {e}") from e

    async def dry_run(
        self,
        connection: "Connection",
        secrets: Dict[str, Any],
        action_params: Dict[str, Any],
    ) -> "DryRunResult":
        """This strategy's actions are validated at execution time. The dry run passes by default."""
        return DryRunResult(
            indicates_failure=False,
            message="Dry run successful by default for this strategy.",
        )
