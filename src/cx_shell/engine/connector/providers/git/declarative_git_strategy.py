# [NEW FILE] ~/repositories/connector-logic/src/connector_logic/providers/git/declarative_git_strategy.py

import asyncio
from contextlib import asynccontextmanager
from typing import Any, Dict, List, TYPE_CHECKING
from datetime import datetime, timezone

import structlog

from .base_git_strategy import BaseGitStrategy
from ..rest.declarative_strategy import DeclarativeRestStrategy
from cx_core_schemas.vfs import VfsFileContentResponse, VfsNodeMetadata
from .....data.agent_schemas import DryRunResult


if TYPE_CHECKING:
    from cx_core_schemas.connection import Connection

logger = structlog.get_logger(__name__)


class DeclarativeGitStrategy(BaseGitStrategy):
    """
    A single, reusable, blueprint-driven strategy for any Git-based service
    that provides a REST API for metadata browsing.

    It acts as a hybrid orchestrator:
    1. It uses an internal DeclarativeRestStrategy to handle API calls for
       listing repositories and branches.
    2. It uses its BaseGitStrategy capabilities to handle cloning, pulling,
       and reading file content from a local cache.
    """

    strategy_key = "git-declarative"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # Composition: The Git strategy contains a REST strategy to handle API calls.
        self.rest_strategy = DeclarativeRestStrategy(**kwargs)
        logger.info("DeclarativeGitStrategy initialized.")

    @asynccontextmanager
    async def get_client(self, connection: "Connection", secrets: Dict[str, Any]):
        """Delegates client creation entirely to the internal REST strategy."""
        async with self.rest_strategy.get_client(connection, secrets) as client:
            yield client

    async def test_connection(
        self, connection: "Connection", secrets: Dict[str, Any]
    ) -> bool:
        """Delegates the connection test to the internal REST strategy's test."""
        return await self.rest_strategy.test_connection(connection, secrets)

    async def browse_path(
        self,
        path_parts: List[str],
        connection: "Connection",
        secrets: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """
        Orchestrates browsing between the REST API for metadata and the local
        Git clone for file listings.
        """
        log = logger.bind(
            connection_id=connection.id, vfs_path=f"/{'/'.join(path_parts)}"
        )
        if not connection.catalog or not connection.catalog.browse_config:
            raise ValueError("Browse config is missing from the ApiCatalog blueprint.")

        # --- The "Hand-off" Point ---
        # The blueprint defines at what path depth we switch from API calls to local git.
        git_handoff_depth = connection.catalog.browse_config.get("git_handoff_depth", 3)

        # If we are "shallower" than the hand-off point, use the REST API.
        if len(path_parts) < git_handoff_depth:
            log.info("Browsing via Metadata API (REST).")
            return await self.rest_strategy.browse_path(path_parts, connection, secrets)

        # If we are at or "deeper" than the hand-off point, use the local Git clone.
        else:
            log.info("Browsing via local Git clone.")
            git_config = connection.catalog.browse_config.get("git_config", {})
            clone_url_template = git_config.get("clone_url_template")
            if not clone_url_template:
                raise ValueError("`clone_url_template` is missing from git_config.")

            # Path parts are expected to be [owner, repo, branch, ...sub_path]
            owner, repo_name, branch_name = (
                path_parts[0],
                path_parts[1],
                path_parts[2],
            )
            sub_path = "/".join(path_parts[3:])

            render_context = {
                "owner": owner,
                "repo_name": repo_name,
                "secrets": secrets,
                "details": connection.details,
            }
            repo_url = self.rest_strategy._render_template(
                clone_url_template, render_context
            )
            auth_header = {}  # Simplified for now; could be configured

            repo = await self._clone_or_pull_repo(repo_url, auth_header)

            def list_files_in_git():
                repo.git.checkout(branch_name)
                repo.git.reset("--hard", f"origin/{branch_name}")
                tree = repo.tree(sub_path) if sub_path else repo.tree()
                results = []
                for blob in tree.blobs:
                    results.append(
                        {
                            "name": blob.name,
                            "path": f"{'/'.join(path_parts)}/{blob.name}",
                            "type": "file",
                            "icon": "IconFileCode",
                        }
                    )
                for subtree in tree.trees:
                    results.append(
                        {
                            "name": subtree.name,
                            "path": f"{'/'.join(path_parts)}/{subtree.name}/",
                            "type": "folder",
                            "icon": "IconFolder",
                        }
                    )
                results.sort(key=lambda x: (x["type"], x["name"]))
                return results

            return await asyncio.to_thread(list_files_in_git)

    async def get_content(
        self,
        path_parts: List[str],
        connection: "Connection",
        secrets: Dict[str, Any],
    ) -> "VfsFileContentResponse":
        """
        Getting content always uses the local Git clone, as the API is not
        typically used for retrieving raw file content.
        """
        if not connection.catalog or not connection.catalog.browse_config:
            raise ValueError("Browse config is missing from the ApiCatalog blueprint.")

        git_config = connection.catalog.browse_config.get("git_config", {})
        clone_url_template = git_config.get("clone_url_template")

        if len(path_parts) < 4:
            raise FileNotFoundError("Invalid file path for Git content retrieval.")

        owner, repo_name, branch_name = path_parts[0], path_parts[1], path_parts[2]
        file_path_in_repo = "/".join(path_parts[3:])
        full_vfs_path = f"vfs://connections/{connection.id}/{'/'.join(path_parts)}"

        render_context = {
            "owner": owner,
            "repo_name": repo_name,
            "secrets": secrets,
            "details": connection.details,
        }
        repo_url = self.rest_strategy._render_template(
            clone_url_template, render_context
        )

        repo = await self._clone_or_pull_repo(repo_url, {})
        file_content_str = await self._get_file_content_from_repo(
            repo, branch_name, file_path_in_repo
        )

        now = datetime.now(timezone.utc)
        commit_hash = repo.head.commit.hexsha
        metadata = VfsNodeMetadata(can_write=True, is_versioned=True, etag=commit_hash)

        return VfsFileContentResponse(
            path=full_vfs_path,
            content=file_content_str,
            mime_type="text/plain",
            last_modified=now,
            size=len(file_content_str.encode("utf-8")),
            metadata=metadata,
        )

    async def dry_run(
        self,
        connection: "Connection",
        secrets: Dict[str, Any],
        action_params: Dict[str, Any],
    ) -> "DryRunResult":
        """A dry run for the Git strategy checks for the clone URL template in the blueprint."""
        if (
            not connection.catalog
            or not connection.catalog.browse_config
            or "clone_url_template"
            not in connection.catalog.browse_config.get("git_config", {})
        ):
            return DryRunResult(
                indicates_failure=True,
                message="Dry run failed: Blueprint is missing `git_config.clone_url_template`.",
            )
        return DryRunResult(
            indicates_failure=False,
            message="Dry run successful: Blueprint contains the necessary Git configuration.",
        )
