# [REPLACE] ~/repositories/connector-logic/src/connector_logic/providers/git/base_git_strategy.py

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Dict, List, TYPE_CHECKING

import structlog
from git import Repo

from ..base import BaseConnectorStrategy

if TYPE_CHECKING:
    from cx_core_schemas.connection import Connection

logger = structlog.get_logger(__name__)

# The root directory on the server where all git checkouts will be cached.
# This should be a persistent volume in a production Kubernetes environment.
DEFAULT_GIT_CACHE_ROOT = Path("/tmp/cgi_git_cache")


class BaseGitStrategy(BaseConnectorStrategy):
    """
    An abstract base for Git-based providers (GitHub, Gitea, etc.).

    This class contains all the shared logic for cloning, pulling, and
    interacting with a Git repository on the local filesystem. Concrete
    subclasses are only responsible for the initial API calls to discover
    repository clone URLs.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.git_cache_root = Path(kwargs.get("git_cache_root", DEFAULT_GIT_CACHE_ROOT))
        self.git_cache_root.mkdir(parents=True, exist_ok=True)
        logger.info(
            "BaseGitStrategy initialized.", git_cache_root=str(self.git_cache_root)
        )

    def _get_repo_cache_path(self, repo_url: str) -> Path:
        """Creates a unique, safe directory name for a repo based on its URL."""
        sanitized = repo_url.split("://")[-1].replace("/", "_").replace(":", "_")
        return self.git_cache_root / sanitized

    async def _clone_or_pull_repo(
        self, repo_url: str, auth_header: Dict[str, str]
    ) -> Repo:
        """
        Idempotently clones a repository. If it already exists locally,
        it performs a `git pull` to ensure it's up to date.
        """
        repo_path = self._get_repo_cache_path(repo_url)
        log = logger.bind(strategy_key=self.strategy_key, repo_path=str(repo_path))

        def git_operation():
            if repo_path.exists():
                log.info("Repo exists locally. Fetching updates...")
                repo = Repo(repo_path)
                with repo.git.custom_environment(
                    GIT_TERMINAL_PROMPT="0", **auth_header
                ):
                    repo.remotes.origin.fetch()
                log.info("Fetch complete.")
                return repo
            else:
                log.info("Cloning new repo.")
                repo = Repo.clone_from(
                    repo_url, repo_path, env={"GIT_TERMINAL_PROMPT": "0", **auth_header}
                )
                log.info("Clone complete.")
                return repo

        return await asyncio.to_thread(git_operation)

    async def _get_file_content_from_repo(
        self, repo: Repo, branch_name: str, file_path_in_repo: str
    ) -> str:
        """
        Checks out a specific branch/commit and reads the content of a file.
        """

        def read_operation():
            repo.git.checkout(branch_name)
            repo.git.reset("--hard", f"origin/{branch_name}")
            tree = repo.tree()
            file_blob = tree[file_path_in_repo]
            return file_blob.data_stream.read().decode("utf-8")

        return await asyncio.to_thread(read_operation)

    # --- Implementations for the abstract methods from BaseConnectorStrategy ---

    async def test_connection(
        self, connection: "Connection", secrets: Dict[str, Any]
    ) -> bool:
        raise NotImplementedError(
            "Concrete Git strategies must implement their own API-based connection test."
        )

    @asynccontextmanager
    async def get_client(self, connection: "Connection", secrets: Dict[str, Any]):
        yield
        raise NotImplementedError(
            "Concrete Git strategies must implement get_client for API calls."
        )

    async def browse_path(
        self,
        path_parts: List[str],
        connection: "Connection",
        secrets: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        raise NotImplementedError("Concrete Git strategies must implement browse_path.")
