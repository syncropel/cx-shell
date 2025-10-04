import urllib.parse
from contextlib import asynccontextmanager
from typing import Any, Dict, List, TYPE_CHECKING

import structlog
from .....data.agent_schemas import DryRunResult


from cx_core_schemas.connection import Connection
from ..base import BaseConnectorStrategy

if TYPE_CHECKING:
    from cx_core_schemas.vfs import VfsFileContentResponse
    from ..fs.declarative_fs_strategy import DeclarativeFilesystemStrategy
    from ..rest.declarative_strategy import DeclarativeRestStrategy

logger = structlog.get_logger(__name__)


class SmartFetcherStrategy(BaseConnectorStrategy):
    """
    A system-level meta-strategy for reading content from a generic source.

    It intelligently determines whether a given path is a remote URL or a local
    filesystem path and delegates the read operation to the appropriate underlying
    strategy (REST or Filesystem).
    """

    strategy_key = "internal-smart_fetcher"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.fs_strategy: "DeclarativeFilesystemStrategy" = kwargs["fs_strategy"]
        self.rest_strategy: "DeclarativeRestStrategy" = kwargs["rest_strategy"]
        logger.info("SmartFetcherStrategy initialized, ready to delegate.")

    async def get_content(
        self, path_parts: List[str], connection: "Connection", secrets: Dict[str, Any]
    ) -> "VfsFileContentResponse":
        """
        Reads content from a source path, delegating to the correct strategy.
        This method is the primary purpose of this strategy.
        """
        if not path_parts:
            raise ValueError("SmartFetcherStrategy requires a path or URL.")

        path = path_parts[0]
        log = logger.bind(source_path=path)

        try:
            if path.startswith("http://") or path.startswith("https://"):
                log.info("Source is a URL. Delegating to REST strategy.")
                parsed_url = urllib.parse.urlparse(path)
                base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
                endpoint_path = parsed_url.path
                # Create a minimal, but fully valid, Connection object that satisfies
                # the Pydantic schema by including all required fields.
                dummy_conn = Connection(
                    id="temp:rest_fetcher",
                    name="Temp REST Fetcher",
                    api_catalog_id="temp:rest_fetcher_catalog",
                    auth_method_type="none",
                    catalog={
                        "id": "temp:rest_fetcher_catalog",
                        "name": "Temp REST Fetcher Catalog",
                        "browse_config": {"base_url_template": base_url},
                    },
                )
                return await self.rest_strategy.get_content(
                    [endpoint_path], dummy_conn, {}
                )
            else:
                log.info("Source is a file path. Delegating to Filesystem strategy.")

                dummy_conn = Connection(
                    id="temp:fs_fetcher",
                    name="Temp FS Fetcher",
                    api_catalog_id="temp:fs_fetcher_catalog",
                    auth_method_type="none",
                    catalog={
                        "id": "temp:fs_fetcher_catalog",
                        "name": "Temp FS Catalog",
                    },
                )
                # Pass the single, full path as the only element in path_parts
                return await self.fs_strategy.get_content([path], dummy_conn, {})
        except Exception as e:
            log.error("smart_fetcher.failed", error=str(e), exc_info=True)
            raise IOError(
                f"Smart fetcher failed to read content from '{path}': {e}"
            ) from e

    # --- Fulfilling the Rest of the Contract ---
    # The following methods are required by the abstract base class but are not
    # applicable to this specialized meta-strategy. We provide a default
    # implementation that raises NotImplementedError.

    async def test_connection(
        self, connection: "Connection", secrets: Dict[str, Any]
    ) -> bool:
        """This meta-strategy does not require a connection test."""
        return True

    @asynccontextmanager
    async def get_client(self, connection: "Connection", secrets: Dict[str, Any]):
        """No client is needed for this meta-strategy."""
        yield None

    async def browse_path(
        self, path_parts: List[str], connection: "Connection", secrets: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Browsing is not supported by the smart fetcher; use a specific strategy."""
        raise NotImplementedError(
            "browse_path is not supported by the SmartFetcherStrategy."
        )

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
