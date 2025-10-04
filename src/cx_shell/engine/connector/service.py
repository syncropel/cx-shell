from contextlib import asynccontextmanager
from typing import Any, Dict, TYPE_CHECKING

import structlog
from cx_core_schemas.connection import Connection

# --- Local & Shared Imports ---
from .providers.base import BaseConnectorStrategy
from .providers.git.declarative_git_strategy import DeclarativeGitStrategy
from .providers.oauth.declarative_oauth_strategy import DeclarativeOauthStrategy
from .providers.rest.api_key_strategy import ApiKeyStrategy
from .providers.rest.declarative_strategy import DeclarativeRestStrategy
from .providers.rest.webhook_strategy import WebhookStrategy
from .providers.sql.mssql_strategy import MssqlStrategy
from .providers.sql.trino_strategy import TrinoStrategy
from .providers.fs.declarative_fs_strategy import DeclarativeFilesystemStrategy
from .providers.py.sandboxed_python_strategy import SandboxedPythonStrategy
from .providers.internal.smart_fetcher_strategy import SmartFetcherStrategy
from .providers.browser.strategy import DeclarativeBrowserStrategy
from ...state import APP_STATE
from .vfs_reader import LocalVfsReader

if TYPE_CHECKING:
    from ...engine.context import RunContext
    from ...interactive.executor import CommandExecutor


logger = structlog.get_logger(__name__)


class ConnectorService:
    """
    The main Connector Service. Manages I/O strategies and provides
    programmatic and script-based access to external services.
    This service is now completely stateless.
    """

    def __init__(self, executor: "CommandExecutor"):
        self.strategies: Dict[str, BaseConnectorStrategy] = {}
        self._register_strategies()
        self.executor = executor  # <-- ADD THIS LINE

        logger.info(
            "ConnectorService initialized.",
            strategy_count=len(self.strategies),
        )

    def _register_strategies(self):
        """
        Discovers and registers all concrete strategy classes, correctly
        handling dependency injection for meta-strategies.
        """
        vfs_reader = LocalVfsReader()
        strategy_instances = {}

        # Stage 1: Explicitly define all base strategy classes to be instantiated.
        # This removes any ambiguity about which strategies should be loaded.
        base_strategy_classes = [
            DeclarativeRestStrategy,
            ApiKeyStrategy,
            WebhookStrategy,
            MssqlStrategy,
            DeclarativeOauthStrategy,
            DeclarativeGitStrategy,
            TrinoStrategy,
            DeclarativeFilesystemStrategy,
            SandboxedPythonStrategy,  # Ensured it is here
            DeclarativeBrowserStrategy,
        ]

        # Directly instantiate and register each one.
        for strategy_cls in base_strategy_classes:
            key = strategy_cls.strategy_key
            if key:
                instance = strategy_cls(vfs_reader=vfs_reader)
                strategy_instances[key] = instance
                self.strategies[key] = instance
                logger.debug("Registered base strategy.", strategy_key=key)
            else:
                logger.warning(
                    "Strategy class is missing a strategy_key.", cls=str(strategy_cls)
                )

        # Stage 2: Instantiate and Register Meta Strategies using the instances we just created.
        smart_fetcher = SmartFetcherStrategy(
            fs_strategy=strategy_instances["fs-declarative"],
            rest_strategy=strategy_instances["rest-declarative"],
        )
        self.strategies[smart_fetcher.strategy_key] = smart_fetcher
        logger.debug(
            "Registered meta strategy.", strategy_key=smart_fetcher.strategy_key
        )

    def _get_strategy_for_connection_model(
        self, connection: Connection
    ) -> BaseConnectorStrategy:
        """
        Finds the correct strategy instance based on the connection's
        embedded ApiCatalog record.
        """
        log = logger.bind(connection_name=connection.name, connection_id=connection.id)

        if not connection.catalog:
            log.error("connection_missing_catalog_data")
            raise ValueError(
                f"Connection '{connection.name}' is missing embedded catalog data."
            )

        strategy_key = connection.catalog.connector_provider_key
        if not strategy_key:
            log.error("catalog_missing_provider_key", catalog_id=connection.catalog.id)
            raise ValueError(
                f"ApiCatalog for '{connection.name}' is missing 'connector_provider_key'."
            )

        strategy = self.strategies.get(strategy_key)
        if not strategy:
            log.error("strategy_not_registered", strategy_key=strategy_key)
            raise NotImplementedError(
                f"No connector strategy registered for key '{strategy_key}'."
            )
        return strategy

    async def test_connection(
        self, run_context: "RunContext", connection_source: str
    ) -> Dict[str, Any]:
        """
        Tests a connection from any valid source. This method now accepts a RunContext.
        """
        log = logger.bind(connection_source=connection_source)
        connection_name = "unknown"

        try:
            connection, secrets = await run_context.services.resolver.resolve(
                connection_source
            )
            connection_name = connection.name
            strategy = self._get_strategy_for_connection_model(connection)

            await strategy.test_connection(connection, secrets)

            log.info("Connection test successful.", connection_name=connection_name)
            return {
                "status": "success",
                "message": f"Connection test for '{connection_name}' successful.",
            }
        except Exception as e:
            log.error(
                "Connection test failed.",
                connection_name=connection_name,
                error=str(e),
                exc_info=APP_STATE.verbose_mode,
            )
            return {"status": "error", "message": str(e)}

    @asynccontextmanager
    async def get_client(self, run_context: "RunContext", connection_source: str):
        """
        Provides a ready-to-use, authenticated client for a given service.
        This is an async context manager that ensures proper cleanup of resources.
        It now operates on a RunContext.
        """
        log = logger.bind(connection_source=connection_source)
        try:
            connection, secrets = await run_context.services.resolver.resolve(
                connection_source
            )
            strategy = self._get_strategy_for_connection_model(connection)

            # Yield the client from the strategy's context manager
            async with strategy.get_client(connection, secrets) as client:
                yield client
        except Exception as e:
            log.error(
                "Failed to get client for connection.", error=str(e), exc_info=True
            )
            # Re-raise the exception to be handled by the caller
            raise
