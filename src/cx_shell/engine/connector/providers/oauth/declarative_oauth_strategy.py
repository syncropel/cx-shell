from contextlib import asynccontextmanager
from typing import Any, Dict, List, TYPE_CHECKING

import structlog

from ..rest.declarative_strategy import DeclarativeRestStrategy
from .base_oauth_strategy import BaseOauth2Strategy
from .....data.agent_schemas import DryRunResult


# --- Conditional Imports for Type Hinting ---
if TYPE_CHECKING:
    from cx_core_schemas.connection import Connection
    from cx_core_schemas.vfs import VfsFileContentResponse

logger = structlog.get_logger(__name__)


class DeclarativeOauthStrategy(BaseOauth2Strategy):
    """
    A single, reusable, blueprint-driven strategy for any service using a
    standard OAuth2 Authorization Code or Refresh Token flow.

    It acts as an authentication wrapper. It first handles the token refresh flow
    using its base class, then delegates the actual API browsing and content
    retrieval logic to an internal instance of the DeclarativeRestStrategy by
    injecting a fully authenticated HTTP client.
    """

    strategy_key = "oauth2-declarative"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # Create an internal instance of the REST strategy to handle API calls.
        # This is the core of the Composition pattern.
        self.rest_strategy = DeclarativeRestStrategy(**kwargs)
        logger.info("DeclarativeOauthStrategy initialized.")

    def _get_token_url(self, connection: "Connection") -> str:
        """
        Dynamically gets the TOKEN_URL from the connection's blueprint.
        This is required to fulfill the contract of the BaseOauth2Strategy.
        """
        if not connection.catalog or not connection.catalog.oauth_config:
            raise ValueError("oauth_config is missing from the ApiCatalog blueprint.")

        token_url = connection.catalog.oauth_config.get("token_url")
        if not token_url:
            raise ValueError("`token_url` is missing from the oauth_config blueprint.")

        return token_url

    # --- FULFILLING THE CONTRACT ---

    @asynccontextmanager
    async def get_client(self, connection: "Connection", secrets: Dict[str, Any]):
        """
        Gets a valid OAuth2 access token and uses it to construct a fully
        authenticated client via the internal DeclarativeRestStrategy.
        This is the central orchestration point for this strategy.
        """
        # Step 1: Get the valid secrets, which includes a fresh access_token.
        # This uses the logic from our base class to perform the refresh if needed.
        valid_secrets = await self._get_valid_secrets(connection, secrets)
        access_token = valid_secrets.get("access_token")

        if not access_token:
            raise ConnectionError("Failed to obtain a valid OAuth2 access token.")

        # Step 2: Create a temporary, deep copy of the connection model.
        # This prevents side effects and allows us to dynamically inject the auth header.
        temp_connection = connection.model_copy(deep=True)
        if not temp_connection.catalog:
            # This should not happen if validation passed, but is a safe check.
            raise ValueError("Cannot create client from connection with no catalog.")

        if not temp_connection.catalog.auth_config:
            temp_connection.catalog.auth_config = {}

        # Step 3: Dynamically inject the Bearer token as a standard header.
        # This "tricks" the DeclarativeRestStrategy into using our just-refreshed token.
        temp_connection.catalog.auth_config["type"] = "header"
        temp_connection.catalog.auth_config["header_name"] = "Authorization"
        temp_connection.catalog.auth_config["value_template"] = f"Bearer {access_token}"

        # Step 4: Now, get a client from the REST strategy using this modified connection.
        # It will use its own logic to build the client, but with our injected auth.
        async with self.rest_strategy.get_client(temp_connection, secrets) as client:
            yield client

    async def browse_path(
        self, path_parts: List[str], connection: "Connection", secrets: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Fulfills the contract by getting an OAuth2-authenticated client and then
        delegating the browse operation to the internal DeclarativeRestStrategy.
        """
        log = logger.bind(
            connection_id=connection.id, vfs_path=f"/{'/'.join(path_parts)}"
        )
        log.info("oauth_browse.begin")

        # Get the authenticated client using THIS class's get_client method.
        async with self.get_client(connection, secrets) as authenticated_client:
            # Pass the authenticated client to the REST strategy's method.
            return await self.rest_strategy.browse_path(
                path_parts, connection, secrets, client=authenticated_client
            )

    async def get_content(
        self, path_parts: List[str], connection: "Connection", secrets: Dict[str, Any]
    ) -> "VfsFileContentResponse":
        """
        Fulfills the contract by getting an OAuth2-authenticated client and then
        delegating the get_content operation to the internal DeclarativeRestStrategy.
        """
        log = logger.bind(
            connection_id=connection.id, vfs_path=f"/{'/'.join(path_parts)}"
        )
        log.info("oauth_get_content.begin")

        # Get the authenticated client.
        async with self.get_client(connection, secrets) as authenticated_client:
            # Pass the client to the REST strategy's get_content method.
            return await self.rest_strategy.get_content(
                path_parts, connection, secrets, client=authenticated_client
            )

    async def dry_run(
        self,
        connection: "Connection",
        secrets: Dict[str, Any],
        action_params: Dict[str, Any],
    ) -> "DryRunResult":
        """A dry run for OAuth checks for the presence of core OAuth secrets."""
        required_secrets = ["client_id", "client_secret", "refresh_token"]
        missing = [key for key in required_secrets if key not in secrets]
        if missing:
            return DryRunResult(
                indicates_failure=True,
                message=f"Dry run failed: Missing required OAuth secrets: {', '.join(missing)}",
            )
        return DryRunResult(
            indicates_failure=False,
            message="Dry run successful: All required OAuth secrets are present.",
        )
