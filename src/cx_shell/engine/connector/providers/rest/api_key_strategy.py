import logging
from typing import TYPE_CHECKING, Dict, Any, List
from contextlib import asynccontextmanager

from cx_core_schemas.vfs import VfsFileContentResponse
import httpx
from .....data.agent_schemas import DryRunResult

from ..base import BaseConnectorStrategy

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from cx_core_schemas.connection import Connection


class ApiKeyStrategy(BaseConnectorStrategy):
    """
    A reusable strategy for connecting to any REST API that uses a simple
    API key or token passed in an HTTP header.

    This single strategy can handle services like Stripe, Mozio, Klipfolio, etc.
    The specifics (header name, URL) are configured in the Connection record's `details`.
    """

    strategy_key = "rest-api_key"

    @asynccontextmanager
    async def get_client(self, details: Dict[str, Any], secrets: Dict[str, Any]):
        """
        Provides an authenticated `httpx.AsyncClient` for making API calls.
        """
        # --- Configuration Extraction ---
        # The base URL for the API endpoint (e.g., "https://api.stripe.com/v1")
        base_url = details.get("base_url")
        if not base_url:
            raise ValueError(
                "Configuration error: 'base_url' is missing from connection details."
            )

        # The name of the header to send the key in (e.g., "Authorization" or "X-API-Key")
        header_name = details.get("header_name", "Authorization")

        # An optional prefix for the header value (e.g., "Bearer " or "Basic ")
        header_prefix = details.get("header_prefix", "")

        # The name of the secret key in Vault (e.g., "api_key" or "secret_token")
        secret_key_name = details.get("secret_key_name", "api_key")

        # --- Secret Extraction ---
        api_key = secrets.get(secret_key_name)
        if not api_key:
            raise ValueError(
                f"Credential error: Secret '{secret_key_name}' not found in Vault."
            )

        # --- Client Initialization ---
        headers = {
            header_name: f"{header_prefix}{api_key}".strip(),
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        # The httpx.AsyncClient is itself a context manager, so we can yield it directly.
        # It handles connection pooling and cleanup gracefully.
        async with httpx.AsyncClient(
            base_url=base_url, headers=headers, timeout=30.0
        ) as client:
            try:
                logger.info(
                    f"[{self.strategy_key}] Providing authenticated HTTP client for base_url: {base_url}"
                )
                yield client
            finally:
                logger.info(
                    f"[{self.strategy_key}] HTTP client context for {base_url} is being closed."
                )
                # The `async with` block automatically handles closing the client.

    async def test_connection(
        self, details: Dict[str, Any], secrets: Dict[str, Any]
    ) -> bool:
        """
        Tests the connection by making a request to a specified health/test endpoint.
        """
        # The path to a simple, lightweight endpoint for testing credentials (e.g., "/v1/users/me")
        test_endpoint = details.get("test_endpoint")
        if not test_endpoint:
            logger.warning(
                f"[{self.strategy_key}] No 'test_endpoint' configured. Skipping live connection test."
            )
            # If no test endpoint is defined, we can't perform a live test.
            # We assume the connection is valid if all required fields are present.
            return True

        logger.info(
            f"[{self.strategy_key}] Performing live connection test to endpoint: {test_endpoint}"
        )

        try:
            # Use the get_client context manager to perform the test
            async with self.get_client(details, secrets) as client:
                response = await client.get(test_endpoint)

                # Check for a successful HTTP status code (2xx)
                response.raise_for_status()

                logger.info(
                    f"[{self.strategy_key}] Connection test successful. Received status {response.status_code}."
                )
                return True
        except httpx.HTTPStatusError as e:
            # Handle 4xx/5xx errors
            error_message = f"Connection test failed: Received status {e.response.status_code}. Response: {e.response.text[:200]}"
            logger.error(f"[{self.strategy_key}] {error_message}", exc_info=True)
            raise ConnectionError(error_message) from e
        except Exception as e:
            # Handle other errors like timeouts or DNS issues
            error_message = f"Connection test failed with an unexpected error: {e}"
            logger.error(f"[{self.strategy_key}] {error_message}", exc_info=True)
            raise ConnectionError(error_message) from e

    async def browse_path(
        self, path_parts: List[str], details: Dict[str, Any], secrets: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        # This generic strategy can be configured via `details` to know what to list.
        browse_config = details.get("browse_config", [])

        # Example for Zendesk: browse_config could be [{"name": "Tickets", "endpoint": "/api/v2/tickets"}]
        # MOCK IMPLEMENTATION
        if not path_parts:
            return [
                {"name": "Users", "path": "users/", "type": "folder"},
                {"name": "Tickets", "path": "tickets/", "type": "folder"},
            ]
        if len(path_parts) == 1 and path_parts[0] == "tickets":
            # A real implementation would call the Zendesk API's /api/v2/tickets endpoint here.
            return [{"name": "Ticket #123.json", "path": "tickets/123", "type": "file"}]

        return []

    async def get_content(
        self, path_parts: List[str], details: Dict[str, Any], secrets: Dict[str, Any]
    ) -> VfsFileContentResponse:
        """
        For a generic API, 'opening a file' could mean hitting a specific GET endpoint.
        This needs to be configured in the connection's `details`.
        """
        # A real implementation would parse path_parts and look up a corresponding
        # endpoint in the connection details.
        logger.warning(
            f"[{self.strategy_key}] get_content is not fully implemented for this generic strategy."
        )
        # For now, we raise an error indicating it's not supported for this specific connection.
        raise FileNotFoundError(
            "Opening a 'file' is not a supported operation for this generic API connection."
        )

    async def dry_run(
        self,
        connection: "Connection",
        secrets: Dict[str, Any],
        action_params: Dict[str, Any],
    ) -> "DryRunResult":
        # A basic dry run for this strategy checks for the presence of the API key.
        secret_key_name = connection.details.get("secret_key_name", "api_key")
        if secret_key_name not in secrets:
            return DryRunResult(
                indicates_failure=True,
                message=f"Dry run failed: Secret '{secret_key_name}' is missing for this connection.",
            )
        return DryRunResult(
            indicates_failure=False, message="Dry run successful: API key is present."
        )
