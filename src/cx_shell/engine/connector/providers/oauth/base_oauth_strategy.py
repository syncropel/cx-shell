import asyncio
from abc import abstractmethod
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, TYPE_CHECKING

import httpx
import structlog

from ..base import BaseConnectorStrategy

# --- Conditional Imports for Type Hinting ---
# This allows our code to have type hints for objects from other modules
# without creating a hard dependency or a circular import at runtime.
if TYPE_CHECKING:
    import hvac
    from cx_core_schemas.connection import Connection
    from cx_core_schemas.vfs import VfsFileContentResponse

# Use structlog for structured, contextual logging.
logger = structlog.get_logger(__name__)


class Oauth2ConnectionError(ConnectionError):
    """Custom exception for OAuth2 related failures for better error handling."""

    pass


class BaseOauth2Strategy(BaseConnectorStrategy):
    """
    A reusable, highly-resilient abstract base class for any service using the
    standard OAuth2 Authorization Code grant flow.

    Its primary responsibility is to manage the token lifecycle: checking token
    validity, performing the refresh flow when necessary, and securely updating
    the stored secrets in Vault. It provides the core building blocks for
    concrete strategies to create fully authenticated clients.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # These dependencies are injected by the ConnectorService.
        self.vault_client: Optional["hvac.Client"] = kwargs.get("vault_client")
        self.vault_mount_point: str = kwargs.get("vault_mount_point", "secret")
        # A Redis client would be injected here for thundering herd prevention.
        # self.redis_client = kwargs.get("redis_client")

    def _get_token_url(self, connection: "Connection") -> str:
        """
        Abstract method. Concrete strategies (like DeclarativeOauthStrategy)
        MUST override this to provide the specific token endpoint URL for a service.
        """
        raise NotImplementedError(
            "OAuth strategy subclass must implement _get_token_url."
        )

    async def _refresh_token(
        self, connection: "Connection", secrets: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        The core token refresh logic. It uses the refresh token to get a new
        access token and, if running in integrated mode (i.e., a Vault client is present),
        updates the secret in Vault.
        """
        log = logger.bind(
            strategy_key=self.strategy_key,
            connection_id=connection.id,
            operation="oauth_refresh",
        )
        log.info("Access token expired or missing. Attempting refresh.")

        # 1. --- Validate Inputs & Dependencies ---
        refresh_token = secrets.get("refresh_token")
        client_id = secrets.get("client_id")
        client_secret = secrets.get("client_secret")

        if not all([refresh_token, client_id, client_secret]):
            log.error("oauth_refresh.missing_credentials")
            raise Oauth2ConnectionError(
                "Missing refresh_token, client_id, or client_secret in secrets."
            )

        token_url = self._get_token_url(connection)

        # 2. --- (Future) Thundering Herd Prevention using Redis Lock ---
        # This is where distributed locking logic would go.

        # 3. --- Perform Token Refresh Request ---
        token_payload = {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        }

        log.info("oauth_refresh.requesting_new_token", token_url=token_url)

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(
                    token_url, data=token_payload, auth=(client_id, client_secret)
                )
                response.raise_for_status()
                new_token_data = response.json()

            # 4. --- Prepare New Secrets Dictionary ---
            expires_in = int(new_token_data.get("expires_in", 3600))
            new_refresh_token = new_token_data.get("refresh_token", refresh_token)
            expires_at_dt = datetime.now(timezone.utc) + timedelta(
                seconds=expires_in - 60
            )

            new_secrets = {
                "access_token": new_token_data["access_token"],
                "refresh_token": new_refresh_token,
                "expires_at": expires_at_dt.isoformat(),
                "client_id": client_id,
                "client_secret": client_secret,
            }

            # 5. --- Conditionally Write Back to Vault ---
            # Only attempt to write to Vault if we are in integrated mode
            # (i.e., a vault_client was provided during initialization).
            if self.vault_client:
                vault_path = connection.vault_secret_path
                if not vault_path:
                    log.error("oauth_refresh.missing_vault_path")
                    raise Oauth2ConnectionError(
                        "Cannot persist token: vault_secret_path is missing from connection model."
                    )

                log.info(
                    "oauth_refresh.writing_new_token_to_vault",
                    vault_path=vault_path,
                )

                # Use the injected vault client and mount point to write the secret.
                # This is a synchronous call, so run it in a threadpool to avoid blocking.
                await asyncio.to_thread(
                    self.vault_client.secrets.kv.v2.create_or_update_secret,
                    path=vault_path,
                    secret=new_secrets,
                    mount_point=self.vault_mount_point,
                )
            else:
                # In standalone mode, we log that we are skipping the Vault write.
                # The new token exists only in memory for this run.
                log.info(
                    "oauth_refresh.skip_vault_write",
                    reason="Standalone mode (no vault client)",
                )

            log.info("oauth_refresh.success")
            return new_secrets

        except httpx.HTTPStatusError as e:
            error_text = e.response.text
            log.error(
                "oauth_refresh.http_error",
                status_code=e.response.status_code,
                response_text=error_text,
            )
            raise Oauth2ConnectionError(
                f"OAuth token refresh failed with status {e.response.status_code}: {error_text}"
            ) from e
        except Exception as e:
            log.error("oauth_refresh.unexpected_error", error=str(e), exc_info=True)
            raise Oauth2ConnectionError(
                f"An unexpected error occurred during token refresh: {e}"
            ) from e

    async def _get_valid_secrets(
        self, connection: "Connection", secrets: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Checks if the current access token is valid. If not, triggers a refresh.
        Returns a dictionary of secrets containing a valid access token.
        """
        expires_at_str = secrets.get("expires_at")

        if expires_at_str:
            try:
                expires_at_dt = datetime.fromisoformat(expires_at_str)
                if expires_at_dt > datetime.now(timezone.utc):
                    logger.debug(
                        "Existing access token is valid.", connection_id=connection.id
                    )
                    return secrets
            except ValueError:
                logger.warning(
                    "Could not parse expires_at timestamp. Assuming token is expired.",
                    expires_at=expires_at_str,
                )

        # If we reach here, the token is expired, missing, has no expiry info, or expiry is malformed.
        return await self._refresh_token(connection, secrets)

    async def test_connection(
        self, connection: "Connection", secrets: Dict[str, Any]
    ) -> bool:
        """
        Tests an OAuth2 connection by verifying that the refresh token flow works.
        A successful refresh proves the client_id, client_secret, and refresh_token are all valid.
        """
        log = logger.bind(connection_id=connection.id, connection_name=connection.name)
        log.info("oauth_test_connection.begin")
        try:
            await self._refresh_token(connection, secrets)
            log.info("oauth_test_connection.success")
            return True
        except Oauth2ConnectionError as e:
            log.error("oauth_test_connection.failed", error=str(e))
            raise ConnectionError(f"OAuth2 connection test failed: {e}") from e

    # --- Abstract methods from BaseConnectorStrategy that MUST be implemented by children ---

    @abstractmethod
    @asynccontextmanager
    async def get_client(self, connection: "Connection", secrets: Dict[str, Any]):
        """
        Concrete strategies MUST implement this to provide the final authenticated client.
        This implementation will typically call `_get_valid_secrets` first.
        """
        yield
        raise NotImplementedError

    @abstractmethod
    async def browse_path(
        self, path_parts: List[str], connection: "Connection", secrets: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    async def get_content(
        self, path_parts: List[str], connection: "Connection", secrets: Dict[str, Any]
    ) -> "VfsFileContentResponse":
        raise NotImplementedError
