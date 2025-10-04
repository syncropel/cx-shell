# [REPLACE] ~/repositories/connector-logic/src/connector_logic/providers/rest/webhook_strategy.py

import logging
from typing import TYPE_CHECKING, Dict, Any, List
from contextlib import asynccontextmanager
import httpx
from ..base import BaseConnectorStrategy
from cx_core_schemas.vfs import VfsFileContentResponse
from .....data.agent_schemas import DryRunResult

if TYPE_CHECKING:
    from cx_core_schemas.connection import Connection

logger = logging.getLogger(__name__)


class WebhookStrategy(BaseConnectorStrategy):
    """A simple strategy for sending data to an incoming webhook URL."""

    strategy_key = "rest-webhook"

    @asynccontextmanager
    async def get_client(self, details: Dict[str, Any], secrets: Dict[str, Any]):
        """Provides an unauthenticated httpx.AsyncClient."""
        # This client doesn't need any special headers as the webhook URL itself is the secret.
        async with httpx.AsyncClient(timeout=30.0) as client:
            yield client

    async def test_connection(
        self, details: Dict[str, Any], secrets: Dict[str, Any]
    ) -> bool:
        """
        Tests the webhook by sending a simple, pre-defined test message.
        """
        webhook_url = secrets.get("webhook_url")
        if not webhook_url:
            raise ValueError("Credential error: 'webhook_url' not found in Vault.")

        logger.info(
            f"[{self.strategy_key}] Performing live connection test to webhook."
        )

        # MS Teams expects a specific JSON format for its cards.
        test_payload = {
            "@type": "MessageCard",
            "@context": "http://schema.org/extensions",
            "summary": "CGI Fabric Connection Test",
            "text": "✅ Connection test from CGI Fabric was successful!",
        }

        try:
            async with self.get_client(details, secrets) as client:
                response = await client.post(webhook_url, json=test_payload)
                response.raise_for_status()  # Will raise an error for non-2xx responses

                logger.info(
                    f"[{self.strategy_key}] Webhook test successful. Received status {response.status_code}."
                )
                return True
        except Exception as e:
            error_message = f"Webhook connection test failed: {e}"
            logger.error(f"[{self.strategy_key}] {error_message}", exc_info=True)
            raise ConnectionError(error_message) from e

    async def browse_path(
        self, path_parts: List[str], details: Dict[str, Any], secrets: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Implements the browse capability for a webhook. Since a webhook is a single
        endpoint and not a browsable hierarchy, this method always returns an empty list.
        """
        logger.info(
            f"[{self.strategy_key}] Browse called, but webhooks are not browsable. Returning empty list."
        )
        return []

    async def get_content(
        self, path_parts: List[str], details: Dict[str, Any], secrets: Dict[str, Any]
    ) -> VfsFileContentResponse:
        """Webhooks are write-only endpoints; they cannot be 'read' or 'opened'."""
        raise NotImplementedError("Cannot 'get_content' from a write-only webhook.")

    async def dry_run(
        self,
        connection: "Connection",
        secrets: Dict[str, Any],
        action_params: Dict[str, Any],
    ) -> "DryRunResult":
        if "webhook_url" not in secrets:
            return DryRunResult(
                indicates_failure=True,
                message="Dry run failed: Secret 'webhook_url' is missing.",
            )
        return DryRunResult(
            indicates_failure=False,
            message="Dry run successful: Webhook URL is present.",
        )
