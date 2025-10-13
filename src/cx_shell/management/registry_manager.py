# ~/repositories/cx-shell/src/cx_shell/management/registry_manager.py

import httpx
import yaml
import structlog
from typing import Dict, Any, List, Optional
from rich.console import Console

# --- Constants ---
console = Console()
logger = structlog.get_logger(__name__)

APPS_REGISTRY_URL = (
    "https://raw.githubusercontent.com/syncropel/applications/main/registry.yaml"
)
BLUEPRINT_REGISTRY_URL = (
    "https://raw.githubusercontent.com/syncropel/blueprints/main/registry.yaml"
)


class RegistryManager:
    """
    A centralized service for fetching and caching the public registries for
    both Applications and Blueprints.
    """

    _app_registry_cache: Optional[Dict[str, Any]] = None
    _blueprint_registry_cache: Optional[Dict[str, Any]] = None

    async def _fetch_and_cache_registry(
        self, url: str, cache_attr: str
    ) -> Dict[str, Any]:
        """
        A generic helper to fetch a registry file from a URL and cache it in memory.

        Args:
            url: The URL of the registry.yaml file to fetch.
            cache_attr: The name of the instance attribute to use for caching (e.g., '_app_registry_cache').

        Returns:
            The parsed registry dictionary.
        """
        cached_data = getattr(self, cache_attr)
        if cached_data is not None:
            logger.debug("Registry found in memory cache.", registry_url=url)
            return cached_data

        try:
            logger.debug("Fetching registry from remote URL.", registry_url=url)
            async with httpx.AsyncClient(timeout=10.0, follow_redirects=True) as client:
                response = await client.get(url)
                response.raise_for_status()

            parsed_data = yaml.safe_load(response.text) or {}
            setattr(self, cache_attr, parsed_data)  # Set the cache
            return parsed_data

        except httpx.HTTPStatusError as e:
            console.print(
                f"[bold red]Error:[/bold red] Could not fetch registry at [dim]{url}[/dim]. Server responded with {e.response.status_code}."
            )
            logger.error(
                "registry.fetch.http_error", url=url, status_code=e.response.status_code
            )
            return {}
        except Exception as e:
            console.print(
                f"[bold red]Error:[/bold red] Could not fetch or parse the registry at [dim]{url}[/dim]."
            )
            logger.error("registry.fetch.failed", url=url, error=str(e))
            return {}

    async def get_application_metadata(
        self, app_id: str, version: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Fetches the full metadata for a specific application from the registry,
        optionally for a specific version.
        """
        apps = await self.get_available_applications()
        for app in apps:
            if app.get("id") == app_id:
                # If a specific version is requested, we will need to handle that in the future.
                # For now, we return the first match, which is the latest version.
                return app
        return None

    async def get_available_blueprints(self) -> List[Dict[str, Any]]:
        """Returns a list of available blueprints from the public blueprint registry."""
        registry = await self._fetch_and_cache_registry(
            BLUEPRINT_REGISTRY_URL, "_blueprint_registry_cache"
        )
        return registry.get("blueprints", [])

    async def get_available_applications(self) -> List[Dict[str, Any]]:
        """Returns a list of available applications from the public application registry."""
        registry = await self._fetch_and_cache_registry(
            APPS_REGISTRY_URL, "_app_registry_cache"
        )
        return registry.get("applications", [])
