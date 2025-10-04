import structlog
from .base_provider import BaseBrowserProvider
from .local_provider import LocalBrowserProvider

logger = structlog.get_logger(__name__)


class BrowserManager:
    """Factory for creating the appropriate browser provider based on config."""

    @staticmethod
    def get_provider(provider_name: str = "local") -> BaseBrowserProvider:
        """
        Reads the configuration and returns an instance of the correct provider.
        """
        logger.info(f"Creating browser provider for type: '{provider_name}'")

        if provider_name == "local":
            return LocalBrowserProvider()
        # In future phases, we can add 'cdp' and 'grid' here.
        else:
            raise ValueError(f"Unsupported browser provider: '{provider_name}'")
