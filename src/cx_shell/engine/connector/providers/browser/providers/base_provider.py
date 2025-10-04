from abc import ABC, abstractmethod

from playwright.async_api import Browser, Page


class BaseBrowserProvider(ABC):
    """Abstract Base Class for all browser providers."""

    @abstractmethod
    async def get_browser(self) -> tuple[Browser, Page]:
        """
        Initializes and returns a connected Playwright Browser and Page object.
        """
        raise NotImplementedError

    @abstractmethod
    async def close(self):
        """
        Cleans up and closes the browser session.
        """
        raise NotImplementedError
