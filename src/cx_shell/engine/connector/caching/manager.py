from typing import Dict

import structlog

from .filesystem import FilesystemCacheProvider
# from .s3 import S3CacheProvider # Future

logger = structlog.get_logger(__name__)


class CacheManager:
    def __init__(self):
        self.providers = {
            "filesystem": FilesystemCacheProvider,
            # "s3": S3CacheProvider, # Future
        }

    async def write_to_cache(self, content: bytes, metadata: Dict, cache_config: Dict):
        """
        Reads the cache config and writes content using the specified provider.
        """
        provider_type = cache_config.get("provider")
        if not provider_type:
            logger.warning(
                "cache.skip", reason="No provider specified in cache_config."
            )
            return

        provider_class = self.providers.get(provider_type)
        if not provider_class:
            logger.error("cache.unknown_provider", provider_type=provider_type)
            return

        try:
            # Initialize the provider with its specific config block
            provider = provider_class(cache_config.get("config", {}))
            cached_path = await provider.write(content, metadata)
            return cached_path
        except Exception as e:
            logger.error(
                "cache.write_failed",
                provider=provider_type,
                error=str(e),
                exc_info=True,
            )
            return None
