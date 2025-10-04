# ~/repositories/cx-shell/src/cx_shell/agent/llm_client.py

import instructor
import yaml
import structlog
from pydantic import BaseModel
from typing import Any, Dict, Type, Optional
import asyncio

from ..interactive.session import SessionState
from ..data.agent_schemas import AgentConfig
from ..utils import CX_HOME

logger = structlog.get_logger(__name__)
AGENT_CONFIG_FILE = CX_HOME / "agents.config.yaml"


class LLMClient:
    """
    A dedicated internal client for making structured, validated calls to LLMs.
    This service is the primary user of the `instructor` library and provides
    a unified, provider-agnostic interface for all specialist agents.
    """

    def __init__(self, state: SessionState):
        """Initializes the client. Performs NO I/O."""
        self.state = state
        self.agent_config: Optional[AgentConfig] = None
        self._config_loaded = False
        self._client_cache: Dict[str, Any] = {}

    def _load_agent_config_sync(self) -> Optional[AgentConfig]:
        """The synchronous part of loading the config file."""
        from ..utils import get_asset_path

        config_file_to_load = AGENT_CONFIG_FILE
        if not config_file_to_load.exists():
            config_file_to_load = get_asset_path("configs/agents.default.yaml")
        if not config_file_to_load.exists():
            logger.error("agent.config.missing", path=AGENT_CONFIG_FILE)
            return None
        try:
            with open(config_file_to_load, "r") as f:
                config_data = yaml.safe_load(f)
            return AgentConfig.model_validate(config_data)
        except Exception as e:
            logger.error("agent.config.invalid", path=config_file_to_load, error=str(e))
            return None

    async def load_config_if_needed(self):
        """Asynchronously loads the agent configuration on first call."""
        if not self._config_loaded:
            self.agent_config = await asyncio.to_thread(self._load_agent_config_sync)
            self._config_loaded = True

    async def _get_client_for_role(self, role_name: str, is_async: bool = True) -> Any:
        """
        Reads the agent config, securely retrieves the API key, and instantiates
        an `instructor` client for the specified role.
        """
        # --- FIX: Ensure config is loaded before any other operation ---
        await self.load_config_if_needed()

        cache_key = f"{role_name}_{is_async}"
        if cache_key in self._client_cache:
            logger.debug("llm_client.cache_hit", role=role_name)
            return self._client_cache[cache_key]

        if not self.agent_config:
            raise RuntimeError(
                "Agent configuration `agents.config.yaml` not found or is invalid."
            )

        profile_name = self.agent_config.default_profile
        profile = self.agent_config.profiles.get(profile_name)
        if not profile:
            raise RuntimeError(
                f"Default agent profile '{profile_name}' not found in configuration."
            )

        role_config = getattr(profile, role_name, None)
        if not role_config:
            raise RuntimeError(
                f"Role '{role_name}' not found in agent profile '{profile_name}'."
            )

        connection_alias = role_config.connection_alias
        logger.debug(
            "llm_client.resolving_connection", role=role_name, alias=connection_alias
        )

        if connection_alias not in self.state.connections:
            raise ValueError(
                f"Required LLM connection '{connection_alias}' is not active in the session."
            )

        secrets = self.state.get_secrets_for_alias(connection_alias)
        api_key = secrets.get("api_key")
        if not api_key:
            raise ValueError(
                f"API key not found in secrets for connection '{connection_alias}'."
            )

        logger.debug("llm_client.api_key_found", role=role_name)
        provider_name = role_config.connection_alias.replace("cx_", "")
        model_name = role_config.parameters.get("model", "default")
        provider_string = f"{provider_name}/{model_name}"
        logger.debug("llm_client.creating_instructor_client", provider=provider_string)

        try:
            client = instructor.from_provider(
                provider_string, api_key=api_key, async_client=is_async
            )
            logger.debug(
                "llm_client.instructor_client_created", provider=provider_string
            )
        except Exception as e:
            logger.error(
                "instructor.client.failed", provider=provider_string, error=str(e)
            )
            raise RuntimeError(
                f"Failed to initialize instructor client for provider '{provider_string}'."
            )

        self._client_cache[cache_key] = client
        return client

    async def create_structured_response(
        self,
        role_name: str,
        response_model: Type[BaseModel],
        messages: list,
        max_retries: int = 2,
    ) -> BaseModel:
        """
        The primary async method for specialist agents to call. It gets an async client
        and makes a structured, validated call to the configured LLM.
        """
        try:
            # --- FIX: Ensure config is loaded before any other operation ---
            await self.load_config_if_needed()

            async_client = await self._get_client_for_role(role_name, is_async=True)

            # This check is now safe because the config has been loaded.
            if not self.agent_config:
                raise RuntimeError("Agent configuration failed to load.")

            profile = self.agent_config.profiles[self.agent_config.default_profile]
            role_config = getattr(profile, role_name)
            model_params = {
                k: v for k, v in role_config.parameters.items() if k != "model"
            }

            logger.debug(
                "llm_client.api_call.begin",
                role=role_name,
                model_params=model_params,
                message_count=len(messages),
            )

            response = await async_client.chat.completions.create(
                response_model=response_model,
                messages=messages,
                max_retries=max_retries,
                **model_params,
            )

            logger.debug("llm_client.api_call.success", role=role_name)
            return response

        except Exception as e:
            logger.error(
                "llm_client.create.failed", role=role_name, error=str(e), exc_info=True
            )
            raise
