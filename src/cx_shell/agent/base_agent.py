from abc import ABC
import yaml
from typing import Optional
import asyncio

from ..interactive.session import SessionState
from .llm_client import LLMClient
from ..data.agent_schemas import AgentConfig
from ..utils import CX_HOME
from ..utils import get_assets_root


AGENT_CONFIG_FILE = CX_HOME / "agents.config.yaml"


class BaseSpecialistAgent(ABC):
    """Abstract base class for a specialist agent in the CARE architecture."""

    def __init__(self, state: SessionState, llm_client: LLMClient):
        self.state = state
        self.llm_client = llm_client
        self.agent_config: Optional[AgentConfig] = None
        self._config_loaded = False

    def _load_agent_config_sync(self) -> Optional[AgentConfig]:
        """The synchronous part of loading the config file."""
        assets_root = get_assets_root()

        config_file_to_load = AGENT_CONFIG_FILE
        if not config_file_to_load.exists():
            default_config_path = assets_root / "configs" / "agents.default.yaml"
            if not default_config_path.exists():
                return None
            config_file_to_load = default_config_path
        try:
            with open(config_file_to_load, "r") as f:
                config_data = yaml.safe_load(f)
            return AgentConfig.model_validate(config_data)
        except Exception:
            return None

    async def load_config_if_needed(self):
        """
        Asynchronously loads the agent configuration on first call,
        running the blocking I/O in a separate thread.
        """
        if not self._config_loaded:
            self.agent_config = await asyncio.to_thread(self._load_agent_config_sync)
            self._config_loaded = True
