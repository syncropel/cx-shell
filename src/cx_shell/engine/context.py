# ~/repositories/cx-shell/src/cx_shell/engine/context.py

from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Literal, Optional, TYPE_CHECKING
from pydantic import BaseModel, Field

from ..utils import resolve_path

if TYPE_CHECKING:
    # These imports are only for static type analysis and IDE support.
    from ..interactive.session import SessionState
    from ..engine.connector.service import ConnectorService
    from ..engine.connector.config import ConnectionResolver
    from ..engine.connector.engine import ScriptEngine
    from ..management.workspace_manager import WorkspaceManager
    from ..management.flow_manager import FlowManager
    from ..management.query_manager import QueryManager
    from ..management.script_manager import ScriptManager
    from ..management.connection_manager import ConnectionManager
    from ..management.open_manager import OpenManager
    from ..management.app_manager import AppManager
    from ..management.process_manager import ProcessManager
    from ..management.compile_manager import CompileManager
    from ..management.index_manager import IndexManager
    from ..management.find_manager import FindManager
    from ..management.install_manager import InstallManager
    from ..engine.transformer.service import TransformerService
    from ..management.publisher import Publisher


@dataclass
class ServiceRegistry:
    """
    A standard dataclass to hold instances of all major services and managers.
    This acts as a dependency injection container for the execution context.
    """

    workspace_manager: "WorkspaceManager"
    resolver: "ConnectionResolver"
    connector_service: "ConnectorService"
    script_engine: "ScriptEngine"
    flow_manager: "FlowManager"
    query_manager: "QueryManager"
    script_manager: "ScriptManager"
    connection_manager: "ConnectionManager"
    open_manager: "OpenManager"
    app_manager: "AppManager"
    process_manager: "ProcessManager"
    compile_manager: "CompileManager"
    index_manager: "IndexManager"
    find_manager: "FindManager"
    install_manager: "InstallManager"
    transformer_service: "TransformerService"
    publisher: "Publisher"


class RunContext(BaseModel):
    """
    The definitive, in-memory container for all state related to a single
    command or flow execution.
    """

    services: ServiceRegistry
    session: "SessionState"
    script_input: Dict[str, Any] = Field(default_factory=dict)
    piped_input: Optional[Any] = Field(None)
    current_flow_path: Optional[Path] = Field(None)
    run_type: Literal["flow", "notebook"] = Field(
        "flow", description="The type of computational document being executed."
    )
    steps: Dict[str, Any] = Field(
        default_factory=dict,
        description="A dictionary holding the results of previously executed steps in the current session.",
    )

    class Config:
        arbitrary_types_allowed = True

    def resolve_path_in_context(self, path_str: str) -> Path:
        """
        Context-aware path resolver.

        This method acts as the bridge between the execution context (which knows
        which file is currently running) and the centralized, stateless path
        resolution utility in `utils.py`. Its sole responsibility is to pass
        its own `current_flow_path` to the utility function.
        """
        # This is the single point of delegation. It passes the path string to be resolved
        # AND the crucial piece of context (the path of the file that is currently
        # being executed) that allows the utility to correctly handle schemes like
        # 'project-asset:' and 'app-asset:'.
        return resolve_path(path_str, current_file_path=self.current_flow_path)
