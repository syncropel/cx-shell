# ~/repositories/cx-shell/src/cx_shell/_bootstrap.py


def bootstrap_models():
    """
    Performs the critical late imports and triggers Pydantic's model_rebuild.

    This function is called once at application startup. By isolating this logic here,
    we break the circular import dependencies and make the import explicit,
    preventing auto-formatters from removing the necessary imports.
    """
    # Import the model that needs rebuilding.
    from .engine.context import RunContext

    # Import all the types that are used as forward references within RunContext.
    # The linter will not remove these because we are "using" them by calling model_rebuild.
    from .interactive.session import SessionState
    from .engine.connector.service import ConnectorService
    from .engine.connector.config import ConnectionResolver
    from .engine.connector.engine import ScriptEngine
    from .management.workspace_manager import WorkspaceManager
    from .management.flow_manager import FlowManager
    from .management.query_manager import QueryManager
    from .management.script_manager import ScriptManager
    from .management.connection_manager import ConnectionManager
    from .management.open_manager import OpenManager
    from .management.app_manager import AppManager
    from .management.process_manager import ProcessManager
    from .management.compile_manager import CompileManager
    from .management.index_manager import IndexManager
    from .management.find_manager import FindManager
    from .management.install_manager import InstallManager
    from .engine.transformer.service import TransformerService
    from .management.publisher import Publisher

    # Now, explicitly tell the RunContext model to rebuild itself.
    # All necessary types are now in the local scope of this function.
    RunContext.model_rebuild(
        _types_namespace={
            "SessionState": SessionState,
            "ConnectorService": ConnectorService,
            "ConnectionResolver": ConnectionResolver,
            "ScriptEngine": ScriptEngine,
            "WorkspaceManager": WorkspaceManager,
            "FlowManager": FlowManager,
            "QueryManager": QueryManager,
            "ScriptManager": ScriptManager,
            "ConnectionManager": ConnectionManager,
            "OpenManager": OpenManager,
            "AppManager": AppManager,
            "ProcessManager": ProcessManager,
            "CompileManager": CompileManager,
            "IndexManager": IndexManager,
            "FindManager": FindManager,
            "InstallManager": InstallManager,
            "TransformerService": TransformerService,
            "Publisher": Publisher,
        }
    )
