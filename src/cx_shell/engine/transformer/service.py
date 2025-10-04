from typing import TYPE_CHECKING, Any, Dict, List

import pandas as pd
import structlog

from .engines.base import BaseTransformEngine
from .engines.file_format_engine import FileFormatEngine
from .engines.jinja_engine import JinjaEngine
from .engines.pandas_engine import PandasEngine
from .vfs_client import AbstractVfsClient, LocalVfsClient

# Use TYPE_CHECKING to import RunContext for type hinting without creating a circular import at runtime.
if TYPE_CHECKING:
    from ...engine.context import RunContext

logger = structlog.get_logger(__name__)


class TransformerService:
    """
    Orchestrates a multi-step data transformation pipeline.

    This service loads raw data, passes it sequentially through a series of
    declarative "engine" steps (e.g., Pandas, Jinja), and produces a final
    output. The output is either a structured "Artifact Manifest" detailing the
    files created, or the in-memory transformed data if no files were saved.
    """

    def __init__(self, vfs_client: AbstractVfsClient | None = None):
        """
        Initializes the service and registers all available transformation engines.

        Args:
            vfs_client: An optional VFS client for handling file I/O. If not
                        provided, it defaults to a client for the local filesystem.
        """
        if vfs_client is None:
            vfs_client = LocalVfsClient()
        self.vfs_client = vfs_client
        self.engines: Dict[str, BaseTransformEngine] = {
            "pandas": PandasEngine(),
            "file_format": FileFormatEngine(self.vfs_client),
            "jinja": JinjaEngine(self.vfs_client),
        }
        logger.info(
            "TransformerService initialized.",
            registered_engines=list(self.engines.keys()),
        )

    async def run(self, script_data: Dict, run_context: "RunContext") -> Any:
        """
        Executes a full transformation pipeline.

        This method serves as the main entry point for the TransformerService. It orchestrates
        the loading of initial data and the sequential execution of transformation steps
        defined in the provided script.

        Args:
            script_data: The parsed dictionary from a .transformer.yaml or the content
                         of a `transform` block in a .cx.md file.
            run_context: The full, stateful RunContext object from the ScriptEngine,
                         containing session state, services, and piped input.

        Returns:
            An artifact manifest dictionary if files were generated, otherwise the
            final transformed DataFrame as a list of dictionaries.
        """
        log = logger.bind(script_name=script_data.get("name"))
        log.info("service.run.begin")

        from jinja2 import Environment

        jinja_env = Environment()

        # --- 1. CONTEXT PREPARATION ---
        # The transformer_context is a flat dictionary built for rendering templates
        # within the transformation steps. It includes:
        # - The actual RunContext object under a reserved key for context-aware engines.
        # - Top-level access to script inputs (aliased as 'inputs' for convenience) and session variables.
        transformer_context: Dict[str, Any] = {
            "run_context": run_context,
            "script_input": run_context.script_input,
            "inputs": run_context.script_input,
            "steps": run_context.steps,
            **run_context.session.variables,
        }
        log.debug(
            "service.run.context_prepared",
            context_keys=list(transformer_context.keys()),
        )

        def recursive_render(data: Any, context: Dict):
            if isinstance(data, dict):
                return {k: recursive_render(v, context) for k, v in data.items()}
            if isinstance(data, list):
                return [recursive_render(i, context) for i in data]
            if isinstance(data, str) and "{{" in data:
                return jinja_env.from_string(data).render(**context)
            return data

        # --- 2. INITIAL DATA LOADING ---
        # The service intelligently loads the initial DataFrame from one of two sources,
        # prioritizing in-memory data passed from a previous step via the RunContext.
        current_df = pd.DataFrame()
        initial_input_data = run_context.piped_input

        if initial_input_data is not None and isinstance(initial_input_data, list):
            log.info(
                "service.run.loading_data_from_memory",
                record_count=len(initial_input_data),
            )
            current_df = pd.DataFrame(initial_input_data)
        else:
            load_ops = script_data.get("load", [])
            if load_ops:
                load_from_path_template = load_ops[0].get("from")
                rendered_load_path_str = recursive_render(
                    load_from_path_template, transformer_context
                )
                absolute_load_path = run_context.resolve_path_in_context(
                    rendered_load_path_str
                )
                log.info(
                    "service.run.loading_data_from_file", path=str(absolute_load_path)
                )
                if not absolute_load_path.exists():
                    raise FileNotFoundError(
                        f"Input file not found by transformer: {absolute_load_path}"
                    )
                suffix = absolute_load_path.suffix.lower()
                if suffix in [".xlsx", ".xls"]:
                    current_df = pd.read_excel(absolute_load_path)
                elif suffix == ".csv":
                    current_df = pd.read_csv(absolute_load_path)
                elif suffix == ".json":
                    current_df = pd.read_json(absolute_load_path)
                else:
                    raise ValueError(f"Unsupported input file format: {suffix}")

        transformer_context["initial_input"] = current_df.to_dict("records")
        log.info("service.run.loaded_initial_data", shape=current_df.shape)

        # --- 3. SEQUENTIAL STEP EXECUTION ---
        artifacts_manifest = {"attachments": []}
        transformer_context["artifacts"] = artifacts_manifest

        for i, step in enumerate(script_data.get("steps", [])):
            engine_name = step.get("engine")
            engine = self.engines.get(engine_name)
            if not engine:
                raise ValueError(f"Unknown transformer engine: '{engine_name}'")

            step_log = log.bind(
                step_index=i, step_name=step.get("name"), engine=engine_name
            )
            step_log.info("service.run.executing_step")

            operations_template = step.get("operations", [])
            if not operations_template and "operation" in step:
                operations_template = [step["operation"]]

            rendered_operations = recursive_render(
                operations_template, transformer_context
            )

            current_df = await engine.transform(
                data=current_df,
                operations=rendered_operations,
                context=transformer_context,
            )

        # --- 4. FINALIZE AND RETURN OUTPUT ---
        log.info("service.run.finished", final_shape=current_df.shape)

        if artifacts_manifest.get("html_body") or artifacts_manifest.get("attachments"):
            return {"artifacts": artifacts_manifest}
        else:
            return current_df.to_dict("records")

    def _get_data_from_input(self, input_data: Any) -> List[Dict]:
        """
        Extracts the list of records from the connector's typical output
        format, which is {"step_name": [...]}. This makes the pipeline robust.
        """
        if isinstance(input_data, list):
            return input_data
        if isinstance(input_data, dict):
            # Find the first value in the dictionary that is a list.
            for value in input_data.values():
                if isinstance(value, list):
                    return value
        raise ValueError("Could not find a list of records in the input JSON data.")
