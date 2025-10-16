# [REPLACE] ~/repositories/cx-shell/src/cx_shell/engine/connector/engine.py

import json
import uuid
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Awaitable,
    Callable,
    Dict,
    Literal,
    Optional,
    Tuple,
)
import structlog
import yaml
import networkx as nx
import pandas as pd
from ...utils import CX_HOME
from jinja2 import Environment, TemplateError
from cx_core_schemas.connector_script import ConnectorStep
from cx_core_schemas.vfs import RunManifest, StepResult, Artifact
from ...management.cache_manager import CacheManager
from ...engine.context import RunContext
from ...management.notebook_parser import NotebookParser
from cx_core_schemas.notebook import ContextualPage
from cx_core_schemas.connector_script import FileToWrite, WriteFilesAction
from cx_core_schemas.server_schemas import BlockOutput, DataRef, SduiPayload
from .utils import safe_serialize
from .config import ConnectionResolver

if TYPE_CHECKING:
    from .service import ConnectorService

    # Add SessionState here for clean type checking

logger = structlog.get_logger(__name__)
RUNS_DIR = CX_HOME / "runs"


def sql_quote_filter(value):
    if value is None:
        return "NULL"
    return f"'{str(value).replace("'", "''")}'"


class ScriptEngine:
    """Orchestrates the execution of a declarative workflow script with caching and lineage."""

    def __init__(self, resolver: "ConnectionResolver", connector: "ConnectorService"):
        """
        Initializes the ScriptEngine.
        This constructor is now stateless and only holds its direct, stateless dependencies.
        """
        self.resolver = resolver
        self.connector = connector
        self.cache_manager = CacheManager()
        RUNS_DIR.mkdir(exist_ok=True, parents=True)

        self.jinja_env = Environment()
        self.jinja_env.filters["sqlquote"] = sql_quote_filter

        def get_now(tz: str | None = None) -> datetime:
            if tz and tz.lower() == "utc":
                return datetime.now(timezone.utc)
            return datetime.now()

        self.jinja_env.globals["now"] = get_now

    def _build_dependency_graph(self, steps: list[ConnectorStep]) -> nx.DiGraph:
        # [This method remains unchanged]
        dag = nx.DiGraph()
        step_map = {step.id: step for step in steps}
        for step in steps:
            dag.add_node(step.id, step_data=step)
            if step.depends_on:
                for dep_id in step.depends_on:
                    if dep_id in step_map:
                        dag.add_edge(dep_id, step.id)
                    else:
                        raise ValueError(
                            f"Step '{step.id}' has an invalid dependency: '{dep_id}'"
                        )
        if not nx.is_directed_acyclic_graph(dag):
            cycle = nx.find_cycle(dag, orientation="original")
            raise ValueError(f"Workflow contains a circular dependency: {cycle}")
        return dag

    def _calculate_cache_key(
        self, step: ConnectorStep, parent_hashes: Dict[str, str]
    ) -> str:
        # [This method remains unchanged]
        hasher = hashlib.sha256()
        step_def_dict = step.model_dump()
        step_def_str = json.dumps(step_def_dict, sort_keys=True)
        hasher.update(step_def_str.encode("utf-8"))
        sorted_parent_hashes = sorted(parent_hashes.items())
        for step_id, hash_val in sorted_parent_hashes:
            hasher.update(f"{step_id}:{hash_val}".encode("utf-8"))
        return f"sha256:{hasher.hexdigest()}"

    def _find_cached_step(self, cache_key: str) -> Optional[StepResult]:
        # [This method remains unchanged]
        try:
            for manifest_file in sorted(
                RUNS_DIR.glob("**/manifest.json"), reverse=True
            )[:100]:
                manifest_data = json.loads(manifest_file.read_text())
                for step_result in manifest_data.get("steps", []):
                    if (
                        step_result.get("cache_key") == cache_key
                        and step_result.get("status") == "completed"
                    ):
                        logger.debug(
                            "engine.cache.hit",
                            cache_key=cache_key,
                            found_in_run=manifest_data.get("run_id"),
                        )
                        return StepResult(**step_result)
        except Exception as e:
            logger.warn("engine.cache.scan_error", error=str(e))
        logger.debug("engine.cache.miss", cache_key=cache_key)
        return None

    def _unwrap_engine_result(self, raw_result: Any) -> Any:
        """
        Applies the default unwrapping logic for results coming from the engine.
        If a result is a dict with a single key, it returns the value of that key.
        This is the standard pattern for results from ad-hoc script runs.
        """
        if (
            isinstance(raw_result, dict)
            and len(raw_result) == 1
            and "error" not in raw_result
        ):
            unwrapped = next(iter(raw_result.values()))
            logger.debug(
                "engine.result.unwrapped", original_keys=list(raw_result.keys())
            )
            return unwrapped
        return raw_result

    async def _execute_step(
        self,
        context: RunContext,
        validated_step: ConnectorStep,
        active_session: Any = None,
        stateful_strategy: Any = None,
    ) -> Any:
        """
        Executes a single, validated step from a computational document.
        This method is the core dispatcher, routing execution to the correct
        strategy based on the step's 'engine' (for notebooks) or 'run' action (for flows).
        """
        log = logger.bind(step_id=validated_step.id, step_name=validated_step.name)

        # Step 1: Handle Non-Executable Blocks
        if validated_step.engine == "markdown":
            log.debug("engine.step.skipping_markdown_block")
            return None

        # Step 2: Resolve Connection & Strategy (but only if a source is provided)
        connection, secrets, strategy = None, None, None
        if validated_step.connection_source:
            connection, secrets = await context.services.resolver.resolve(
                validated_step.connection_source
            )
            strategy = self.connector._get_strategy_for_connection_model(connection)

        # Step 3: Engine-Aware Dispatcher

        # --- PATH A: Step is defined by a `run` block (from a flow OR a notebook) ---
        if validated_step.run:
            action = validated_step.run
            log.debug(
                "engine.step.execution_started_from_run_block", action=action.action
            )

            # Certain actions are connectionless and handled by internal strategies.
            if action.action == "run_python_script":
                python_strategy = self.connector.strategies.get("python-sandboxed")
                return await python_strategy.run_python_script(
                    None, action.model_dump(), context
                )
            if action.action == "run_flow":
                flow_path = context.services.flow_manager._find_flow(action.flow_name)
                sub_flow_context = RunContext(
                    services=context.services,
                    session=context.session,
                    script_input=action.inputs,
                    piped_input=context.piped_input,
                    current_flow_path=flow_path,
                )
                return await self.run_script(sub_flow_context)

            # All other actions require a connection and strategy.
            if not strategy:
                raise ValueError(
                    f"Step '{validated_step.name or validated_step.id}' with action '{action.action}' requires a 'connection_source'."
                )

            if hasattr(strategy, action.action):
                method_to_call = getattr(strategy, action.action)
                # Dispatch to the correct strategy method with the correct arguments.
                if action.action in ["write_files", "aggregate_content"]:
                    return await method_to_call(
                        connection, action.model_dump(), context
                    )
                elif action.action == "run_declarative_action":
                    return await method_to_call(
                        connection=connection,
                        secrets=secrets,
                        action_params=action.model_dump(),
                        script_input=action.context,
                    )
                elif action.action == "read_content":
                    vfs_response = await strategy.get_content(
                        path_parts=[action.path], connection=connection, secrets=secrets
                    )
                    return vfs_response.content
                elif action.action == "run_sql_query":
                    # The ScriptEngine's responsibility is now much simpler.
                    # It just needs to call the strategy's public method that matches the action name.
                    # It passes the full action payload and the run context. The strategy itself
                    # is now responsible for parsing these arguments.
                    return await strategy.run_sql_query(
                        connection, secrets, action.model_dump(), context
                    )

            raise NotImplementedError(
                f"Action '{action.action}' is not implemented by the '{strategy.strategy_key}' strategy."
            )

        # --- PATH A: Notebook Block Execution (driven by `engine` key) ---
        if validated_step.engine:
            engine_name = validated_step.engine
            log.debug("engine.step.execution_started_from_notebook", engine=engine_name)

            if engine_name == "artifact":
                log.info("artifact_engine.begin")

                # 1. Get the input data from the context
                if not validated_step.inputs:
                    raise ValueError(
                        "`engine: artifact` requires an `inputs` field specifying the data to save."
                    )

                block_id, output_name = validated_step.inputs[0].split(".", 1)
                input_data = context.steps[block_id]["outputs"][output_name]

                # 2. Parse the operation from the block's content
                operation = yaml.safe_load(validated_step.content)
                target_format = operation.get("format")
                target_path_template = operation.get("target_path")

                if not all([target_format, target_path_template]):
                    raise ValueError(
                        "`artifact` block requires 'format' and 'target_path' in its content."
                    )

                # --- START OF DEFINITIVE FIX ---
                # 3. Resolve the path using the CONTEXT-AWARE resolver
                resolved_target_path = context.resolve_path_in_context(
                    target_path_template
                )
                log.info(
                    "artifact_engine.path_resolved",
                    source=target_path_template,
                    resolved=str(resolved_target_path),
                )
                # --- END OF DEFINITIVE FIX ---

                # 4. Convert the in-memory data to the desired string format
                output_content = ""
                df = pd.DataFrame(input_data)

                if target_format == "json":
                    output_content = df.to_json(orient="records", indent=2)
                elif target_format == "csv":
                    output_content = df.to_csv(index=False)
                else:
                    raise NotImplementedError(
                        f"Artifact format '{target_format}' is not supported."
                    )

                # 5. Construct a 'write_files' action with the ABSOLUTE path and dispatch it
                write_action = WriteFilesAction(
                    action="write_files",
                    files=[
                        FileToWrite(
                            path=str(resolved_target_path), content=output_content
                        )
                    ],
                )

                fs_strategy = context.services.connector_service.strategies.get(
                    "fs-declarative"
                )
                if not fs_strategy:
                    raise RuntimeError("Internal Error: Filesystem strategy not found.")

                fs_connection, _ = await context.services.resolver.resolve(
                    "user:fs_generic"
                )

                return await fs_strategy.write_files(
                    connection=fs_connection,
                    action_params=write_action.model_dump(),
                    run_context=context,
                )

            if engine_name == "transform":
                transformer = context.services.transformer_service
                script_data = yaml.safe_load(validated_step.content)

                # --- START OF DEFINITIVE, CONTEXT-AWARE LOGIC ---

                # 1. Determine the primary input data for the transformer.
                #    The 'inputs' field of the block tells us which previous step's
                #    output to use.
                input_data_for_transformer = None
                if validated_step.inputs:
                    # For simplicity, we assume the first declared input provides the primary DataFrame.
                    # e.g., inputs: [get_data.data]
                    block_id, output_name = validated_step.inputs[0].split(".", 1)

                    # Safely access the nested data from the context's steps dictionary
                    input_data_for_transformer = (
                        context.steps.get(block_id, {})
                        .get("outputs", {})
                        .get(output_name)
                    )

                # 2. The RunContext is already correctly configured. The transformer's
                #    `run` method is now designed to handle it. We just need to
                #    update its `piped_input` for this specific call.
                context.piped_input = input_data_for_transformer

                log.debug(
                    "engine.dispatch.transform",
                    input_source_block=validated_step.inputs[0]
                    if validated_step.inputs
                    else "N/A",
                    input_data_type=type(input_data_for_transformer).__name__,
                )

                # 3. Call the transformer's run method, passing the main context directly.
                return await transformer.run(
                    script_data=script_data, run_context=context
                )
                # --- END OF DEFINITIVE FIX ---

            elif engine_name in ["cx-action", "ui-component"]:
                if engine_name == "cx-action":
                    command_text_template = validated_step.content
                    if not command_text_template:
                        return None
                    render_context = {
                        **context.model_dump(),
                        **context.session.variables,
                    }
                    rendered_command = self.jinja_env.from_string(
                        command_text_template
                    ).render(render_context)
                    log.info("engine.cx-action.executing", command=rendered_command)
                    result, _, _ = await self.connector.executor._execute_pipeline(
                        run_context=context, command_text=rendered_command
                    )
                    return result

                elif engine_name == "ui-component":
                    log.debug("engine.step.passing_through_ui_component")
                    ui_component_definition = yaml.safe_load(validated_step.content)
                    if validated_step.inputs:
                        input_data = {}
                        for input_str in validated_step.inputs:
                            # This assumes a simple 1:1 mapping for now.
                            block_id, output_name = input_str.split(".", 1)
                            input_data.update(context.steps[block_id]["outputs"])

                        def render_ui(data_struct, render_ctx):
                            if isinstance(data_struct, dict):
                                return {
                                    k: render_ui(v, render_ctx)
                                    for k, v in data_struct.items()
                                }
                            if isinstance(data_struct, list):
                                return [render_ui(i, render_ctx) for i in data_struct]
                            if isinstance(data_struct, str) and "{{" in data_struct:
                                template = self.jinja_env.from_string(data_struct)
                                return template.render(**render_ctx)
                            return data_struct

                        render_context = {"steps": context.steps, **input_data}
                        return render_ui(ui_component_definition, render_context)
                    return ui_component_definition
            # For all other engines, a strategy (and thus a connection) is required.
            elif not strategy:
                raise ValueError(
                    f"Block '{validated_step.id}' with engine '{engine_name}' requires a 'connection_source'."
                )

            elif engine_name == "sql":
                return await strategy.execute_query(
                    query=validated_step.content,
                    params=context.script_input,
                    connection=connection,
                    secrets=secrets,
                )

            else:
                raise NotImplementedError(
                    f"Execution for notebook engine '{engine_name}' is not yet implemented."
                )

        # --- PATH B: Flow/Notebook `run` Block Execution ---
        elif validated_step.run:
            # The 'run' block from the notebook will now correctly enter this path,
            # just like a step from a .flow.yaml file.
            action = validated_step.run
            log.debug(
                "engine.step.execution_started_from_run_block", action=action.action
            )

            if not strategy:
                raise ValueError(
                    f"Step '{validated_step.name or validated_step.id}' requires a 'connection_source' for action '{action.action}'."
                )

            # This is the explicit dispatcher for all known flow actions.
            if action.action == "read_content":
                vfs_response = await strategy.get_content(
                    path_parts=[action.path], connection=connection, secrets=secrets
                )
                return vfs_response.content

            elif action.action == "browse_path":
                return await strategy.browse_path(
                    path_parts=[action.path], connection=connection, secrets=secrets
                )

            elif action.action == "run_declarative_action":
                return await strategy.run_declarative_action(
                    connection=connection,
                    secrets=secrets,
                    action_params=action.model_dump(),
                    script_input=action.context,
                )

            elif action.action in ["write_files", "aggregate_content"]:
                method_to_call = getattr(strategy, action.action)
                return await method_to_call(connection, action.model_dump(), context)

            elif action.action == "run_python_script":
                python_strategy = self.connector.strategies.get("python-sandboxed")
                return await python_strategy.run_python_script(
                    connection, action.model_dump(), context
                )

            elif action.action == "run_sql_query":
                # The ScriptEngine's responsibility is now much simpler.
                # It just needs to call the strategy's public method that matches the action name.
                # It passes the full action payload and the run context. The strategy itself
                # is now responsible for parsing these arguments.
                return await strategy.run_sql_query(
                    connection, secrets, action.model_dump(), context
                )

            elif action.action == "run_flow":
                flow_path = context.services.flow_manager._find_flow(action.flow_name)
                sub_flow_context = RunContext(
                    services=context.services,
                    session=context.session,
                    script_input=action.inputs,
                    piped_input=context.piped_input,
                    current_flow_path=flow_path,
                )
                return await self.run_script(sub_flow_context)

            raise NotImplementedError(
                f"Action '{action.action}' is not implemented by the '{strategy.strategy_key}' strategy."
            )

        else:
            raise ValueError(
                f"Step '{validated_step.id}' is invalid: must have either an 'engine' or a 'run' block."
            )

    async def run_script(
        self,
        context: RunContext,
        no_cache: bool = False,
        status_callback: Optional[Callable[[str, str, Any], Awaitable[None]]] = None,
    ):
        """
        Loads a computational document (.flow.yaml or .cx.md) from the path
        specified in the RunContext, detects its type, parses it, validates inputs,
        and hands off to the execution model.
        """
        script_path = context.current_flow_path
        if not script_path:
            raise ValueError(
                "RunContext must have a current_flow_path to execute a script."
            )

        log = logger.bind(script_path=str(script_path))
        log.info("engine.load_script.begin")

        script_data: Any
        run_type: Literal["flow", "notebook"]

        if script_path.name.endswith(".cx.md"):
            log.debug("engine.load_script.detected_notebook")
            parser = NotebookParser()
            script_data = parser.parse(script_path)
            run_type = "notebook"
        elif script_path.name.endswith((".flow.yaml", ".flow.yml")):
            log.debug("engine.load_script.detected_flow")
            with open(script_path, "r", encoding="utf-8") as f:
                script_data = yaml.safe_load(f)
            run_type = "flow"
        else:
            raise ValueError(f"Unsupported script type: '{script_path.suffix}'")

        context.run_type = run_type

        if run_type == "flow":
            defined_inputs = script_data.get("inputs", {})
        else:
            defined_inputs = {k: v.model_dump() for k, v in script_data.inputs.items()}

        if defined_inputs:
            log.debug(
                "engine.input.validating",
                defined_inputs=defined_inputs,
                received_inputs=context.script_input,
            )
            for name, param_spec in defined_inputs.items():
                if not isinstance(param_spec, dict):
                    log.warn(
                        "engine.input.invalid_spec", param_name=name, spec=param_spec
                    )
                    continue
                is_required = param_spec.get("required", False)
                if is_required and name not in context.script_input:
                    raise ValueError(
                        f"Missing required input parameter for script: '{name}'"
                    )
                if name not in context.script_input and "default" in param_spec:
                    context.script_input[name] = param_spec.get("default")
                    log.debug(
                        "engine.input.default_applied",
                        param_name=name,
                        value=context.script_input[name],
                    )

        log.debug("engine.input.final_script_input", script_input=context.script_input)

        # Pass the status_callback down to the next method in the chain.
        return await self.run_script_model(
            context, script_data, no_cache, status_callback=status_callback
        )

    async def run_script_model(
        self,
        context: RunContext,
        script_data: Any,
        no_cache: bool = False,
        status_callback: Optional[Callable[[str, str, Any], Awaitable[None]]] = None,
    ):
        """
        Executes a pre-parsed computational document, providing real-time status
        updates via an optional async callback. This version implements the
        "Hybrid Claim Check" pattern for all block outputs.

        It intelligently decides whether to embed small results directly in the
        event payload for speed, or to create a secure, retrievable reference
        (a "claim check") for large data artifacts to ensure a responsive UI and
        scalable data handling.

        Args:
            context: The stateful RunContext for this execution.
            script_data: The pre-parsed ContextualPage or ConnectorScript model.
            no_cache: If True, bypasses cache lookups and forces re-execution.
            status_callback: An async function to call for real-time status updates.
        """
        if isinstance(script_data, ContextualPage):
            flow_id = script_data.name
            raw_steps_list = [
                block.model_dump(by_alias=True) for block in script_data.blocks
            ]
        else:
            flow_id = script_data.get("name")
            raw_steps_list = script_data.get("steps", [])

        log = logger.bind(script_name=flow_id, no_cache=no_cache)
        log.info("engine.run.begin")

        run_id = f"run_{uuid.uuid4().hex[:12]}"
        run_dir = RUNS_DIR / run_id
        run_dir.mkdir(parents=True)
        manifest = RunManifest(
            run_id=run_id,
            flow_id=flow_id,
            status="running",
            timestamp_utc=datetime.now(timezone.utc),
            parameters=context.script_input,
            steps=[],
        )

        dag = self._build_dependency_graph([ConnectorStep(**s) for s in raw_steps_list])
        topological_generations = list(nx.topological_generations(dag))
        final_results: Dict[str, Any] = {}
        recursive_render = recursive_render_factory(self.jinja_env)

        # Define the threshold for embedding data directly in bytes
        EMBED_THRESHOLD_BYTES = 256 * 1024  # 256KB

        try:
            for generation in topological_generations:
                for step_id in generation:
                    step_start_time = datetime.now(timezone.utc)
                    raw_step_dict = dag.nodes[step_id]["step_data"].model_dump(
                        by_alias=True
                    )

                    full_render_context = {
                        "page": script_data.model_dump()
                        if isinstance(script_data, ContextualPage)
                        else script_data,
                        "inputs": context.script_input,
                        "steps": context.steps,
                        **context.session.variables,
                        **(raw_step_dict.get("context") or {}),
                    }

                    if if_condition := raw_step_dict.get("if"):
                        try:
                            if not self.jinja_env.compile_expression(if_condition)(
                                **full_render_context
                            ):
                                log.info(
                                    "engine.step.skipped",
                                    step_id=step_id,
                                    reason="if_condition",
                                )
                                if status_callback:
                                    await status_callback(
                                        step_id, "skipped", "Conditional returned false"
                                    )
                                manifest.steps.append(
                                    StepResult(
                                        step_id=step_id,
                                        status="skipped",
                                        summary="Skipped: 'if' condition was false.",
                                        cache_key="",
                                        cache_hit=False,
                                    )
                                )
                                context.steps[step_id] = {
                                    "result": None,
                                    "outputs": {},
                                    "output_hash": None,
                                }
                                final_results[step_id] = None
                                continue
                        except Exception as e:
                            raise ValueError(
                                f"Failed to evaluate 'if' condition for step '{step_id}': {e}"
                            ) from e

                    if status_callback:
                        await status_callback(step_id, "running", None)

                    try:
                        rendered_step_dict = recursive_render(
                            raw_step_dict, full_render_context
                        )
                        validated_step = ConnectorStep(**rendered_step_dict)
                    except Exception as e:
                        raise ValueError(
                            f"Failed to render/validate step '{step_id}': {e}"
                        ) from e

                    parent_hashes = {
                        pred: context.steps[pred]["output_hash"]
                        for pred in dag.predecessors(step_id)
                    }
                    cache_key = self._calculate_cache_key(validated_step, parent_hashes)
                    cached_step = (
                        None if no_cache else self._find_cached_step(cache_key)
                    )

                    if cached_step:
                        raw_result_wrapped = (
                            json.loads(
                                self.cache_manager.read_bytes(cached_step.output_hash)
                            )
                            if cached_step.output_hash
                            else None
                        )
                        step_result_obj = cached_step
                        step_result_obj.cache_hit = True
                        log.info("engine.step.cache_hit", step_id=step_id)
                    else:
                        raw_result_wrapped = await self._execute_step(
                            context, validated_step
                        )
                        output_hash = self.cache_manager.write_json(raw_result_wrapped)
                        step_result_obj = StepResult(
                            step_id=step_id,
                            status="completed",
                            summary="Completed successfully.",
                            cache_key=cache_key,
                            cache_hit=False,
                            output_hash=output_hash,
                        )

                    step_end_time = datetime.now(timezone.utc)
                    duration_ms = int(
                        (step_end_time - step_start_time).total_seconds() * 1000
                    )

                    raw_result = self._unwrap_engine_result(raw_result_wrapped)

                    if isinstance(raw_result, dict) and "error" in raw_result:
                        error_message = raw_result["error"]
                        if status_callback:
                            await status_callback(
                                step_id,
                                "error",
                                {"error": error_message, "duration_ms": duration_ms},
                            )
                        raise RuntimeError(f"Step '{step_id}' failed: {error_message}")

                    if status_callback:
                        try:
                            result_bytes = json.dumps(
                                safe_serialize(raw_result)
                            ).encode("utf-8")
                            result_size = len(result_bytes)
                        except (TypeError, OverflowError):
                            result_size = float("inf")

                        if result_size < EMBED_THRESHOLD_BYTES:
                            log.debug(
                                "engine.output.embedding_inline",
                                step_id=step_id,
                                size_bytes=result_size,
                            )
                            sdui_payload = self._schematize_result(raw_result)
                            block_output = BlockOutput(inline_data=sdui_payload)
                        else:
                            log.info(
                                "engine.output.creating_data_ref",
                                step_id=step_id,
                                size_bytes=result_size,
                            )
                            renderer_hint, metadata = self._get_result_metadata(
                                raw_result
                            )
                            access_url = f"http://localhost:8888/artifacts/{step_result_obj.output_hash}"
                            data_ref = DataRef(
                                artifact_id=step_result_obj.output_hash,
                                renderer_hint=renderer_hint,
                                metadata=metadata,
                                access_url=access_url,
                            )
                            block_output = BlockOutput(data_ref=data_ref)

                        await status_callback(
                            step_id,
                            "success",
                            {"output": block_output, "duration_ms": duration_ms},
                        )

                    step_outputs = {}
                    outputs_spec = raw_step_dict.get("outputs")
                    if isinstance(outputs_spec, list):
                        for output_name in outputs_spec:
                            step_outputs[output_name] = raw_result
                    elif isinstance(outputs_spec, dict):
                        import jmespath

                        for output_name, jmespath_query in outputs_spec.items():
                            try:
                                step_outputs[output_name] = jmespath.search(
                                    jmespath_query, raw_result
                                )
                            except Exception as e:
                                log.warning(
                                    "engine.outputs.jmespath_failed",
                                    query=jmespath_query,
                                    error=str(e),
                                )
                                step_outputs[output_name] = None

                    manifest.steps.append(step_result_obj)
                    final_results[step_id] = raw_result
                    context.steps[step_id] = {
                        "result": raw_result,
                        "outputs": step_outputs,
                        "output_hash": step_result_obj.output_hash,
                    }

                    if isinstance(raw_result, dict) and "artifacts" in raw_result:
                        artifacts_data = raw_result.get("artifacts")
                        if isinstance(artifacts_data, dict):
                            for artifact_type, paths in artifacts_data.items():
                                path_list = (
                                    paths if isinstance(paths, list) else [paths]
                                )
                                for file_path_str in path_list:
                                    try:
                                        file_path = Path(
                                            file_path_str.replace("file://", "")
                                        )
                                        if file_path.exists():
                                            file_bytes = file_path.read_bytes()
                                            content_hash = self.cache_manager.write(
                                                file_bytes
                                            )
                                            manifest.artifacts[file_path.name] = (
                                                Artifact(
                                                    content_hash=content_hash,
                                                    mime_type="application/octet-stream",  # A generic default
                                                    size_bytes=file_path.stat().st_size,
                                                )
                                            )
                                    except Exception as e:
                                        log.warning(
                                            "engine.artifact.processing_failed",
                                            path=file_path_str,
                                            error=str(e),
                                        )

            manifest.status = "completed"
            log.info("engine.run.success")
            return final_results
        except Exception as e:
            if (
                status_callback
                and "step_id" in locals()
                and "step_start_time" in locals()
            ):
                duration_ms = int(
                    (datetime.now(timezone.utc) - step_start_time).total_seconds()
                    * 1000
                )
                await status_callback(
                    locals()["step_id"],
                    "error",
                    {"error": str(e), "duration_ms": duration_ms},
                )

            manifest.status = "failed"
            log.error("engine.run.failed", error=str(e), exc_info=True)
            manifest.steps.append(
                StepResult(
                    step_id="error",
                    status="failed",
                    summary=str(e),
                    cache_key="",
                    cache_hit=False,
                )
            )
            final_results["error"] = f"{type(e).__name__}: {e}"
            return final_results
        finally:
            manifest_path = run_dir / "manifest.json"
            manifest_path.write_text(manifest.model_dump_json(indent=2))
            log.info("engine.run.manifest_written", path=str(manifest_path))

    def _schematize_result(self, raw_result: Any) -> SduiPayload:
        """Inspects a raw result and wraps it in a default SDUI payload."""
        if (
            isinstance(raw_result, list)
            and raw_result
            and isinstance(raw_result[0], dict)
        ):
            return SduiPayload(ui_component="table", props={"data": raw_result})
        if isinstance(raw_result, (dict, list)):
            return SduiPayload(ui_component="json", props={"data": raw_result})
        if isinstance(raw_result, str):
            return SduiPayload(ui_component="text", props={"content": raw_result})
        # Default fallback for other types
        return SduiPayload(
            ui_component="json", props={"data": safe_serialize(raw_result)}
        )

    def _get_result_metadata(self, raw_result: Any) -> Tuple[str, Dict]:
        """Inspects a raw result and extracts metadata for the DataRef."""
        if (
            isinstance(raw_result, list)
            and raw_result
            and isinstance(raw_result[0], dict)
        ):
            renderer_hint = "table"
            metadata = {
                "record_count": len(raw_result),
                "columns": list(raw_result[0].keys()),
            }
            return renderer_hint, metadata

        # Default fallback
        return "json", {}


def recursive_render_factory(jinja_env):
    """Factory to create a simple, non-mutating recursive rendering function."""

    def recursive_render(data: Any, context: Dict):
        if isinstance(data, dict):
            return {k: recursive_render(v, context) for k, v in data.items()}
        if isinstance(data, list):
            return [recursive_render(i, context) for i in data]
        if isinstance(data, str):
            if "{{" in data:
                try:
                    return jinja_env.from_string(data).render(**context)
                except TemplateError as e:
                    raise ValueError(
                        f"Jinja rendering failed for template '{data}': {e}"
                    ) from e
        return data

    return recursive_render
