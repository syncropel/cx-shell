# ~/repositories/cx-shell/src/cx_shell/engine/transformer/engines/jinja_engine.py

from typing import Any, Dict, List, TYPE_CHECKING

import pandas as pd
import structlog
from jinja2 import ChoiceLoader, Environment, FileSystemLoader, select_autoescape
from pydantic import BaseModel, Field

from ..operations.file_format_ops import ArtifactType
from ..vfs_client import AbstractVfsClient
from .base import BaseTransformEngine

# Use TYPE_CHECKING to import RunContext for type hinting without creating a circular import at runtime
if TYPE_CHECKING:
    from ....engine.context import RunContext

logger = structlog.get_logger(__name__)


class RenderTemplateOp(BaseModel):
    """
    Defines the declarative operation for rendering a Jinja2 template.
    This model is used to validate the 'operation' block in a transformer script.
    """

    type: str = "render_template"
    template_path: str = Field(
        ...,
        description="Path to the Jinja2 template file (can be relative, absolute, or use a cx URI scheme).",
    )
    target_path: str = Field(
        ..., description="Output path for the rendered artifact file."
    )
    artifact_type: ArtifactType = Field(
        "attachment",
        description="The semantic role of the rendered file (e.g., 'html_body', 'attachment').",
    )


class JinjaEngine(BaseTransformEngine):
    """
    A transformation engine that uses Jinja2 to render a template into a file.

    This engine is designed for creating presentation artifacts, such as HTML reports
    or email bodies, from a DataFrame and other summary data calculated in previous
    steps. It does not modify the DataFrame itself but produces a file as a
    side-effect and updates the run's "Artifact Manifest".
    """

    engine_name = "jinja"

    def __init__(self, vfs_client: AbstractVfsClient):
        """
        Initializes the JinjaEngine with a VFS client and a robust Jinja environment.

        Args:
            vfs_client: An instance of a VFS client for writing the output file.
        """
        self.vfs = vfs_client

        # This robust loader configuration handles absolute paths and paths relative
        # to the current working directory as a fallback.
        self.jinja_env = Environment(
            loader=ChoiceLoader([FileSystemLoader("."), FileSystemLoader("/")]),
            autoescape=select_autoescape(["html", "xml"]),  # Security best practice
            lstrip_blocks=True,
            trim_blocks=True,
        )

    async def transform(
        self,
        data: pd.DataFrame,
        operations: List[Dict[str, Any]],
        context: Dict[str, Any],
    ) -> pd.DataFrame:
        """
        Renders a Jinja2 template using the DataFrame and the run context,
        then saves the result to a file and updates the Artifact Manifest.

        Args:
            data: The input DataFrame, which will be made available to the template.
            operations: A list of declarative operations (expects one 'render_template' op).
            context: The shared run context from the TransformerService, which must contain
                    a 'run_context' key pointing to the main ScriptEngine's RunContext.

        Returns:
            The original, unmodified DataFrame to be passed to the next step.
        """
        log = logger.bind(engine=self.engine_name)
        op_data = operations[0]
        op_model = RenderTemplateOp.model_validate(op_data)

        log.info(
            "Executing: render_template",
            template=op_model.template_path,
            target=op_model.target_path,
            artifact_type=op_model.artifact_type,
        )

        try:
            # --- START OF DEFINITIVE, CONTEXT-AWARE PATH RESOLUTION ---
            # 1. Get the RunContext object that was passed into the context dictionary.
            run_context: "RunContext" = context.get("run_context")
            if not run_context:
                raise RuntimeError(
                    "JinjaEngine failed: RunContext was not found in the transformer's execution context."
                )

            # 2. Use the RunContext's own resolver method, which has the necessary context.
            template_path = run_context.resolve_path_in_context(op_model.template_path)
            log.info("jinja.template_path_resolved", resolved_path=str(template_path))

            # 3. Get the template using its fully resolved, absolute path.
            template = self.jinja_env.get_template(str(template_path))
            # --- END OF DEFINITIVE FIX ---

        except Exception as e:
            log.error(
                "jinja.template_load_failed",
                path=op_model.template_path,
                error=str(e),
                exc_info=True,
            )
            raise IOError(
                f"Failed to load Jinja2 template from '{op_model.template_path}': {e}"
            ) from e

        # Prepare the full context available to the template.
        template_context = {
            **context,
            "records": data.to_dict("records"),
            "column_names": data.columns.tolist(),
            "record_count": len(data),
        }

        # Render the template with the combined context
        rendered_content = template.render(template_context)
        content_bytes = rendered_content.encode("utf-8")

        # Save the rendered content to the target file via the VFS client
        canonical_path = await self.vfs.write(
            path=op_model.target_path, content=content_bytes, context=context
        )

        # Populate the structured Artifact Manifest in the run context
        artifacts_manifest = context.get("artifacts", {})
        if op_model.artifact_type == "html_body":
            artifacts_manifest["html_body"] = canonical_path
        else:  # The default is 'attachment'.
            artifacts_manifest.setdefault("attachments", []).append(canonical_path)

        log.info(
            "template.render.success",
            path=canonical_path,
            bytes_written=len(content_bytes),
        )

        # This engine produces an artifact; it doesn't modify the DataFrame.
        return data
