# ~/repositories/cx-shell/src/cx_shell/management/publisher.py
from typing import Dict, Any
from pathlib import Path
import structlog

from ..engine.context import RunContext
from .notebook_parser import NotebookParser
from .renderers.base import BaseRenderer
from .renderers.archive_renderer import ArchiveRenderer
from .renderers.html_renderer import HTMLRenderer  # <-- ADD THIS IMPORT
from .renderers.pdf_renderer import PDFRenderer  # <-- ADD THIS IMPORT

logger = structlog.get_logger(__name__)


class Publisher:
    """
    Orchestrates the execution and rendering of a Contextual Page into a
    static, shareable artifact.
    """

    def __init__(self):
        self.renderers: Dict[str, BaseRenderer] = {
            "archive": ArchiveRenderer(),
            "html": HTMLRenderer(),  # <-- REGISTER HTML RENDERER
            "pdf": PDFRenderer(),  # <-- REGISTER PDF RENDERER
        }

    async def publish(self, run_context: RunContext, named_args: Dict[str, Any]):
        # ... existing code for parsing named_args ...
        page_name = named_args.pop("name")
        target_format = named_args.pop("to")
        output_path_str = named_args.pop("output", None)

        # All remaining key-values are renderer parameters
        renderer_params = named_args

        log = logger.bind(page_name=page_name, format=target_format)
        log.info("publish.begin")

        # 1. Find and Execute the Page
        page_path = run_context.services.flow_manager._find_flow(page_name)

        page_run_context = RunContext(
            services=run_context.services,
            session=run_context.session,
            current_flow_path=page_path,
        )
        results = await run_context.services.script_engine.run_script(
            context=page_run_context
        )

        # 2. Parse the original page structure
        parser = NotebookParser()
        page_model = parser.parse(page_path)

        # 3. Select and run the renderer
        renderer = self.renderers.get(target_format)
        if not renderer:
            raise ValueError(
                f"Unsupported publish format: '{target_format}'. Supported formats: {list(self.renderers.keys())}"
            )

        # --- PASS RENDERER PARAMS TO THE RENDER METHOD ---
        rendered_output = await renderer.render(page_model, results, renderer_params)

        # 4. Save the artifact
        if not output_path_str:
            ext_map = {"archive": ".cx.md", "html": ".html", "pdf": ".pdf"}
            ext = ext_map.get(target_format, f".{target_format}")
            output_path_str = f"./{page_path.stem}{ext}"

        output_path = Path(output_path_str).resolve()
        output_path.parent.mkdir(parents=True, exist_ok=True)

        if isinstance(rendered_output, str):
            output_path.write_text(rendered_output, encoding="utf-8")
        else:
            output_path.write_bytes(rendered_output)

        log.info("publish.success", output_path=str(output_path))
        return f"Successfully published page to: {output_path}"
