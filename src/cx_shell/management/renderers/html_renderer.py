# ~/repositories/cx-shell/src/cx_shell/management/renderers/html_renderer.py

from datetime import datetime, timezone
from typing import Any, Dict

import markdown
import pandas as pd
import structlog
import yaml
from jinja2 import Environment, PackageLoader, select_autoescape

from cx_core_schemas.notebook import ContextualPage
from .base import BaseRenderer

logger = structlog.get_logger(__name__)


class HTMLRenderer(BaseRenderer):
    """
    Renders a Contextual Page and its results into a beautiful, standalone HTML report.
    """

    renderer_key = "html"

    def __init__(self):
        """Initializes the renderer and its Jinja2 environment."""
        try:
            # The PackageLoader is the robust way to find templates inside an installed package.
            # This ensures it works correctly in both development and a frozen PyInstaller executable.
            self.jinja_env = Environment(
                loader=PackageLoader("cx_shell", "assets/templates/publish"),
                autoescape=select_autoescape(["html", "xml"]),
                lstrip_blocks=True,
                trim_blocks=True,
            )

            # --- Filter for rendering Markdown content ---
            self.jinja_env.filters["markdown"] = lambda text: markdown.markdown(
                text, extensions=["fenced_code", "tables"]
            )
            # --- Filter for pretty-printing YAML in code blocks ---
            self.jinja_env.filters["yaml_dump"] = lambda data: yaml.dump(
                data, sort_keys=False, indent=2
            )

            # --- Definitive Fix: Add a shared 'now' function to the environment's globals ---
            def get_now(tz: str | None = None) -> datetime:
                """A Jinja-friendly function to get the current time, with UTC option."""
                if tz and tz.lower() == "utc":
                    return datetime.now(timezone.utc)
                return datetime.now()

            self.jinja_env.globals["now"] = get_now

        except Exception as e:
            logger.error("html_renderer.init.failed", error=str(e), exc_info=True)
            raise RuntimeError(f"Failed to initialize HTMLRenderer: {e}") from e

    async def render(
        self, page: ContextualPage, results: Dict[str, Any], params: Dict[str, Any]
    ) -> str:
        """
        Generates a static HTML document by rendering a master Jinja2 template.
        """
        log = logger.bind(page_name=page.name, renderer="html")
        log.info("render.begin")

        try:
            template = self.jinja_env.get_template("report.html")
        except Exception as e:
            log.error("render.template_load_failed", error=str(e))
            raise IOError(f"Could not load the HTML report template: {e}") from e

        # Prepare a list of final, renderable blocks for the template
        renderable_blocks = []
        for block in page.blocks:
            block_result = results.get(block.id)

            result_html = None
            result_type = "raw"  # Default to raw/json rendering

            # Intelligently convert list-of-dicts (typical SQL/API result) to a rich HTML table
            if (
                isinstance(block_result, list)
                and block_result
                and isinstance(block_result[0], dict)
            ):
                try:
                    df = pd.DataFrame(block_result)
                    # Use pandas styling for a clean, professional table
                    result_html = df.to_html(
                        index=False, classes="table table-striped", border=0
                    )
                    result_type = "html"
                except Exception as df_error:
                    log.warning(
                        "render.dataframe_conversion_failed",
                        block_id=block.id,
                        error=str(df_error),
                    )
                    # Fallback to raw rendering if DataFrame conversion fails
                    result_html = block_result

            elif block_result is not None:
                result_html = block_result

            renderable_blocks.append(
                {"block": block, "result": result_html, "result_type": result_type}
            )

        # The full context passed to the Jinja template for rendering
        render_context = {"page": page, "blocks": renderable_blocks, "params": params}

        log.info("render.rendering_template")
        final_html = template.render(render_context)
        log.info("render.success", byte_count=len(final_html.encode("utf-8")))

        return final_html
