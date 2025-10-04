# ~/repositories/cx-shell/src/cx_shell/management/renderers/pdf_renderer.py

from typing import Any, Dict
import structlog
from playwright.async_api import async_playwright

from cx_core_schemas.notebook import ContextualPage
from .base import BaseRenderer
from .html_renderer import HTMLRenderer

logger = structlog.get_logger(__name__)


class PDFRenderer(BaseRenderer):
    """
    Renders a Contextual Page into a professional PDF document by first
    rendering it to HTML and then using a headless browser to print it.
    """

    renderer_key = "pdf"

    def __init__(self):
        self.html_renderer = HTMLRenderer()

    async def render(
        self, page: ContextualPage, results: Dict[str, Any], params: Dict[str, Any]
    ) -> bytes:
        """
        Generates a static PDF document as raw bytes.
        """
        log = logger.bind(page_name=page.name, renderer="pdf")
        log.info("render.begin")

        # 1. Render the HTML in-memory using the existing HTMLRenderer
        log.info("render.generating_html_content")
        html_content = await self.html_renderer.render(page, results, params)

        # 2. Use Playwright to launch a headless browser and print the HTML to PDF
        log.info("render.launching_headless_browser")
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch()
                page_instance = await browser.new_page()

                # Load our in-memory HTML into the browser page
                await page_instance.set_content(html_content, wait_until="networkidle")

                # Define PDF generation options based on user parameters
                pdf_options = {
                    "format": params.get("page_size", "Letter"),
                    "landscape": params.get("page_orientation", "portrait")
                    == "landscape",
                    "print_background": True,
                    "margin": {
                        "top": "0.75in",
                        "bottom": "0.75in",
                        "left": "0.75in",
                        "right": "0.75in",
                    },
                }

                # Add a professional header and footer unless disabled
                if params.get("include_header_footer", True):
                    header_template = f"""
                    <div style="font-size: 9px; width: 100%; padding: 0 0.5in; display: flex; justify-content: space-between; color: #666;">
                        <span>{params.get("title", page.name)}</span>
                        <span>{params.get("author", "")}</span>
                    </div>"""
                    footer_template = """
                    <div style="font-size: 9px; width: 100%; padding: 0 0.5in; display: flex; justify-content: space-between; color: #666;">
                        <span class="date"></span>
                        <div>Page <span class="pageNumber"></span> of <span class="totalPages"></span></div>
                    </div>"""
                    pdf_options["display_header_footer"] = True
                    pdf_options["header_template"] = header_template
                    pdf_options["footer_template"] = footer_template

                log.info("render.printing_to_pdf", options=pdf_options)
                pdf_bytes = await page_instance.pdf(**pdf_options)
                await browser.close()

                log.info("render.success", byte_count=len(pdf_bytes))
                return pdf_bytes
        except Exception as e:
            log.error("render.pdf_generation_failed", error=str(e), exc_info=True)
            raise IOError(f"Failed to generate PDF from HTML: {e}") from e
