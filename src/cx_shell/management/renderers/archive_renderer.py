# ~/repositories/cx-shell/src/cx_shell/management/renderers/archive_renderer.py
import json
from typing import Any, Dict
import yaml

from cx_core_schemas.notebook import ContextualPage
from .base import BaseRenderer


class ArchiveRenderer(BaseRenderer):
    """
    Renders a Contextual Page into a static, self-contained `.cx.md` file
    with all execution outputs embedded.
    """

    renderer_key = "archive"

    async def render(self, page: ContextualPage, results: Dict[str, Any]) -> str:
        """
        Generates a static Markdown document with embedded, syntax-highlighted output blocks.
        """
        output_parts = []

        # 1. Re-create the main front matter
        front_matter = page.model_dump(
            exclude={"blocks"}, exclude_none=True, by_alias=True
        )
        output_parts.append("---")
        output_parts.append(yaml.dump(front_matter, sort_keys=False).strip())
        output_parts.append("---")

        # 2. Iterate through the original blocks
        for block in page.blocks:
            # Render the original block content (metadata, code, or markdown)
            if block.engine == "markdown":
                output_parts.append(block.content)
            else:
                metadata_dict = block.model_dump(
                    exclude={"content", "run"}, exclude_none=True, by_alias=True
                )
                metadata_dict["cx_block"] = True

                code_lang = "yaml"
                if block.engine and block.engine not in ["run", "transform", "publish"]:
                    code_lang = block.engine

                # Reconstruct the original block pair
                output_parts.append("\n```yaml")
                output_parts.append(yaml.dump(metadata_dict, sort_keys=False).strip())
                output_parts.append("```\n")

                output_parts.append(f"```{code_lang}")
                if block.run:
                    output_parts.append(
                        yaml.dump(
                            block.run.model_dump(exclude_unset=True), sort_keys=False
                        ).strip()
                    )
                elif block.content:
                    output_parts.append(block.content.strip())
                output_parts.append("```\n")

            # --- START OF DEFINITIVE FIX ---
            # 3. If there's a result for this block, append a clean, syntax-highlighted output block
            if block.id in results:
                block_result = results[block.id]
                if block_result is not None:
                    # For now, we assume all results are JSON-like.
                    # Future versions could inspect the result type to choose 'yaml', 'csv', etc.
                    output_lang = "json"

                    # Embed metadata as a language-appropriate comment
                    metadata_comment = f'// cx:source_block_id="{block.id}"'

                    # Serialize the result
                    result_content = json.dumps(block_result, indent=2)

                    output_parts.append(f"```{output_lang}")
                    output_parts.append(metadata_comment)
                    output_parts.append(result_content)
                    output_parts.append("```\n")
            # --- END OF DEFINITIVE FIX ---

        return "\n".join(output_parts)
