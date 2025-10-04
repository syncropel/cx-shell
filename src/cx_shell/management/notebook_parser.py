# ~/repositories/cx-shell/src/cx_shell/management/notebook_parser.py

import re
from pathlib import Path
from typing import List, Dict, Any, Tuple

import yaml
import structlog
from pydantic import ValidationError

from cx_core_schemas.notebook import ContextualPage
from cx_core_schemas.connector_script import ConnectorStep

logger = structlog.get_logger(__name__)

# This regex finds ALL fenced code blocks, capturing their language and the entire inner content.
# It is the first pass for separating code from markdown.
GENERIC_FENCED_BLOCK_REGEX = re.compile(r"```(\w*)\n(.*?)\n```", re.DOTALL)


class NotebookParser:
    """
    Parses a `.cx.md` file into a structured, executable ContextualPage Pydantic model
    by associating `cx_block` metadata blocks with their immediately subsequent code blocks.
    """

    def parse(self, file_path: Path) -> ContextualPage:
        """
        The main entry point for parsing a file.

        Args:
            file_path: The path to the `.cx.md` file.

        Returns:
            A validated ContextualPage model instance.
        """
        log = logger.bind(file_path=str(file_path))
        log.info("notebook_parser.begin_parsing")

        try:
            content = file_path.read_text(encoding="utf-8")
        except Exception as e:
            log.error("notebook_parser.file_read_error", error=str(e))
            raise IOError(f"Could not read file {file_path}: {e}") from e

        # Step 1: Parse the main document front matter.
        page_front_matter, main_content = self._parse_main_front_matter(content)

        # Step 2: Parse the main content into a sequence of Blocks.
        blocks = self._parse_blocks(main_content)

        # Step 3: Assemble and validate the final ContextualPage model.
        try:
            page_data = {
                **page_front_matter,
                "blocks": [b.model_dump() for b in blocks],
            }
            page_model = ContextualPage.model_validate(page_data)
            log.info(
                "notebook_parser.parsing_successful",
                page_name=page_model.name,
                block_count=len(blocks),
            )
            return page_model
        except ValidationError as e:
            log.error("notebook_parser.validation_error", errors=e.errors())
            raise ValueError(
                f"Failed to validate the overall structure of '{file_path.name}'. Please check the format."
            ) from e

    def _parse_main_front_matter(self, content: str) -> Tuple[Dict[str, Any], str]:
        """Extracts and parses the top-level YAML front matter from the document."""
        front_matter_match = re.match(r"^\s*---(.*?)---", content, re.DOTALL)
        if front_matter_match:
            yaml_content = front_matter_match.group(1)
            main_content = content[front_matter_match.end() :].lstrip()
            try:
                return yaml.safe_load(yaml_content) or {}, main_content
            except yaml.YAMLError as e:
                raise ValueError(f"Invalid YAML in page front matter: {e}") from e
        else:
            return {}, content

    def _parse_blocks(self, content: str) -> List[ConnectorStep]:
        """
        Parses the main content into a stream of blocks, then intelligently pairs
        {cx} metadata blocks with their subsequent code blocks to form a list of
        executable ConnectorStep models.
        """

        # --- PASS 1: Split content into a raw stream of parts ---
        raw_parts: List[Tuple[str, str]] = []
        last_end = 0
        for match in GENERIC_FENCED_BLOCK_REGEX.finditer(content):
            markdown_content = content[last_end : match.start()].strip()
            if markdown_content:
                raw_parts.append(("markdown", markdown_content))
            raw_parts.append(("code_block", match.group(0)))
            last_end = match.end()
        final_markdown = content[last_end:].strip()
        if final_markdown:
            raw_parts.append(("markdown", final_markdown))

        # --- PASS 2: Process the stream and build the final ConnectorStep list ---
        blocks: List[ConnectorStep] = []
        i = 0
        while i < len(raw_parts):
            part_type, part_content = raw_parts[i]

            if part_type == "markdown":
                blocks.append(
                    ConnectorStep(
                        id=f"md_{len(blocks)}", engine="markdown", content=part_content
                    )
                )
                i += 1
                continue

            lang, inner_content = self._parse_fenced_block(part_content)

            is_cx_metadata_block = False
            metadata_yaml = {}
            try:
                if lang == "yaml":
                    yaml_data = yaml.safe_load(inner_content)
                    if (
                        isinstance(yaml_data, dict)
                        and yaml_data.get("cx_block") is True
                    ):
                        is_cx_metadata_block = True
                        metadata_yaml = yaml_data
            except yaml.YAMLError:
                pass

            if is_cx_metadata_block:
                if (i + 1) < len(raw_parts) and raw_parts[i + 1][0] == "code_block":
                    next_code_block_full_content = raw_parts[i + 1][1]
                    code_lang, code_content = self._parse_fenced_block(
                        next_code_block_full_content
                    )

                    try:
                        if "id" not in metadata_yaml:
                            raise ValueError(
                                "An executable {cx} block is missing a required 'id' field."
                            )

                        final_engine = metadata_yaml.get("engine", code_lang)
                        if not final_engine:
                            raise ValueError(
                                f"Engine for block '{metadata_yaml['id']}' is not specified in YAML metadata or code fence."
                            )

                        # --- DEFINITIVE FIX: Build the correct dictionary for ConnectorStep ---
                        if final_engine == "run":
                            # For 'run' blocks, the content is a YAML payload for the 'run' field.
                            run_payload = yaml.safe_load(code_content)
                            if (
                                not isinstance(run_payload, dict)
                                or "action" not in run_payload
                            ):
                                raise ValueError(
                                    "Content of a 'run' block must be a YAML dictionary with an 'action' key."
                                )

                            # Construct a dictionary that maps to the ConnectorStep model.
                            # It has a 'run' key, but no 'engine' or 'content' key.
                            block_data = {**metadata_yaml, "run": run_payload}

                            # Explicitly remove engine if it was in the metadata to avoid validation conflicts
                            block_data.pop("engine", None)
                            # print("_parse_blocks.block_data")
                            # print(block_data)
                        else:
                            # For all other engines, we populate the 'engine' and 'content' fields.
                            block_data = {
                                **metadata_yaml,
                                "engine": final_engine,
                                "content": code_content,
                            }

                        blocks.append(ConnectorStep.model_validate(block_data))
                        # --- END FIX ---

                        i += 2
                        continue
                    except (ValidationError, ValueError) as e:
                        raise ValueError(
                            f"Invalid executable block structure for block ID '{metadata_yaml.get('id', 'unknown')}': {e}"
                        ) from e
                else:
                    # Orphaned metadata block. Treat as Markdown.
                    logger.warning(
                        f"Orphaned cx_block found with id '{metadata_yaml.get('id', 'unknown')}'. Treating as simple Markdown."
                    )
                    blocks.append(
                        ConnectorStep(
                            id=f"md_{len(blocks)}",
                            engine="markdown",
                            content=part_content,
                        )
                    )
                    i += 1
            else:
                # Standard, non-executable code block. Treat as Markdown.
                blocks.append(
                    ConnectorStep(
                        id=f"md_{len(blocks)}", engine="markdown", content=part_content
                    )
                )
                i += 1

        return blocks

    def _parse_fenced_block(self, block_str: str) -> Tuple[str, str]:
        """Helper to extract the language and inner content from a full ```...``` block string."""
        match = re.match(r"```(\w*)\n(.*?)\n```", block_str, re.DOTALL)
        if match:
            # Return language (or 'text' if none) and the inner content.
            return match.group(1).lower() or "text", match.group(2)
        return "text", block_str  # Fallback for malformed blocks.
