import io
from typing import Any, Dict, List

import pandas as pd
import structlog
from pydantic import TypeAdapter

from ..operations.file_format_ops import AnyFileFormatOperation, SaveOperation
from ..vfs_client import AbstractVfsClient
from .base import BaseTransformEngine

logger = structlog.get_logger(__name__)
AnyFileFormatOperationAdapter = TypeAdapter(AnyFileFormatOperation)


class FileFormatEngine(BaseTransformEngine):
    """
    A transformation engine that saves a DataFrame to a specified file format.

    This engine is responsible for all file I/O. It can produce simple data dumps
    (CSV, JSON, Parquet) or generate professionally formatted reports (Excel).
    It populates an "Artifact Manifest" with the path and type of each created file.
    """

    engine_name = "file_format"

    def __init__(self, vfs_client: AbstractVfsClient):
        """
        Initializes the FileFormatEngine.

        Args:
            vfs_client: An instance of a VFS client for writing the output file.
        """
        self.vfs = vfs_client

    async def transform(
        self,
        data: pd.DataFrame,
        operations: List[Dict[str, Any]],
        context: Dict[str, Any],
    ) -> pd.DataFrame:
        """
        Saves the current DataFrame to a file based on the declarative operation.
        """
        log = logger.bind(engine=self.engine_name)
        op_model = AnyFileFormatOperationAdapter.validate_python(operations[0])

        if isinstance(op_model, SaveOperation):
            if not self.vfs:
                raise RuntimeError(
                    "VFS client not configured. Cannot perform 'save' operation."
                )

            log.info(
                "Executing operation: save",
                format=op_model.format,
                target=op_model.target_path,
                artifact_type=op_model.artifact_type,
            )

            output_buffer = io.BytesIO()

            # --- Advanced Formatting Dispatcher ---
            # If the format is Excel and advanced formatting is requested, use the dedicated helper.
            if op_model.format == "excel" and op_model.excel_formatting:
                self._write_formatted_excel(data, output_buffer, op_model)
            else:
                # Otherwise, use the standard, fast pandas writers for other formats or basic excel.
                format_options = op_model.options or {}
                if op_model.format == "excel":
                    data.to_excel(output_buffer, index=False, **format_options)
                elif op_model.format == "csv":
                    data.to_csv(output_buffer, index=False, **format_options)
                elif op_model.format == "json":
                    data.to_json(
                        output_buffer, orient="records", indent=2, **format_options
                    )
                elif op_model.format == "parquet":
                    data.to_parquet(output_buffer, index=False, **format_options)

            content_bytes = output_buffer.getvalue()

            canonical_path = await self.vfs.write(
                path=op_model.target_path, content=content_bytes, context=context
            )

            # Populate the structured Artifact Manifest
            artifacts_manifest = context.get("artifacts", {})
            if op_model.artifact_type == "html_body":
                artifacts_manifest["html_body"] = canonical_path
            else:
                artifacts_manifest.setdefault("attachments", []).append(canonical_path)

            log.info(
                "save.success", path=canonical_path, bytes_written=len(content_bytes)
            )

        return data

    def _write_formatted_excel(
        self, df: pd.DataFrame, buffer: io.BytesIO, op: SaveOperation
    ):
        """
        Writes a beautifully formatted Excel file using the openpyxl engine,
        applying table styles, datetime formats, and auto-sizing columns.

        Args:
            df: The DataFrame to write.
            buffer: The in-memory byte buffer to write to.
            op: The validated SaveOperation model containing formatting options.
        """
        if not op.excel_formatting:
            # This is a safeguard; this method shouldn't be called without formatting options.
            df.to_excel(buffer, index=False, **(op.options or {}))
            return

        formatting_options = op.excel_formatting

        # Pass the datetime_format directly to the ExcelWriter. This is the most
        # robust way to ensure all datetime columns are formatted correctly upon writing.
        with pd.ExcelWriter(
            buffer,
            engine="openpyxl",
            datetime_format=formatting_options.datetime_format,
        ) as writer:
            sheet_name = op.options.get("sheet_name", "Sheet1")
            df.to_excel(writer, sheet_name=sheet_name, index=False)

            # Get the openpyxl worksheet object for further direct manipulation
            worksheet = writer.sheets[sheet_name]

            # 1. Apply Table Style for professional look and filtering capabilities
            if formatting_options.table_style:
                from openpyxl.worksheet.table import Table, TableStyleInfo

                tab = Table(displayName="DataTable", ref=worksheet.dimensions)
                style = TableStyleInfo(
                    name=formatting_options.table_style,
                    showFirstColumn=False,
                    showLastColumn=False,
                    showRowStripes=formatting_options.show_banded_rows,
                    showColumnStripes=False,
                )
                tab.tableStyleInfo = style
                worksheet.add_table(tab)

            # 2. Auto-size columns for readability (should run after data is written)
            if formatting_options.auto_size_columns:
                for column_cells in worksheet.columns:
                    max_length = 0
                    column_letter = column_cells[0].column_letter

                    for cell in column_cells:
                        try:
                            # Add a check for None to avoid errors on empty cells
                            if (
                                cell.value is not None
                                and len(str(cell.value)) > max_length
                            ):
                                max_length = len(str(cell.value))
                        except:
                            pass  # Ignore errors for complex cell types

                    # Set the column width with a little extra padding
                    adjusted_width = max_length + 2
                    worksheet.column_dimensions[column_letter].width = adjusted_width
