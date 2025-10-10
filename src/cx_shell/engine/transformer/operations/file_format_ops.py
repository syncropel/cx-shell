from typing import Literal, Dict, Any, Union, Optional
from pydantic import BaseModel, Field

from .base_op import BaseOperation

ArtifactType = Literal["attachment", "html_body", "main_output"]


class ExcelFormattingOptions(BaseModel):
    """Defines advanced formatting options for Excel output."""

    auto_size_columns: bool = Field(
        True, description="Automatically adjust column widths to fit content."
    )
    table_style: Optional[str] = Field(
        None,
        description="The name of the Excel table style to apply. If None, no table is created.",
    )
    show_banded_rows: bool = Field(
        True, description="Toggle the banded row (stripe) effect in the table style."
    )
    datetime_format: Optional[str] = Field(
        "yyyy-mm-dd hh:mm:ss",
        description="The Excel format code for all datetime columns.",
    )


class SaveOperation(BaseOperation):
    type: Literal["save"]
    format: Literal["json", "csv", "excel", "parquet"]
    target_path: str = Field(..., description="The output file path (local or vfs://).")
    artifact_type: ArtifactType = Field(
        "attachment", description="The semantic role of the saved file."
    )
    excel_formatting: Optional[ExcelFormattingOptions] = Field(
        None, description="Advanced formatting options specific to Excel files."
    )
    options: Dict[str, Any] = Field(
        default_factory=dict, description="Format-specific save options."
    )


# A discriminated union of all possible file operations
AnyFileFormatOperation = Union[SaveOperation]
