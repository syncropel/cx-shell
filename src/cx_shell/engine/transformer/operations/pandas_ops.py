from typing import Any, Dict, Literal, Optional, Union, List
from pydantic import BaseModel, Field

from .base_op import BaseOperation


class RenameColumnsOp(BaseOperation):
    """Defines an operation to rename all columns according to a style."""

    type: Literal["rename_columns"]
    style: Literal[
        "title_case", "snake_case", "upper_case", "lower_case", "pascal_case"
    ] = Field(..., description="The naming convention to apply to all column headers.")


class FilterRowsOp(BaseOperation):
    """Defines an operation to filter rows based on a Pandas query expression."""

    type: Literal["filter_rows"]
    expression: str = Field(
        ...,
        description="A standard Pandas query string. Column names with spaces must be backticked.",
        examples=["`Profit Margin` > 0.1 and `Status` == 'Booked'"],
    )


class DropColumnsOp(BaseOperation):
    """Defines an operation to remove one or more columns."""

    type: Literal["drop_columns"]
    columns: List[str] = Field(..., description="A list of column names to remove.")


class AddColumnOp(BaseOperation):
    """Defines an operation to add a new column based on an expression."""

    type: Literal["add_column"]
    column_name: str = Field(..., description="The name of the new column to create.")
    expression: str = Field(
        ...,
        description="An expression evaluated by `DataFrame.eval()`. Can reference other columns.",
        examples=["`Package Total In Usd` - `Base Price In Usd`"],
    )


class AggregateToContextOp(BaseModel):
    """
    Performs aggregations on the DataFrame and saves the results as variables
    in the run context for use by later steps (like Jinja templates).
    """

    type: Literal["aggregate_to_context"]
    context_key: str = Field(
        ..., description="The key to save the summary under in the run context."
    )
    aggregations: Dict[str, Any] = Field(
        ..., description="A dictionary defining the aggregations to perform."
    )

    # Example:
    # aggregations: {
    #   "total_revenue": {"column": "Collected In Usd", "function": "sum"},
    #   "booking_type_counts": {"column": "Booking Type", "function": "value_counts"}
    # }


class ConvertColumnTypesOp(BaseModel):
    """
    Converts the data type of specified columns. Includes a special, robust
    handler for converting timezone-aware datetimes to timezone-naive UTC
    for Excel compatibility.
    """

    type: Literal["convert_column_types"]

    # Simple key-value mapping for standard type conversions.
    type_mapping: Optional[Dict[str, str]] = Field(
        None,
        description="A mapping of column names to target pandas dtypes, e.g., {'Amount': 'float64'}",
    )

    # A dedicated list for the more complex datetime conversion.
    to_naive_utc_datetimes: Optional[List[str]] = Field(
        None,
        description="A list of columns to convert from any format to timezone-naive UTC datetimes.",
    )


class ColumnFormatRule(BaseModel):
    """Defines the formatting rules for a single column."""

    dtype: Optional[str] = Field(
        None, description="The target Pandas data type (e.g., 'Int64', 'float')."
    )
    round: Optional[int] = Field(
        None, description="The number of decimal places to round to."
    )


class ApplyColumnFormatsOp(BaseOperation):
    """
    Applies rounding and data type casting to specified columns. This is ideal
    for final presentation formatting before saving to a file.
    """

    type: Literal["apply_column_formats"]
    formats: Dict[str, ColumnFormatRule] = Field(
        ..., description="A dictionary mapping column names to their formatting rules."
    )


# A discriminated union of all possible Pandas operations.
# When a new operation is created, it must be added to this list.
AnyPandasOperation = Union[
    RenameColumnsOp,
    FilterRowsOp,
    DropColumnsOp,
    AddColumnOp,
    AggregateToContextOp,
    ConvertColumnTypesOp,
    ApplyColumnFormatsOp,
]
