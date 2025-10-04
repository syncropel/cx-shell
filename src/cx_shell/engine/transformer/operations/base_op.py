from typing import Literal, Union
from pydantic import Field, BaseModel


class BaseOperation(BaseModel):
    """The base model for all declarative transformation operations."""

    type: str


class RenameColumnsOp(BaseOperation):
    type: Literal["rename_columns"]
    style: Literal["title_case", "snake_case", "upper_case", "lower_case"]


class FilterRowsOp(BaseOperation):
    type: Literal["filter_rows"]
    expression: str = Field(..., description="A standard Pandas query string.")


# A discriminated union of all possible Pandas operations
AnyPandasOperation = Union[RenameColumnsOp, FilterRowsOp]
