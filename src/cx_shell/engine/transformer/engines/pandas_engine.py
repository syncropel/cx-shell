from typing import Any, Dict, List

import pandas as pd
import structlog
from pydantic import TypeAdapter

from ..operations.pandas_ops import (
    AddColumnOp,
    AnyPandasOperation,
    FilterRowsOp,
    RenameColumnsOp,
    AggregateToContextOp,
    ConvertColumnTypesOp,
    ApplyColumnFormatsOp,
)
from .base import BaseTransformEngine

logger = structlog.get_logger(__name__)

AnyPandasOperationAdapter = TypeAdapter(AnyPandasOperation)


class PandasEngine(BaseTransformEngine):
    engine_name = "pandas"

    async def transform(
        self,
        data: pd.DataFrame,
        operations: List[Dict[str, Any]],
        context: Dict[str, Any],
    ) -> pd.DataFrame:
        log = logger.bind(engine=self.engine_name)
        log.info("Applying pandas transformations.", operation_count=len(operations))

        data = data.copy()

        for op_data in operations:
            op_model = AnyPandasOperationAdapter.validate_python(op_data)

            if isinstance(op_model, RenameColumnsOp):
                log.info("Executing: rename_columns", style=op_model.style)
                if op_model.style == "title_case":
                    data.columns = [
                        col.replace("_", " ").title() for col in data.columns
                    ]

                elif op_model.style == "upper_case":
                    data.columns = [col.upper() for col in data.columns]

                elif op_model.style == "pascal_case":
                    data.columns = [
                        col.replace("_", " ").title().replace(" ", "")
                        for col in data.columns
                    ]

            elif isinstance(op_model, FilterRowsOp):
                log.info("Executing: filter_rows", expression=op_model.expression)
                try:
                    data = data.query(op_model.expression)
                except Exception as e:
                    log.error(
                        "pandas.query.failed",
                        expression=op_model.expression,
                        error=str(e),
                    )
                    raise ValueError(
                        f"Pandas query failed: '{op_model.expression}'. Error: {e}"
                    )

            elif isinstance(op_model, AddColumnOp):
                log.info("Executing: add_column", new_column=op_model.column_name)
                try:
                    calculated_results = data.eval(op_model.expression, engine="python")
                    data[op_model.column_name] = calculated_results
                except Exception as e:
                    log.error(
                        "pandas.eval.failed",
                        expression=op_model.expression,
                        error=str(e),
                    )
                    raise ValueError(
                        f"Pandas eval failed for expression: '{op_model.expression}'. Error: {e}"
                    )
            elif isinstance(op_model, AggregateToContextOp):
                log.info("Executing: aggregate_to_context", key=op_model.context_key)
                summary_data = {}
                for key, agg_spec in op_model.aggregations.items():
                    col = agg_spec["column"]
                    func = agg_spec["function"]

                    if func == "sum":
                        summary_data[key] = data[col].sum()
                    elif func == "count":
                        summary_data[key] = len(data)
                    elif func == "value_counts":
                        # .value_counts() returns a Series, convert it to a dict
                        summary_data[key] = data[col].value_counts().to_dict()
                    # ... can add more functions like 'mean', 'median', etc.

                # Save the generated summary dict into the main run_context
                context[op_model.context_key] = summary_data

            elif isinstance(op_model, ConvertColumnTypesOp):
                log.info("Executing: convert_column_types")

                if op_model.type_mapping:
                    for col, dtype in op_model.type_mapping.items():
                        try:
                            data[col] = data[col].astype(dtype)
                        except Exception as e:
                            log.error(
                                "column_conversion.failed",
                                column=col,
                                dtype=dtype,
                                error=str(e),
                            )

                if op_model.to_naive_utc_datetimes:
                    log.info("Converting datetimes to timezone-naive UTC...")
                    for col in op_model.to_naive_utc_datetimes:
                        try:
                            # --- THIS IS THE ROBUST FIX ---
                            # Use format='ISO8601' to handle variations in fractional seconds.
                            # The utc=True flag is no longer needed as the format implies UTC.
                            s = pd.to_datetime(data[col], format="ISO8601")

                            # Ensure the Series is timezone-aware in UTC before converting
                            if s.dt.tz is None:
                                s = s.dt.tz_localize("UTC")
                            else:
                                s = s.dt.tz_convert("UTC")

                            # Localize to None, which strips the timezone info.
                            data[col] = s.dt.tz_localize(None)
                            # --- END ROBUST FIX ---
                        except Exception as e:
                            log.error(
                                "datetime_to_naive.failed", column=col, error=str(e)
                            )
                            # Continue on error to process other columns
            elif isinstance(op_model, ApplyColumnFormatsOp):
                log.info("Applying column formats...")

                for column_name, rule in op_model.formats.items():
                    if column_name not in data.columns:
                        log.warning(
                            "column_format.skip",
                            column=column_name,
                            reason="Column not found in DataFrame.",
                        )
                        continue

                    # --- Step 1: Apply Rounding (if specified) ---
                    # Rounding should happen before type casting to integer.
                    if rule.round is not None:
                        try:
                            log.debug(
                                "Rounding column",
                                column=column_name,
                                decimals=rule.round,
                            )
                            # Coerce errors to NaN to handle non-numeric values gracefully
                            data[column_name] = pd.to_numeric(
                                data[column_name], errors="coerce"
                            ).round(rule.round)
                        except Exception as e:
                            log.error(
                                "column_format.round.failed",
                                column=column_name,
                                error=str(e),
                            )
                            # Continue to the next rule/column even if rounding fails
                            continue

                    # --- Step 2: Apply Data Type Conversion (if specified) ---
                    if rule.dtype:
                        try:
                            log.debug(
                                "Casting column", column=column_name, dtype=rule.dtype
                            )
                            # Special handling for Pandas nullable integer type to preserve NaNs
                            if rule.dtype.lower() == "int64":
                                # Coerce to numeric first to handle potential strings, then cast
                                numeric_series = pd.to_numeric(
                                    data[column_name], errors="coerce"
                                )
                                data[column_name] = numeric_series.astype("Int64")
                            else:
                                data[column_name] = data[column_name].astype(rule.dtype)
                        except Exception as e:
                            log.error(
                                "column_format.cast.failed",
                                column=column_name,
                                dtype=rule.dtype,
                                error=str(e),
                            )
                            # Continu

        return data
