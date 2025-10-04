# [REPLACE] ~/repositories/connector-logic/src/connector_logic/providers/sql/base_sql_strategy.py

from ..base import BaseConnectorStrategy


class BaseSqlStrategy(BaseConnectorStrategy):
    """
    An intermediate base class for strategies that connect to SQL-like databases.
    This can be used to share common logic for constructing connection strings
    or managing connection pools in the future.
    """

    pass
