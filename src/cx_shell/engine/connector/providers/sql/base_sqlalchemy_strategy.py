from contextlib import asynccontextmanager
from typing import Any, Dict, List, TYPE_CHECKING

from jinja2 import Environment
import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine

from ...utils import safe_serialize
from ..base import BaseConnectorStrategy
from .....state import APP_STATE

if TYPE_CHECKING:
    from .....engine.context import RunContext
    from cx_core_schemas.connection import Connection
    from cx_core_schemas.vfs import VfsFileContentResponse

logger = structlog.get_logger(__name__)


class BaseSqlAlchemyStrategy(BaseConnectorStrategy):
    """
    A reusable, production-grade base strategy for connecting to any
    SQLAlchemy-compatible database using its asyncio interface.
    """

    dialect_driver: str = ""

    def _get_connection_url(
        self, connection: "Connection", secrets: Dict[str, Any]
    ) -> str:
        """Constructs the SQLAlchemy connection URL. Must be implemented by subclass."""
        raise NotImplementedError

    @asynccontextmanager
    async def get_client(self, connection: "Connection", secrets: Dict[str, Any]):
        """
        Provides a ready-to-use, pooled SQLAlchemy async engine.
        The engine is the primary client for all operations.
        """
        engine: AsyncEngine | None = None
        log = logger.bind(connection_id=connection.id, dialect=self.dialect_driver)
        try:
            connection_url = self._get_connection_url(connection, secrets)
            engine = create_async_engine(connection_url)
            log.info("sqlalchemy.engine.created")
            yield engine
        finally:
            if engine:
                await engine.dispose()
                log.info("sqlalchemy.engine.disposed")

    async def test_connection(
        self, connection: "Connection", secrets: Dict[str, Any]
    ) -> bool:
        """Tests the connection by executing a simple 'SELECT 1' query."""
        log = logger.bind(connection_id=connection.id, dialect=self.dialect_driver)
        log.info("sqlalchemy.test_connection.begin")
        try:
            async with self.get_client(connection, secrets) as engine:
                async with engine.connect() as conn:
                    result = await conn.execute(text("SELECT 1"))
                    if result.scalar_one() != 1:
                        raise ConnectionError("Test query 'SELECT 1' did not return 1.")
            log.info("sqlalchemy.test_connection.success")
            return True
        except Exception as e:
            log.error(
                "sqlalchemy.test_connection.failed",
                error=str(e),
                exc_info=APP_STATE.verbose_mode,
            )
            raise ConnectionError(f"Database connection test failed: {e}") from e

    async def run_sql_query(
        self,
        connection: "Connection",
        secrets: Dict[str, Any],  # <-- ADD secrets here
        action_params: Dict[str, Any],
        run_context: "RunContext",  # <-- Keep run_context for path resolution
    ) -> List[Dict[str, Any]]:
        """
        The public, action-dispatchable method for running a SQL query.
        This method is called directly by the ScriptEngine.
        """
        query_str = action_params.get("query")
        params = action_params.get("parameters", {})

        if not query_str:
            raise ValueError("'query' field is required for the run_sql_query action.")

        # Resolve file paths using the context
        if query_str.startswith(("file:", "app-asset:", "project-asset:")):
            query_path = run_context.resolve_path_in_context(query_str)
            query_str = query_path.read_text(encoding="utf-8")

        # Delegate to the existing, powerful execute_query method,
        # passing the secrets directly.
        return await self.execute_query(query_str, params, connection, secrets)

    async def execute_query(
        self,
        query: str,
        params: Dict,
        connection: "Connection",
        secrets: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """
        Executes a SQL query, manually handling parameter expansion for IN clauses
        to ensure compatibility with drivers like pyodbc.
        """
        log = logger.bind(connection_id=connection.id, dialect=self.dialect_driver)
        log.info("sqlalchemy.execute_query.begin")

        connection_url = self._get_connection_url(connection, secrets)
        engine = create_async_engine(connection_url)

        try:
            final_query = query
            final_params = params.copy()
            list_params = {k: v for k, v in final_params.items() if isinstance(v, list)}

            if list_params:
                log.info(
                    "Manually expanding list parameters for IN clause.",
                    params=list(list_params.keys()),
                )
                for key, values in list_params.items():
                    if not values:  # Handle empty lists to avoid invalid SQL
                        final_query = final_query.replace(f"(:{key})", "(NULL)")
                        del final_params[key]
                        continue
                    new_param_names = [f"{key}_{i}" for i in range(len(values))]
                    placeholders = ", ".join([f":{p}" for p in new_param_names])
                    final_query = final_query.replace(f"(:{key})", f"({placeholders})")
                    del final_params[key]
                    final_params.update(zip(new_param_names, values))

            stmt = text(final_query)
            log.info("sqlalchemy.execute_query.executing", final_params=final_params)

            async with engine.connect() as conn:
                result_proxy = await conn.execute(stmt, final_params)

                # --- DEFINITIVE FIX for "no rows" queries ---
                # Before trying to fetch results, check if the query was expected to return rows.
                if result_proxy.returns_rows:
                    mapping_results = result_proxy.mappings().all()
                    log.info(
                        "sqlalchemy.execute_query.success",
                        row_count=len(mapping_results),
                    )
                    dict_results = [dict(row) for row in mapping_results]
                    return safe_serialize(dict_results)
                else:
                    # If no rows were returned (e.g., a comment, DDL, or an UPDATE statement),
                    # return an empty list, which is the correct representation of "no data".
                    log.info(
                        "sqlalchemy.execute_query.success_no_rows",
                        row_count=result_proxy.rowcount,
                    )
                    return []
                # --- END FIX ---

        except Exception as e:
            log.error("sqlalchemy.execute_query.failed", error=str(e), exc_info=True)
            raise IOError(f"Query execution failed: {e}") from e
        finally:
            await engine.dispose()

    # async def execute_query(
    #     self,
    #     query: str,
    #     params: Dict,
    #     connection: "Connection",
    #     secrets: Dict[str, Any],
    # ) -> List[Dict[str, Any]]:
    #     """
    #     Executes a SQL query, ensuring the connection engine is created and
    #     disposed of correctly within a managed context.
    #     """
    #     log = logger.bind(connection_id=connection.id, dialect=self.dialect_driver)
    #     log.info("sqlalchemy.execute_query.begin")

    #     # --- THIS IS THE DEFINITIVE FIX ---
    #     # We now use this strategy's own get_client as an async context manager.
    #     # This guarantees that the engine and its connection pool are disposed of
    #     # as soon as the 'with' block is exited.
    #     try:
    #         async with self.get_client(connection, secrets) as engine:
    #             final_query = query
    #             final_params = params.copy()
    #             list_params = {k: v for k, v in final_params.items() if isinstance(v, list)}

    #             if list_params:
    #                 # ... (the IN clause expansion logic remains the same)
    #                 for key, values in list_params.items():
    #                     if not values:
    #                         final_query = final_query.replace(f"(:{key})", "(NULL)")
    #                         del final_params[key]
    #                         continue
    #                     new_param_names = [f"{key}_{i}" for i in range(len(values))]
    #                     placeholders = ", ".join([f":{p}" for p in new_param_names])
    #                     final_query = final_query.replace(f"(:{key})", f"({placeholders})")
    #                     del final_params[key]
    #                     final_params.update(zip(new_param_names, values))

    #             stmt = text(final_query)
    #             log.info("sqlalchemy.execute_query.executing", final_params=final_params)

    #             async with engine.connect() as conn:
    #                 result_proxy = await conn.execute(stmt, final_params)
    #                 if result_proxy.returns_rows:
    #                     mapping_results = result_proxy.mappings().all()
    #                     log.info(
    #                         "sqlalchemy.execute_query.success",
    #                         row_count=len(mapping_results),
    #                     )
    #                     dict_results = [dict(row) for row in mapping_results]
    #                     return safe_serialize(dict_results)
    #                 else:
    #                     log.info(
    #                         "sqlalchemy.execute_query.success_no_rows",
    #                         row_count=result_proxy.rowcount,
    #                     )
    #                     return []
    #     except Exception as e:
    #         log.error("sqlalchemy.execute_query.failed", error=str(e), exc_info=True)
    #         raise IOError(f"Query execution failed: {e}") from e
    #     # --- END FIX ---

    async def browse_path(
        self, path_parts: List[str], connection: "Connection", secrets: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        logger.warning("browse_path.not_implemented", strategy_key=self.strategy_key)
        return []

    async def get_content(
        self, path_parts: List[str], connection: "Connection", secrets: Dict[str, Any]
    ) -> "VfsFileContentResponse":
        logger.warning("get_content.not_implemented", strategy_key=self.strategy_key)
        raise NotImplementedError(
            "get_content is not implemented for this SQL strategy."
        )

    async def run_declarative_action(
        self,
        connection: "Connection",
        secrets: Dict[str, Any],
        action_params: Dict[str, Any],
        script_input: Dict[str, Any],
    ) -> Dict[str, Any]:  # <-- Changed return type annotation to Dict
        """
        Executes a declarative action for SQL strategies by dynamically building
        and running a SQL query from a blueprint's action template.
        Returns a rich report object including the data and context.
        """
        log = logger.bind(connection_id=connection.id, dialect=self.dialect_driver)

        if not connection.catalog or not connection.catalog.browse_config:
            raise ValueError(
                "Cannot run declarative action: connection is missing its catalog/blueprint."
            )

        template_key = action_params.get("template_key")
        action_template = connection.catalog.browse_config.get(
            "action_templates", {}
        ).get(template_key)

        if not action_template:
            raise ValueError(
                f"Action template '{template_key}' not found in blueprint for connection '{connection.name}'."
            )

        query_template = action_template.get("api_endpoint")
        if not query_template:
            raise ValueError(
                f"Action template '{template_key}' is missing the 'api_endpoint' which should contain the SQL query."
            )

        user_context = action_params.get("context", {})

        jinja_env = Environment()
        template = jinja_env.from_string(query_template)

        render_context = {"context": user_context}
        final_query = template.render(render_context)

        final_params = {}
        if "parameters_model" in action_template:
            params_from_context = {"where_clause": user_context.get("filter")}
            final_params.update(params_from_context)

        log.info(
            "sql.declarative_action.executing",
            dynamic_query=final_query,
            params=final_params,
        )

        # --- START OF NEW, IMPROVED RETURN STRUCTURE ---
        try:
            # Execute the query to get the raw data
            data_results = await self.execute_query(
                final_query, final_params, connection, secrets
            )

            record_count = len(data_results)

            # Build the rich report object
            report = {
                "status": "success",
                "parameters": user_context,
                "record_count": record_count,
                "columns": list(data_results[0].keys()) if record_count > 0 else [],
                "data": data_results,
            }
            return report

        except Exception as e:
            log.error("sql.declarative_action.failed", error=str(e), exc_info=True)
            return {
                "status": "error",
                "parameters": user_context,  # <-- Include context even in errors
                "message": f"Query execution failed: {str(e)}",
            }
