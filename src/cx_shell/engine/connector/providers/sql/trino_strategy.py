import json
from datetime import datetime, timezone
from typing import Any, Dict, List, TYPE_CHECKING

import structlog
from sqlalchemy import text

from ...utils import safe_serialize
from .base_sqlalchemy_strategy import BaseSqlAlchemyStrategy
from cx_core_schemas.vfs import VfsFileContentResponse, VfsNodeMetadata
from .....data.agent_schemas import DryRunResult


if TYPE_CHECKING:
    from cx_core_schemas.connection import Connection

logger = structlog.get_logger(__name__)


class TrinoStrategy(BaseSqlAlchemyStrategy):
    """
    SQLAlchemy-based strategy for connecting to a Trino cluster.

    This strategy leverages the BaseSqlAlchemyStrategy for connection pooling,
    testing, and generic query execution, while providing Trino-specific
    implementations for schema browsing.
    """

    strategy_key = "sql-trino"
    dialect_driver = "trino"

    def _get_connection_url(
        self, connection: "Connection", secrets: Dict[str, Any]
    ) -> str:
        """
        Constructs a SQLAlchemy connection URL for the Trino dialect.
        Example: trino://user@host:port/catalog
        """
        config = {**connection.details, **secrets}
        required = ["host", "port", "user", "catalog"]
        if not all(k in config for k in required):
            raise ValueError(
                f"Trino connection is missing one or more required fields: {required}"
            )

        # The trino-python-client supports password auth via the 'auth' parameter
        # to connect(), but the SQLAlchemy dialect uses a standard URL format.
        # For now, we assume simple user-based authentication.
        conn_url = (
            f"{self.dialect_driver}://{config['user']}@{config['host']}:{config['port']}"
            f"/{config['catalog']}"
        )
        logger.info(
            "Constructed Trino connection URL.",
            host=config["host"],
            catalog=config["catalog"],
        )
        return conn_url

    async def browse_path(
        self, path_parts: List[str], connection: "Connection", secrets: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Implements hierarchical browsing using SQLAlchemy's async engine:
        Catalogs -> Schemas -> Tables.
        """
        async with self.get_client(connection, secrets) as engine:
            async with engine.connect() as conn:
                # Depth 0: List Catalogs
                if not path_parts:
                    result = await conn.execute(text("SHOW CATALOGS"))
                    rows = result.fetchall()
                    return [
                        {
                            "name": row[0],
                            "path": f"{row[0]}/",
                            "type": "folder",
                            "icon": "IconBox",
                        }
                        for row in rows
                    ]

                # Depth 1: List Schemas in a Catalog
                elif len(path_parts) == 1:
                    catalog = path_parts[0]
                    result = await conn.execute(text(f"SHOW SCHEMAS FROM {catalog}"))
                    rows = result.fetchall()
                    return [
                        {
                            "name": row[0],
                            "path": f"{catalog}/{row[0]}/",
                            "type": "folder",
                            "icon": "IconSchema",
                        }
                        for row in rows
                    ]

                # Depth 2: List Tables in a Schema
                elif len(path_parts) == 2:
                    catalog, schema = path_parts[0], path_parts[1]
                    result = await conn.execute(
                        text(f"SHOW TABLES FROM {catalog}.{schema}")
                    )
                    rows = result.fetchall()
                    return [
                        {
                            "name": row[0],
                            "path": f"{catalog}/{schema}/{row[0]}",
                            "type": "file",
                            "icon": "IconTable",
                        }
                        for row in rows
                    ]
        return []

    async def get_content(
        self, path_parts: List[str], connection: "Connection", secrets: Dict[str, Any]
    ) -> "VfsFileContentResponse":
        """
        Reads a preview of a table's content using the SQLAlchemy engine.
        """
        if len(path_parts) != 3:
            raise FileNotFoundError(
                "Invalid path for Trino table. Expected: [catalog]/[schema]/[table]"
            )

        catalog, schema, table = path_parts
        full_vfs_path = f"vfs://connections/{connection.id}/{'/'.join(path_parts)}"

        async with self.get_client(connection, secrets) as engine:
            async with engine.connect() as conn:
                query = text(f'SELECT * FROM "{catalog}"."{schema}"."{table}" LIMIT 10')
                result_proxy = await conn.execute(query)

                # Use .mappings().all() to get a list of dict-like objects
                rows = result_proxy.mappings().all()
                columns = list(result_proxy.keys())

        # Safely serialize the results to handle complex data types
        content_payload = {"columns": columns, "rows": safe_serialize(rows)}
        content_as_string = json.dumps(content_payload, indent=2)

        now = datetime.now(timezone.utc)
        metadata = VfsNodeMetadata(
            can_write=False, is_versioned=False, etag=now.isoformat()
        )

        return VfsFileContentResponse(
            path=full_vfs_path,
            content=content_as_string,
            mime_type="application/json",
            last_modified=now,
            size=len(content_as_string.encode("utf-8")),
            metadata=metadata,
        )

    async def dry_run(
        self,
        connection: "Connection",
        secrets: Dict[str, Any],
        action_params: Dict[str, Any],
    ) -> "DryRunResult":
        """A dry run for Trino checks if all required connection parameters are present."""
        config = {**connection.details, **secrets}
        required_fields = ["host", "port", "user", "catalog"]
        missing = [field for field in required_fields if field not in config]

        if missing:
            return DryRunResult(
                indicates_failure=True,
                message=f"Dry run failed: Missing required Trino connection fields: {', '.join(missing)}",
            )

        return DryRunResult(
            indicates_failure=False,
            message="Dry run successful: All required connection parameters are present.",
        )
