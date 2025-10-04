from typing import Any, Dict, TYPE_CHECKING
import structlog
from urllib.parse import quote_plus

from .base_sqlalchemy_strategy import BaseSqlAlchemyStrategy
from .....data.agent_schemas import DryRunResult


if TYPE_CHECKING:
    from cx_core_schemas.connection import Connection

logger = structlog.get_logger(__name__)


class MssqlStrategy(BaseSqlAlchemyStrategy):
    """
    SQLAlchemy-based strategy for connecting to Microsoft SQL Server.
    """

    strategy_key = "sql-mssql"
    dialect_driver = "mssql+aioodbc"

    def _get_connection_url(
        self, connection: "Connection", secrets: Dict[str, Any]
    ) -> str:
        """
        Constructs a safe SQLAlchemy connection URL for MSSQL,
        ensuring that special characters in credentials are properly escaped.
        """
        config = {**connection.details, **secrets}

        username = config.get("username")
        password = config.get("password")
        server = config.get("server")
        database = config.get("database")

        if not all([username, password, server, database]):
            raise ValueError(
                "Missing required fields (server, database, username, password) for MSSQL connection."
            )

        driver = "ODBC Driver 18 for SQL Server"

        # URL-encode the username and password to handle any special characters safely.
        # This prevents characters like '@', ':', '/', '$' from breaking the connection string format.
        encoded_username = quote_plus(username)
        encoded_password = quote_plus(password)

        # Construct the URL with the now-safe, encoded credentials.
        conn_url = (
            f"{self.dialect_driver}://{encoded_username}:{encoded_password}@{server}/{database}"
            f"?driver={driver.replace(' ', '+')}&TrustServerCertificate=yes"
        )

        logger.debug(
            "Constructed MSSQL connection URL.", server=server, database=database
        )
        return conn_url

    async def dry_run(
        self,
        connection: "Connection",
        secrets: Dict[str, Any],
        action_params: Dict[str, Any],
    ) -> "DryRunResult":
        """
        A dry run for MSSQL checks if all required connection details and secrets are present.
        """
        config = {**connection.details, **secrets}
        required_fields = ["server", "database", "username", "password"]

        missing_fields = [field for field in required_fields if field not in config]

        if missing_fields:
            return DryRunResult(
                indicates_failure=True,
                message=f"Dry run failed: Missing required connection fields: {', '.join(missing_fields)}",
            )

        return DryRunResult(
            indicates_failure=False,
            message="Dry run successful: All required connection parameters are present.",
        )
