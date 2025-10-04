# ~/repositories/cx-shell/src/cx_shell/engine/connector/config.py
import os
import re
import zipfile
import io
import shutil
from pathlib import Path
from typing import Any, Dict, Tuple

import structlog
import yaml
import httpx
from dotenv import dotenv_values
from pydantic import ValidationError

from cx_core_schemas.connection import Connection
from cx_core_schemas.api_catalog import ApiCatalog
from ...utils import get_assets_root, CX_HOME  # Import from the correct, new location

logger = structlog.get_logger(__name__)

# The BLUEPRINTS_BASE_PATH now depends on the imported CX_HOME
BLUEPRINTS_BASE_PATH = Path(os.getenv("CX_BLUEPRINTS_PATH", CX_HOME / "blueprints"))
BLUEPRINTS_GITHUB_ORG = "syncropel"
BLUEPRINTS_GITHUB_REPO = "blueprints"


class ConnectionResolver:
    """
    Abstracts away the source of connection details and secrets. It also
    handles the on-demand downloading and caching of blueprint packages.
    """

    blueprint_regex = re.compile(
        r"^(?P<namespace>[\w-]+)/(?P<name>[\w-]+)@(?P<version>[\w\.-]+)$"
    )

    def __init__(
        self, db_client: Any = None, vault_client: Any = None, cx_home_path: Path = None
    ):
        self.db = db_client
        self.vault = vault_client
        self.is_standalone = not (db_client and vault_client)

        # Use the provided path, or fall back to the default.
        _cx_home = cx_home_path or CX_HOME
        self.user_connections_dir = _cx_home / "connections"
        self.user_secrets_dir = _cx_home / "secrets"
        # We also need to update where it looks for blueprints
        global BLUEPRINTS_BASE_PATH
        BLUEPRINTS_BASE_PATH = _cx_home / "blueprints"

        logger.info(
            "ConnectionResolver initialized.", blueprints_path=str(BLUEPRINTS_BASE_PATH)
        )

    def _ensure_blueprint_exists_locally(self, blueprint_match: re.Match):
        """
        Ensures a blueprint package is available locally by checking bundled assets,
        then the user cache, and finally attempting to download it.
        """
        parts = blueprint_match.groupdict()
        namespace, name, version_from_id = (
            parts["namespace"],
            parts["name"],
            parts["version"],
        )
        version = version_from_id.lstrip("v")

        user_cache_path = BLUEPRINTS_BASE_PATH / namespace / name / version
        assets_root = get_assets_root()
        bundled_asset_path = assets_root / "blueprints" / namespace / name

        if user_cache_path.is_dir() and any(user_cache_path.iterdir()):
            logger.debug(
                "Blueprint package found in user cache.", path=str(user_cache_path)
            )
            return

        if bundled_asset_path.is_dir() and any(bundled_asset_path.iterdir()):
            logger.debug(
                "Blueprint package found in bundled application assets.",
                path=str(bundled_asset_path),
            )
            return

        logger.info(
            "Blueprint not found locally, attempting remote download...",
            blueprint=blueprint_match.string,
        )
        tag_version = f"v{version}"
        tag_name = f"{namespace}-{name}-{tag_version}"
        asset_name = f"{name}.zip"
        asset_url = f"https://github.com/{BLUEPRINTS_GITHUB_ORG}/{BLUEPRINTS_GITHUB_REPO}/releases/download/{tag_name}/{asset_name}"

        try:
            with httpx.stream(
                "GET", asset_url, follow_redirects=True, timeout=30.0
            ) as response:
                response.raise_for_status()
                zip_content = io.BytesIO(response.read())

            user_cache_path.mkdir(parents=True, exist_ok=True)

            with zipfile.ZipFile(zip_content) as zf:
                for member in zf.infolist():
                    target_path = user_cache_path / member.filename
                    if not member.is_dir():
                        target_path.parent.mkdir(parents=True, exist_ok=True)
                        with open(target_path, "wb") as f:
                            f.write(zf.read(member))

            logger.info(
                "Successfully downloaded and extracted blueprint.",
                path=str(user_cache_path),
            )
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                raise FileNotFoundError(
                    f"Blueprint '{blueprint_match.string}' not found. It does not exist as a bundled asset, in your local cache, or as a remote release at {asset_url}"
                ) from e
            raise IOError(
                f"Failed to download blueprint. HTTP error: {e.response.status_code}"
            ) from e
        except Exception as e:
            if user_cache_path.exists():
                shutil.rmtree(user_cache_path)
            raise IOError(
                f"Failed to download or extract blueprint '{blueprint_match.string}'. Error: {e}"
            ) from e

    def load_blueprint_by_id(self, blueprint_id: str) -> ApiCatalog:
        log = logger.bind(blueprint_id=blueprint_id)
        log.info("Attempting to load blueprint by ID.")
        blueprint_match = self.blueprint_regex.match(blueprint_id)
        if not blueprint_match:
            raise ValueError(
                f"'{blueprint_id}' is not a valid blueprint ID format (e.g., 'namespace/name@version')."
            )
        self._ensure_blueprint_exists_locally(blueprint_match)
        blueprint_data = self._load_blueprint_package(blueprint_match)
        return ApiCatalog(**blueprint_data)

    async def resolve(self, source: str) -> Tuple[Connection, Dict[str, Any]]:
        log = logger.bind(source=source)
        log.info("Resolving connection source.")
        if source.startswith("user:"):
            conn_name = source.split(":", 1)[1]
            conn_path = self.user_connections_dir / f"{conn_name}.conn.yaml"
            if not conn_path.exists():
                raise FileNotFoundError(
                    f"User connection '{conn_name}' not found at: {conn_path}"
                )
            return self._resolve_from_file(conn_path)
        raise ValueError(f"Unknown connection source protocol: {source}")

    def _resolve_from_file(
        self, conn_file: Path
    ) -> Tuple["Connection", Dict[str, Any]]:
        log = logger.bind(path=str(conn_file))
        if not conn_file.is_file():
            raise FileNotFoundError(
                f"Connection configuration file not found: {conn_file}"
            )
        with open(conn_file, "r", encoding="utf-8") as f:
            raw_data = yaml.safe_load(f)
        try:
            if "id" not in raw_data:
                raw_data["id"] = f"user:{conn_file.stem.replace('.conn', '')}"
            connection_model = Connection(**raw_data)
            if self.blueprint_regex.match(connection_model.api_catalog_id or ""):
                try:
                    blueprint_catalog = self.load_blueprint_by_id(
                        connection_model.api_catalog_id
                    )
                    connection_model.catalog = blueprint_catalog
                    log.info(
                        "Successfully loaded and merged blueprint package.",
                        blueprint=connection_model.api_catalog_id,
                    )
                except (FileNotFoundError, ValidationError, ValueError) as e:
                    log.error(
                        "Failed to load blueprint.",
                        blueprint=connection_model.api_catalog_id,
                        error=str(e),
                    )
        except ValidationError as e:
            raise ValueError(f"Invalid schema in '{conn_file.name}': {e}") from e
        secrets: Dict[str, Any] = {}
        secrets_file = (
            self.user_secrets_dir / f"{conn_file.stem.replace('.conn', '')}.secret.env"
        )
        if secrets_file.exists():
            secrets = {
                k.lower(): v
                for k, v in dotenv_values(dotenv_path=secrets_file).items()
                if v is not None
            }
        return connection_model, secrets

    def _load_blueprint_package(self, blueprint_match: re.Match) -> Dict[str, Any]:
        parts = blueprint_match.groupdict()
        namespace, name, version = (
            parts["namespace"],
            parts["name"],
            parts["version"].lstrip("v"),
        )

        user_cache_dir = BLUEPRINTS_BASE_PATH / namespace / name / version
        assets_root = get_assets_root()
        bundled_asset_dir = assets_root / "blueprints" / namespace / name

        blueprint_dir = None
        if (user_cache_dir / "blueprint.cx.yaml").is_file():
            blueprint_dir = user_cache_dir
            logger.debug("Loading blueprint from user cache.", path=str(blueprint_dir))
        elif (bundled_asset_dir / "blueprint.cx.yaml").is_file():
            blueprint_dir = bundled_asset_dir
            logger.debug(
                "Loading blueprint from bundled assets.", path=str(blueprint_dir)
            )
        else:
            raise FileNotFoundError(
                f"Blueprint package '{namespace}/{name}@{version}' could not be found after checks."
            )

        blueprint_path = blueprint_dir / "blueprint.cx.yaml"
        schemas_py_path = blueprint_dir / "schemas.py"

        with open(blueprint_path, "r", encoding="utf-8") as f:
            blueprint_data = yaml.safe_load(f)
        if schemas_py_path.is_file():
            blueprint_data["schemas_module_path"] = str(schemas_py_path)
        return blueprint_data
