import asyncio
import json
import shutil
import tarfile
import tempfile
from pathlib import Path
from typing import Dict, Any, Optional

import httpx
import yaml
from rich.console import Console
from rich.table import Table
from rich.markdown import Markdown
from prompt_toolkit import PromptSession
import structlog

from ..engine.connector.config import ConnectionResolver, CX_HOME
from .registry_manager import RegistryManager

console = Console()
logger = structlog.get_logger(__name__)
APPS_DOWNLOAD_URL_TEMPLATE = (
    "https://github.com/syncropel/applications/releases/download/{tag}/{asset_name}"
)
APPS_MANIFEST_FILE = CX_HOME / "apps.json"


class AppManager:
    """A service for discovering, installing, and managing Syncropel Applications."""

    def __init__(self, executor=None, cx_home_path: Optional[Path] = None):
        _cx_home = cx_home_path or CX_HOME
        # Update the module-level constants used by the manager
        global APPS_MANIFEST_FILE
        APPS_MANIFEST_FILE = _cx_home / "apps.json"

        self.resolver = ConnectionResolver(cx_home_path=cx_home_path)
        self.registry_manager = RegistryManager()
        self.executor = executor
        _cx_home.mkdir(exist_ok=True, parents=True)

    def _load_local_manifest(self) -> Dict[str, Any]:
        if not APPS_MANIFEST_FILE.exists():
            return {"installed_apps": {}}
        try:
            return json.loads(APPS_MANIFEST_FILE.read_text())
        except (json.JSONDecodeError, FileNotFoundError):
            return {"installed_apps": {}}

    def _save_local_manifest(self, manifest_data: Dict[str, Any]):
        json_content = json.dumps(manifest_data, indent=2)
        APPS_MANIFEST_FILE.write_text(json_content)

    async def search(self, query: Optional[str] = None):
        """Searches the public application registry."""
        with console.status("Fetching public application registry..."):
            apps = await self.registry_manager.get_available_applications()
        if not apps:
            console.print(
                "[yellow]No applications found in the public registry.[/yellow]"
            )
            return

        table = Table(title="Publicly Available Applications")
        table.add_column("ID", style="cyan", no_wrap=True)
        table.add_column("Version", style="magenta")
        table.add_column("Description", overflow="fold")

        for app in apps:
            if (
                not query
                or query.lower()
                in f"{app.get('id', '')} {app.get('description', '')}".lower()
            ):
                table.add_row(app.get("id"), app.get("version"), app.get("description"))
        console.print(table)

    async def list_installed_apps(self):
        """Lists locally installed applications."""
        manifest = self._load_local_manifest()
        apps = manifest.get("installed_apps", {})
        if not apps:
            console.print(
                "No applications are currently installed. Use `app search` to discover new ones."
            )
            return

        table = Table(title="Locally Installed Applications")
        table.add_column("ID", style="cyan")
        table.add_column("Version", style="magenta")
        table.add_column("Asset Count", style="green", justify="right")
        for app_id, details in apps.items():
            table.add_row(
                app_id,
                details.get("version", "N/A"),
                str(len(details.get("assets", []))),
            )
        console.print(table)

    async def install(self, args: Dict[str, Any], no_interactive: bool = False):
        """Installs an application from a specified source using named arguments."""

        # --- NEW, ROBUST LOGIC ---
        source_id = args.get("--id")
        source_path = args.get("--path")
        source_url = args.get("--url")
        # --- END NEW LOGIC ---

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            archive_path = tmp_path / "app.tar.gz"

            if source_path:
                console.print(f"Installing from local file: [cyan]{source_path}[/cyan]")
                shutil.copy(source_path, archive_path)
            elif source_url:
                with console.status(
                    f"Downloading application from {source_url[:70]}..."
                ):
                    async with httpx.AsyncClient(
                        follow_redirects=True, timeout=120.0
                    ) as client:
                        response = await client.get(source_url)
                        response.raise_for_status()
                    archive_path.write_bytes(response.content)
            elif source_id:
                with console.status(f"Resolving '{source_id}' from registry..."):
                    apps = await self.registry_manager.get_available_applications()
                    app_meta = next(
                        (app for app in apps if app.get("id") == source_id), None
                    )
                if not app_meta:
                    console.print(
                        f"[bold red]Error:[/bold red] Application '{source_id}' not found in the public registry."
                    )
                    return
                namespace, name = source_id.split("/")
                version = app_meta["version"]
                tag = f"{namespace}-{name}-v{version}"
                asset_name = f"{name}-v{version}.tar.gz"
                download_url = APPS_DOWNLOAD_URL_TEMPLATE.format(
                    tag=tag, asset_name=asset_name
                )

                with console.status(f"Downloading {source_id}@{version}..."):
                    async with httpx.AsyncClient(
                        follow_redirects=True, timeout=120.0
                    ) as client:
                        response = await client.get(download_url)
                        response.raise_for_status()
                    archive_path.write_bytes(response.content)
            else:
                console.print(
                    "[bold red]Error:[/bold red] Install source not specified. Use --id, --path, or --url."
                )
                return

            with console.status("Unpacking application assets..."):
                with tarfile.open(archive_path, "r:gz") as tar:
                    tar.extractall(path=tmp_path)

            app_manifest_path = next(tmp_path.rglob("app.cx.yaml"), None)
            if not app_manifest_path:
                console.print(
                    "[bold red]Error:[/bold red] Application package is invalid: missing 'app.cx.yaml'."
                )
                return

            app_source_dir = app_manifest_path.parent
            with open(app_manifest_path, "r") as f:
                app_manifest = yaml.safe_load(f)

            app_id = f"{app_manifest.get('namespace', 'unknown')}/{app_manifest.get('name', 'unknown')}"
            version = app_manifest.get("version", "0.0.0")

            local_manifest = self._load_local_manifest()
            if app_id in local_manifest["installed_apps"]:
                console.print(
                    f"[yellow]Application '{app_id}' is already installed.[/yellow]"
                )
                return

            await self._install_assets_and_run_wizard(
                app_id, version, app_manifest, app_source_dir, no_interactive
            )

    async def _install_assets_and_run_wizard(
        self, app_id, version, app_manifest, app_source_dir, no_interactive
    ):
        """Helper to perform the installation after downloading and unpacking."""
        with console.status("Resolving blueprint dependencies..."):
            for blueprint_id in app_manifest.get("dependencies", {}).get(
                "blueprints", []
            ):
                try:
                    await asyncio.to_thread(
                        self.resolver.load_blueprint_by_id, blueprint_id
                    )
                except Exception as e:
                    console.print(
                        f"[bold red]Failed to resolve blueprint dependency '{blueprint_id}': {e}[/bold red]"
                    )
                    return

        with console.status("Installing application assets..."):
            installed_assets = []
            for asset_type in ["flows", "queries", "scripts", "templates"]:
                source_dir = app_source_dir / asset_type
                if source_dir.is_dir():
                    target_dir = CX_HOME / asset_type
                    target_dir.mkdir(exist_ok=True)
                    for item in source_dir.iterdir():
                        shutil.copy(item, target_dir)
                        installed_assets.append(f"{asset_type}/{item.name}")

        local_manifest = self._load_local_manifest()
        local_manifest["installed_apps"][app_id] = {
            "version": version,
            "assets": installed_assets,
            "dependencies": app_manifest.get("dependencies", {}),
        }
        self._save_local_manifest(local_manifest)
        console.print(
            "  [green]✓[/green] All assets installed and tracked in manifest."
        )

        if not no_interactive and self.executor:
            required_conns = app_manifest.get("required_connections", [])
            if required_conns:
                console.print("\n[bold]Application Setup: Required Connections[/bold]")
                for conn_req in required_conns:
                    console.print(
                        f"\n--- Setting up connection: [bold cyan]{conn_req['id']}[/bold cyan] ---"
                    )
                    console.print(f"[dim]{conn_req['description']}[/dim]")
                    await self.executor.connection_manager.create_interactive(
                        preselected_blueprint_id=conn_req["blueprint"]
                    )

        console.print(
            f"\n[bold green]✓[/bold green] Successfully installed application [cyan]{app_id}@{version}[/cyan]."
        )

        readme_path = app_source_dir / "README.md"
        if readme_path.exists():
            console.print("\n--- Application README ---")
            console.print(Markdown(readme_path.read_text()))

    async def uninstall(self, app_id: str):
        """Uninstalls an application and removes its assets."""
        manifest = self._load_local_manifest()
        app_to_remove = manifest.get("installed_apps", {}).get(app_id)
        if not app_to_remove:
            console.print(
                f"[bold red]Error:[/bold red] Application '{app_id}' is not installed."
            )
            return

        console.print(
            "The following assets will be [bold red]DELETED[/bold red] from your `~/.cx` directory:"
        )
        for asset in app_to_remove.get("assets", []):
            console.print(f"- {asset}")

        session = PromptSession()
        confirmed = await session.prompt_async(
            f"\nAre you sure you want to uninstall '{app_id}'? [y/n]: "
        )
        if confirmed.lower() == "y":
            with console.status(f"Uninstalling {app_id}..."):
                for asset_path_str in app_to_remove.get("assets", []):
                    full_path = CX_HOME / asset_path_str
                    if full_path.exists():
                        full_path.unlink()
                del manifest["installed_apps"][app_id]
                self._save_local_manifest(manifest)
            console.print(
                f"[bold green]✓[/bold green] Application '{app_id}' has been uninstalled."
            )
        else:
            console.print("[yellow]Uninstallation cancelled.[/yellow]")

    async def package(self, app_path_str: str):
        """Packages a local application directory into a distributable archive."""
        app_path = Path(app_path_str).resolve()
        manifest_path = app_path / "app.cx.yaml"
        if not manifest_path.exists():
            console.print(
                f"[bold red]Error:[/bold red] Manifest 'app.cx.yaml' not found in '{app_path}'."
            )
            return

        with open(manifest_path, "r") as f:
            manifest = yaml.safe_load(f)
        name = manifest.get("name", app_path.name)
        version = manifest.get("version", "0.0.0")
        archive_name = f"{name}-v{version}.tar.gz"

        with console.status(f"Creating package '{archive_name}'..."):
            with tarfile.open(archive_name, "w:gz") as tar:
                # Add the contents of the app_path directory to the archive's root.
                for item in app_path.iterdir():
                    tar.add(item, arcname=item.name)

        console.print(
            f"[bold green]✓[/bold green] Successfully created application package: [cyan]{archive_name}[/cyan]"
        )
