# /src/cx_shell/management/app_manager.py

import asyncio
import json
import shutil
import tarfile
import tempfile
from pathlib import Path
from typing import Optional

import httpx
import yaml
from rich.console import Console
from rich.table import Table
import structlog

# Use the new project schemas and our refined naming conventions
from cx_core_schemas.project import ProjectManifest, Lockfile, LockedPackage
from ..utils import CX_HOME
from .registry_manager import RegistryManager

console = Console()
logger = structlog.get_logger(__name__)

# --- NEW: Consistent paths inspired by Nix and pnpm ---
# Global, immutable cache for downloaded application tarballs
APPS_CACHE_DIR = CX_HOME / "store" / "app-cache"
# Global, immutable store where unpacked applications live
APPS_STORE_DIR = CX_HOME / "store" / "syncropel-apps"


class AppManager:
    """
    A service for managing Syncropel Applications as project-level dependencies,
    inspired by modern package managers like uv and pnpm.
    """

    def __init__(self, executor=None, cx_home_path: Optional[Path] = None):
        _cx_home = cx_home_path or CX_HOME
        # Update module-level constants
        global APPS_CACHE_DIR, APPS_STORE_DIR
        APPS_CACHE_DIR = _cx_home / "store" / "app-cache"
        APPS_STORE_DIR = _cx_home / "store" / "syncropel-apps"

        APPS_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        APPS_STORE_DIR.mkdir(parents=True, exist_ok=True)

        self.registry_manager = RegistryManager()
        self.executor = executor  # Retained for potential future interactive features

    # --- Core Package Management Logic (New Architecture) ---

    async def install(self, project_root: Path, app_id: Optional[str]):
        """
        Primary entry point for the `cx app install` command.
        - If app_id is provided, adds it as a new dependency.
        - If app_id is None, syncs dependencies from the lockfile.
        """
        if app_id:
            await self._install_package(project_root, app_id)
        else:
            await self._install_from_lockfile(project_root)

    async def _install_from_lockfile(self, project_root: Path):
        """
        "Hydrates" a project's workspace by reading the lockfile, downloading
        any missing dependencies, and creating the local symlink structure.
        """
        lockfile_path = project_root / "cx.lock.json"
        if not lockfile_path.exists():
            console.print(
                "[yellow]No `cx.lock.json` file found. Use `cx app install <app-id>` to add a dependency.[/yellow]"
            )
            return

        with console.status("Syncing project dependencies from lockfile...") as status:
            lockfile = self._load_lockfile(lockfile_path)

            download_tasks = []
            for app_id, locked_pkg in lockfile.packages.items():
                status.update(f"Checking {app_id}@{locked_pkg.version}...")
                download_tasks.append(
                    self._download_and_unpack_to_store(app_id, locked_pkg.version)
                )
            await asyncio.gather(*download_tasks)

            status.update("Creating virtual workspace...")
            assets_dir = project_root / ".cx" / "store"
            if assets_dir.exists():
                shutil.rmtree(assets_dir)
            assets_dir.mkdir(parents=True)

            for app_id, locked_pkg in lockfile.packages.items():
                namespace, name = app_id.split("/")
                source_path = APPS_STORE_DIR / namespace / name / locked_pkg.version
                link_path = assets_dir / namespace / name

                link_path.parent.mkdir(parents=True, exist_ok=True)
                link_path.symlink_to(source_path, target_is_directory=True)

        console.print("[bold green]✅ Project synced successfully.[/bold green]")

    async def _install_package(self, project_root: Path, app_id: str):
        """
        Adds an application as a new dependency to a project.
        """
        console.print(f"Resolving and adding [cyan]{app_id}[/cyan] to project...")

        manifest_path = project_root / "cx.project.yaml"
        lockfile_path = project_root / "cx.lock.json"

        manifest = self._load_project_manifest(manifest_path)
        lockfile = self._load_lockfile(lockfile_path)

        with console.status(f"Fetching metadata for '{app_id}' from registry..."):
            app_meta = await self.registry_manager.get_application_metadata(app_id)
            if not app_meta:
                console.print(
                    f"[bold red]Error:[/bold red] Application '{app_id}' not found in registry."
                )
                return

        version = app_meta["version"]

        # The download function will now return the checksum of the downloaded archive.
        integrity_hash = await self._download_and_unpack_to_store(app_id, version)

        # Correctly initialize the nested Pydantic model if it's None.
        if manifest.syncropel is None:
            from cx_core_schemas.project import SyncropelSpec

            manifest.syncropel = SyncropelSpec()

        # Use a caret for the version constraint in the human-readable manifest.
        manifest.syncropel.apps[app_id] = f"^{version}"

        # Store the exact, resolved information in the machine-readable lockfile.
        lockfile.packages[app_id] = LockedPackage(
            version=version,
            source="registry",
            integrity=f"sha256-{integrity_hash}",  # Use the hash returned by the download function.
        )

        self._save_project_manifest(manifest_path, manifest)
        self._save_lockfile(lockfile_path, lockfile)

        console.print(
            f"Added [cyan]{app_id}@{version}[/cyan] to project dependencies and updated lockfile."
        )

        # After adding the new package, re-sync the entire project to ensure all
        # symlinks are correctly in place.
        await self._install_from_lockfile(project_root)

    # --- Other Commands ---

    async def search(self, query: Optional[str] = None):
        """Searches the public application registry."""
        # This method's logic remains unchanged.
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

    def list_apps(self, project_root: Path):
        """Lists the applications declared in the project's manifest."""
        # This method now reads from cx.project.yaml
        manifest_path = project_root / "cx.project.yaml"
        if not manifest_path.exists():
            console.print("Not inside a Syncropel project (missing `cx.project.yaml`).")
            return

        manifest = self._load_project_manifest(manifest_path)
        apps = (
            manifest.syncropel.apps
            if manifest.syncropel and manifest.syncropel.apps
            else {}
        )

        if not apps:
            console.print(
                "No applications are declared as dependencies in this project."
            )
            return

        table = Table(title="Project Application Dependencies")
        table.add_column("Application ID", style="cyan")
        table.add_column("Version Constraint", style="magenta")
        for app_id, version in apps.items():
            table.add_row(app_id, version)
        console.print(table)

    # --- Helper and Utility Methods ---

    async def _download_and_unpack_to_store(self, app_id: str, version: str) -> str:
        """
        Idempotent function to download, verify, and unpack an application
        into the global immutable store. Returns the sha256 hexdigest of the archive.
        """
        namespace, name = app_id.split("/")
        final_path = APPS_STORE_DIR / namespace / name / version

        # If the application is already in our global store, the work is done. Return immediately.
        if final_path.exists():
            return ""  # Return empty string as hash is not needed for existing packages

        with console.status(f"Downloading {app_id}@{version}...") as status:
            app_meta = await self.registry_manager.get_application_metadata(
                app_id, version
            )
            if not app_meta:
                raise ValueError(f"Could not find metadata for {app_id}@{version}")

            source_repo = app_meta.get("repository")
            if not source_repo:
                raise ValueError(
                    f"Registry entry for '{app_id}' is missing the 'repository' field."
                )

            tag_name = f"v{version}"
            repo_name = source_repo.split("/")[1]
            asset_name = f"{repo_name}-{tag_name}.tar.gz"
            download_url = f"https://github.com/{source_repo}/releases/download/{tag_name}/{asset_name}"

            status.update(f"Downloading from {download_url}")

            # Implement a robust retry mechanism for transient network errors.
            max_retries = 3
            archive_bytes = None
            for attempt in range(max_retries):
                try:
                    async with httpx.AsyncClient(
                        follow_redirects=True, timeout=120.0
                    ) as client:
                        response = await client.get(download_url)
                        response.raise_for_status()
                    archive_bytes = response.content
                    break  # Success, exit the loop
                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 404 and attempt < max_retries - 1:
                        wait_time = 2 ** (attempt + 1)  # Exponential backoff: 2s, 4s
                        status.update(
                            f"Asset not found (404), likely propagation delay. Retrying in {wait_time}s..."
                        )
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        raise  # Re-raise the error on the final attempt or for non-404 errors.

            if archive_bytes is None:
                raise IOError(
                    f"Failed to download {app_id}@{version} after {max_retries} attempts."
                )

            import hashlib

            integrity_hash = hashlib.sha256(archive_bytes).hexdigest()

        with console.status(f"Installing {app_id}@{version} to global store..."):
            with tempfile.TemporaryDirectory() as tmpdir:
                tmp_path = Path(tmpdir)
                archive_path = tmp_path / asset_name
                archive_path.write_bytes(archive_bytes)

                unpacked_dir = tmp_path / "unpacked"
                with tarfile.open(archive_path, "r:gz") as tar:
                    tar.extractall(path=unpacked_dir)

                final_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(unpacked_dir, final_path)

        logger.info("Installed app to global store.", path=str(final_path))
        return integrity_hash

    # --- Manifest/Lockfile I/O Helpers ---

    def _load_project_manifest(self, path: Path) -> ProjectManifest:
        if not path.exists():
            return ProjectManifest()
        data = yaml.safe_load(path.read_text())
        return ProjectManifest.model_validate(data or {})

    def _save_project_manifest(self, path: Path, manifest: ProjectManifest):
        path.write_text(
            yaml.dump(
                manifest.model_dump(
                    mode="json", exclude_none=True, exclude_defaults=True
                ),
                sort_keys=False,
            )
        )

    def _load_lockfile(self, path: Path) -> Lockfile:
        if not path.exists():
            return Lockfile(metadata={"version": "1"}, packages={})
        data = json.loads(path.read_text())
        return Lockfile.model_validate(data)

    def _save_lockfile(self, path: Path, lockfile: Lockfile):
        path.write_text(lockfile.model_dump_json(indent=2))
