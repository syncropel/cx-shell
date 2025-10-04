import sys
import httpx
import tempfile
import tarfile
import zipfile
import shutil
import os
import stat
from pathlib import Path
import importlib.metadata

from rich.console import Console
from rich.progress import Progress
from packaging.version import parse as parse_version

console = Console()

GITHUB_REPO = "syncropel/cx-shell"
API_URL = f"https://api.github.com/repos/{GITHUB_REPO}/releases/latest"


class UpgradeManager:
    """Handles the self-upgrade logic for the cx shell."""

    def get_current_version(self):
        """Gets the currently installed version of the application."""
        try:
            # For packaged app, this reads from METADATA.
            # For dev env, it reads from pyproject.toml.
            return importlib.metadata.version("cx-shell")
        except importlib.metadata.PackageNotFoundError:
            return "0.0.0-dev"

    def get_platform_asset_identifier(self) -> str:
        """Determines the string identifier for the current OS and architecture."""
        os_name = sys.platform
        if os_name == "linux":
            return "linux-x86_64"
        elif os_name == "darwin":
            return "macos-x86_64"
        elif os_name == "win32":
            return "windows-amd64"
        else:
            raise NotImplementedError(f"Unsupported operating system: {os_name}")

    def run_upgrade(self):
        """The main entry point for the upgrade process."""
        current_version_str = self.get_current_version()
        current_version = parse_version(current_version_str)
        console.print(f"Current version: [cyan]{current_version}[/cyan]")

        # --- THIS IS THE FIX ---
        # Phase 1: Check for updates with a spinner.
        with console.status("Checking for the latest version...") as status:
            try:
                response = httpx.get(API_URL, timeout=10.0)
                response.raise_for_status()
                latest_release = response.json()
                latest_version_str = latest_release["tag_name"].lstrip("v")
                latest_version = parse_version(latest_version_str)
            except Exception as e:
                console.print(
                    f"[bold red]Error:[/bold red] Could not check for updates. {e}"
                )
                return

        # The spinner automatically stops when the `with` block exits.

        # Phase 2: Handle comparison and confirmation without a spinner.
        if latest_version <= current_version:
            console.print(
                "[bold green]You are already running the latest version.[/bold green]"
            )
            return

        console.print(
            f"A new version is available: [bold green]{latest_version}[/bold green]"
        )

        asset_identifier = self.get_platform_asset_identifier()
        asset_to_download = next(
            (
                asset
                for asset in latest_release.get("assets", [])
                if asset_identifier in asset["name"]
            ),
            None,
        )

        if not asset_to_download:
            console.print(
                f"[bold red]Error:[/bold red] Could not find a suitable download for your platform ({asset_identifier})."
            )
            return

        # Now that the spinner is gone, this input prompt will work without deadlocking.
        confirmed = console.input(
            f"Do you want to upgrade to version {latest_version}? [Y/n]: "
        ).lower()
        if confirmed not in ("y", "yes", ""):
            console.print("Upgrade cancelled.")
            return

        # Phase 3: Perform the actual upgrade (we can use a new spinner here).
        self._perform_upgrade(asset_to_download, latest_version_str)
        # --- END FIX ---

    def _perform_upgrade(self, asset: dict, latest_version_str: str):
        """Handles the download, extraction, and replacement of the binary."""
        download_url = asset["browser_download_url"]
        asset_name = asset["name"]
        asset_size = asset["size"]
        current_executable_path = Path(sys.executable)

        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                tmp_path = Path(tmpdir)
                archive_path = tmp_path / asset_name

                console.print(f"Downloading [cyan]{asset_name}[/cyan]...")
                with Progress() as progress:
                    task = progress.add_task("[green]Downloading...", total=asset_size)
                    with httpx.stream(
                        "GET", download_url, follow_redirects=True, timeout=60.0
                    ) as response:
                        response.raise_for_status()
                        with open(archive_path, "wb") as f:
                            for chunk in response.iter_bytes():
                                f.write(chunk)
                                progress.update(task, advance=len(chunk))

                with console.status("Extracting and replacing executable...") as status:
                    status.update("Extracting new version...")
                    binary_name = "cx.exe" if "windows" in asset_name.lower() else "cx"
                    extracted_binary_path = tmp_path / binary_name

                    if asset_name.endswith(".tar.gz"):
                        with tarfile.open(archive_path, "r:gz") as tar:
                            tar.extract(binary_name, path=tmp_path)
                    elif asset_name.endswith(".zip"):
                        with zipfile.ZipFile(archive_path, "r") as zipf:
                            zipf.extract(binary_name, path=tmp_path)

                    if not extracted_binary_path.exists():
                        raise FileNotFoundError(
                            f"Could not find '{binary_name}' in the downloaded archive."
                        )

                    st = os.stat(extracted_binary_path)
                    os.chmod(extracted_binary_path, st.st_mode | stat.S_IEXEC)

                    status.update("Replacing current executable...")
                    old_executable_path = current_executable_path.with_suffix(
                        f"{current_executable_path.suffix}.old"
                    )

                    try:
                        os.replace(extracted_binary_path, current_executable_path)
                    except OSError:
                        if current_executable_path.exists():
                            current_executable_path.rename(old_executable_path)
                        shutil.move(extracted_binary_path, current_executable_path)

                    if old_executable_path.exists():
                        try:
                            old_executable_path.unlink()
                        except OSError:
                            pass

            console.print(
                f"\n[bold green]✓ Upgrade to version {latest_version_str} successful![/bold green]"
            )
            console.print(
                "Please close this terminal and open a new one to use the new version."
            )

        except PermissionError:
            console.print("\n[bold red]Permission Denied.[/bold red]")
            console.print(f"Could not write to [cyan]{current_executable_path}[/cyan].")
            console.print(
                "Please try again with `sudo cx upgrade`, or perform a manual upgrade."
            )
        except Exception as e:
            console.print(
                f"\n[bold red]An error occurred during the upgrade process:[/bold red] {e}"
            )
            if "old_executable_path" in locals() and old_executable_path.exists():
                try:
                    if not current_executable_path.exists():
                        old_executable_path.rename(current_executable_path)
                        console.print(
                            "[yellow]Attempted to restore the previous version.[/yellow]"
                        )
                except Exception as restore_e:
                    console.print(
                        f"[bold red]Failed to restore previous version: {restore_e}[/bold red]"
                    )
