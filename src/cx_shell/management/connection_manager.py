from pathlib import Path
import yaml
from typing import Dict, Optional

from rich.console import Console
from rich.table import Table
from prompt_toolkit.completion import WordCompleter
from prompt_toolkit.shortcuts import PromptSession
from prompt_toolkit.formatted_text import HTML
from ..engine.connector.config import CX_HOME
from rich import box
from .registry_manager import RegistryManager  # ADD THIS


# Use a single, shared console for all rich output.
console = Console()


class ConnectionManager:
    """A service for managing local connection configurations."""

    def __init__(self, cx_home_path: Optional[Path] = None):
        _cx_home = cx_home_path or CX_HOME
        self.connections_dir = _cx_home / "connections"
        self.secrets_dir = _cx_home / "secrets"
        self.connections_dir.mkdir(exist_ok=True, parents=True)
        self.secrets_dir.mkdir(exist_ok=True, parents=True)
        self.registry_manager = RegistryManager()

    def list_connections(self) -> list[dict]:  # Change return type
        """Lists all locally configured connections, returning data."""
        connections_data = []
        if not any(self.connections_dir.iterdir()):
            # Return an empty list instead of printing
            return connections_data

        for conn_file in sorted(self.connections_dir.glob("*.conn.yaml")):
            try:
                with open(conn_file, "r") as f:
                    data = yaml.safe_load(f)
                    conn_id = data.get("id", "user:N/A").split(":", 1)[1]
                    connections_data.append(
                        {
                            "Name": data.get("name", "N/A"),
                            "ID": conn_id,
                            "Blueprint ID": data.get("api_catalog_id", "N/A"),
                        }
                    )
            except Exception:
                connections_data.append(
                    {
                        "Name": f"[red]Error parsing: {conn_file.name}[/red]",
                        "ID": "",
                        "Blueprint ID": "",
                    }
                )

        return connections_data  # Return the structured data

    async def create_interactive(
        self, preselected_blueprint_id: Optional[str] = None
    ) -> Optional[str]:
        """
        Asynchronously and interactively creates a new connection, with a discovery wizard.
        Returns the ID of the created connection on success, otherwise None.
        """
        console.print("[bold green]--- Create a New Connection ---[/bold green]")

        blueprint_id = preselected_blueprint_id
        session = PromptSession()

        if not blueprint_id:
            # --- DISCOVERY WIZARD ---
            console.print(
                "\n[bold]How would you like to find a blueprint for your connection?[/bold]"
            )
            console.print("  [cyan]1[/cyan]: Search the Public Registry (Recommended)")
            console.print("  [cyan]2[/cyan]: Enter a Blueprint ID manually")

            choice = await session.prompt_async("Choose an option [1]: ", default="1")

            if choice == "1":
                try:
                    with console.status(
                        "[yellow]Fetching public blueprint registry...[/yellow]"
                    ):
                        blueprints = (
                            await self.registry_manager.get_available_blueprints()
                        )

                    if not blueprints:
                        console.print(
                            "[yellow]Could not find any blueprints in the public registry.[/yellow]"
                        )
                        return None

                    table = Table(title="Available Blueprints", box=box.ROUNDED)
                    table.add_column("#", style="yellow")
                    table.add_column("ID", style="cyan")
                    table.add_column("Version", style="magenta")
                    table.add_column("Description", overflow="fold")

                    for i, bp in enumerate(blueprints):
                        table.add_row(
                            str(i + 1),
                            bp.get("id"),
                            bp.get("version"),
                            bp.get("description"),
                        )

                    console.print(table)
                    bp_choice_str = await session.prompt_async(
                        f"Enter the number of the blueprint to use [1-{len(blueprints)}]: "
                    )

                    chosen_index = int(bp_choice_str) - 1
                    if 0 <= chosen_index < len(blueprints):
                        bp_meta = blueprints[chosen_index]
                        blueprint_id = f"{bp_meta['id']}@{bp_meta['version']}"
                    else:
                        raise ValueError("Selection out of range.")

                except (ValueError, IndexError):
                    console.print("[bold red]Invalid selection. Aborting.[/bold red]")
                    return None
                except Exception as e:
                    console.print(f"[bold red]Could not fetch registry: {e}[/bold red]")
                    return None

            else:  # Fallback to manual entry
                blueprint_id = await session.prompt_async(
                    "Enter the Blueprint ID to use (e.g., community/spotify@1.0.0): "
                )

        if not blueprint_id or not blueprint_id.strip():
            console.print("[yellow]No blueprint selected. Aborting.[/yellow]")
            return None

        console.print(
            f"\nGreat! Let's set up a connection for '[bold magenta]{blueprint_id}[/bold magenta]'."
        )

        status_text = (
            f"Loading blueprint [bold magenta]{blueprint_id}[/bold magenta]..."
        )
        with console.status(status_text, spinner="dots"):
            try:
                catalog = self.resolver.load_blueprint_by_id(blueprint_id)
                auth_methods = catalog.supported_auth_methods
                if not auth_methods:
                    raise ValueError(
                        "Blueprint does not define any `supported_auth_methods`."
                    )
            except Exception as e:
                console.print(
                    f"\n[bold red]Error:[/bold red] Could not load blueprint '{blueprint_id}'."
                )
                console.print(f"[dim]Details: {e}[/dim]")
                return None

        chosen_method = auth_methods[0]
        if len(auth_methods) > 1:
            console.print("\n[bold]Select an authentication method:[/bold]")
            choices = {str(i + 1): method for i, method in enumerate(auth_methods)}
            for i, method in choices.items():
                console.print(f"  [cyan]{i}[/cyan]: {method.display_name}")
            choice_str = await session.prompt_async(
                "Enter your choice (1): ",
                completer=WordCompleter(list(choices.keys())),
                default="1",
            )
            chosen_method = choices.get(choice_str, auth_methods[0])

        console.print(
            f"\nPlease provide the following details for '[yellow]{chosen_method.display_name}[/yellow]':"
        )
        conn_name = await session.prompt_async(
            "Enter a friendly name for this connection: "
        )
        default_id = conn_name.lower().replace(" ", "-").replace("_", "-")
        conn_id = await session.prompt_async(
            f"Enter a unique ID (alias) for this connection [{default_id}]: ",
            default=default_id,
        )

        details, secrets = {}, {}
        for field in chosen_method.fields:
            value = await session.prompt_async(
                f"{field.label}: ", is_password=field.is_password
            )
            if field.type == "secret":
                secrets[field.name] = value
            else:
                details[field.name] = value

        conn_content = {
            "name": conn_name,
            "id": f"user:{conn_id}",
            "api_catalog_id": blueprint_id,
            "auth_method_type": chosen_method.type,
            "details": details,
        }
        secrets_content = "\n".join(
            [f"{key.upper()}={value}" for key, value in secrets.items()]
        )

        conn_file = self.connections_dir / f"{conn_id}.conn.yaml"
        secret_file = self.secrets_dir / f"{conn_id}.secret.env"

        console.print("\n[bold]Configuration to be saved:[/bold]")
        console.print(yaml.dump(conn_content, sort_keys=False))

        confirmed = await session.prompt_async(
            HTML("\nDo you want to save this connection? [<b>y</b>/n]: "), default="y"
        )
        if confirmed.lower() != "n":
            conn_file.write_text(yaml.dump(conn_content, sort_keys=False))
            if secrets:
                secret_file.write_text(secrets_content)
            console.print(
                f"\n[bold green]✅ Connection '{conn_name}' saved successfully![/bold green]"
            )
            return conn_id
        else:
            console.print("\n[bold yellow]Aborted.[/bold yellow]")
            return None

    def create_non_interactive(
        self, name: str, id: str, blueprint_id: str, details: Dict, secrets: Dict
    ):
        """Creates a connection non-interactively from provided arguments."""
        console.print(
            f"[bold green]--- Creating Connection '{name}' (Non-Interactive) ---[/bold green]"
        )

        # In a future version, this would also load the blueprint to validate the provided fields.
        # For now, we trust the user has provided the correct details and secrets.
        conn_content = {
            "name": name,
            "id": f"user:{id}",
            "api_catalog_id": blueprint_id,
            "auth_method_type": "credentials",  # A reasonable default, may need to be specified in the future.
            "details": details,
        }
        secrets_content = "\n".join(
            [f"{key.upper()}={value}" for key, value in secrets.items()]
        )

        conn_file = self.connections_dir / f"{id}.conn.yaml"
        secret_file = self.secrets_dir / f"{id}.secret.env"

        conn_file.write_text(yaml.dump(conn_content, sort_keys=False))
        secret_file.write_text(secrets_content)

        console.print(
            f"✅ Connection '{name}' saved successfully to [dim]{conn_file}[/dim]"
        )
