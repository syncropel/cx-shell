from pathlib import Path
import pytest

from cx_shell.engine.connector.config import ConnectionResolver
from cx_core_schemas.api_catalog import ApiCatalog
from cx_shell import utils


def test_init_command_succeeds_locally(isolated_cx_home: Path, monkeypatch):
    """
    Unit Test: Verifies the `cx init` command correctly populates a clean
    workspace directory from its bundled assets.
    """
    from cx_shell.cli import init as cx_init_func

    # For this specific CLI function test, monkeypatching is still the cleanest way
    # to override the global constants it directly imports.
    monkeypatch.setattr(utils, "CX_HOME", isolated_cx_home)
    # This was the missing piece: we must also patch the config module's constant.
    from cx_shell.engine.connector import config as connector_config

    monkeypatch.setattr(
        connector_config, "BLUEPRINTS_BASE_PATH", isolated_cx_home / "blueprints"
    )

    # Act: Run the global workspace initialization.
    cx_init_func(project_name=None)

    # Assert: Verify assets were copied into our temporary directory.
    assert (isolated_cx_home / "connections").is_dir()
    assert (isolated_cx_home / "connections" / "github.conn.yaml").is_file()
    assert (isolated_cx_home / "blueprints" / "community" / "github" / "0.1.0").is_dir()


@pytest.mark.network
def test_resolver_on_demand_blueprint_download(isolated_cx_home: Path):
    """
    Integration Test: Verifies the ConnectionResolver can successfully
    download and cache a real blueprint to a specified directory.
    """
    # --- Definitive Fix: Explicitly instantiate the resolver with the test path ---
    resolver = ConnectionResolver(cx_home_path=isolated_cx_home)

    blueprint_id_to_test = "community/sendgrid@0.3.0"
    catalog = resolver.load_blueprint_by_id(blueprint_id_to_test)

    assert isinstance(catalog, ApiCatalog)
    assert catalog.name == "SendGrid API"

    # Assert that the file was downloaded into our isolated directory
    expected_path = isolated_cx_home / "blueprints" / "community" / "sendgrid" / "0.3.0"
    assert (expected_path / "blueprint.cx.yaml").is_file()
    assert (expected_path / "schemas.py").is_file()
