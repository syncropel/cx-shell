# /home/dpwanjala/repositories/cx-shell/tests/engine/connector/test_resolver.py

import io
import zipfile
from pathlib import Path
from pytest_mock import MockerFixture

from cx_shell.engine.connector.config import ConnectionResolver
from cx_core_schemas.api_catalog import ApiCatalog
from cx_shell import utils

# Note: The 'isolated_cx_home' fixture is defined in conftest.py and automatically available.


def test_init_command_succeeds_locally(isolated_cx_home: Path, monkeypatch):
    """
    Unit Test: Verifies the `cx init` command correctly populates a clean
    workspace directory from its bundled assets, using an isolated environment.
    """
    from cx_shell.cli import init as cx_init_func
    from cx_shell.engine.connector import config as connector_config

    # Monkeypatch the global constants to point to our temporary, isolated directory.
    # This ensures the `init` command operates only within the test's scope.
    monkeypatch.setattr(utils, "CX_HOME", isolated_cx_home)
    monkeypatch.setattr(
        connector_config, "BLUEPRINTS_BASE_PATH", isolated_cx_home / "blueprints"
    )

    # Act: Run the global workspace initialization logic from the CLI command.
    cx_init_func(project_name=None)

    # Assert: Verify that the expected directories and sample files were copied correctly.
    assert (isolated_cx_home / "connections").is_dir()
    assert (isolated_cx_home / "connections" / "github.conn.yaml").is_file()

    # Check that a bundled blueprint was correctly copied during initialization.
    bundled_blueprint_path = (
        isolated_cx_home / "blueprints" / "community" / "github" / "0.1.0"
    )
    assert bundled_blueprint_path.is_dir()
    assert (bundled_blueprint_path / "blueprint.cx.yaml").is_file()


def test_resolver_on_demand_blueprint_download(
    isolated_cx_home: Path, mocker: MockerFixture
):
    """
    Integration Test: Verifies the ConnectionResolver can successfully "download"
    and extract a blueprint, using a mocked network request.
    This test is fast, reliable, and does not depend on a live network connection.
    """
    # 1. Arrange: Create a fake in-memory zip file to simulate the download.
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w") as zf:
        # Create valid YAML content that includes all required fields for the ApiCatalog model.
        mock_blueprint_content = """
id: "community/sendgrid@0.3.0"
name: "Mocked SendGrid Blueprint"
connector_provider_key: "mock-provider"
"""
        zf.writestr("blueprint.cx.yaml", mock_blueprint_content)
        zf.writestr("schemas.py", "class MockSchema: pass")
    zip_content = zip_buffer.getvalue()

    # 2. Arrange: Mock the `httpx.stream` call to return our fake zip file.
    mock_response = mocker.MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.read.return_value = (
        zip_content  # Simulate reading the response body bytes
    )

    mock_stream_context = mocker.MagicMock()
    # When the 'with' block is entered, it returns our mock response.
    mock_stream_context.__enter__.return_value = mock_response

    # Patch the httpx.stream function to return our mock context manager.
    mocker.patch("httpx.stream", return_value=mock_stream_context)

    # 3. Act: Instantiate the resolver with the isolated path and call the method under test.
    resolver = ConnectionResolver(cx_home_path=isolated_cx_home)
    blueprint_id_to_test = "community/sendgrid@0.3.0"

    catalog = resolver.load_blueprint_by_id(blueprint_id_to_test)

    # 4. Assert: Verify the results.
    assert isinstance(catalog, ApiCatalog)
    assert catalog.name == "Mocked SendGrid Blueprint"
    assert (
        catalog.id == "community/sendgrid@0.3.0"
    )  # Check that the ID was loaded correctly.

    # Assert that the files were correctly extracted into our isolated test directory.
    expected_path = isolated_cx_home / "blueprints/community/sendgrid/0.3.0"
    assert expected_path.is_dir()
    assert (expected_path / "blueprint.cx.yaml").is_file()
    assert (expected_path / "schemas.py").is_file()
    import yaml

    blueprint_content = (expected_path / "blueprint.cx.yaml").read_text()
    parsed_yaml = yaml.safe_load(blueprint_content)

    assert parsed_yaml["name"] == "Mocked SendGrid Blueprint"
    assert parsed_yaml["id"] == "community/sendgrid@0.3.0"
