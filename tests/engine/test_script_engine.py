from pathlib import Path
import pytest
import yaml
from pytest_mock import MockerFixture

from cx_shell.interactive.executor import CommandExecutor
from cx_shell.interactive.session import SessionState
from cx_shell.engine.context import RunContext
from cx_core_schemas.api_catalog import ApiCatalog
from cx_shell.engine.connector.config import ConnectionResolver


@pytest.mark.asyncio
async def test_script_engine_executes_blueprint_action_with_mocks(
    isolated_cx_home: Path,
    mocker: MockerFixture,  # <--- CORRECT FIXTURE NAME
):
    """
    Integration Test: Verifies the ScriptEngine's end-to-end execution of a
    declarative action, using an explicitly configured, isolated environment.
    """
    # Arrange 1: Create the temporary connection file inside the isolated test directory.
    connection_dir = isolated_cx_home / "connections"
    connection_dir.mkdir(parents=True)
    (connection_dir / "github.conn.yaml").write_text(
        yaml.dump(
            {
                "name": "GitHub Public API",
                "id": "user:github",
                "api_catalog_id": "community/github@v0.1.0",
                "auth_method_type": "none",
            }
        )
    )

    # Arrange 2: Mock external dependencies.
    mock_catalog = ApiCatalog.model_validate(
        {
            "id": "bp:github",
            "name": "GH",
            "connector_provider_key": "rest-declarative",
            "browse_config": {
                "base_url_template": "https://api.github.com",
                "action_templates": {
                    "getUser": {
                        "http_method": "GET",
                        "api_endpoint": "/users/{{ context.username }}",
                    }
                },
            },
        }
    )
    mocker.patch.object(
        ConnectionResolver, "load_blueprint_by_id", return_value=mock_catalog
    )

    mock_response = mocker.Mock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = {"login": "torvalds", "id": 1024025}
    mocker.patch("httpx.AsyncClient.request", return_value=mock_response)

    # Arrange 3: Create the workflow script file.
    script_file = isolated_cx_home / "test.flow.yaml"
    script_file.write_text(
        yaml.dump(
            {
                "name": "Test Script",
                "steps": [
                    {
                        "id": "get_user",
                        "name": "Get User",
                        "connection_source": "user:github",
                        "run": {
                            "action": "run_declarative_action",
                            "template_key": "getUser",
                            "context": {"username": "torvalds"},
                        },
                    }
                ],
            }
        )
    )

    # Act:
    # 1. Create a session state.
    state = SessionState(is_interactive=False)

    # 2. Instantiate the CommandExecutor, crucially passing `cx_home_path=clean_cx_home`.
    #    This ensures all its sub-services are initialized with the correct, isolated directory.
    executor = CommandExecutor(
        state, output_handler=None, cx_home_path=isolated_cx_home
    )

    # 3. Create the RunContext for the script engine to use.
    run_context = RunContext(
        services=executor.registry, session=state, current_flow_path=script_file
    )
    results = await executor.script_engine.run_script(context=run_context)

    # Assert: Verify the result.
    assert "get_user" in results
    user_data = results["get_user"]
    assert "error" not in user_data, user_data.get("error")
    assert user_data["id"] == 1024025
