from pathlib import Path
import pytest
from unittest.mock import AsyncMock, MagicMock
from cx_shell.interactive.executor import CommandExecutor
from cx_shell.interactive.session import SessionState


@pytest.fixture
def executor(isolated_cx_home: Path):  # <--- CORRECT FIXTURE NAME
    """Provides a clean executor for each test."""
    state = SessionState(is_interactive=False)
    mock_output_handler = AsyncMock()
    executor_instance = CommandExecutor(state, mock_output_handler)

    executor_instance.registry.connector_service.test_connection = AsyncMock(
        return_value={"status": "success"}
    )
    return executor_instance


@pytest.mark.asyncio
async def test_executor_connect_command_updates_state(executor: CommandExecutor):
    """Unit Test: Verifies the 'connect' command correctly updates session state."""
    await executor.execute("connect user:github --as gh")

    assert "gh" in executor.state.connections
    assert executor.state.connections["gh"] == "user:github"


@pytest.mark.asyncio
async def test_executor_variable_assignment(executor: CommandExecutor, mocker):
    """Unit Test: Verifies a command result can be assigned to a variable."""
    executor.state.connections["gh"] = "user:github"
    mock_result = {"user": "test", "id": 123}

    # --- START OF DEFINITIVE, FINAL FIX ---

    # 1. We mock `_execute_executable`, but we must tell `mocker` that it's replacing
    #    an async function. We do this by providing `AsyncMock` as the `new_callable`.
    #    We then configure the mock's `return_value` in the standard way.
    mock_execute_executable = mocker.patch.object(
        executor, "_execute_executable", new_callable=AsyncMock
    )
    mock_execute_executable.return_value = mock_result

    # 2. We must also fix the mock for the output handler, as it was the second
    #    point of failure. We configure its `_apply_formatters` method to be a
    #    simple function that returns its input.
    executor.output_handler._apply_formatters = MagicMock(
        side_effect=lambda result, options: result
    )
    # --- END OF DEFINITIVE, FINAL FIX ---

    await executor.execute('my_var = gh.getUser(username="test")')

    # Assert that our mock was called correctly by the pipeline
    mock_execute_executable.assert_awaited_once()

    # Assert that the final, unwrapped result was stored in the variable
    assert "my_var" in executor.state.variables
    assert executor.state.variables["my_var"] == mock_result


@pytest.mark.asyncio
async def test_executor_session_persistence(executor: CommandExecutor):
    """Integration Test: Verifies that a session can be saved and then loaded correctly."""
    executor.state.connections["test_alias"] = "user:test_conn"
    executor.state.variables["test_var"] = "hello world"

    await executor.execute("session save test-persistence")

    new_executor = CommandExecutor(
        SessionState(is_interactive=False), output_handler=AsyncMock()
    )
    loaded_state = await new_executor.execute("session load test-persistence")

    assert isinstance(loaded_state, SessionState)
    assert loaded_state.connections.get("test_alias") == "user:test_conn"
    assert loaded_state.variables.get("test_var") == "hello world"
