# ~/repositories/cx-shell/tests/management/test_upgrade_manager.py

import sys
import pytest
from pytest_mock import MockerFixture
from pathlib import Path

from cx_shell.management.upgrade_manager import UpgradeManager


@pytest.fixture
def fake_executable(tmp_path: Path, monkeypatch) -> Path:
    """
    A fixture to create a fake 'cx' executable in a temporary directory
    and monkeypatch sys.executable to point to it. This allows us to
    safely test the file replacement logic.
    """
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()

    executable_path = bin_dir / ("cx.exe" if sys.platform == "win32" else "cx")
    executable_path.write_text("fake binary content")
    executable_path.chmod(0o755)

    monkeypatch.setattr(sys, "executable", str(executable_path))

    return executable_path


def test_upgrade_when_already_latest(mocker: MockerFixture, capsys):
    """
    Unit Test: Verifies the manager correctly identifies when no upgrade is needed.
    """
    mocker.patch("importlib.metadata.version", return_value="1.2.3")
    mock_response = mocker.Mock()
    mock_response.json.return_value = {"tag_name": "v1.2.3"}
    mocker.patch("httpx.get", return_value=mock_response)

    manager = UpgradeManager()
    manager.run_upgrade()

    captured = capsys.readouterr()
    assert "You are already running the latest version" in captured.out


@pytest.mark.parametrize(
    "archive_name", ["cx-v1.1.0-linux-x86_64.tar.gz", "cx-v1.1.0-windows-amd64.zip"]
)
def test_upgrade_successful_flow(
    mocker: MockerFixture, fake_executable: Path, capsys, archive_name
):
    """
    Integration Test: Verifies the full happy-path upgrade flow by mocking
    all external I/O (network and archive handling).
    """
    # Arrange: Mock all external dependencies.
    mocker.patch("importlib.metadata.version", return_value="1.0.0")
    mocker.patch("rich.console.Console.input", return_value="y")
    mock_api_response = mocker.Mock()
    mock_api_response.json.return_value = {
        "tag_name": "v1.1.0",
        "assets": [
            {
                "name": archive_name,
                "browser_download_url": "https://fake.url/download",
                "size": 100,
            }
        ],
    }
    mocker.patch("httpx.get", return_value=mock_api_response)

    mock_stream_response = mocker.MagicMock()
    mock_stream_response.iter_bytes.return_value = [b"fake-data"]
    mock_stream_context = mocker.MagicMock()
    mock_stream_context.__enter__.return_value = mock_stream_response
    mocker.patch("httpx.stream", return_value=mock_stream_context)

    # --- THIS IS THE FINAL, CORRECT MOCK ---
    def mock_archive_open(archive_path, mode):
        # This function will replace both `tarfile.open` and `zipfile.ZipFile`
        mock_archive = mocker.MagicMock()

        def mock_extract(member, path):
            # This inner function simulates the behavior of the `extract` method
            binary_name = "cx.exe" if "windows" in archive_name else "cx"
            (Path(path) / binary_name).write_text("new binary content")

        mock_archive.extract.side_effect = mock_extract

        # We need to return a context manager
        context_manager = mocker.MagicMock()
        context_manager.__enter__.return_value = mock_archive
        return context_manager

    mocker.patch("tarfile.open", side_effect=mock_archive_open)
    mocker.patch("zipfile.ZipFile", side_effect=mock_archive_open)
    # --- END FINAL MOCK ---

    manager = UpgradeManager()
    mocker.patch.object(
        manager,
        "get_platform_asset_identifier",
        return_value="linux-x86_64" if ".tar.gz" in archive_name else "windows-amd64",
    )

    # Act
    manager.run_upgrade()

    # Assert
    captured = capsys.readouterr()
    assert "Upgrade to version 1.1.0 successful!" in captured.out
    assert fake_executable.read_text() == "new binary content"
    old_executable = fake_executable.with_suffix(f"{fake_executable.suffix}.old")
    assert not old_executable.exists()
