import pytest
from pathlib import Path
import shutil

from cx_shell._bootstrap import bootstrap_models


@pytest.fixture(scope="session", autouse=True)
def bootstrap_pydantic_models():
    """Ensures all Pydantic forward references are resolved once for the entire test suite."""
    bootstrap_models()


@pytest.fixture
def isolated_cx_home(tmp_path: Path) -> Path:
    """
    Provides a pristine, isolated, and empty ~/.cx directory for each test function.
    This fixture ensures that tests do not interfere with each other.
    """
    test_cx_home = tmp_path / ".cx"
    if test_cx_home.exists():
        shutil.rmtree(test_cx_home)
    test_cx_home.mkdir()

    yield test_cx_home
