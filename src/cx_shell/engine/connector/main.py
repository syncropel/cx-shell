# Import the typer app object, not a function named 'main'
from .cli import app


def main():
    """The main entrypoint for the CLI script defined in pyproject.toml."""
    app()


if __name__ == "__main__":
    main()
