class AppState:
    """A simple singleton-like class to hold global application state."""

    def __init__(self):
        self.verbose_mode: bool = False


# Create a single, global instance that all modules can import.
APP_STATE = AppState()
