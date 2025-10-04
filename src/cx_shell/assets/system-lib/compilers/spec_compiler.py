# ~/repositories/cx-shell/src/cx_shell/assets/system-lib/compilers/spec_compiler.py

import sys
import json
import yaml
import traceback
from pathlib import Path

# --- THIS IS THE FINAL FIX ---
# When run as a script, this directory isn't in the Python path.
# We add this script's own directory to the path so it can find its sibling adapter modules.
current_dir = Path(__file__).parent
sys.path.append(str(current_dir))

# Now, we can use absolute imports from the perspective of this directory.
import openapi_adapter
import google_discovery_adapter
# --- END FIX ---


def log_to_stderr(message: str):
    """Writes a log message to stderr, prefixed for clarity."""
    print(f"spec_compiler: {message}", file=sys.stderr)


def main():
    """
    The main entry point for the universal specification compiler.
    """
    try:
        log_to_stderr("Universal spec compiler started.")
        spec_content = yaml.safe_load(sys.stdin.read())

        blueprint_yaml = ""
        schemas_py = ""

        # --- DISPATCHER LOGIC ---
        if "openapi" in spec_content or "swagger" in spec_content:
            log_to_stderr(
                "Specification format detected: OpenAPI/Swagger. Delegating to adapter..."
            )
            blueprint_yaml, schemas_py = openapi_adapter.parse(spec_content)

        elif spec_content.get("kind") == "discovery#restDescription":
            log_to_stderr(
                "Specification format detected: Google API Discovery. Delegating to adapter..."
            )
            blueprint_yaml, schemas_py = google_discovery_adapter.parse(spec_content)

        else:
            log_to_stderr(
                "FATAL ERROR: Could not determine the specification format from its content."
            )
            raise ValueError("Unknown or unsupported specification format.")

        # --- Output ---
        output = {"blueprint_yaml": blueprint_yaml, "schemas_py": schemas_py}
        print(json.dumps(output, indent=2))

        log_to_stderr("Compiler finished successfully.")
        sys.exit(0)

    except Exception as e:
        log_to_stderr(f"FATAL ERROR: {type(e).__name__} - {e}")
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
