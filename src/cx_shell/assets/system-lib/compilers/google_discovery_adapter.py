# ~/repositories/cx-shell/src/cx_shell/assets/system-lib/compilers/google_discovery_adapter.py

import sys
import re
from typing import Any, Dict, List, Tuple

import yaml

# --- Utility Functions ---

TYPE_MAP = {
    "string": "str",
    "number": "float",
    "integer": "int",
    "boolean": "bool",
    "array": "List",
    "object": "Dict[str, Any]",
    "any": "Any",
}


def log_to_stderr(message: str):
    """Writes a log message to stderr for the parent process to see."""
    print(f"google_discovery_adapter: {message}", file=sys.stderr)


def to_pascal_case(s: str) -> str:
    """Converts a snake_case or dot.case string to PascalCase."""
    s = s.replace(".", "_")
    return "".join(word.capitalize() for word in s.split("_"))


def safe_snake_case(name: str) -> str:
    """Converts any string to a valid Python identifier in snake_case."""
    s1 = re.sub(r"[-\s\.]+", "_", name)
    s2 = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", s1)
    return re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s2).lower()


# --- Pydantic Model Generation Logic ---


def _generate_data_models(schemas: Dict[str, Any]) -> List[str]:
    """Generates Pydantic models from the 'schemas' block of a Discovery Document."""
    code_lines = []
    if not schemas:
        return code_lines

    for schema_name, schema_def in schemas.items():
        class_name = to_pascal_case(schema_name)
        code_lines.append(f"class {class_name}(BaseModel):")

        properties = schema_def.get("properties", {})
        if not properties:
            code_lines.append("    pass\n")
            continue

        fields = []
        for prop_name, prop_def in properties.items():
            field_name = safe_snake_case(prop_name)
            python_type = "Any"
            # Google Discovery docs don't typically have a 'required' list at the schema level,
            # so we default to Optional for robustness.
            is_required = False

            if "$ref" in prop_def:
                ref_name = prop_def["$ref"]
                python_type = f'"{to_pascal_case(ref_name)}"'
            elif "type" in prop_def:
                prop_type = prop_def["type"]
                if prop_type == "array":
                    items_def = prop_def.get("items", {})
                    item_type = "Any"
                    if "$ref" in items_def:
                        item_type = f'"{to_pascal_case(items_def["$ref"])}"'
                    elif "type" in items_def:
                        item_type = TYPE_MAP.get(items_def["type"], "Any")
                    python_type = f"List[{item_type}]"
                else:
                    python_type = TYPE_MAP.get(prop_type, "Any")

            field_type_hint = python_type if is_required else f"Optional[{python_type}]"
            default_value = "" if is_required else " = None"

            fields.append(f"    {field_name}: {field_type_hint}{default_value}")

        code_lines.extend(fields if fields else ["    pass"])
        code_lines.append("\n")

    return code_lines


def _generate_parameter_models(resources: Dict[str, Any]) -> List[str]:
    """Recursively traverses resources to generate Pydantic models for method parameters."""
    code_lines = []

    def traverse(resource_dict: Dict):
        for method_name, method_def in resource_dict.get("methods", {}).items():
            op_id = method_def.get("id")
            if not op_id or not method_def.get("parameters"):
                continue

            class_name = f"{to_pascal_case(op_id)}Parameters"
            code_lines.append(f"class {class_name}(BaseModel):")

            fields = []
            param_order = method_def.get("parameterOrder", [])
            all_params = method_def.get("parameters", {})

            # Combine ordered and unordered parameters
            sorted_param_names = param_order + [
                p for p in all_params if p not in param_order
            ]

            for param_name in sorted_param_names:
                param_def = all_params[param_name]
                field_name = safe_snake_case(param_name)
                is_required = param_def.get("required", False)
                python_type = TYPE_MAP.get(param_def.get("type", "string"), "Any")

                field_type_hint = (
                    python_type if is_required else f"Optional[{python_type}]"
                )
                default_value = "" if is_required else " = None"

                fields.append(f"    {field_name}: {field_type_hint}{default_value}")

            code_lines.extend(fields if fields else ["    pass"])
            code_lines.append("\n")

        for sub_resource_name, sub_resource_def in resource_dict.items():
            if sub_resource_name != "methods" and isinstance(sub_resource_def, dict):
                traverse(sub_resource_def)

    traverse(resources)
    return code_lines


def _generate_pydantic_code(spec: Dict[str, Any]) -> str:
    """Orchestrates the generation of the entire schemas.py file content."""
    log_to_stderr("Generating Pydantic data models...")
    data_model_code = _generate_data_models(spec.get("schemas", {}))

    log_to_stderr("Generating Pydantic parameter models...")
    param_model_code = _generate_parameter_models(spec.get("resources", {}))

    header = [
        "# Generated by the Syncropel Blueprint Compiler (Google Discovery Adapter)",
        "from __future__ import annotations",
        "from typing import Any, Dict, List, Optional",
        "from pydantic import BaseModel, Field",
        "\n",
    ]
    return "\n".join(header + data_model_code + param_model_code)


# --- Blueprint YAML Generation Logic ---


def _extract_actions_from_resources(spec: Dict[str, Any]) -> Dict[str, Any]:
    """Recursively traverses the 'resources' block to build action_templates."""
    actions = {}
    resources = spec.get("resources", {})

    def traverse(resource_dict: Dict):
        for method_name, method_def in resource_dict.get("methods", {}).items():
            op_id = method_def.get("id")
            if not op_id:
                continue

            action_name = safe_snake_case(op_id)
            path = (
                method_def.get("path", "")
                .replace("{+", "{")
                .replace("{", "{{ context.")
                .replace("}", " }}")
            )

            action = {
                "http_method": method_def.get("httpMethod", "GET").upper(),
                "api_endpoint": f"/{path}",
                "description": method_def.get("description", ""),
            }

            if method_def.get("parameters"):
                action["parameters_model"] = (
                    f"schemas.{to_pascal_case(op_id)}Parameters"
                )
            if "request" in method_def and "$ref" in method_def["request"]:
                action["payload_constructor"] = {
                    "_model": f"schemas.{to_pascal_case(method_def['request']['$ref'])}"
                }

            actions[action_name] = action

        for sub_resource_name, sub_resource_def in resource_dict.items():
            if sub_resource_name != "methods" and isinstance(sub_resource_def, dict):
                traverse(sub_resource_def)

    traverse(resources)
    return actions


def _generate_blueprint_yaml(spec: Dict[str, Any]) -> str:
    """Generates the blueprint.cx.yaml content as a YAML string."""
    log_to_stderr("Generating blueprint YAML...")

    title = spec.get("title", "Untitled Google API")
    # Google's `baseUrl` is the full path, we need to extract the root.
    base_url = spec.get("rootUrl", "https://api.example.com").rstrip("/")

    blueprint = {
        "id": f"blueprint:community-{safe_snake_case(title)}",
        "name": title,
        "version": spec.get("version", "0.1.0"),
        "connector_provider_key": "oauth2-declarative",
        "supported_auth_methods": [
            {
                "type": "oauth2_refresh_token",
                "display_name": "OAuth 2.0 (Refresh Token)",
                "fields": [
                    {
                        "name": "client_id",
                        "label": "Google Cloud Client ID",
                        "type": "secret",
                    },
                    {
                        "name": "client_secret",
                        "label": "Google Cloud Client Secret",
                        "type": "secret",
                        "is_password": True,
                    },
                    {
                        "name": "refresh_token",
                        "label": "Refresh Token",
                        "type": "secret",
                        "is_password": True,
                    },
                ],
            }
        ],
        "oauth_config": {"token_url": "https://oauth2.googleapis.com/token"},
        "browse_config": {
            "base_url_template": base_url,
            "action_templates": _extract_actions_from_resources(spec),
        },
    }

    return yaml.dump(blueprint, sort_keys=False, indent=2, width=120)


# --- Main Adapter Entry Point ---


def parse(spec: Dict[str, Any]) -> Tuple[str, str]:
    """
    The main entry point for the Google Discovery adapter.
    """
    log_to_stderr("Google Discovery adapter started.")

    schemas_py_content = _generate_pydantic_code(spec)
    blueprint_yaml_content = _generate_blueprint_yaml(spec)

    log_to_stderr("Google Discovery adapter finished successfully.")
    return blueprint_yaml_content, schemas_py_content
