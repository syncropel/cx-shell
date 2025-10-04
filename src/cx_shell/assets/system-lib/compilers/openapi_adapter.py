import sys
import re
from typing import Any, Dict, List, Tuple

import yaml

# --- Utility Functions (Correct) ---
TYPE_MAP = {
    "string": "str",
    "number": "float",
    "integer": "int",
    "boolean": "bool",
    "array": "List",
    "object": "Dict[str, Any]",
}
FORMAT_MAP = {"date-time": "datetime", "date": "date", "uuid": "UUID"}
PYTHON_KEYWORDS = {
    "in",
    "from",
    "for",
    "is",
    "while",
    "class",
    "def",
    "return",
    "True",
    "False",
    "None",
}


def log_to_stderr(message: str):
    print(f"openapi_adapter: {message}", file=sys.stderr)


def to_pascal_case(s: str) -> str:
    return "".join(word.capitalize() for word in s.split("_"))


def safe_snake_case(name: str) -> str:
    if not name:
        return "_unknown"
    s1 = re.sub(r"[-\s\.]+", "_", name)
    s2 = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", s1)
    s3 = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s2).lower()
    cleaned_name = re.sub(r"\W+", "", s3)
    if cleaned_name and cleaned_name[0].isdigit():
        cleaned_name = "_" + cleaned_name
    if cleaned_name in PYTHON_KEYWORDS:
        return f"{cleaned_name}_"
    return cleaned_name or "_unknown"


def _get_schemas(spec: Dict[str, Any]) -> Dict[str, Any]:
    return spec.get("components", {}).get("schemas", {}) or spec.get("definitions", {})


# --- Pydantic Model Generation Logic (Refactored) ---


def _generate_data_models(schemas: Dict[str, Any]) -> Tuple[List[str], List[str]]:
    """
    Generates Pydantic models and returns the code lines and a list of generated class names.
    This version is robustly handles schemas that don't explicitly declare 'type: object'.
    """
    code_lines = []
    class_names = []
    if not schemas:
        return code_lines, class_names

    for schema_name, schema_def in schemas.items():
        class_name = to_pascal_case(safe_snake_case(schema_name))

        # --- THIS IS THE CRITICAL FIX ---
        # Heuristic to determine if a schema should be a Pydantic BaseModel.
        # It's a model if it has properties, uses composition, or is explicitly an object.
        is_object_like = (
            "properties" in schema_def
            or "allOf" in schema_def
            or "anyOf" in schema_def
            or "oneOf" in schema_def
            or schema_def.get("type") == "object"
        )
        # --- END FIX ---

        if is_object_like:
            code_lines.append(f"class {class_name}(BaseModel):")
            class_names.append(class_name)

            # This logic needs to be expanded to handle allOf, etc., but for now,
            # we'll focus on getting the properties. A full implementation
            # would merge properties from 'allOf' directives.
            properties = schema_def.get("properties", {})
            required_fields = set(schema_def.get("required", []))

            fields = []
            for prop_name, prop_def in properties.items():
                field_name = safe_snake_case(prop_name)
                is_required = prop_name in required_fields
                python_type = "Any"

                if "$ref" in prop_def:
                    ref_name = prop_def["$ref"].split("/")[-1]
                    python_type = f'"{to_pascal_case(safe_snake_case(ref_name))}"'
                elif "type" in prop_def:
                    prop_type = prop_def["type"]
                    if prop_type == "array":
                        items_def = prop_def.get("items", {})
                        item_type = "Any"
                        if "$ref" in items_def:
                            item_type = f'"{to_pascal_case(safe_snake_case(items_def["$ref"].split("/")[-1]))}"'
                        elif "type" in items_def:
                            item_type = TYPE_MAP.get(items_def["type"], "Any")
                        python_type = f"List[{item_type}]"
                    else:
                        python_type = TYPE_MAP.get(prop_type, "Any")
                        if prop_def.get("format") in FORMAT_MAP:
                            python_type = FORMAT_MAP[prop_def["format"]]
                # Fallback for complex types without a clear definition
                elif "oneOf" in prop_def or "anyOf" in prop_def:
                    # A simple but effective way to handle unions is to type them as 'Any'
                    python_type = "Any"

                alias = prop_name if field_name != prop_name else None

                if is_required:
                    if alias:
                        fields.append(
                            f'    {field_name}: {python_type} = Field(alias="{alias}")'
                        )
                    else:
                        fields.append(f"    {field_name}: {python_type}")
                else:
                    field_type_hint = f"Optional[{python_type}]"
                    field_args = ["None"]
                    if alias:
                        field_args.append(f'alias="{alias}"')
                    fields.append(
                        f"    {field_name}: {field_type_hint} = Field({', '.join(field_args)})"
                    )

            code_lines.extend(fields if fields else ["    pass"])
            code_lines.append("\n")

        elif schema_def.get("type") in TYPE_MAP:
            # This handles simple type aliases like `type: string`
            if (
                description := schema_def.get("description", "")
                .replace("\n", " ")
                .strip()
            ):
                code_lines.append(f"# {class_name}: {description}")
            code_lines.append(
                f"{class_name} = TypeAlias('{class_name}', {TYPE_MAP[schema_def.get('type')]})\n"
            )

        else:
            log_to_stderr(f"Skipping unhandled schema definition: {schema_name}")

    return code_lines, class_names


def _generate_parameter_models(spec: Dict[str, Any]) -> Tuple[List[str], List[str]]:
    """
    Generates Pydantic models for the parameters of each operation and returns
    both the code lines and a list of the generated class names.
    """
    code_lines = []
    class_names = []  # --- FIX: Initialize the list to track class names ---

    for path, path_item in spec.get("paths", {}).items():
        for method, operation in path_item.items():
            if "operationId" not in operation or not operation.get("parameters"):
                continue

            action_key = safe_snake_case(operation["operationId"])
            class_name = f"{to_pascal_case(action_key)}Parameters"

            fields = []
            for param in operation.get("parameters", []):
                if param.get("in") not in ["path", "query", "header"]:
                    continue
                param_name = param["name"]
                field_name = safe_snake_case(param_name)
                is_required = param.get("required", False)
                schema = param.get("schema", param)
                python_type = TYPE_MAP.get(schema.get("type"), "Any")
                alias = param_name if field_name != param_name else None

                if is_required:
                    if alias:
                        fields.append(
                            f'    {field_name}: {python_type} = Field(alias="{alias}")'
                        )
                    else:
                        fields.append(f"    {field_name}: {python_type}")
                else:
                    field_type_hint = f"Optional[{python_type}]"
                    field_args = ["None"]
                    if alias:
                        field_args.append(f'alias="{alias}"')
                    fields.append(
                        f"    {field_name}: {field_type_hint} = Field({', '.join(field_args)})"
                    )

            if fields:
                code_lines.append(f"class {class_name}(BaseModel):")
                code_lines.extend(fields)
                code_lines.append("\n")
                class_names.append(
                    class_name
                )  # --- FIX: Track the generated class name ---

    return code_lines, class_names  # --- FIX: Return the tuple ---


def generate_pydantic_code(spec: Dict[str, Any]) -> str:
    """
    Generates the complete schemas.py file content, including the forward
    reference resolution footer.
    """
    schemas = _get_schemas(spec)
    header = [
        "# Generated by the Syncropel Blueprint Compiler (OpenAPI Adapter)",
        "from __future__ import annotations",
        "import sys",
        "from typing import Any, Dict, List, Optional, TypeAlias",
        "from datetime import date, datetime",
        "from uuid import UUID",
        "from pydantic import BaseModel, Field",
        "\n",
    ]

    # This unpacking logic is now correct.
    data_model_code, data_model_names = _generate_data_models(schemas)
    param_model_code, param_model_names = _generate_parameter_models(spec)

    all_model_names = sorted(list(set(data_model_names + param_model_names)))

    footer_code = [
        "\n# --- Forward Reference Resolution ---",
        "from pydantic import BaseModel",
        "import sys",
        "",
        "all_models = {",
        "    name: obj",
        "    for name, obj in sys.modules[__name__].__dict__.items()",
        "    if isinstance(obj, type) and issubclass(obj, BaseModel) and obj is not BaseModel",
        "}",
        "BaseModel.model_rebuild(force=True, raise_errors=False, _parent_namespace=all_models)",
        "",
    ]

    return "\n".join(header + data_model_code + param_model_code + footer_code)


def generate_ccl_blueprint(spec: Dict[str, Any]) -> str:
    """Generates the blueprint.cx.yaml content as a YAML string."""
    info = spec.get("info", {})

    if "servers" in spec and spec["servers"]:
        server_url = spec["servers"][0].get("url", "https://api.example.com")
    elif "host" in spec:
        server_url = f"{spec.get('schemes', ['https'])[0]}://{spec['host']}{spec.get('basePath', '')}"
    else:
        server_url = "https://api.example.com"

    blueprint = {
        "id": f"blueprint:{safe_snake_case(info.get('title', 'untitled'))}",
        "name": info.get("title", "Untitled API"),
        "version": info.get("version", "1.0.0"),
        "connector_provider_key": "rest-declarative",
        "supported_auth_methods": [
            {"type": "none", "display_name": "No Authentication", "fields": []}
        ],
    }

    action_templates = {}
    for path, path_item in spec.get("paths", {}).items():
        for method, operation in path_item.items():
            if (
                method.lower() not in ["get", "post", "put", "patch", "delete"]
                or "operationId" not in operation
            ):
                continue

            action_key = safe_snake_case(operation["operationId"])
            api_endpoint = path.replace("{", "{{ context.").replace("}", " }}")
            action = {"http_method": method.upper(), "api_endpoint": api_endpoint}

            if any(
                p.get("in") in ["path", "query", "header"]
                for p in operation.get("parameters", [])
            ):
                params_class_name = f"{to_pascal_case(action_key)}Parameters"
                action["parameters_model"] = f"schemas.{params_class_name}"

            body_schema_ref = None
            if "requestBody" in operation:
                try:
                    body_schema_ref = operation["requestBody"]["content"][
                        "application/json"
                    ]["schema"]["$ref"]
                except KeyError:
                    pass
            else:
                for param in operation.get("parameters", []):
                    if param.get("in") == "body" and "$ref" in param.get("schema", {}):
                        body_schema_ref = param["schema"]["$ref"]
                        break

            if body_schema_ref:
                body_schema_name = to_pascal_case(
                    safe_snake_case(body_schema_ref.split("/")[-1])
                )
                action["payload_constructor"] = {
                    "_model": f"schemas.{body_schema_name}"
                }

            action_templates[action_key] = action

    blueprint["browse_config"] = {
        "base_url_template": server_url,
        "action_templates": action_templates,
    }
    return yaml.dump(blueprint, sort_keys=False, indent=2, width=120)


# --- Main Adapter Entry Point ---


def parse(spec: Dict[str, Any]) -> Tuple[str, str]:
    """
    The main entry point for the OpenAPI/Swagger adapter.

    Args:
        spec: The parsed JSON/YAML content of the OpenAPI or Swagger spec.

    Returns:
        A tuple containing the generated blueprint YAML and schemas.py content.
    """
    log_to_stderr("OpenAPI adapter started.")

    log_to_stderr("Generating Pydantic models...")
    schemas_py_content = generate_pydantic_code(spec)

    log_to_stderr("Generating Syncropel blueprint...")
    blueprint_yaml_content = generate_ccl_blueprint(spec)

    log_to_stderr("OpenAPI adapter finished successfully.")
    return blueprint_yaml_content, schemas_py_content
