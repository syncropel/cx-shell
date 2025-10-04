from typing import Any, Dict

"""
The Custom Syncropel Blueprint Generator.

This module is responsible for parsing an OpenAPI/Swagger specification and
generating the proprietary `blueprint.cx.yaml` file. It contains all the
business logic for how an API definition is translated into an executable
Syncropel blueprint.
"""


def generate_ccl_blueprint_from_spec(spec: Dict[str, Any]) -> Dict[str, Any]:
    """
    The main entry point for generating a blueprint dictionary from a spec.
    """
    info = spec.get("info", {})
    server_url = _get_server_url(spec)
    auth_config, auth_comments = _get_auth_config(spec)
    action_templates = _get_action_templates(spec)

    blueprint = {
        "name": info.get("title", "Untitled API"),
        "version": info.get("version", "1.0.0"),
        "connector_provider_key": "rest-declarative",
        "auth_config": auth_config,
        "browse_config": {
            "base_url_template": server_url,
            "action_templates": action_templates,
        },
    }
    return blueprint, auth_comments


def _get_server_url(spec: Dict[str, Any]) -> str:
    """Robustly parses the server URL from both OpenAPI 3 and Swagger 2 formats."""
    if "servers" in spec and spec["servers"]:
        return spec["servers"][0].get("url", "https://api.example.com")
    elif "host" in spec:
        scheme = spec.get("schemes", ["https"])[0]
        host = spec["host"]
        base_path = spec.get("basePath", "")
        return f"{scheme}://{host}{base_path}"
    return "https://api.example.com"


def _get_auth_config(spec: Dict[str, Any]) -> (Dict, str):
    """Inspects security definitions and generates a placeholder auth_config and helpful comments."""
    security_schemes = spec.get("components", {}).get(
        "securitySchemes", {}
    ) or spec.get("securityDefinitions", {})

    auth_config = {"type": "none"}
    comments = "# TODO: Review and configure authentication."

    if not security_schemes:
        return auth_config, comments

    first_scheme = next(iter(security_schemes.values()), {})
    scheme_type = first_scheme.get("type")

    if scheme_type == "apiKey":
        name = first_scheme.get("name", "X-API-KEY")
        comments += f"""
# This API appears to use an API key. A recommended configuration is:
# auth_config:
#   type: "header"
#   header_name: "{name}"
#   value_template: "{{{{ secrets.api_key }}}}"
"""
    elif scheme_type == "http" and first_scheme.get("scheme") == "basic":
        comments += """
# This API appears to use HTTP Basic Auth. A recommended configuration is:
# auth_config:
#   type: "basic"
#   username_template: "{{ secrets.username }}"
#   password_template: "{{ secrets.password }}"
"""
    elif scheme_type == "oauth2":
        comments += """
# This API uses OAuth2. For standard refresh token flows, change the provider key
# and configure the oauth_config block.
# connector_provider_key: "oauth2-declarative"
# oauth_config:
#   token_url: "https://auth.example.com/oauth/token" # TODO: Set the correct token URL
"""
    return auth_config, comments


def _get_action_templates(spec: Dict[str, Any]) -> Dict[str, Any]:
    """Iterates over API paths and generates action templates for each operation."""
    action_templates = {}
    for path, path_item in spec.get("paths", {}).items():
        for method, operation in path_item.items():
            if method.lower() not in ["get", "post", "put", "patch", "delete"]:
                continue
            if "operationId" not in operation:
                continue

            op_id = operation["operationId"]
            action = {"http_method": method.upper(), "api_endpoint": path}

            has_body = "requestBody" in operation or any(
                p.get("in") == "body" for p in operation.get("parameters", [])
            )
            if has_body:
                action["payload_constructor"] = {
                    "_constructor": "schemas.ModelName",
                    "comment": "TODO: Replace 'ModelName' with the correct Pydantic model for the request body.",
                }

            action_templates[op_id] = action
    return action_templates
