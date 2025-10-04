# ~/repositories/cx-shell/src/cx_shell/management/flow_converter.py
import yaml

from cx_core_schemas.notebook import ContextualPage
from cx_core_schemas.connector_script import (
    ConnectorScript,
    ConnectorStep,
)  # <-- CORRECTED IMPORT


class FlowConverter:
    """
    A service to convert a ConnectorScript (from a .flow.yaml file) into a
    renderable ContextualPage model for the cx-studio UI.
    """

    def convert(self, script: ConnectorScript) -> ContextualPage:
        """
        Performs the conversion.
        """
        blocks: list[ConnectorStep] = []  # <-- USE ConnectorStep

        # Create a header block from the flow's top-level info
        header_content = f"# {script.name}\n\n{script.description or ''}"
        blocks.append(
            ConnectorStep(id="md_header", engine="markdown", content=header_content)
        )

        # Convert each step in the flow into a renderable ConnectorStep
        for step in script.steps:
            # For conversion to a notebook view, we represent the step's 'run' dictionary
            # as a YAML string in the 'content' field for display in the code editor.

            # Create a dictionary representing the original step for YAML conversion
            step_dict_for_yaml = step.model_dump(
                exclude={"content"},  # Exclude content as we are generating it
                exclude_unset=True,
                by_alias=True,
            )
            # The 'run' payload becomes the content of the code block
            run_payload = step_dict_for_yaml.pop("run", {})
            code_content = yaml.dump(run_payload, sort_keys=False, indent=2)

            # The rest of the step's metadata goes into the new step model
            notebook_block = ConnectorStep(
                id=step.id,
                name=step.name,
                engine="run",  # We will use the 'run' engine for these blocks
                content=code_content,
                connection_source=step.connection_source,
                depends_on=step.depends_on,
                inputs=step.inputs,
                outputs=step.outputs,
                if_condition=step.if_condition,
            )
            blocks.append(notebook_block)

        return ContextualPage(
            name=script.name, description=script.description, blocks=blocks
        )
