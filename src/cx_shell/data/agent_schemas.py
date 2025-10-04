from pydantic import BaseModel, Field
from typing import List, Dict, Any, Literal, Optional

# --- Agent Configuration Schemas (for agents.config.yaml) ---


class AgentRoleConfig(BaseModel):
    """Defines the configuration for a single agent role (e.g., Planner)."""

    connection_alias: str = Field(
        ...,
        description="The alias of the connection to an LLM provider (e.g., 'cx_openai').",
    )
    action: str = Field(
        ...,
        description="The action from the connection's blueprint to invoke (e.g., 'createChatCompletion').",
    )
    parameters: Dict[str, Any] = Field(
        default_factory=dict,
        description="Model-specific parameters to pass to the action (e.g., model, temperature).",
    )


class AgentProfile(BaseModel):
    """A complete profile defining the models and configurations for a full agent team."""

    description: str = Field(
        ..., description="A human-readable description of the profile."
    )
    planner: AgentRoleConfig
    tool_specialist: AgentRoleConfig
    analyst: AgentRoleConfig
    co_pilot: AgentRoleConfig


class AgentConfig(BaseModel):
    """The root model for the agents.config.yaml file."""

    default_profile: str = Field(
        ..., description="The name of the default profile to use."
    )
    profiles: Dict[str, AgentProfile] = Field(
        ..., description="A dictionary of available agent profiles."
    )


# --- Agent Belief State Schemas (for the in-memory state) ---


class PlanStep(BaseModel):
    """Represents a single step in the agent's high-level plan."""

    step: str = Field(
        ..., description="A natural language description of the step's goal."
    )
    status: Literal["pending", "in_progress", "completed", "failed"] = "pending"
    result_summary: Optional[str] = Field(
        None, description="A summary of the outcome of this step."
    )


class AgentBeliefs(BaseModel):
    """The structured, in-memory 'latent space' of the agent's reasoning process."""

    original_goal: str = Field(
        ..., description="The initial, unmodified goal from the user."
    )
    plan: List[PlanStep] = Field(
        default_factory=list, description="The agent's strategic plan."
    )
    discovered_facts: Dict[str, Any] = Field(
        default_factory=dict,
        description="A key-value store of facts extracted by the Analyst from observations.",
    )
    conversation_history: List[Dict[str, str]] = Field(
        default_factory=list,
        description="A summarized history of the agent's turns for context.",
    )

    class Config:
        # Allows for easy updating of nested fields.
        validate_assignment = True


# --- Agent Invocation & Output Schemas ---


class AnalystResponse(BaseModel):
    """The structured output from the Analyst Agent."""

    belief_update: Dict[str, Any] = Field(
        ..., description="A JSON Patch operation to update the AgentBeliefs."
    )
    summary_text: str = Field(
        ...,
        description="A concise, natural language summary of the turn for the history log.",
    )
    indicates_strategic_failure: bool = Field(
        False,
        description="Set to true if the observation reveals the overall plan is flawed and needs revision by the Planner.",
    )


class DryRunResult(BaseModel):
    """Represents the predicted outcome of a command's execution."""

    indicates_failure: bool = Field(
        False, description="True if the simulation predicts a runtime error."
    )
    message: str = Field(
        ...,
        description="A human-readable summary of the predicted outcome (e.g., 'Would make a POST request...').",
    )
    predicted_effect: Optional[Dict[str, Any]] = Field(
        None,
        description="A structured summary of potential state changes (e.g., {'files_written': 1}).",
    )


class CommandOption(BaseModel):
    """Represents a single, proposed command in the agent's 'thought tree'."""

    cx_command: str
    reasoning: str
    confidence: float = Field(..., ge=0.0, le=1.0)
    preview: Optional[DryRunResult] = Field(None, exclude=True)


class LLMResponse(BaseModel):
    """
    A structured model for the expected output from the ToolSpecialist agent.
    It now contains a list of potential commands, representing a 'Tree of Thoughts'
    with a width determined by the number of options.
    """

    command_options: List[CommandOption] = Field(
        ...,
        min_length=1,
        description="A list of potential commands the agent considered, which should be ordered from most to least confident.",
    )
