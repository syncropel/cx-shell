from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional, Literal

# --- Pydantic Models for OpenAI's Chat Completions API ---
# These models are a subset of the official API, tailored for our agent's needs.


class Message(BaseModel):
    """A single message in a chat conversation."""

    role: Literal["system", "user", "assistant", "tool"]
    content: str
    name: Optional[str] = None
    tool_calls: Optional[List[Dict[str, Any]]] = None


class Tool(BaseModel):
    """A tool the model can call."""

    type: Literal["function"] = "function"
    function: Dict[str, Any]


class ResponseFormat(BaseModel):
    """Specifies the response format, e.g., for JSON mode."""

    type: Literal["text", "json_object"]


# --- Input/Payload Models ---


class CreateChatCompletionPayload(BaseModel):
    """The JSON body for the /v1/chat/completions endpoint."""

    model: str
    messages: List[Message]
    temperature: Optional[float] = Field(None, ge=0.0, le=2.0)
    top_p: Optional[float] = Field(None, ge=0.0, le=1.0)
    tools: Optional[List[Tool]] = None
    tool_choice: Optional[Any] = None  # Can be str or dict
    response_format: Optional[ResponseFormat] = None

    class Config:
        # Allows Pydantic to create the model from our internal parameter object.
        from_attributes = True


class CreateChatCompletionParameters(BaseModel):
    """
    Defines the user-facing parameters for the `createChatCompletion` action.
    This model is used by the Pydantic "Pre-flight Check" for validation.
    It deliberately mirrors the payload for simplicity in this case.
    """

    model: str
    messages: List[Message]
    temperature: Optional[float] = Field(None, ge=0.0, le=2.0)
    top_p: Optional[float] = Field(None, ge=0.0, le=1.0)
    tools: Optional[List[Tool]] = None
    tool_choice: Optional[Any] = None
    response_format: Optional[ResponseFormat] = None
