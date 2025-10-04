from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional, Literal

# --- Pydantic Models for Anthropic's Messages API ---


class Message(BaseModel):
    """A single message in a chat conversation for Anthropic."""

    role: Literal["user", "assistant"]
    content: str


# --- Input/Payload Models ---


class CreateMessagePayload(BaseModel):
    """The JSON body for the /v1/messages endpoint."""

    model: str
    messages: List[Message]
    system: Optional[str] = None
    max_tokens: int
    temperature: Optional[float] = Field(None, ge=0.0, le=1.0)
    tools: Optional[List[Dict[str, Any]]] = None

    class Config:
        from_attributes = True


class CreateMessageParameters(BaseModel):
    """Defines the user-facing parameters for the `createMessage` action."""

    model: str
    messages: List[Message]
    system: Optional[str] = None
    max_tokens: int = Field(
        4096, description="The maximum number of tokens to generate."
    )
    temperature: Optional[float] = Field(None, ge=0.0, le=1.0)
    tools: Optional[List[Dict[str, Any]]] = None
