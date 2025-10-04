from pydantic import BaseModel, Field
from typing import List, Any, Optional

# --- Pydantic Models for Google's Generative Language (Gemini) API ---
# These models reflect the structure of the generateContent method.


class Part(BaseModel):
    """A single part of a multi-modal content block."""

    text: str


class Content(BaseModel):
    """Represents a piece of content with a specific role."""

    role: str = Field(..., description="Typically 'user' or 'model'.")
    parts: List[Part]


class SafetySetting(BaseModel):
    """Configuration for safety filters."""

    category: str
    threshold: str


class GenerationConfig(BaseModel):
    """Configuration for the generation process."""

    temperature: Optional[float] = Field(None, ge=0.0)
    top_p: Optional[float] = Field(None, ge=0.0)
    top_k: Optional[int] = Field(None, ge=0)
    max_output_tokens: Optional[int] = Field(8192, alias="maxOutputTokens")

    class Config:
        populate_by_name = True  # Allows using both snake_case and alias (camelCase)


class Tool(BaseModel):
    """Defines a tool the model can call, such as a function declaration."""

    function_declarations: List[Any] = Field(
        default_factory=list, alias="functionDeclarations"
    )

    class Config:
        populate_by_name = True


# --- Input/Payload Models ---


class GenerateContentPayload(BaseModel):
    """The JSON body for the :generateContent endpoint."""

    contents: List[Content]
    safety_settings: Optional[List[SafetySetting]] = Field(None, alias="safetySettings")
    generation_config: Optional[GenerationConfig] = Field(
        None, alias="generationConfig"
    )
    tools: Optional[List[Tool]] = None

    class Config:
        from_attributes = True
        populate_by_name = True


class GenerateContentParameters(BaseModel):
    """
    Defines the user-facing parameters for the `generateContent` action.
    This model is used for validation.
    """

    contents: List[Content]
    safety_settings: Optional[List[SafetySetting]] = Field(None, alias="safetySettings")
    generation_config: Optional[GenerationConfig] = Field(
        None, alias="generationConfig"
    )
    tools: Optional[List[Tool]] = None

    class Config:
        populate_by_name = True
