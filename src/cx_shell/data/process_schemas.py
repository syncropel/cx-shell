from pydantic import BaseModel, Field
from typing import Literal, Optional
from datetime import datetime
from pathlib import Path


class Process(BaseModel):
    """
    Represents the state and metadata of a single background process.
    This model is what will be serialized to a state file (e.g., JSON).
    """

    id: str = Field(
        ..., description="The unique ID for the process, e.g., 'proc-20250904-01'."
    )
    status: Literal["pending", "running", "completed", "failed", "cancelled"] = Field(
        "pending", description="The current status of the process."
    )
    goal: str = Field(
        ...,
        description="The original high-level user goal that initiated this process.",
    )
    flow_path: Path = Field(
        ..., description="The absolute path to the .flow.yaml file being executed."
    )
    log_path: Path = Field(
        ...,
        description="The absolute path to the file containing the process's stdout/stderr.",
    )
    owner: str = Field(..., description="The user who started the process.")
    created_at: datetime = Field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    pid: Optional[int] = Field(
        None, description="The OS Process ID, if currently running."
    )


class ProcessLogEntry(BaseModel):
    """Represents a single, timestamped log line from a process."""

    timestamp: datetime
    message: str
