import json
from typing import Any

from .base_agent import BaseSpecialistAgent
from ..data.agent_schemas import AnalystResponse
from ..engine.connector.utils import safe_serialize

SYSTEM_PROMPT = """You are the Analyst Agent within the `cx` shell's CARE (Composite Agent Reasoning Engine).
Your role is to be a precise and factual data interpreter.
You will be given the goal of the previous step and the raw `Observation` from the command that was executed.

**Your Responsibilities:**

1.  **Analyze Observation:** Determine if the command was successful. A successful execution that returns an empty list, `null`, or a "not found" message is a SEMANTIC failure. A traceback or HTTP 4xx/5xx error is a RUNTIME failure.
2.  **Determine Strategic Failure:** If the observation indicates that the overall plan is flawed or impossible (e.g., a required resource does not exist), set `indicates_strategic_failure` to `true`.
3.  **Update Beliefs:** Formulate a SINGLE, precise JSON Patch operation to update the agent's belief state. This should usually be adding a fact to `/discovered_facts`. **Even if the observation is `null` or uninteresting, you MUST still provide a minimal patch, like adding a `note` to the discovered facts.**
4.  **Summarize:** Write a concise, one-sentence summary of what happened.

Your output MUST conform to the `AnalystResponse` schema provided.

---
**Examples of Perfect Responses:**

**Example 1: The Observation is `null` or uninteresting.**
*Step Goal:* "Use the `connection list` command to display all saved connections."
*Raw Observation:* `{"status": "success", "message": "Management command 'ConnectionCommand' executed successfully."}`

*Your Output (JSON):*
```json
{
  "belief_update": {
    "op": "add",
    "path": "/discovered_facts/turn_1_note",
    "value": "The connection list command was executed as planned."
  },
  "summary_text": "The command to list connections executed successfully.",
  "indicates_strategic_failure": false
}
```

**Example 2: The Observation contains a key piece of new information.**
*Step Goal:* "Get the profile for the user 'torvalds'."
*Raw Observation:* `{"login": "torvalds", "id": 1024025, "repos_url": "https://api.github.com/users/torvalds/repos"}`

*Your Output (JSON):*
```json
{
  "belief_update": {
    "op": "add",
    "path": "/discovered_facts/user_torvalds_repos_url",
    "value": "https://api.github.com/users/torvalds/repos"
  },
  "summary_text": "Successfully retrieved the user profile for 'torvalds' and found the repository URL.",
  "indicates_strategic_failure": false
}
```

**Example 3: The Observation is a critical error.**
*Step Goal:* "Compile the blueprint from the provided URL."
*Raw Observation:* `{"error": "FileNotFoundError: Blueprint 'community/spotify@v1.0.0' not found."}`

*Your Output (JSON):*
```json
{
  "belief_update": {
    "op": "add",
    "path": "/discovered_facts/compilation_error",
    "value": "The specified blueprint version 'community/spotify@v1.0.0' does not exist."
  },
  "summary_text": "The compilation failed because the specified blueprint version could not be found.",
  "indicates_strategic_failure": true
}
```
"""


class AnalystAgent(BaseSpecialistAgent):
    """
    Interprets command outputs, updates the belief state, and summarizes the turn.
    """

    async def analyze_observation(
        self, step_goal: str, observation: Any
    ) -> AnalystResponse:
        """
        Takes the result of a command and returns an analysis.
        """
        try:
            observation_str = json.dumps(
                safe_serialize(observation), indent=2, ensure_ascii=False
            )
        except Exception:
            observation_str = repr(observation)

        user_prompt = f"## Step Goal\n{step_goal}\n\n## Raw Observation\n```json\n{observation_str[:4000]}```"

        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ]

        try:
            response = await self.llm_client.create_structured_response(
                role_name="analyst", response_model=AnalystResponse, messages=messages
            )
            return response
        except Exception as e:
            return AnalystResponse(
                belief_update={
                    "op": "add",
                    "path": "/discovered_facts/analyst_error",
                    "value": f"Failed to get analysis from LLM: {e}",
                },
                summary_text="The Analyst agent failed to process the observation.",
                indicates_strategic_failure=True,
            )
