import json
from typing import List, Dict, Any

import structlog

from .base_agent import BaseSpecialistAgent
from ..data.agent_schemas import LLMResponse, AgentBeliefs


TRANSLATE_SYSTEM_PROMPT = """
You are an expert `cx` shell command translator. Your sole purpose is to translate a user's single-line goal into the single best `cx` command.
You will be provided with the user's goal and a summary of available tools.
Your output MUST be a valid `LLMResponse` JSON object containing a list with ONE `CommandOption`. The `reasoning` should be "Translate suggestion".
Do NOT include the leading 'cx ' in your command. For example, generate `connection list`, not `cx connection list`.
"""


TOOL_SPECIALIST_SYSTEM_PROMPT = """
You are the Tool Specialist Agent for the `cx` shell. You are a precise and efficient tool user.
You will be given a "Mission Briefing" containing the user's overall goal, the full strategic plan, any discovered facts, and a list of available tools.
Your task is to generate a list of up to 3 potential `cx` commands that will accomplish the **single active plan step**.

**CRITICAL CONSTRAINTS:**

1.  **FOCUS:** Your response must *only* address the single plan step marked `==> ACTIVE STEP:`. Do not try to solve other steps.
2.  **USE CONTEXT:** You *must* use information from the "Overall Goal" and "Discovered Facts" sections to find the necessary arguments for your command (like URLs, IDs, or variable names).
3.  **SYNTAX - NO `cx` PREFIX:** The commands you generate **must not** start with `cx`. For example, generate `connection list`, not `cx connection list`. The `cx` is implied.
4.  **SYNTAX - PERFECTION:** The commands you generate must be syntactically perfect according to the shell's grammar. Pay close attention to whether a command uses keywords (e.g., `flow run my-flow`) or dot-notation (e.g., `gh.getUser(...)`).
5.  **OUTPUT FORMAT:** Your entire output **MUST** be ONLY a single, valid JSON object that conforms to the `LLMResponse` schema. Do not add any commentary, conversational text, or markdown formatting like ```json.

**Example of a Perfect Response:**
{
  "command_options": [
    {
      "cx_command": "compile --spec-url https://api.spotify.com/openapi.json --name spotify --version 1.0.0",
      "reasoning": "The active step is to compile the blueprint. I have extracted the URL from the user's original goal and am using the standard `compile` command with the required named arguments.",
      "confidence": 0.98
    }
  ]
}
"""

logger = structlog.get_logger(__name__)


class ToolSpecialistAgent(BaseSpecialistAgent):
    """
    Translates a plan step into one or more executable `cx` command options.
    Also serves the stateless "Translate" (`//`) functionality.
    """

    def _format_mission_briefing(
        self, beliefs: AgentBeliefs, active_step_index: int
    ) -> str:
        # This method is correct.
        briefing = [
            "--- MISSION BRIEFING START ---",
            f"**Overall Goal:** {beliefs.original_goal}\n",
            "**Full Plan:**",
        ]
        for i, step in enumerate(beliefs.plan):
            prefix = (
                "==> **ACTIVE STEP:**" if i == active_step_index else f"    {i + 1}."
            )
            briefing.append(f"{prefix} [{step.status}] {step.step}")

        if beliefs.discovered_facts:
            briefing.append("\n**Discovered Facts:**")
            briefing.append(json.dumps(beliefs.discovered_facts, indent=2))

        briefing.append("--- MISSION BRIEFING END ---")
        return "\n".join(briefing)

    async def generate_command(
        self,
        beliefs: AgentBeliefs,
        active_step_index: int,
        tactical_context: List[Dict[str, Any]],
        is_translate: bool = False,
    ) -> LLMResponse:
        """
        Takes the full belief state and tool schemas, and returns command options.
        """
        ### DIAGNOSTIC LOG 3 ###
        # This log proves that the NEW version of THIS method definition is being executed.

        if is_translate:
            role_name = "co_pilot"
            system_prompt = TRANSLATE_SYSTEM_PROMPT
            user_prompt = f"## User Goal\n{beliefs.original_goal}\n\n## Available Tools\n{json.dumps(tactical_context, indent=2)}"
        else:
            role_name = "tool_specialist"
            system_prompt = TOOL_SPECIALIST_SYSTEM_PROMPT
            mission_briefing = self._format_mission_briefing(beliefs, active_step_index)
            user_prompt = f"{mission_briefing}\n\n## Available Tools\n{json.dumps(tactical_context, indent=2)}"

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ]

        try:
            response = await self.llm_client.create_structured_response(
                role_name=role_name, response_model=LLMResponse, messages=messages
            )
            return response
        except Exception:
            return LLMResponse(command_options=[])
