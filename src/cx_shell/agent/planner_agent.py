# ~/repositories/cx-shell/src/cx_shell/agent/planner_agent.py

from typing import List

from .base_agent import BaseSpecialistAgent
from ..data.agent_schemas import PlanStep

SYSTEM_PROMPT = """
You are the Planner Agent, a high-level strategic thinker for the `cx` shell. Your sole purpose is to decompose a user's goal into a logical sequence of high-level steps.

**CRITICAL CONSTRAINTS:**
1.  Your world is limited to the capabilities of the `cx` shell. You CANNOT write code, access external websites, or perform tasks outside of this shell.
2.  Your plan steps must be described as goals for another AI agent to accomplish using `cx` commands.
3.  The plan should be concise, logical, and directly address the user's goal using the tools and assets mentioned in the provided context.
4.  Your output MUST be ONLY a valid JSON array of `PlanStep` objects. Do not add any commentary or conversational text.

**Example Plan:**
[
  {"step": "Use the `compile` command to generate a blueprint from the provided URL."},
  {"step": "Use the `connection create` command to set up a new connection for the newly created blueprint."},
  {"step": "Activate the new connection using the `connect` command."},
  {"step": "Test a simple action from the new blueprint to verify it works."}
]
"""


class PlannerAgent(BaseSpecialistAgent):
    """
    The Planner Agent is responsible for high-level strategic planning.
    It decomposes a user's goal into a sequence of logical, abstract steps.
    """

    async def generate_plan(self, goal: str, strategic_context: str) -> List[PlanStep]:
        """
        Takes a user goal and context, and returns a structured plan.
        """
        user_prompt = (
            f"## User Goal\n{goal}\n\n## Strategic Context\n{strategic_context}"
        )

        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ]

        try:
            plan = await self.llm_client.create_structured_response(
                role_name="planner", response_model=List[PlanStep], messages=messages
            )
            return plan
        except Exception as e:
            return [
                PlanStep(
                    step=f"The Planner Agent failed to generate a valid plan: {e}",
                    status="failed",
                )
            ]
