import jsonpatch
from typing import Dict, Any, List

from ..interactive.session import SessionState
from ..data.agent_schemas import AgentBeliefs


class BeliefManager:
    """
    Manages the agent's structured belief state within the interactive session.

    This service acts as the interface to the agent's "latent space" or memory,
    storing its understanding of the user's goal, its strategic plan, and any
    facts it has discovered.
    """

    BELIEF_STATE_VARIABLE = "_agent_beliefs"

    def initialize_beliefs(self, state: SessionState, goal: str) -> AgentBeliefs:
        """
        Creates a new, empty belief state for the start of an agentic session.

        Args:
            state: The current user session state.
            goal: The initial, top-level goal provided by the user.

        Returns:
            The newly created AgentBeliefs object.
        """
        if self.is_session_active(state):
            # Prevent starting a new session if one is already in progress.
            raise RuntimeError(
                "An agentic session is already active. Use 'agent cancel' to end it first."
            )

        beliefs = AgentBeliefs(original_goal=goal)
        state.variables[self.BELIEF_STATE_VARIABLE] = beliefs
        return beliefs

    def get_beliefs(self, state: SessionState) -> AgentBeliefs | None:
        """Retrieves the current belief state from the session."""
        return state.variables.get(self.BELIEF_STATE_VARIABLE)

    def is_session_active(self, state: SessionState) -> bool:
        """Checks if an agentic session is currently active."""
        return self.BELIEF_STATE_VARIABLE in state.variables

    def update_beliefs(
        self, state: SessionState, patch: Dict[str, Any] | List[Dict[str, Any]]
    ):
        """
        Applies a JSON Patch (RFC 6902) to the current belief state.

        This is the primary method for the Analyst agent to update the plan,
        add discovered facts, or modify the state based on new observations.

        Args:
            state: The current user session state.
            patch: A JSON Patch document as a dict or list of dicts.
        """
        beliefs = self.get_beliefs(state)
        if not beliefs:
            raise ValueError("Cannot update beliefs: no active agentic session.")

        # Convert the Pydantic model to a dict for patching
        beliefs_dict = beliefs.model_dump()

        # Apply the patch
        patch_list = patch if isinstance(patch, list) else [patch]
        updated_dict = jsonpatch.apply_patch(beliefs_dict, patch_list)

        # Re-validate and update the state with the new Pydantic model
        state.variables[self.BELIEF_STATE_VARIABLE] = AgentBeliefs(**updated_dict)

    def end_session(self, state: SessionState):
        """Removes the belief state from the session, ending the agentic run."""
        if self.is_session_active(state):
            del state.variables[self.BELIEF_STATE_VARIABLE]
