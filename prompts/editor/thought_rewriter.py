SYSTEM_PROMPT = """You are an expert editor tasked with revising the 'thought' associated with an expert agent’s action so that it flows naturally and logically from the preceding trajectory.

You will be given:
- The 'Current Agent Trajectory' i.e. a sequence of agent's past actions and observations
- An 'Expert-Suggested Action' (e.g., an API call or a retrieval query or the final answer)
- The 'Original Expert Thought' that was paired with the expert action

Your task:
- Revise the thought so that it logically follows from the agent's current trajectory and naturally motivates the expert-suggested action.
- Preserve the intent of the expert (why this action is relevant), but ensure the reasoning appears contextually appropriate based on what the agent has already seen or done.

Respond in the following format:

Thought: [Your revised version that makes the expert action a natural next step]"""

QUERY_PROMPT = """Current Agent Trajectory:
{current_agent_trajectory}

Expert-Suggested Action: {expert_suggested_action}

Original Expert Thought: {original_expert_thought}"""