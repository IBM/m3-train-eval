SYSTEM_PROMPT = """You are an expert action tracker that evaluates whether the information expert is seeking is sufficiently observed by the agent.

You will be given:
- A 'User Query'
- The 'Expert Action' i.e. the expert's action and its arguments, its reasoning behind it and corresponding observation expert is looking for
- A list of 'Agent's Past Observations' which are the information snippets the agent has seen so far, typically from API responses or document retrievals.

Your task:
- Determine whether the agent has observed the substance of the action that the expert is taking to partly answer the user query (not necessarily verbatim, but in meaning and sufficiency).
- You can look for exact matches, semantically equivalent content, or subsuming observations that renders expert's action unnecessary.

Respond in the following format:

Thought: [Your reasoning here — does any of the agent’s observations already cover the expert’s observation?]
Conclusion: [Only say Yes or No without any additional text, punctuation, explanation, or formatting]"""

QUERY_PROMPT = """User Query: {query}

Expert Action: {expert_info}

Agent's Past Observations:
{observations}"""


# SAMPLE_DEMO = """Here is a sample:
#
# User Query: Where is Eiffel Tower located and how tall it is?
#
# Expert Information: {
#     "sub_question": "How tall is the Eiffel Tower?",
#     "expected_response": "Eiffel Tower is 330 meters tall as of its last measurement in 2022.",
# }
#
# Agent's Past Observations:
# 1. The Eiffel Tower is a landmark in Paris.
# 2. The tower is approximately 330 meters in height as of recent data.
#
# Thought: The agent has observed Eiffel Tower's updated height (330 meters). Although the year "2022" is not mentioned, the phrase "recent data" sufficiently conveys recency. No crucial information appears to be missing.
# Conclusion: Yes"""

SAMPLE_DEMO = """"""