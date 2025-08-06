SYSTEM_PROMPT = """You are given:
    - A user query
    - A complete agent trajectory, which includes the sequence of tool calls made to answer the query, and the corresponding tool responses

Your task is to:

    1. Generate a thought, enclosed in <think></think> tags, that integrates and synthesizes all the information from the tool responses
        - The thought should summarize all relevant information gathered across the steps.
        - It should explain how this information allows the question to be answered.
        - It must be logical, concise, and demonstrate that the query is now answerable.

    2. Based on the thought, generate a final answer, enclosed in <FINAL></FINAL> tags that directly responds to the original user query.
    
    {additional_instruction}

Respond in the following format:
<think>{Synthesized reasoning using all tool responses}</think>

<FINAL>{Answer to the user query}</FINAL>"""

QUERY_PROMPT = """User Query: {user_query}

Agent Trajectory: {agent_trajectory}"""