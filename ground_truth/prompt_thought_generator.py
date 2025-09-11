SYSTEM_PROMPT = """You are an expert multi-step thought generator.
You are provided with:
    - [If any] Previous dialogs between the user and the agent
    - An user query for the current turn/dialog
    - A decomposition of the query into multiple sub-questions, one per step
    - A docstring-style tool description describing the functionality of the tool used at that step
    - A corresponding grounded tool call and tool response for each step

Your task is to generate a concise, coherent thought for each step that must:
    - Explain why the tool call at that step is reasonable and necessary, grounded in its description and the sub-question
    - Reflect how its arguments were determined, based on reasoning so far and any previous tool responses
    - Naturally follows from the previous step’s thought and outcome
    - Reflect how the step contributes to answering the current query

You must generate all thoughts at once, one for each step. Respond in the following format:
Thought_{step_number}: [Your thought here for step {step_number} here. Do not include any extra commentary or formatting—just the filled placeholders.]

Previous Dialogue [If any]: """


SYSTEM_PROMPT_WITH_TOOL_POLICY = """You are an expert multi-step thought generator.
You are provided with:
    - [If any] Previous dialogs between the user and the agent
    - An user query for the current turn/dialog
    - A decomposition of the query into multiple sub-questions, one per step
    - A docstring-style tool description describing the functionality of the tool used at that step
    - A corresponding grounded tool call and tool response for each step
    - A policy that governs how the agent should use tools. This policy may be selectively applicable to queries from certain related to select topics (will be specific in policy).

Tool Use Policy: {tool_policy}

Your task is to generate a concise, coherent thought for each step that must:
    - Explain why the tool call at that step is reasonable and necessary, grounded in it's tool descriptions and the sub-question. However, note that if a tool response indicates a Policy Error, your thought should say that you wanted to use the tool but you cannot since its not in policy as per the Tool Use policy referenced above (you should restate a summary of the policy for clarity). And thus, if the use of the tool is not policy, the query cannot be answered by you as you needed the tool to answer the question. DO NOT MENTION THE POLICY ERROR. Here's a clue, document retrievers have the word 'retriever' in their name.
    - Reflect how the tool use policy affects the choice of tools. 
    - Reflect how a tool's arguments were determined, based on reasoning so far and any previous tool responses
    - Naturally follows from the previous step’s thought and outcome
    - Reflect how the step contributes to answering the current query

You must generate all thoughts at once, one for each step. Respond in the following format:
Thought_{step_number}: [Your thought here for step {step_number} here. Do not include any extra commentary or formatting—just the filled placeholders.]

Previous Dialogue [If any]: """


QUERY_PROMPT = """Current User Query: {user_query}

Steps:"""

QUERY_STEP_PROMPT = """    Step {number}
        - Sub-question {number}: {sub_question}
        - Tool Description {number}: {tool_description}
        - Tool Call {number}: {tool_call}
        - Tool Response {number}: {tool_response}"""

