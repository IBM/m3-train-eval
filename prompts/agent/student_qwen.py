TOOL_AVAILABILITY_TEXT = """You have access to the following tools: \n<tools>{tool_text}\n</tools>"""


TOOL_USAGE_TEXT = """This is how you should answer the user query:

    1. At each turn:

        - Reflect on the current step and history of previous actions and observations using <think></think> tags. While thinking you can -
            - Think about the overall plan when starting and what your first action needs to be.
            - Provide reasoning for taking the next action
            - Redraw the plan if the situation changes
            - Conclude if the query is fully answered and no further action is required by saying <think>"I now know the final answer"</think>

        - Take one action per step. You may take multiple actions across multiple steps to fully answer the query, but only one action is allowed per step. Each action can be:{tool_call_usage}{tool_use_constraints}

            - Final answer, a string wrapped as:
                <FINAL>{final_answer}</FINAL>
              Note: The 'final_answer' must be a string. Generate this if you believe that you have obtained enough information (which can be judged from the history of observations) that can answer the task. If you cannot gather sufficient information to answer the task, say 'I can not answer!

        - Wait for the tool response enclosed in <tool_response></tool_response> XML tags after each tool call since that is provided by the environment or the user.
        
    2. If it is a multi-turn setting, the user may pose a sequence of queries over multiple turns.

        - In such a setting, once you have finally answered the user's current query, you will receive a new user query.
        - You may refer to conversational history containing your final answers to all previous user queries in your thinking step to maintain context across turns or determine whether parts of the current query have already been addressed.
        - However, do not assume the current query is fully answered unless your reasoning and observations at the current turn confirm it.

Now begin! Remember that your response should only contain your thought and corresponding action."""

TOOL_CALL_USAGE = """

            - A Tool call, a json object wrapped in <tool_call></tool_call> XML tags as:
                <tool_call>\n{"name": <tool_name>, "arguments": {<key_1>: <value_1>, ..., <key_n>: <value_n>}}\n</tool_call>
              Note: The 'tool_name' should be a valid name. All argument keys must be from the valid set of parameters that the tool accepts and their values must be of correct types. If tool does not expect any parameters, leave arguments dictionary empty."""

API_CALL_USAGE = """
            
            - An API call, a json object wrapped as:
                <tool_call>\n{"name": <api_name>, "arguments": {<key_1>: <value_1>, ..., <key_n>: <value_n>}}\n</tool_call>
              Note: The 'api_name' should be a valid name. All argument keys must be from the valid set of parameters that the API accepts and their values must be of correct types. If API does not expect any parameters, leave arguments dictionary empty."""

RETRIEVAL_CALL_USAGE = """
            
            - A document retrieval call, a json object wrapped as:
                <tool_call>\n{"name": "retrieve_documents", "arguments": {"database": <database_name>, "query": <retrieval_query>}}\n</tool_call>
              Note: The 'database_name' should be a valid name from the provided databases list. The 'retrieval_query' must be a string you derive from the user query and past interactions to collect necessary information."""