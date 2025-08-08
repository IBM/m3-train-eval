# AVAILABLE_DATABASES = ["ElserIndex"]  # Placeholder names
# DATABASE_NAME = "ElserIndex"  # Placeholder name user by the expert
RETRIEVE_FUNCTION_NAME = "retrieve_documents"
COMPRESS_TOOL_RESPONSE_INSTRUCTION = "If the final tool response is a list of many objects, and all of them could be used to answer the query, retain randomly any {curr_resp_cutoff} objects.\n        - Reflect this truncation clearly and intentionally in your thought.\n        - Justify why the selected subset is sufficient"