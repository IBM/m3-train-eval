#SYSTEM_PROMPT = """You are an intelligent agent that solves/answers user queries step by step. At each step, either call a tool or provide a final answer if you have enough information. Follow any tool usage constraints strictly. Aim for accurate, efficient answers.\n\n"""
SYSTEM_PROMPT = "You are a helpful assistant with access to the following tools. You may call one or more tools to assist with the user query.\n"
# QUERY_PROMPT = """<Query>{query}</Query>"""
QUERY_PROMPT = """{query}"""


FINAL_ANSWER_FALLBACKS = [
    "Sorry, I cannot answer your query!",   
    "I don't know.",
    "No answer available.",
    "I'm unable to answer.",
    "Insufficient information to respond."
]

FINAL_ANSWER_INSUFFICIENCY_TEMPLATES = [
    "If you don't have enough information to complete the task, respond with '{fb}'",
    "If the available information is insufficient, reply with '{fb}'",
    "When you lack enough details to solve the task, say '{fb}'",
    "If you cannot find adequate information, respond by saying '{fb}'",
    "If the information you have is incomplete, your reply should be '{fb}'"
]

TOOL_USE_RAG_TEMPLATES_WITH_DOMAINS=[
    "If a user's query pertains to {domains}, make sure you try answering them by only using document retrievers. Do not use other types of tools.",
 ]


TOOL_USE_RAG_TEMPLATES_GENERAL=[
    "Use document retrievers to answer questions. Do not use any other type of tool."
]



TOOL_USE_API_TEMPLATES_WITH_DOMAINS=[
    "If a user's query pertains to {domains}, make sure you do not use document retrievers to try answering those questions. Use other types of tools. ",
]

TOOL_USE_API_TEMPLATES_GENERAL=[
    "Do not use document retrievers to answer questions. Use other types of tools."
]


TOOL_FIRST_API_TEMPLATES=[
    "If a user's query pertains to {domains}, first try answering those questions without invoking document retrievers. ",
    "If you have to use tools, first try answering them without using document retrievers."
]


TOOL_FIRST_RAG_TEMPLATES=[
    "If a user's query pertains to {domains}, first try answering those questions by using document retrievers. ",
    "If you have to use tools, firt try using document retrievers for answering them."
]