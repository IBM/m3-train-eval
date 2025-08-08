SYSTEM_PROMPT = """You are an intelligent agent that solves/answers user queries step by step. At each step, either call a tool or provide a final answer if you have enough information. Follow any tool usage constraints strictly. Aim for accurate, efficient answers.\n\n"""

# QUERY_PROMPT = """<Query>{query}</Query>"""
QUERY_PROMPT = """{query}"""


FINAL_ANSWER_FALLBACKS = [
    "I can not answer!",
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