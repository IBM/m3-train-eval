from transformers import AutoTokenizer
import json
from transformers.utils import get_json_schema

model_path = "ibm-granite/granite-3.3-8b-instruct" # "Qwen/Qwen3-8B", "mistralai/Mistral-7B-Instruct-v0.3"
template_type = "granite3"

# model_path = "/dccstor/belder1/cache/hub/granite-4.0-tiny-prerelease-greylock/r250825a" # "Qwen/Qwen3-8B", "mistralai/Mistral-7B-Instruct-v0.3"
# template_type = "student_granite4"

def get_current_weather(location: str, _format: str):
    """
    Get the current weather
    Args:
        location: The city and state, e.g. San Francisco, CA
        _format: The temperature unit to use. Infer this from the users location. (choices: ["celsius", "fahrenheit"])
    """
    pass

tokenizer = AutoTokenizer.from_pretrained(model_path)
conversation = [
    {"role": "system", "content": "You are an AI assistant."},
    {"role": "user", "content": "What's the weather like in Paris?"},
    {"role": "assistant", "content": "<|tool_call|> get_current_weather('Paris, France', 'celsius')"}
]

print("############ Chat Template ############")
print(tokenizer.get_chat_template())

print("\n\n############ Json Schema ############")
print(json.dumps(get_json_schema(get_current_weather), indent=4))  # Converts the function to json based on Google docstring format

print("\n\n############ Input prompt ############")
tools = [get_current_weather]
tok_text = tokenizer.apply_chat_template(conversation, tokenize=False, thinking=True, add_generation_prompt=False, tools=tools)
print(tok_text)

# from data_utils.template import TEMPLATES
# template = TEMPLATES[template_type]
# data = json.load(open("./data/api_before_rag-dev.json", 'r'))
# tools = data[0]["API_info"]["tools"]
# tools_str = template.format_tools.apply(content=json.dumps(tools))
# print(tools_str)


