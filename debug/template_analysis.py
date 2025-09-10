from transformers import AutoTokenizer
import json
from transformers.utils import get_json_schema

print("############ Finished Imports ############")

def get_current_weather(location: str, _format: str):
    """
    Get the current weather
    Args:
        location: The city and state, e.g. San Francisco, CA
        _format: The temperature unit to use. Infer this from the users location. (choices: ["celsius", "fahrenheit"])
    """
    pass

def add_integers(a: int, b: int) -> int:
    """Add two integers and return the result.

    Args:
        a: The first integer.
        b: The second integer.

    Returns:
        int: The sum of the two integers.

    Raises:
        TypeError: If either `a` or `b` is not an integer.
    """
    if not isinstance(a, int) or not isinstance(b, int):
        raise TypeError("Both arguments must be integers.")

    return a + b

model_paths=[
    "ibm-granite/granite-3.3-8b-instruct",
    "/proj/m3benchmark/granite4_ckpts/granite-4.0-tiny-prerelease-greylock/r250825a/"
]

template_types = [
    "student_granite3",
    "student_granite4"
]
results = {}
for model_path, template_type in zip(model_paths, template_types):
    results[template_type] = {
        "model": model_path
    }
    print(f"RUNNING MODEL: {model_path}")
    tokenizer = AutoTokenizer.from_pretrained(model_path)
    conversation = [
        {"role": "system", "content": "You are an AI assistant."},
        {"role": "user", "content": "What's the weather like in Paris?"},
        {"role": "assistant", "content": "<|tool_call|> get_current_weather('Paris, France', 'celsius')"}
    ]

    print("############ Chat Template ############")
    print(tokenizer.get_chat_template())
    results[template_type]['chat_template'] = tokenizer.get_chat_template()

    # print("\n\n############ Json Schema ############")
    # print(json.dumps(get_json_schema(get_current_weather), indent=4))  # Converts the function to json based on Google docstring format

    print("\n\n############ Input prompt ############")
    tools = [get_current_weather, add_integers]
    tok_text = tokenizer.apply_chat_template(conversation, tokenize=False, thinking=True, add_generation_prompt=False, tools=tools)
    print(tok_text)
    results[template_type]['tokenized_text'] = tok_text


    print("\n\n############ Tools Format from Template ############")
    tool_specs = json.dumps([get_json_schema(get_current_weather), get_json_schema(add_integers)])
    from data_utils.template import TEMPLATES
    from envs.base_env import ToolPolicy
    # import pdb; pdb.set_trace()
    template = TEMPLATES[template_type]
    # data = json.load(open("./data/api_before_rag-dev.json", 'r'))
    # tools = data[0]["API_info"]["tools"]
    tools_str = template.format_tools.apply(content=tool_specs, tool_policy=ToolPolicy())
    print(tools_str)
    results[template_type]['tools_string'] = tool_specs

    print("\n\n############ Conversation from Template ############")

    encoded_convo = template.encode_multiturn(tokenizer, conversation[1:],
            system = "You are an AI assistant.",
            tools = tool_specs,
            tool_policy = ToolPolicy(), 
            return_text = True)
    results[template_type]['templated_text'] = encoded_convo

try:
    with open("logging/template_analysis.json", "w") as f:
        json.dump(results, f)
except:
    print("BIG FAILURE")
    print(results)

