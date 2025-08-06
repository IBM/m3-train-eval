import copy
import json
from typing import List, Union, Any, Tuple

from transformers.utils import get_json_schema


def reformat_tools(tools: Union[dict, List[dict]]) -> List[dict]:
    """
    Reformat the provided set of tools into the Google docstring format. For sample, run print_google_docstring_format()
    to see how the Google docstring format works.
    :param tools: List of tools to reformat with each tool in the format {"name": str, "description": str, "parameters": dict}
    :return:
    """
    if isinstance(tools, dict):
        tools = [tools]

    reformatted_tools = []
    for tool in tools:

        # The tool does not accept any parameters
        if 'parameters' not in tool:
            tool["parameters"] = {}

        _type: str = tool["parameters"]["type"] if "type" in tool["parameters"] else "object"

        _required: List[str] = []
        if 'required' in tool["parameters"]:
            _required = tool["parameters"]["required"]
        else:
            if tool["parameters"]:
                _required = list(tool["parameters"].keys())

        _properties: dict = {}  # Will be used as default if tool does not accept any parameters
        if 'properties' in tool["parameters"]:
            _properties = tool["parameters"]["properties"]
        else:
            if tool["parameters"]:
                _properties = tool["parameters"]

        # Formatted tool
        _tool = {
            "type": "function",
            "function": {
                "name": tool["name"],
                "description": tool["description"],
                "parameters": {
                    "type": _type,
                    "properties": _properties,
                    "required": _required,
                }
            }
        }
        reformatted_tools.append(_tool)

    return reformatted_tools


def get_available_databases_from_tools(tools: list[dict[str, Any]]) -> List[str]:
    from envs.constants import RETRIEVE_FUNCTION_NAME
    tool_found = False
    for tool in tools:
        tool_name = tool["name"] if 'name' in tool else tool['function']['name']
        if tool_name == RETRIEVE_FUNCTION_NAME:
            tool_found = True
            break

    if not tool_found:
        raise ValueError("The database retrieval tool was not found!")
    else:
        key = 'database'
        available_databases: List[str] = tool['function']['parameters']['properties'][key]['enum']
        return available_databases


def split_retriever_tool_from_rest(tools: list[dict[str, Any]]) -> Tuple[List[dict[str, Any]], dict[str, Any]]:
    from envs.constants import RETRIEVE_FUNCTION_NAME
    retriever_tool = None
    other_tools = []
    for tool in tools:
        tool_name = tool["name"] if 'name' in tool else tool['function']['name']
        if tool_name == RETRIEVE_FUNCTION_NAME:
            retriever_tool = copy.deepcopy(tool)
        else:
            other_tools.append(tool)
    return other_tools, retriever_tool


def print_google_docstring_format():

    def get_current_weather(location: str, format: str):
        """
        Get the current weather
        Args:
            location: The city and state, e.g. San Francisco, CA
            format: The temperature unit to use. Infer this from the users location. (choices: ["celsius", "fahrenheit"])
        """
        pass

    print(json.dumps(get_json_schema(get_current_weather), indent=4))


if __name__ == "__main__":
    print_google_docstring_format()


def ground_tool_availability(tools: list[dict[str, Any]], policy: str) -> list[dict[str, Any]]:
    r"""[My custom added] Ground tool availability instructions to the given policy"""
    other_tools, retriever_tool = split_retriever_tool_from_rest(tools)
    if policy == 'only_api':
        tools = other_tools
    elif policy == 'only_rag':
        tools = [retriever_tool]
    elif policy == 'neither_api_rag':
        tools = []
    return tools


def ground_tool_usage(policy: str) -> str:
    r"""[My custom added] Ground tool usage instructions to the given policy"""
    tool_use_constraints = ""
    if len(policy) > 0:
        tool_use_constraints = f"\n\n            Tool Usage Constraint: {policy}"
    return tool_use_constraints
