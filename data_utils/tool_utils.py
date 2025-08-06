import json
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Any, NamedTuple, Union

from typing_extensions import override

from envs.base_env import ToolPolicy
from envs.utils import ground_tool_availability, ground_tool_usage


class FunctionCall(NamedTuple):
    name: str
    arguments: str


DEFAULT_TOOL_PROMPT = (
    "You have access to the following tools:\n{tool_text}"
    "Use the following format if using a tool:\n"
    "```\n"
    "Action: tool name (one of [{tool_names}])\n"
    "Action Input: the input to the tool, in a JSON format representing the kwargs "
    """(e.g. ```{{"input": "hello world", "num_beams": 5}}```)\n"""
    "```\n"
)

GLM4_TOOL_PROMPT = (
    "你是一个名为 ChatGLM 的人工智能助手。你是基于智谱AI训练的语言模型 GLM-4 模型开发的，"
    "你的任务是针对用户的问题和要求提供适当的答复和支持。# 可用工具{tool_text}"
)

LLAMA3_TOOL_PROMPT = (
    "Cutting Knowledge Date: December 2023\nToday Date: {date}\n\n"
    "You have access to the following functions. To call a function, please respond with JSON for a function call. "
    """Respond in the format {{"name": function name, "parameters": dictionary of argument name and its value}}. """
    "Do not use variables.\n\n{tool_text}"
)

QWEN_TOOL_PROMPT = (
    "\n\n# Tools\n\nYou may call one or more functions to assist with the user query.\n\n"
    "You are provided with function signatures within <tools></tools> XML tags:\n<tools>{tool_text}"
    "\n</tools>\n\nFor each function call, return a json object with function name and arguments within "
    """<tool_call></tool_call> XML tags:\n<tool_call>\n{{"name": <function-name>, """
    """"arguments": <args-json-object>}}\n</tool_call>"""
)


@dataclass
class ToolUtils(ABC):
    """Base class for tool utilities."""

    @staticmethod
    @abstractmethod
    def tool_formatter(tools: list[dict[str, Any]], tool_policy: ToolPolicy = None) -> str:
        r"""Generate the system message describing all the available tools."""
        ...

    @staticmethod
    @abstractmethod
    def function_formatter(functions: list["FunctionCall"]) -> str:
        r"""Generate the assistant message including all the tool calls."""
        ...

    @staticmethod
    @abstractmethod
    def tool_extractor(content: str) -> Union[str, list["FunctionCall"]]:
        r"""Extract all the function calls from the assistant message.

        It should be an inverse function of `function_formatter`.
        """
        ...


class DefaultToolUtils(ToolUtils):
    r"""Default tool using template."""

    @override
    @staticmethod
    def tool_formatter(tools: list[dict[str, Any]]) -> str:
        tool_text = ""
        tool_names = []
        for tool in tools:
            tool = tool.get("function", "") if tool.get("type") == "function" else tool
            param_text = ""
            for name, param in tool["parameters"]["properties"].items():
                required, enum, items = "", "", ""
                if name in tool["parameters"].get("required", []):
                    required = ", required"

                if param.get("enum", None):
                    enum = ", should be one of [{}]".format(", ".join(param["enum"]))

                if param.get("items", None):
                    items = ", where each item should be {}".format(param["items"].get("type", ""))

                param_text += "  - {name} ({type}{required}): {desc}{enum}{items}\n".format(
                    name=name,
                    type=param.get("type", ""),
                    required=required,
                    desc=param.get("description", ""),
                    enum=enum,
                    items=items,
                )

            tool_text += "> Tool Name: {name}\nTool Description: {desc}\nTool Args:\n{args}\n".format(
                name=tool["name"], desc=tool.get("description", ""), args=param_text
            )
            tool_names.append(tool["name"])

        return DEFAULT_TOOL_PROMPT.format(tool_text=tool_text, tool_names=", ".join(tool_names))

    @override
    @staticmethod
    def function_formatter(functions: list["FunctionCall"]) -> str:
        return "\n".join([f"Action: {name}\nAction Input: {arguments}" for name, arguments in functions])

    @override
    @staticmethod
    def tool_extractor(content: str) -> Union[str, list["FunctionCall"]]:
        regex = re.compile(r"Action:\s*([a-zA-Z0-9_]+)\s*Action Input:\s*(.+?)(?=\s*Action:|\s*$)", re.DOTALL)
        action_match: list[tuple[str, str]] = re.findall(regex, content)
        if not action_match:
            return content

        results = []
        for match in action_match:
            tool_name = match[0].strip()
            tool_input = match[1].strip().strip('"').strip("```")
            try:
                arguments = json.loads(tool_input)
                results.append(FunctionCall(tool_name, json.dumps(arguments, ensure_ascii=False)))
            except json.JSONDecodeError:
                return content

        return results


class GLM4ToolUtils(ToolUtils):
    r"""GLM-4 tool using template."""

    @override
    @staticmethod
    def tool_formatter(tools: list[dict[str, Any]]) -> str:
        tool_text = ""
        for tool in tools:
            tool = tool.get("function", "") if tool.get("type") == "function" else tool
            tool_text += "\n\n## {name}\n\n{body}\n在调用上述函数时，请使用 Json 格式表示调用的参数。".format(
                name=tool["name"], body=json.dumps(tool, indent=4, ensure_ascii=False)
            )

        return GLM4_TOOL_PROMPT.format(tool_text=tool_text)

    @override
    @staticmethod
    def function_formatter(functions: list["FunctionCall"]) -> str:
        if len(functions) > 1:
            raise ValueError("GLM-4 does not support parallel functions.")

        return f"{functions[0].name}\n{functions[0].arguments}"

    @override
    @staticmethod
    def tool_extractor(content: str) -> Union[str, list["FunctionCall"]]:
        if "\n" not in content:
            return content

        tool_name, tool_input = content.split("\n", maxsplit=1)
        try:
            arguments = json.loads(tool_input.strip())
        except json.JSONDecodeError:
            return content

        return [FunctionCall(tool_name, json.dumps(arguments, ensure_ascii=False))]


class Llama3ToolUtils(ToolUtils):
    r"""Llama 3.x tool using template with `tools_in_user_message=False`.

    Reference: https://www.llama.com/docs/model-cards-and-prompt-formats/llama3_1/#json-based-tool-calling
    """

    @override
    @staticmethod
    def tool_formatter(tools: list[dict[str, Any]]) -> str:
        date = datetime.now().strftime("%d %b %Y")
        tool_text = ""
        for tool in tools:
            wrapped_tool = tool if tool.get("type") == "function" else {"type": "function", "function": tool}
            tool_text += json.dumps(wrapped_tool, indent=4, ensure_ascii=False) + "\n\n"

        return LLAMA3_TOOL_PROMPT.format(date=date, tool_text=tool_text)

    @override
    @staticmethod
    def function_formatter(functions: list["FunctionCall"]) -> str:
        function_objects = [{"name": name, "parameters": json.loads(arguments)} for name, arguments in functions]
        return json.dumps(function_objects[0] if len(function_objects) == 1 else function_objects, ensure_ascii=False)

    @override
    @staticmethod
    def tool_extractor(content: str) -> Union[str, list["FunctionCall"]]:
        try:
            tools = json.loads(content.strip())
        except json.JSONDecodeError:
            return content

        tools = [tools] if not isinstance(tools, list) else tools
        try:
            return [FunctionCall(tool["name"], json.dumps(tool["parameters"], ensure_ascii=False)) for tool in tools]
        except KeyError:
            return content


class MistralToolUtils(ToolUtils):
    r"""Mistral v0.3 tool using template."""

    @override
    @staticmethod
    def tool_formatter(tools: list[dict[str, Any]]) -> str:
        wrapped_tools = []
        for tool in tools:
            wrapped_tools.append(tool if tool.get("type") == "function" else {"type": "function", "function": tool})

        return "[AVAILABLE_TOOLS] " + json.dumps(wrapped_tools, ensure_ascii=False) + "[/AVAILABLE_TOOLS]"

    @override
    @staticmethod
    def function_formatter(functions: list["FunctionCall"]) -> str:
        return json.dumps(
            [{"name": name, "arguments": json.loads(arguments)} for name, arguments in functions], ensure_ascii=False
        )

    @override
    @staticmethod
    def tool_extractor(content: str) -> Union[str, list["FunctionCall"]]:
        try:
            tools = json.loads(content.strip())
        except json.JSONDecodeError:
            return content

        tools = [tools] if not isinstance(tools, list) else tools
        try:
            return [FunctionCall(tool["name"], json.dumps(tool["arguments"], ensure_ascii=False)) for tool in tools]
        except KeyError:
            return content


class QwenToolUtils(ToolUtils):
    r"""Qwen 2.5 tool using template."""

    @override
    @staticmethod
    def tool_formatter(tools: list[dict[str, Any]]) -> str:
        tool_text = ""
        for tool in tools:
            wrapped_tool = tool if tool.get("type") == "function" else {"type": "function", "function": tool}
            tool_text += "\n" + json.dumps(wrapped_tool, ensure_ascii=False)

        return QWEN_TOOL_PROMPT.format(tool_text=tool_text)

    @override
    @staticmethod
    def function_formatter(functions: list["FunctionCall"]) -> str:
        function_texts = [
            json.dumps({"name": name, "arguments": json.loads(arguments)}, ensure_ascii=False)
            for name, arguments in functions
        ]
        return "\n".join([f"<tool_call>\n{text}\n</tool_call>" for text in function_texts])

    @override
    @staticmethod
    def tool_extractor(content: str) -> Union[str, list["FunctionCall"]]:
        regex = re.compile(r"<tool_call>(.+?)</tool_call>(?=\s*<tool_call>|\s*$)", re.DOTALL)
        tool_match: list[str] = re.findall(regex, content)
        if not tool_match:
            return content

        results = []
        for tool in tool_match:
            try:
                tool = json.loads(tool.strip())
            except json.JSONDecodeError:
                return content

            if "name" not in tool or "arguments" not in tool:
                return content

            results.append(FunctionCall(tool["name"], json.dumps(tool["arguments"], ensure_ascii=False)))

        return results


class TeacherToolUtils(ToolUtils):
    r"""Teacher tool using template [for my use case]."""

    @override
    @staticmethod
    def tool_formatter(tools: list[dict[str, Any]], tool_policy: ToolPolicy = None) -> str:
        from prompts.agent.teacher_monolithic import TOOL_AVAILABILITY_TEXT, TOOL_USAGE_TEXT, TOOL_CALL_USAGE

        # [1] Create the tool availability text
        # db_text = f"{get_available_databases_from_tools(tools)}"
        if tool_policy is not None:
            tools = ground_tool_availability(tools=tools, policy=tool_policy.tool_availability_policy)
        tool_text = json.dumps(tools, ensure_ascii=False)
        tool_availability_text = TOOL_AVAILABILITY_TEXT.format(tool_text=tool_text)

        # [2] Create the tool usage text
        if tool_policy is not None:
            tool_use_constraints = ground_tool_usage(policy=tool_policy.tool_usage_policy)
        else:
            tool_use_constraints = ''
        slots = {
            "tool_call_usage": TOOL_CALL_USAGE,
            "tool_use_constraints": tool_use_constraints,
        }
        tool_usage_text = TOOL_USAGE_TEXT
        for name, value in slots.items():
            tool_usage_text =  tool_usage_text.replace("{" + name + "}", value, 1)

        final_tool_text = tool_availability_text + "\n\n" + tool_usage_text
        return final_tool_text

    @override
    @staticmethod
    def function_formatter(functions: list["FunctionCall"]) -> str:
        function_texts = []
        for fn in functions:
            name, arguments = fn
            fn_text = f"{json.dumps({'name': name, 'arguments': arguments}, ensure_ascii=False)}"
            function_texts.append(fn_text)

        return "\n".join([f"<Action>{text}</Action>" for text in function_texts])

    @override
    @staticmethod
    def tool_extractor(content: str) -> Union[str, list["FunctionCall"]]:

        # Extract action
        matches = re.findall(r"<Action>(.*?)</Action>", content, re.DOTALL)
        if len(matches) > 1:
            error = "ParallelToolCallingError: Parallel Tool calls found. Only one tool call is supported at a time."
            return error
        elif len(matches) == 0:
            error = "ParsingError: The predicted tool call must be properly enclosed within <Action></Action> tags."
            return error
        else:
            tool = matches[0].strip()
            try:
                tool = json.loads(tool)
                if not isinstance(tool['arguments'], dict):
                    tool['arguments'] = json.loads(tool['arguments'])
            except json.JSONDecodeError:
                error = f"JSONDecodeError: Provided tool specification {tool} is not a valid JSON. Follow the instructions to provide a valid JSON string."
                return error

            # We now have extracted a successful tool call.
            # Following errors are not unique to the agent, but we keep it in agent's tool_extractor
            # because the fn needs to return a valid FunctionCall.
            if "name" not in tool or "arguments" not in tool:
                if "name" not in tool and "arguments" in tool:
                    error = f"MissingKeyError(name): name key is missing from the tool specification."
                    return error
                elif "name" in tool and "arguments" not in tool:
                    error = f"MissingKeyError(arguments): arguments key is missing from the tool specification."
                    return error
                elif "name" not in tool and "arguments" not in tool:
                    error = f"MissingKeysError(name,arguments): name and arguments keys are missing from the tool specification."
                    return error

        return [FunctionCall(tool["name"], json.dumps(tool["arguments"], ensure_ascii=False))]


class StudentMistralToolUtils(ToolUtils):
    r"""Mistral v0.3 tool using template [for my use case]."""

    @override
    @staticmethod
    def tool_formatter(tools: list[dict[str, Any]], tool_policy: ToolPolicy = None) -> str:
        from prompts.agent.student_mistral import TOOL_AVAILABILITY_TEXT, TOOL_USAGE_TEXT, TOOL_CALL_USAGE

        # [1] Create the tool availability text
        if tool_policy is not None:
            tools = ground_tool_availability(tools=tools, policy=tool_policy.tool_availability_policy)
        tool_text = json.dumps(tools, ensure_ascii=False)
        tool_availability_text = TOOL_AVAILABILITY_TEXT.format(tool_text=tool_text)

        # [2] Create the tool usage text
        # [2] Create the tool usage text
        if tool_policy is not None:
            tool_use_constraints = ground_tool_usage(policy=tool_policy.tool_usage_policy)
        else:
            tool_use_constraints = ''
        slots = {
            "tool_call_usage": TOOL_CALL_USAGE,
            "tool_use_constraints": tool_use_constraints,
        }
        tool_usage_text = TOOL_USAGE_TEXT
        for name, value in slots.items():
            tool_usage_text = tool_usage_text.replace("{" + name + "}", value, 1)

        final_tool_text = tool_availability_text + "\n\n" + tool_usage_text
        return final_tool_text

    @override
    @staticmethod
    def function_formatter(functions: list["FunctionCall"]) -> str:
        """All the tool calls (as a list) will be enclosed within a common XML tag"""
        # Since we are allowing thinking before calling tools, we bring [TOOL_CALLS] inside so that it only wraps the tool calls not the thinking part.
        return "[TOOL_CALLS] " + json.dumps(
            [{"name": name, "arguments": json.loads(arguments)} for name, arguments in functions], ensure_ascii=False
        )

    @override
    @staticmethod
    def tool_extractor(content: str) -> Union[str, list["FunctionCall"]]:
        """Inverse of function_formatter"""
        tool = content.strip()
        if tool.startswith('[TOOL_CALLS]'):
            tool = tool[len('[TOOL_CALLS]'):]
            tool = tool.strip()

        # # We don't want to return the parsing error for mistral, [TOOL_CALLS] is a special token which gets removed
        # # when we are decoding using skip_special_tokens=True. We don't want to set it to False since other special
        # # tokens will interfere with the current parsing logic
        # else:
        #     # Improper formatting
        #     error = "ParsingError: The predicted tool call(s) must be preceded by [TOOL_CALLS] tag."
        #     return error
        try:
            tool = json.loads(tool)
            tool = tool[0] if isinstance(tool, list) else tool  # Only one tool call supported in our student agent
            if 'arguments' in tool.keys() and not isinstance(tool['arguments'], dict):
                tool['arguments'] = json.loads(tool['arguments'])
        except json.JSONDecodeError:
            error = f"JSONDecodeError: Provided tool specification {tool} is not a valid JSON. Follow the instructions to provide a valid JSON string."
            return error

        # We now have extracted a successful tool call.
        # Following errors are not unique to the agent, but we keep it in agent's tool_extractor
        # because the fn needs to return a valid FunctionCall.
        if "name" not in tool or "arguments" not in tool:
            if "name" not in tool and "arguments" in tool:
                error = f"MissingKeyError(name): name key is missing from the tool specification."
                return error
            elif "name" in tool and "arguments" not in tool:
                error = f"MissingKeyError(arguments): arguments key is missing from the tool specification."
                return error
            elif "name" not in tool and "arguments" not in tool:
                error = f"MissingKeysError(name,arguments): name and arguments keys are missing from the tool specification."
                return error

        return [FunctionCall(tool["name"], json.dumps(tool["arguments"], ensure_ascii=False))]


class StudentQwenToolUtils(ToolUtils):
    r"""Qwen 2.5 tool using template [for my use case]."""

    @override
    @staticmethod
    def tool_formatter(tools: list[dict[str, Any]], tool_policy: ToolPolicy = None) -> str:
        from prompts.agent.student_qwen import TOOL_AVAILABILITY_TEXT, TOOL_USAGE_TEXT, TOOL_CALL_USAGE

        # [1] Create the tool availability text
        if tool_policy is not None:
            tools = ground_tool_availability(tools=tools, policy=tool_policy.tool_availability_policy)
        tool_text = ""
        for tool in tools:
            tool_text += "\n" + json.dumps(tool, ensure_ascii=False)
        tool_availability_text = TOOL_AVAILABILITY_TEXT.format(tool_text=tool_text)

        # [2] Create the tool usage text
        if tool_policy is not None:
            tool_use_constraints = ground_tool_usage(policy=tool_policy.tool_usage_policy)
        else:
            tool_use_constraints = ''
        slots = {
            "tool_call_usage": TOOL_CALL_USAGE,
            "tool_use_constraints": tool_use_constraints,
        }
        tool_usage_text = TOOL_USAGE_TEXT
        for name, value in slots.items():
            tool_usage_text = tool_usage_text.replace("{" + name + "}", value, 1)

        final_tool_text = tool_availability_text + "\n\n" + tool_usage_text
        return final_tool_text

    @override
    @staticmethod
    def function_formatter(functions: list["FunctionCall"]) -> str:
        """Each independent tool call is enclosed within its own XML tags"""
        function_texts = [
            json.dumps({"name": name, "arguments": json.loads(arguments)}, ensure_ascii=False)
            for name, arguments in functions
        ]
        return "\n".join([f"<tool_call>\n{text}\n</tool_call>" for text in function_texts])

    @override
    @staticmethod
    def tool_extractor(content: str) -> Union[str, list["FunctionCall"]]:
        """Inverse of function_formatter"""
        # regex = re.compile(r"<tool_call>(.+?)</tool_call>(?=\s*<tool_call>|\s*$)", re.DOTALL)
        # tool_match: list[str] = re.findall(regex, content)

        tool_match = re.findall(r"<tool_call>(.*?)</tool_call>", content, re.DOTALL)
        if len(tool_match) > 1:
            error = "ParallelToolCallingError: Parallel Tool calls found. Only one tool call is supported at a time."
            return error
        # # Here we can implement the parsing error because for qwen <tool_call> is not a special token
        elif len(tool_match) == 0:
            error = "ParsingError: The predicted tool call must be properly enclosed within <tool_call></tool_call> tags."
            return error
        else:
            # Read the tool as a json
            tool = tool_match[0].strip()
            try:
                tool = json.loads(tool)
                if 'arguments' in tool.keys() and not isinstance(tool['arguments'], dict):
                    tool['arguments'] = json.loads(tool['arguments'])
            except json.JSONDecodeError:
                error = f"JSONDecodeError: Provided tool specification {tool} is not a valid JSON. Follow the instructions to provide a valid JSON string."
                return error

            # We now have extracted a successful tool call.
            # Following errors are not unique to the agent, but we keep it in agent's tool_extractor
            # because the fn needs to return a valid FunctionCall.
            if "name" not in tool or "arguments" not in tool:
                if "name" not in tool and "arguments" in tool:
                    error = f"MissingKeyError(name): name key is missing from the tool specification."
                    return error
                elif "name" in tool and "arguments" not in tool:
                    error = f"MissingKeyError(arguments): arguments key is missing from the tool specification."
                    return error
                elif "name" not in tool and "arguments" not in tool:
                    error = f"MissingKeysError(name,arguments): name and arguments keys are missing from the tool specification."
                    return error

        return [FunctionCall(tool["name"], json.dumps(tool["arguments"], ensure_ascii=False))]


class StudentGraniteToolUtils(ToolUtils):
    r"""Granite 3.3 tool using template [for my use case]."""

    @override
    @staticmethod
    def tool_formatter(tools: list[dict[str, Any]], tool_policy: ToolPolicy = None) -> str:
        from prompts.agent.student_granite import TOOL_AVAILABILITY_TEXT, TOOL_USAGE_TEXT, TOOL_CALL_USAGE

        # [1] Create the tool availability text
        if tool_policy is not None:
            tools = ground_tool_availability(tools=tools, policy=tool_policy.tool_availability_policy)
        tool_text = json.dumps(tools, ensure_ascii=False)
        tool_availability_text = TOOL_AVAILABILITY_TEXT.format(tool_text=tool_text)

        # [2] Create the tool usage text
        if tool_policy is not None:
            tool_use_constraints = ground_tool_usage(policy=tool_policy.tool_usage_policy)
        else:
            tool_use_constraints = ''
        slots = {
            "tool_call_usage": TOOL_CALL_USAGE,
            "tool_use_constraints": tool_use_constraints,
        }
        tool_usage_text = TOOL_USAGE_TEXT
        for name, value in slots.items():
            tool_usage_text = tool_usage_text.replace("{" + name + "}", value, 1)

        final_tool_text = tool_availability_text + "\n\n" + tool_usage_text
        return final_tool_text

    @override
    @staticmethod
    def function_formatter(functions: list["FunctionCall"]) -> str:
        """All the tool calls (as a list) will be enclosed within a common XML tag"""
        return "<|tool_call|> " + json.dumps(
            [{"name": name, "arguments": json.loads(arguments)} for name, arguments in functions], ensure_ascii=False
        )

    @override
    @staticmethod
    def tool_extractor(content: str) -> Union[str, list["FunctionCall"]]:
        """Inverse of function_formatter"""
        tool = content.strip()
        if tool.startswith('<|tool_call|>'):
            tool = tool[len('<|tool_call|>'):]
            tool = tool.strip()

        # # We don't want to return the parsing error for granite, <|tool_call|> is a special token which gets removed
        # # when we are decoding using skip_special_tokens=True. We don't want to set it to False since other special
        # # tokens will interfere with the current parsing logic
        # else:
        #     # Improper formatting
        #     error = "ParsingError: The predicted tool call(s) must be preceded by <|tool_call|> tag."
        #     return error

        try:
            tool = json.loads(tool)
            tool = tool[0] if isinstance(tool, list) else tool  # Only one tool call supported in our student agent
            if 'arguments' in tool.keys() and not isinstance(tool['arguments'], dict):
                tool['arguments'] = json.loads(tool['arguments'])
        except json.JSONDecodeError:
            error = f"JSONDecodeError: Provided tool specification {tool} is not a valid JSON. Follow the instructions to provide a valid JSON string."
            return error

        # We now have extracted a successful tool call.
        # Following errors are not unique to the agent, but we keep it in agent's tool_extractor
        # because the fn needs to return a valid FunctionCall.
        if "name" not in tool or "arguments" not in tool:
            if "name" not in tool and "arguments" in tool:
                error = f"MissingKeyError(name): name key is missing from the tool specification."
                return error
            elif "name" in tool and "arguments" not in tool:
                error = f"MissingKeyError(arguments): arguments key is missing from the tool specification."
                return error
            elif "name" not in tool and "arguments" not in tool:
                error = f"MissingKeysError(name,arguments): name and arguments keys are missing from the tool specification."
                return error

        return [FunctionCall(tool["name"], json.dumps(tool["arguments"], ensure_ascii=False))]


TOOLS = {
    "default": DefaultToolUtils(),
    "glm4": GLM4ToolUtils(),
    "llama3": Llama3ToolUtils(),
    "mistral": MistralToolUtils(),
    "qwen": QwenToolUtils(),
    "teacher_rits": TeacherToolUtils(),  # My custom
    "student_mistral": StudentMistralToolUtils(),  # My custom
    "student_qwen": StudentQwenToolUtils(),  # My custom
    "student_granite": StudentGraniteToolUtils(),  # My custom
}


def get_tool_utils(name: str) -> "ToolUtils":
    tool_utils = TOOLS.get(name, None)
    if tool_utils is None:
        raise ValueError(f"Tool utils `{name}` not found.")

    return tool_utils
