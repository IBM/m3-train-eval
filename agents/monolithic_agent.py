import json
import re
from typing import Optional, Dict, Any, Union

from agents.base_agent import Agent
from agents.llm import invoke_llm
from data_utils.template import Template
from data_utils.tool_utils import FunctionCall
from data_utils.utils import Role
from envs.constants import RETRIEVE_FUNCTION_NAME
from openai import OpenAI


class Monolithic(Agent):

    def __init__(
            self,
            llm = None,
            llm_parameters = None,
            tokenizer_id_hf = "",
            hf_token = "",
            agent_template: "Template" = None
    ):
        super().__init__(llm=llm, llm_parameters=llm_parameters, tokenizer_id_hf=tokenizer_id_hf, hf_token=hf_token,
                         agent_template=agent_template)

    def parse_llm_response(self, response: str, prompt_type: str = "") -> Dict[str, Any]:
        """Parsing of the response from an LLM Agent is unique to each Agent (we use Agent's template).
        Corresponding error analysis from parsing is kept here instead of the environment.
        :param response:
        :param prompt_type:
        :return: parsed_response
        """
        parsed_response = {
            "role": Role.ASSISTANT.value,  # Default
            "type": None,
            "value": None,  # Dict or a string
            "thought": None,
            "template_free_response": response,  # Default the templatized response to the llm's response
            "response": response,  # Original response including thought and action
            "error": None,
        }

        # Extract thought
        matches = self.agent_template.extract_thoughts(response)
        if len(matches) == 1:
            thought = matches[0].strip()
            parsed_response['thought'] = thought
        else:
            if len(matches) == 0:
                error = (f"NoThoughtError: You did not think before taking the next action. "
                         f"Enclose your thought process within the <think></think> tags.")
            else:
                error = (f"MultipleThoughtsError: You thought multiple times before taking the next action. "
                         f"Enclose only one thought process within the <think></think> tags.")
            parsed_response['error'] = error
            return parsed_response

        # Remove thought
        no_thought_response: str = self.agent_template.remove_thought(response)

        # Check if it is the final answer
        matches = re.findall(r"<FINAL>(.*?)</FINAL>", no_thought_response, re.DOTALL)
        if len(matches):
            parsed_response['type'] = "FINAL"
            parsed_response['value'] = matches[0].strip()
            # For final action, template free response is the same as actual response. No update for these fields
            return parsed_response

        # Extract tool call
        actionic_response: Union[str, list["FunctionCall"]] = self.agent_template.extract_tool(no_thought_response)

        if isinstance(actionic_response, str):
            if 'error' in actionic_response.split(":")[0].lower():  # Error
                parsed_response['error'] = actionic_response
                return parsed_response
            else:
                raise NotImplementedError(f"Parsing of actionic response is not implemented: {actionic_response}.")
        else:
            function_call = actionic_response[0]
            name, arguments = function_call
            if name == RETRIEVE_FUNCTION_NAME:
                parsed_response['type'] = "RETRIEVE"
            else:
                parsed_response['type'] = "API"
            parsed_response['value'] = {"name": name, "arguments": json.loads(arguments)}
            parsed_response['role'] = Role.FUNCTION.value  # With a successful tool extraction, we designate the Function role

            # Update the template_free_response to remove the agent template's specific tokens for tool calling
            template_free_response = f"{self.agent_template.thought_words[0]}{thought}{self.agent_template.thought_words[1]}"  # Add the thought
            template_free_response += json.dumps(parsed_response['value'])  # Add the tool call
            parsed_response['template_free_response'] = template_free_response

        return parsed_response

        # thought = None
        # action = None
        # action_arguments = None
        # error = None  # Define such that the word 'Error' must be present in the error msg
        # # role = Role.ASSISTANT.value  # Default is the assistant's response  # TODO: this should be returned by invoke_llm
        #
        # # Extract thought
        # matches = re.findall(f"{self.thought_start}(.*?){self.thought_end}", response, re.DOTALL)
        # if len(matches) == 1:
        #     thought = matches[0].strip()
        # elif len(matches) > 1:
        #     error = (f"MultipleThoughtError: You thought multiple times before taking the next action. "
        #              f"Enclose only one thought process within the {self.thought_start}{self.thought_end} tags.")
        #     return {
        #         "response": response,
        #         "thought": thought,
        #         "action": action,
        #         "action_arguments": action_arguments,
        #         "error": error,
        #         # "role": role,
        #     }
        # else:
        #     error = (f"NoThoughtError: You did not think before taking the next action. "
        #              f"Enclose your thought process within the {self.thought_start}{self.thought_end} tags.")
        #     return {
        #         "response": response,
        #         "thought": thought,
        #         "action": action,
        #         "action_arguments": action_arguments,
        #         "error": error,
        #         # "role": role,
        #     }
        #
        # # Extract action
        # if '<Action>' in response and '</Action>' in response:
        #
        #     # Check: Only one predicted action
        #     if response.count('<Action>') > 1 or response.count('</Action>') > 1:
        #         error = "MultiplePredictedActionError: Predict only one action and wait for the user response."
        #
        #     else:
        #         actionic_response = response.split('<Action>')[-1].split('</Action>')[0].strip()
        #
        #         # The predicted action should be one of
        #         if actionic_response.startswith('<API>') and actionic_response.endswith('</API>'):
        #             action = "API"
        #             # role = Role.FUNCTION.value  # The assistant tried to invoke tool/func
        #
        #             # Get the API name and arguments
        #             action_arguments = actionic_response.split('<API>')[-1].split('</API>')[0].strip()
        #             action_arguments = action_arguments.replace("\\n", "")
        #             action_arguments = action_arguments.replace("\n", "")
        #             action_arguments = action_arguments.replace("\\", "")
        #             action_arguments = action_arguments.strip()
        #
        #             if len(action_arguments):
        #                 try:
        #                     # Parse the arguments dictionary
        #                     action_arguments = json.loads(action_arguments)
        #                 except json.decoder.JSONDecodeError:
        #                     error = f"JSONDecodeError: Provided API specification {action_arguments} is not a valid JSON. Follow the instructions to provide a valid JSON string."
        #                 else:  # Executed if no exception occurred while parsing
        #                     # Check for correct keys present in the dictionary
        #                     if "name" not in action_arguments or "arguments" not in action_arguments:
        #
        #                         if "name" not in action_arguments and "arguments" in action_arguments:
        #                             error = "MissingKeyError(name): API Name is missing from the API specification."
        #                             action_arguments = None
        #                         elif "name" in action_arguments and "arguments" not in action_arguments:
        #                             error = "MissingKeyError(arguments): API Arguments are missing from the API specification."
        #                             action_arguments = None
        #                         elif "name" not in action_arguments and "arguments" not in action_arguments:
        #                             error = "MissingKeysError(name,arguments): API Name and Arguments are missing from the API specification."
        #                             action_arguments = None
        #             else:
        #                 # This should only be reachable when API does not expect any arguments
        #                 error = "NoAPISpecificationError: API specification is not provided. Follow the instructions to provide a valid JSON string."
        #
        #         elif actionic_response.startswith('<RETRIEVE>') and actionic_response.endswith('</RETRIEVE>'):
        #             action = "RETRIEVE"
        #             action_arguments =  actionic_response.split('<RETRIEVE>')[-1].split('</RETRIEVE>')[0].strip() # Get the retrieval query
        #             # role = Role.FUNCTION.value  # The assistant tried to invoke tool/func
        #
        #             # Do this when retrieval call is a tool call
        #             action_arguments = action_arguments.replace("\\n", "")
        #             action_arguments = action_arguments.replace("\n", "")
        #             action_arguments = action_arguments.replace("\\", "")
        #             action_arguments = action_arguments.strip()
        #
        #             if len(action_arguments):
        #                 try:
        #                     # Parse the arguments dictionary
        #                     action_arguments = json.loads(action_arguments)
        #                 except json.decoder.JSONDecodeError:
        #                     error = f"JSONDecodeError: Provided RETRIEVE specification {action_arguments} is not a valid JSON. Follow the instructions to provide a valid JSON string."
        #
        #                 # Check for correct keys present in the dictionary
        #                 if "name" not in action_arguments or "arguments" not in action_arguments:
        #
        #                     if "name" not in action_arguments and "arguments" in action_arguments:
        #                         error = "MissingKeyError(name): Database Name is missing from the RETRIEVE specification."
        #                         action_arguments = None
        #                     elif "name" in action_arguments and "arguments" not in action_arguments:
        #                         error = "MissingKeyError(arguments): RETRIEVE Arguments are missing from the RETRIEVE specification."
        #                         action_arguments = None
        #                     elif "name" not in action_arguments and "arguments" not in action_arguments:
        #                         error = "MissingKeysError(name, arguments): Database Name and Arguments are missing from the RETRIEVE specification."
        #                         action_arguments = None
        #
        #                 else:
        #                     # We only expect the retrieval query as the parameter
        #                     if 'query' not in action_arguments["arguments"]:
        #                         error = "MissingKeyError(query): Retrieval query is missing from the RETRIEVE's arguments."
        #                         action_arguments = None
        #
        #             else:
        #                 # This should only be reachable when RETRIEVE does not expect any arguments
        #                 error = "NoRETRIEVESpecificationError: RETRIEVE specification is not provided. Follow the instructions to provide a valid JSON string."
        #
        #         elif actionic_response.startswith('<FINAL>') and actionic_response.endswith('</FINAL>'):
        #             action = "FINAL"
        #             action_arguments = actionic_response.split('<FINAL>')[-1].split('</FINAL>')[0].strip() # Get the final answer
        #
        #         # Check: Incorrect action
        #         else:
        #             error = "InvalidPredictedActionError: Invalid action. Predicted action must be one of <API> or <RETRIEVE> or <FINAL>."
        #
        # else:
        #     error = "ParsingError: The predicted action must be properly enclosed within <Action></Action> tags."
        #
        # return {
        #     "response": response,  # Original response including thought and action
        #     "thought": thought,
        #     "action": action,
        #     "action_arguments": action_arguments,  # Dict or a string
        #     "error": error,
        #     # "role": role,
        # }

    def get_action(self, state, include_thoughts):
        # For OpenAI typed llms, we only use two roles - system, user and assistant. For others, add the special tokens
        if isinstance(self.llm, OpenAI):
            reformatted_state = []
            for message in state:
                if message['role'] in [Role.SYSTEM.value, Role.USER.value, Role.ASSISTANT.value]:
                    reformatted_state.append(message)
                else:
                    if message['role'] == Role.OBSERVATION.value:
                        reformatted_state.append(
                            {
                                'role': Role.USER.value,
                                'content': self.agent_template.format_observation.apply(content=message["content"])[0]
                            }
                        )
                    elif message['role'] == Role.FUNCTION.value:
                        reformatted_state.append(
                            {
                                'role': Role.ASSISTANT.value,
                                'content': self.agent_template.format_function.apply(content=message["content"])[0]
                            }
                        )
            state = reformatted_state

        response = invoke_llm(self.llm, self.llm_parameters, state)
        action = self.parse_llm_response(response)
        return action, None

    def take_action(self, state, include_thoughts: bool=True, reward: Optional[float] = None) -> Dict[str, Any]:
        action, num_transitions = self.get_action(state, include_thoughts)
        return action

    def tracked_action(self, state, reward: Optional[float] = None) -> Dict:
        result = self.take_action(state, reward)
        return {"output_text": result["response"], "output_meta": {}, "tracking_result": result}