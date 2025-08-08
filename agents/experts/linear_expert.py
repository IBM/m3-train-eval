import json
import sys
from typing import List
from typing import Optional, Tuple, Dict, Any

from loguru import logger

from agents.base_agent import Agent
from agents.llm import invoke_llm
from data_utils import Role
from data_utils.template import Template
from data_utils.tool_utils import FunctionCall
from envs.tool_call_env import ToolCallEnv
from envs.constants import RETRIEVE_FUNCTION_NAME
from prompts.utils import get_witness_prompt, get_thought_rewriter_prompt, parse_witness_response, \
    parse_rewriter_response


class LinearExpert(Agent):
    def __init__(
            self,
            tokenizer_id_hf="",
            hf_token="",
            llm=None,
            llm_parameters=None,
            agent_template: "Template" = None
    ):
        """
        Expert has access to G.T. trajectory and expects the agent to follow it sequentially i.e.
        assumes pairwise dependence b/w consecutive actions.


        Implementation Logic:
        Step-by-step look at the information returned by taking expert actions and check if the agent's past interactions
        have already obtained it (with the help of a ObservationWitnessLLM).
        If not, then take that expert action else go to next action. Repeat.
        """
        super().__init__(tokenizer_id_hf=tokenizer_id_hf, hf_token=hf_token, llm=llm, llm_parameters=llm_parameters,
                         agent_template=agent_template)

        self.env: "ToolCallEnv" = None
        self.trajectory: Optional[List[Dict[str, Any]]] = None
        self.global_action_idx: int = -1  # Action idx that traverses multiple-turns
        self.local_action_idx: int = -1  # Action idx that traverses multiple steps/hops within a turn

    def get_trajectory(self) -> Optional[dict]:
        """
        Logic to get the ground truth trajectory specific to parsing of a given benchmark.
        :return: List[dict]   # List of Expert actionic data for every turn
        where, for each turn,
         {
            "actions": List[str]  # single-hop action of type "FINAL", "API", "RETRIEVE", we make a distinction here within type of tool calling for the later two
            "action_arguments": List[dict]
                # arguments dict specific to each action type. If
                #   - FINAL: {"thought": str, "final_answer": str}
                #   - API: {"thought": str, "name": str, "arguments": dict, "response": Any}
                #   - RETRIEVE: {"thought": str, "name": str, "arguments": dict, "response": Any}
                # where, "arguments" is the parameter dict the tool expects
        }
        """
        raise NotImplementedError()

    def initialize(self, env: "ToolCallEnv"):
        self.env = env
        self.trajectory = self.get_trajectory()
        self.global_action_idx = 0
        self.local_action_idx = 0

    def update_thought(self, state, result) -> dict:
        thought_rewriter_prompt = get_thought_rewriter_prompt(
            current_agent_trajectory=state,
            expert_suggested_action={
                "action": result["type"],
                "action_arguments": result["value"],
            },
            original_expert_thought=result["thought"],
        )
        response = invoke_llm(self.llm, self.llm_parameters, thought_rewriter_prompt)
        updated_thought = parse_rewriter_response(response)

        logger.info(f"[External Agent Call] Agent = Thought_Rewriter")
        logger.info(f"Original thought: {result['thought'].strip()}")
        logger.info(f"Updated thought: {updated_thought}")

        orig_thought = result["thought"]
        assert orig_thought in result["response"]
        result['response'] = result['response'].replace(orig_thought, updated_thought)
        assert orig_thought in result["template_free_response"]
        result['template_free_response'] = result['template_free_response'].replace(orig_thought, updated_thought)
        result["thought"] = updated_thought
        return result

    def has_observed_curr_action_response(self, expert_interaction_info: dict, agentic_observations: List[str]) -> bool:
        # Process the observation before adding them to the prompt
        parsed_observations = []
        for obs in agentic_observations:
            if not obs.endswith("."):
                obs = obs + "."
            parsed_observations.append(obs)
        parsed_observations = [f"{i + 1}. {obs}" for i, obs in enumerate(parsed_observations)]
        observations_str = "\n".join(parsed_observations)

        expert_interaction_info = json.dumps(expert_interaction_info, indent=2)

        witness_prompt = get_witness_prompt(
            query=self.env.curr_query,
            expert_interaction_info=expert_interaction_info,
            observations=observations_str,
            use_sample=False
        )
        response = invoke_llm(self.llm, self.llm_parameters, witness_prompt)
        parsed_response = parse_witness_response(response)
        # conclusion = parsed_response["conclusion"]

        logger.info(f"[External Agent Call] Agent = Observational_Witness")
        logger.info(f"Asking Judge LLM whether it has seen the response for the expert action: ({expert_interaction_info}) in the agent's observational history ({', '.join(agentic_observations)}).")
        logger.info(f"Judge LLM said: {json.dumps(parsed_response, indent=2)}")

        if parsed_response["witnessed"]:
            return True
        else:
            return False

    def map_idx_to_action(self, act_idx: int) -> Tuple[dict, str]:
        """Get expert action corresponding to the given action index."""
        parsed_response = dict()

        curr_turn_trajectory = self.trajectory[self.env.curr_turn]
        action_type = curr_turn_trajectory["actions"][act_idx]
        action_arguments = curr_turn_trajectory["action_arguments"][act_idx]
        parsed_response["type"] = action_type

        thought = action_arguments["thought"]
        parsed_response["thought"] = thought

        if action_type == "API":
            tool_call = {
                "name": action_arguments["name"],
                "arguments": action_arguments["arguments"],
            }
            parsed_response["value"] = tool_call
            parsed_response["role"] = Role.FUNCTION.value  # Assistant invokes a tool
            parsed_response["template_free_response"] = f"{self.thought_start}{thought}{self.thought_end}{json.dumps(tool_call, ensure_ascii=False)}"
            parsed_response["response"] = f"{self.thought_start}{thought}{self.thought_end}" + self.agent_template.format_tools.tool_utils.function_formatter(
                [FunctionCall(tool_call["name"], json.dumps(tool_call["arguments"], ensure_ascii=False))]
            )

            observation = action_arguments["response"]  # Result of API Call
        elif action_type == "RETRIEVE":
            tool_call = {
                "name": action_arguments["name"],
                "arguments": action_arguments["arguments"],
            }
            parsed_response["value"] = tool_call
            parsed_response["role"] = Role.FUNCTION.value  # Assistant invokes a tool
            parsed_response["template_free_response"] = f"{self.thought_start}{thought}{self.thought_end}{json.dumps(tool_call, ensure_ascii=False)}"
            parsed_response["response"] = f"{self.thought_start}{thought}{self.thought_end}" + self.agent_template.format_tools.tool_utils.function_formatter(
                [FunctionCall(tool_call["name"], json.dumps(tool_call["arguments"], ensure_ascii=False))]
            )

            observation = action_arguments["response"]  # Documents from the Retrieval
        elif action_type == "FINAL":
            final_answer = action_arguments["final_answer"]
            parsed_response["value"] = final_answer
            parsed_response["role"] = Role.ASSISTANT.value  # Assistant simply replies
            parsed_response["template_free_response"] = f"{self.thought_start}{thought}{self.thought_end}<FINAL>{final_answer}</FINAL>"
            parsed_response["response"] = f"{self.thought_start}{thought}{self.thought_end}<FINAL>{final_answer}</FINAL>"

            observation = 'Done!'
        else:
            raise ValueError(f"Unknown action {action_type}")

        parsed_response['error'] = None
        return parsed_response, observation

    def get_action(self, state) -> Tuple[dict, int]:
        """ This function basically represents the transition and output function of the Finite-State Machine (or Finite-State Transducer)
            Based on the agent's execution trace that acts as the input to the FSM,
                > it transitions to the expert's next internal state
                > generates the corresponding output which is the next expert action

            :param state: History of agent interactions. List of agentic actions, arguments and observations.
                [
                    {
                        "action"
                        "action_arguments"
                        "observation"
                    }
                ]
            :return:
                action: Next Expert Action
                num_transitions: How many transitions to perform from current expert's action to get the next (this is a function of agent's observations)
        """
        num_transitions = 0
        if self.env.expert_assist.mode == "ground_truth":
            # The trajectories are being collected using the expert (no agent presence)
            act_idx = self.get_curr_action_idx()
            action, observation = self.map_idx_to_action(act_idx)
            num_transitions += 1
            # No need to update thought
            return action, num_transitions
        else:
            no_error_state = [item for item in state if 'error' not in item["observation"].lower()]
            if len(no_error_state) == 0:
                # Edge case: This is either the first action requested from the expert or no error-free action was taken by the agent.
                act_idx = self.get_curr_action_idx()
                action, observation = self.map_idx_to_action(act_idx)
                num_transitions += 1
                # No need to update thought
                return action, num_transitions
            else:
                curr_turn_max_expert_actions = len(self.trajectory[self.env.curr_turn]["actions"])
                expert_action_found = False
                act_idx = self.get_curr_action_idx()
                while act_idx < curr_turn_max_expert_actions or not expert_action_found:

                    action, observation = self.map_idx_to_action(act_idx)
                    action_type = action["type"]

                    if action_type == "FINAL":
                        # Expert has reached the point where all its expected actions are present in agent's trajectory
                        # Update thought
                        action = self.update_thought(state, action)
                        num_transitions += 1
                        return action, num_transitions

                    else:
                        # Call ObservationWitnessLLM to determine if the expected response to expert's current action is already observed by the agent.
                        agentic_observations = [item["observation"] for item in no_error_state]
                        expert_interaction_info = {
                            "thought": action["thought"],
                            "action": {
                                "type": action["type"],
                                "name": action["value"]["name"],
                                "arguments": action["value"]["arguments"],
                            },
                            "observation": observation,
                        }
                        has_observed_curr_action_response = self.has_observed_curr_action_response(
                            expert_interaction_info,
                            agentic_observations
                        )

                        act_idx += 1
                        num_transitions += 1

                        if not has_observed_curr_action_response:
                            # Update thought
                            action = self.update_thought(state, action)
                            return action, num_transitions

                logger.error("Reached a point where we looked for the next expert action but can not find any.")
                sys.exit(-1)

    def take_action(self, state, reward: Optional[float] = None) -> Dict[str, Any]:
        """
        :param state: History of agent interactions. List of agentic actions, arguments and observations.
            [
                {
                    "action"
                    "action_arguments"
                    "observation"
                }
            ]
        :param reward:
        :return:
        """
        # Get the expert action
        action, num_transitions = self.get_action(state)
        # Update the expert's internal state
        while num_transitions > 0:
            self.transition_to_next_action()
            num_transitions -= 1
        return action

    def get_curr_action_idx(self) -> int:
        """This function in a way represents the internal state of the Expert when realised as an FSM"""
        # Based on env turn determine the range of global action
        _min = 0
        t = 0
        while t < self.env.curr_turn:
            _min += len(self.trajectory[t]["actions"])
            t += 1
        _max = _min + len(self.trajectory[t]["actions"])

        if self.global_action_idx <= _min:  # This implies that agent has transitioned to the next turn w/o needing expert's assistance
            self.global_action_idx = _min
            self.local_action_idx = 0
        return self.local_action_idx

    def transition_to_next_action(self):
        # Determine the next expert action.
        self.global_action_idx += 1
        self.local_action_idx += 1  # This is what makes this expert linear


class M3Expert(LinearExpert):
    def __init__(
            self,
            tokenizer_id_hf="",
            hf_token="",
            llm=None,
            llm_parameters=None,
            agent_template: "Template" = None
    ):
        super().__init__(tokenizer_id_hf=tokenizer_id_hf, hf_token=hf_token, llm=llm, llm_parameters=llm_parameters,
                         agent_template=agent_template)

    def get_single_turn_trajectory(self, trajectory) -> Dict[str, Any]:
        actions, action_arguments = [], []
        hops = [(trajectory[i], trajectory[i + 1]) for i in range(0, len(trajectory), 2)]  # (s, a)
        for hop_idx, (item_0, item_1) in enumerate(hops):

            # # State is either the user query or the response for the previous action. Parse Action
            if "answer" in item_1.keys():  # i) If action is a Final action
                actions.append("FINAL")
                action_arguments.append(
                    {
                        "thought": "I now know the final answer." if 'plan' not in item_1.keys() else item_1['plan'],
                        "final_answer": item_1['answer'],  # For expert, have it return the wrapped answer always as it is being used for model training
                    }
                )
            else:
                assert "agent" in item_1.keys()
                assert "response" in hops[hop_idx + 1][0].keys()  # The next state should have the response

                if item_1['agent'] == "api_agent":  # ii) If action is an API call

                    actions.append("API")
                    action_arguments.append(
                        {
                            "thought": item_1["plan"],
                            "name": item_1['output']["name"],
                            "arguments": item_1['output']['arguments'],
                            "response": hops[hop_idx + 1][0]['response']
                        }
                    )
                elif item_1['agent'] == "retriever_agent" or item_1[
                    'agent'] == "rag_agent":  # iii) If action is a db retriever call
                    actions.append("RETRIEVE")
                    # If the trajectory contains queries for hops with retrieval call
                    if isinstance(item_1['output'], str):
                        action_arguments.append(
                            {
                                "thought": item_1["plan"],
                                "name": RETRIEVE_FUNCTION_NAME,
                                "arguments": {
                                    "collection": self.env.es_config["index_name"],
                                    "query": item_1['output'],
                                },
                                "response": hops[hop_idx + 1][0]['response']
                            }
                        )
                    # If the trajectory contains tool call as retrieval call for hops
                    elif isinstance(item_1['output'], dict):
                        action_arguments.append(
                            {
                                "thought": item_1["plan"],
                                "name": item_1['output']["name"],
                                "arguments": item_1['output']['arguments'],
                                "response": hops[hop_idx + 1][0]['response']
                            }
                        )
                    else:
                        raise NotImplementedError(f"Unsupported type for output in retrieve agent: {item_1['output']}")

                else:
                    raise ValueError(f"Unknown agent of type {item_1['agent']}")

        expert_traj = {
            "actions": actions,
            "action_arguments": action_arguments,
        }
        return expert_traj

    def get_trajectory(self) -> Optional[List[dict]]:
        # TODO: Adapt this for multi-turn
        curr_instance_data = self.env.data[self.env.curr_instance_idx]

        # Following logic to parse the G.T. Trajectory should work for all the tool availability/usage policies
        if 'trajectory' in curr_instance_data:
            traj_data = curr_instance_data['trajectory']
        else:
            return None

        # Parse
        if self.env.sub_domain.mode == "rest":

            # For Multi-turn data
            if isinstance(traj_data[0], list):
                trajectory = [self.get_single_turn_trajectory(curr_turn_traj) for curr_turn_traj in traj_data]

            else:
                trajectory = [self.get_single_turn_trajectory(traj_data)]
        # elif self.env.sub_domain.mode in ['selection', 'slot_filling']:
        #     num_steps = len(trajectory)
        #     assert 'input' in trajectory[0].keys()  # First step must have the user query
        #     i = 1
        #     while i < num_steps:
        #         step = trajectory[i]
        #
        #         if "answer" in step.keys():
        #             actions.append("FINAL")
        #             action_arguments.append(
        #                 {
        #                     "thought": "I now know the final answer." if 'plan' not in step.keys() else step['plan'],
        #                     "final_answer": step['answer'],
        #                 }
        #             )
        #             i += 1
        #         elif 'agent' in step.keys() and step['agent'] == "rag_agent":
        #             actions.append("RETRIEVE")
        #             _args = {
        #                     "thought": step["plan"],
        #                     "database_name": database_name,
        #                     "retrieval_query": step['question'],
        #             }
        #             # The next step `must` be the response from the env with the retrieved doc
        #             assert 'response' in trajectory[i+1].keys()
        #             _args['response'] = trajectory[i+1]['response']
        #             action_arguments.append(_args)
        #             i += 2
        #         elif 'agent' in step.keys() and step['agent'] == "api_agent":
        #             actions.append("API")
        #             _args = {
        #                 "thought": step["plan"],
        #                 "name": step['output']["name"],
        #                 "arguments": step['output']['arguments'],
        #             }
        #             # The next step `could` be the response of calling the API or another API call if no response is expected. FIXME: Each API call should have the corresponding response.
        #             if 'response' in trajectory[i+1].keys():
        #                 _args['response'] = trajectory[i+1]['response']
        #                 i += 2
        #             else:
        #                 # Add a place_holder response that action ran successfully
        #                 _args['response'] = f"Action = {_args['name']} executed successfully."
        #                 i += 1
        #             action_arguments.append(_args)
        #         else:
        #             raise ValueError(f"Implemented Parsing not valid for the step {step}")
        #
        #     expert_traj = {
        #         "actions": actions,
        #         "action_arguments": action_arguments,
        #     }
        else:
            raise NotImplementedError(f"Parsing G.T. trajectory logic not implemented for {self.env}")


        return trajectory