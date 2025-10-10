import copy
import json
import os
import traceback
from collections import defaultdict
from datetime import datetime
from typing import List, Dict, Any, Union

from loguru import logger
from tqdm import tqdm
import argparse

from envs.loader import get_agent_env
from extras.custom import set_run_environment
from agents.llm import invoke_llm, get_lm, get_lm_hf
from data_utils.utils import Role
from openai import OpenAI
from envs.constants import RETRIEVER_FUNCTION_PREFIX
from data_utils.template import Template, TEMPLATES


def parse_llm_response(response: str, agent_template: Template) -> Dict[str, Any]:
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
        "template_free_response": response,  # Default the templatized response to the llm's response
        "response": response,  # Original response including thought and action
        "error": None,
    }

    # Extract tool call
    actionic_response: Union[str, list["FunctionCall"]] = agent_template.extract_tool(response)

    if isinstance(actionic_response, str):
        if 'error' in actionic_response.split(":")[0].lower():  # Error
            parsed_response['error'] = actionic_response
            return parsed_response
        else:
            raise NotImplementedError(f"Parsing of actionic response is not implemented: {actionic_response}.")
    else:
        function_call = actionic_response[0]
        name, arguments = function_call
        
        # # Check: The predicted action should be one of API or RETRIEVE
        if name.strip().startswith(RETRIEVER_FUNCTION_PREFIX):
            parsed_response['type'] = "RETRIEVE"
        else:
            parsed_response['type'] = "API"
        parsed_response['value'] = {"name": name, "arguments": json.loads(arguments)}
        parsed_response[
            'role'] = Role.FUNCTION.value  # With a successful tool extraction, we designate the Function role

        # Update the template_free_response to remove the agent template's specific tokens for tool calling
        template_free_response = f"{agent_template.thought_words[0]}{thought}{agent_template.thought_words[1]}"  # Add the thought
        template_free_response += json.dumps(parsed_response['value'])  # Add the tool call
        parsed_response['template_free_response'] = template_free_response

    return parsed_response

def get_action(llm, llm_parameters, state, agent_template):
    # For OpenAI typed llms, we only use two roles - system, user and assistant. For others, add the special tokens
    if isinstance(llm, OpenAI):
        reformatted_state = []
        for message in state:
            if message['role'] in [Role.SYSTEM.value, Role.USER.value, Role.ASSISTANT.value]:
                reformatted_state.append(message)
            else:
                if message['role'] == Role.OBSERVATION.value:
                    reformatted_state.append(
                        {
                            'role': Role.USER.value,
                            'content': agent_template.format_observation.apply(content=message["content"])[0]
                        }
                    )
                elif message['role'] == Role.FUNCTION.value:
                    reformatted_state.append(
                        {
                            'role': Role.ASSISTANT.value,
                            'content': agent_template.format_function.apply(content=message["content"])[0]
                        }
                    )
        state = reformatted_state

    response = invoke_llm(llm, llm_parameters, state)
    action = parse_llm_response(response)
    return action



def run_agent(args):
    # ########################################## Load the config file ########################################## #
    path_to_config = args.infer_config
    with open(path_to_config) as f:
        config = json.load(f)
    logger.info("Loaded the agent run config from {}".format(path_to_config))

    # ########################################## Load tool config ########################################## #
    path_to_tool_config = os.path.join('config_files', 'setup_tools.json')
    with open(path_to_tool_config) as f:
        config.update(json.load(f))
    logger.info("Loaded the tools config from {}".format(path_to_tool_config))

    # ############################################## Set up the run ############################################## #
    curr_time = datetime.now()
    curr_time = "_".join(str(curr_time).split(" ")).replace(":", ".")
    if args.output_dir:
        config['log_dir'] = f"{args.output_dir}"
    else:
        config['log_dir'] = f"./logging/{curr_time}"
    os.makedirs(config['log_dir'], exist_ok=True)

    if args.input_filename:
        config["path_to_env_data"] = args.input_filename

    # Set up the os environment and logging
    set_run_environment(dotenv_path=config['path_to_env_vars'], log_dir=config['log_dir'])

    logger.info(json.dumps(config, indent=4))
    save_traj_at = os.path.join(config['log_dir'], 'trajectories')
    save_metadata_at = os.path.join(config['log_dir'], 'metadata')
    os.makedirs(save_traj_at, exist_ok=True)
    os.makedirs(save_metadata_at, exist_ok=True)

    # ########################################## Configure the Agent ########################################## #

    llm_parameters = {
        "model_name_or_path": config["model_name_or_path"],
        "max_new_tokens": config["max_new_tokens"],
        "temperature": config["temperature"],
        "stop_sequences": [],
    }
    if not config["is_hf_agent"]:
        llm = get_lm(model_id=llm_parameters["model_name_or_path"], parameters=llm_parameters)
    else:
        with open(config["path_to_hf_config"], 'r') as f:
            hf_config = json.load(f)

        # We will load this into the hf config (so that we declare these vars in one place only)
        hf_config["model_name_or_path"] = config["model_name_or_path"]
        hf_config['template'] = config['agent_template']
        hf_config["max_new_tokens"] = config["max_new_tokens"]
        hf_config["temperature"] = config["temperature"]

        llm = get_lm_hf(hf_config=hf_config)



    # ######################################## Configure the Environment ######################################## #
    agent_template = TEMPLATES[config['agent_template']]
    import pdb; pdb.set_trace()
    env = get_agent_env(
        mode="evaluate",
        path_to_env_data=config['path_to_env_data'],
        db_config=config['db_config'],
        api_config=config['api_config'],
        horizon=config["horizon"],
        scorer_llm_params={
            "model_name_or_path": config['scorer_model_name_or_path'],
            "max_new_tokens": config['scorer_max_new_tokens'],
            "temperature": config['temperature'],
            "stop_sequences": ["User Query"]
        },
        env_subdomain_mode='rest',
    )

    # ########################################## Run the Agent ########################################## #
    metrics = defaultdict(int)
    total_runs = len(env) # len(env)
    env_instances_idxs: List[int] = list(range(total_runs))

    for i in tqdm(env_instances_idxs, total=len(env_instances_idxs), desc='Environment instance'):
        
        logger.info("="*100)
        logger.info(f"Environment Instantiated ({i})")
        
        # ######################################## Reset the environment ######################################## #
        try:
            state, reward, done, env_metadata = env.reset(inst_idx=i)
        except Exception as e:
            logger.error(f"Environment Reset Exception for env instance {i}: {e}. Skipping!")
            traceback.print_exc()
            continue

        agent_trajectory = {
            'guid': env.guid,
            'system': env.system,
            'domain': env.domain,
            'num_turns':env.num_turns,
            'num_hops':env.num_hops,
            'type':env.type,
            'sample_id': env.sample_id,
            'tools': env.tools,
            'interactions': {},
            'scenarios': env.scenarios,
            'tool_availability_policy': env.tool_policy.tool_availability_policy,
            'tool_usage_policy': env.tool_policy.tool_usage_policy,
            "final_answer_policy": env.tool_policy.final_answer_policy
        }


        logger.debug("Init State: \n{}".format(json.dumps(state, indent=2)))

        t = 0
        next_example=False
        next_example_inference=False       
        while not done:
            logger.info(f"Current time step: {t}")
            # ######################################## Take Action ######################################## #
            # Only take Agentic Actions
            logger.info("Tasking Agent to take the action")
            try:
                parsed_response = parse_llm_response(llm, llm_parameters, state, agent_template)

                actor = 'agent'
            except Exception as e:
                logger.error(f"Couldn't process example for env instance {i} within inference to take action task due to error {e}. Skipping!")
                traceback.print_exc()
                next_example_inference=True # At inference we still want to write a failed log
                break

            logger.info(f"(t={t}) Action Data: {json.dumps(parsed_response, indent=2)}")

            if next_example:
                break # Break out of the while loop because informed mode is erroring out. 

            # # Trajectory contains the state-action pairs for training/evaluation
            # - Store the state w/o system prompt.
            # - The input should be stored before calling the step fn since in multi-turn setting, once the final answer
            #   is generated, current state is reinit. to the summarised state after transitioning to next turn
            curr_interaction: Dict[str, Any] = {
                "input": copy.deepcopy(state[1:]),
                "metadata": {
                    "thought": parsed_response["thought"],
                    "action": parsed_response["type"],
                    "action_arguments": parsed_response["value"],
                },
                "actor": actor,  # Add the actor
                "output": {
                    "role": parsed_response["role"],
                    "content": parsed_response["template_free_response"],
                }
            }

            # #################################### Step through the environment #################################### #
            state, reward, done, env_metadata = env.step(action=parsed_response) # Generally shouldn't fail as it is just getting the API or retrieval response.

            curr_observation = state[-1]["content"]
            logger.info(f"(t={t}) Observation: {json.dumps(curr_observation, indent=2)}")

            # Add the observation and reward after stepping through the env.
            curr_interaction["metadata"]["observation"] = curr_observation
            curr_interaction["reward"] = reward  # Here the reward placeholder is added, not the actual value

            if next_example:
                break # Break out of the while loop because we couldn't verigy is agent is stuck.

            # Store the current interaction in the agent trajectory
            agent_trajectory['interactions'][t] = curr_interaction

            t += 1
        
        # Continue to the next example in for loop.
        if next_example:
            continue

        # ###################################### Compute metrics/Save Metadata ###################################### #
        if not next_example_inference:
            metrics["truncated"] += env_metadata['truncated']
            metrics["terminated"] += env_metadata['terminated']
            metrics["success"] += env_metadata['success']
            # Let's store other metadata as well
            agent_metadata = {
                "sample_id": env.sample_id,
                "domain": env.domain,
                "truncated": env_metadata['truncated'],
                "terminated": env_metadata['terminated'],
                "success": env_metadata['success'],
                "total_time_steps": t,
            }
        else:
            metrics["terminated"] += 1
            # Let's store other metadata as well
            agent_metadata = {
                "sample_id": env.sample_id,
                "domain": env.domain,
                "truncated": False,
                "terminated": True,
                "success": False,
                "total_time_steps": t,
            }
        agent_trajectory["metadata"] = agent_metadata

        # Save the Agent trajectory
        with open(os.path.join(save_traj_at, f"trajectory_{env.domain}_{env.sample_id}.json"), "w") as f:
            json.dump(agent_trajectory, f, indent=2)

    metrics["total_runs"] = total_runs
    logger.info("Metrics: \n{}".format(json.dumps(metrics, indent=2)))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--output_dir', '-o', help="Output directory to save trajectories to")
    parser.add_argument('--input_filename', '-i', default="data/soccer_2016_multiturn_bird_chunked_final.json", help="Input filename.")
    parser.add_argument('--infer_config','-ic', default="config_files/evaluate_tool_calling.json", 
                        help="Config file for model training and evaluation. Default value config_files/evaluate_tool_calling.json")
    args = parser.parse_args()
    run_agent(args)