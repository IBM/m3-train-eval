import copy
import json
import os
import traceback
from collections import defaultdict
from datetime import datetime
import re
from typing import List, Dict, Any, Union

import torch
from transformers import AutoTokenizer, AutoModelForCausalLM, PreTrainedModel, PreTrainedTokenizerBase
from vllm import LLM, SamplingParams
from loguru import logger
from tqdm import tqdm
import argparse

from extras.custom import set_run_environment
from data_utils.utils import Role
from envs.constants import RETRIEVER_FUNCTION_PREFIX
from prompts.agent.system import SYSTEM_PROMPT
from data_utils.template import Template, TEMPLATES
from envs.tool_call_env import M3EvalEnv
from envs.base_env import SubDomain


def load_model(model_name: str) -> tuple[PreTrainedTokenizerBase, PreTrainedModel]:

    logger.info(f"Downloading {model_name} to cache location {os.environ['HF_HOME']}")
    tokenizer = AutoTokenizer.from_pretrained(model_name, trust_remote_code=True)
    # model = AutoModelForCausalLM.from_pretrained(model_name, torch_dtype=torch.bfloat16)
    model = LLM(
            model=model_name,
            tokenizer=model_name,
            tensor_parallel_size=1,
            dtype=torch.float16,
            gpu_memory_utilization=0.9,
            trust_remote_code=True,
        )
    device=torch.device("cpu")

    # # [Optionally] Put the model on cuda
    # if torch.cuda.is_available():
    #     device = torch.device("cuda")
    #     # Check if model is already on a CUDA device
    #     if not next(model.parameters()).is_cuda:
    #         logger.info("CUDA is available. Moving model to CUDA...")
    #         model = model.cuda()
    #     else:
    #         logger.info("Model is already on CUDA.")
    # else:
    #     logger.info("CUDA not available. Model stays on CPU.")

    return tokenizer, model, device

def extract_thought(text: str, tag: str = "think") -> tuple[str | None, str]:
    """
    Extract thoughts
    Returns:
        str | None: The substring found between the tags, or None if no match is found.
    """
    pattern = fr"<{tag}>(.*?)</{tag}>"
    match = re.search(pattern, text, re.DOTALL)
    if match:
        extracted = match.group(1)
        remainder = text[:match.start()] + text[match.end():]
        return extracted, remainder
    else:
        return None, text

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

    thought_words = ['think', 'thought']
    thought = ""
    for word in thought_words:
        found_thought, no_thought_response = extract_thought(response, tag=word)
        if found_thought:
            thought = found_thought
            break
    parsed_response['thought'] = thought
    parsed_response["no_thought_response"] = no_thought_response

    # Extract tool call
    actionic_response: Union[str, list["FunctionCall"]] = agent_template.extract_tool(no_thought_response)

    if isinstance(actionic_response, str):
        if agent_template.format_function.tool_utils.tool_call_start_tag in response:
            # Tried and failed to make a tool call
            parsed_response['error'] = actionic_response
            return parsed_response
        else:
            # Treat this as the final answer
            parsed_response['type'] = "FINAL"
            parsed_response['value'] = no_thought_response
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
        
    if not found_thought:
        # No thought was found. Use the text before the tool call (if there was one), or all of the text (FINAL)
        if parsed_response['type'] == "FINAL":
            parsed_response['thought'] = parsed_response['value']
        else:
            parsed_response['thought'] = response.split(agent_template.format_function.tool_utils.tool_call_start_tag)[0]
    return parsed_response



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
    os.makedirs(save_traj_at, exist_ok=True)

    # ########################################## Configure the Agent ########################################## #

    llm_parameters = {
        "model_name_or_path": config["model_name_or_path"],
        "max_new_tokens": config["max_new_tokens"],
        "temperature": config["temperature"],
        "stop_sequences": [],
    }
    sampling_params = SamplingParams(n=1, temperature=config["temperature"], max_tokens=config["max_new_tokens"])



    # ######################################## Configure the Environment ######################################## #
    agent_template = TEMPLATES[config['agent_template']]
    sub_domain = SubDomain(
        mode='rest',
    )
    scorer_llm_params={
            "model_name_or_path": config['scorer_model_name_or_path'],
            "max_new_tokens": config['scorer_max_new_tokens'],
            "temperature": config['temperature'],
            "stop_sequences": ["User Query"]
        }
    # scorer_tokenizer, scorer_llm, scorer_device = load_model(scorer_llm_params["model_name_or_path"])
    env = M3EvalEnv(
        path_to_env_data=config['path_to_env_data'],
        es_config=config['db_config'],
        api_config=config['api_config'],  # Local end point: "end_point": "http://127.0.0.1:8000",
        horizon=config["horizon"],
        sub_domain=sub_domain,
        agent_template=agent_template,
        scorer_llm=None,
        scorer_llm_tokenizer=None, 
        scorer_llm_parameters=scorer_llm_params,
        scorer_device=None
    )
    tokenizer, model, device = load_model(config["model_name_or_path"])

    # ########################################## Run the Agent ########################################## #
    metrics = defaultdict(int)
    total_runs = len(env) # len(env)
    env_instances_idxs: List[int] = list(range(total_runs))

    for i in tqdm(env_instances_idxs, total=len(env_instances_idxs), desc='Environment instance'):

        if i > 2:
            print("LET'S CUT THIS SHORT!")
            break
        
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
                system_prompt = SYSTEM_PROMPT if env.tool_policy.tool_use_policy is None else SYSTEM_PROMPT + env.tool_policy.tool_use_policy
                formatted_text = tokenizer.apply_chat_template(state, tokenize=False, add_generation_prompt=True, tools=json.loads(env.tools), system=system_prompt)
                
                # # Huggingface
                # inputs = tokenizer(formatted_text, padding=False, return_attention_mask=True, return_tensors='pt')
                # inputs = {k: v.to(device) for k, v in inputs.items()}
                # input_len = len(inputs["input_ids"][0])
                # with torch.no_grad():
                #     generated_ids = model.generate(
                #         input_ids=inputs["input_ids"],
                #         attention_mask=inputs["attention_mask"], 
                #         max_new_tokens=llm_parameters['max_new_tokens'],
                #         do_sample=False)
                # generated_text = tokenizer.decode(generated_ids[0][input_len:], skip_special_tokens=True)
                
                # vllm
                generated_text = model.generate(formatted_text, sampling_params=sampling_params, use_tqdm=False)
                parsed_response = parse_llm_response(generated_text, agent_template)

            except Exception as e:
                logger.error(f"Couldn't process example for env instance {i} within inference to take action task due to error {e}. Skipping!")
                traceback.print_exc()
                next_example_inference=True # At inference we still want to write a failed log
                next_example=True
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
    parser.add_argument('--output_dir', '-o', help="Output directory to save trajectories to", default=".")
    parser.add_argument('--input_filename', '-i', default="./eval_debug_samples.json", help="Input filename.")
    parser.add_argument('--infer_config','-ic', default="config_files/evaluate_tool_calling.json", 
                        help="Config file for model training and evaluation. Default value config_files/evaluate_tool_calling.json")
    args = parser.parse_args()
    run_agent(args)