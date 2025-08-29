import copy
import json
import os
import random
from collections import defaultdict
from datetime import datetime
from typing import List, Dict, Any, Optional

from loguru import logger
from tqdm import tqdm
import argparse

from envs.expert_assist import setup_expert_assist
from agents.loader import get_agent, get_expert
from envs.loader import get_agent_env
from envs.tool_call_env import ERRONEOUS_CATEGORIES
from extras.custom import set_run_environment
from extras.misc import updated_nested_dict


def get_alternate_action_trace(state, env, policy, initiating_actor: str) -> List[dict]:
    logger.info(f"[Alternate Tracing Call] Tasking {initiating_actor} to take the action")

    # 1. Get the action
    _parsed_response, _num_transitions = policy.get_action(state)

    # 2. Get the observation if the alternate action had been taken
    _env_role, _observation, _terminated, _truncated = env.get_observation(_parsed_response)

    # 3. Compute reward
    _reward, _success = env.get_reward(_parsed_response, _observation)  # Call after updating the history

    alternate_interaction = {
        "metadata": {
            "thought": _parsed_response["thought"],
            "action": _parsed_response["type"],
            "action_arguments": _parsed_response["value"],
            "observation": _observation,
        },
        "actor": initiating_actor,
        "output": {
                "role": _parsed_response["role"],
                "content": _parsed_response["template_free_response"],
            },
        "reward": _reward,
    }
    return [alternate_interaction]


def run_agent(args):
    # ########################################## Load the config file ########################################## #
    path_to_config = os.path.join('config_files', 'infer_agent.json')
    with open(path_to_config) as f:
        config = json.load(f)
    logger.info("Loaded the agent run config from {}".format(path_to_config))

    # ############################################## Set up the run ############################################## #
    curr_time = datetime.now()
    curr_time = "_".join(str(curr_time).split(" ")).replace(":", ".")
    if args.output_dir:
        config['log_dir'] = f"{args.output_dir}"
    else:
        config['log_dir'] = f"./logging/{curr_time}"
    os.makedirs(config['log_dir'], exist_ok=True)

    # Set up the os environment and logging
    set_run_environment(dotenv_path=config['path_to_env_vars'], log_dir=config['log_dir'])

    logger.info(json.dumps(config, indent=4))
    save_traj_at = os.path.join(config['log_dir'], 'trajectories')
    os.makedirs(save_traj_at, exist_ok=True)

    # ########################################## Configure the Agent ########################################## #
    stop_sequences = []
    # 1. Add the special tags to indicate that the follow-up content is a tool response -> We don't want the model to generate it
    stop_sequences.extend(["<Observation>", "<tool_response>",
                           "[TOOL_RESULTS]"])  # For Teacher RITS, qwen, mistral (granite does not have any special)
    # 2. Add the special eos tokens to mark completion of the response. Usually available within the tokenizer
    stop_sequences.extend(["<|im_end|>", "</s>", "<|end_of_text|>"])  # qwen, mistral, granite resp.
    logger.info("Using stop sequences {}".format(stop_sequences))
    agent = get_agent(
        model_name_or_path=config['agent_model_name_or_path'],
        max_new_tokens=config['max_new_tokens'],
        temperature=config['temperature'],
        stop_sequences=stop_sequences,
        template_type=config['agent_template'],
        is_hf_agent=config['is_hf_agent'],
        path_to_hf_config=config['path_to_hf_config'],
    )

    # ########################################## Configure the Expert ########################################## #
    expert_agent = get_expert(
        model_name_or_path=config['expert_model_name_or_path'],
        max_new_tokens=config['expert_max_new_tokens'],
        temperature=config['temperature'],
        stop_sequences=["Current Agent Trajectory", "User Query"],
        template_type=config['agent_template'],
    )

    # ###################################### Specify scenario specific data ###################################### #
    # scenario_specific_data = {
    #     'both_api_rag-api_before_rag': {
    #         'path_to_env_data': "./data/bird-dev/test/both_api_rag-api_before_rag.json",
    #         'db_config': {
    #             "index_name": "api-before-rag-dev"
    #         }
    #     },
    #     'both_api_rag-rag_before_api': {
    #         'path_to_env_data': "./data/bird-dev/test/both_api_rag-rag_before_api.json",
    #         'db_config': {
    #             "index_name": "rag-before-api-dev"
    #         }
    #     }
    # }
    # scenario_specific_ckpt = {
    #     'both_api_rag-api_before_rag': {
    #         'skip': False,
    #         'resume_instance': None,
    #         'path_to_prev_run_dir': None,
    #     },
    #     'both_api_rag-rag_before_api': {
    #         'skip': False,
    #         'resume_instance': None,
    #         'path_to_prev_run_dir': None,
    #     }
    # }

    # config = copy.deepcopy(config)
    # config = updated_nested_dict(config, scenario_specific_data[scenario])

    # ######################################## Configure the Environment ######################################## #
    expert_assist = setup_expert_assist(
        mode=config["expert_assist_mode"],
        init_limit=config["expert_assist_init_limit"],
        recent_limit=config["expert_assist_recent_limit"],
        random_epsilon=config["expert_assist_random_epsilon"],
    )
    env = get_agent_env(
        path_to_env_data=config['path_to_env_data'],
        db_config=config['db_config'],
        api_config=config['api_config'],
        expert_assist=expert_assist,
        horizon=config["horizon"],
        template_type=config['agent_template'],
        overseer_llm_params={
            "model_name_or_path": config['overseer_model_name_or_path'],
            "max_new_tokens": config['overseer_max_new_tokens'],
            "temperature": config['temperature'],
            "stop_sequences": ["User Query"]
        },
        scorer_llm_params={
            "model_name_or_path": config['scorer_model_name_or_path'],
            "max_new_tokens": config['scorer_max_new_tokens'],
            "temperature": config['temperature'],
            "stop_sequences": ["User Query"]
        },
        env_subdomain_mode=config['env_subdomain_mode'],
    )

    # ########################################## Run the Agent ########################################## #
    metrics = defaultdict(int)
    total_runs = len(env) # len(env)
    if "include_thoughts" in config and config["include_thoughts"] == False:
        include_thoughts = False
    else:
        include_thoughts = True
    logger.info(f"Thought-inclusion in final trajectories has been set to {str(include_thoughts)}")

    if config['resume_instance'] is not None:
        env_instances_idxs: List[int] = list(range(config['resume_instance'], total_runs))
        # Calculate the metrics
        files = os.listdir(config['path_to_prev_run_dir'])
        metadata_files = [f for f in files if f.endswith('.json') and f.startswith('metadata')]
        metadata_files.sort(reverse=False)
        for m in metadata_files:
            with open(os.path.join(config['path_to_prev_run_dir'], m), 'r') as f:
                metadata = json.load(f)
            metrics["truncated"] += metadata['truncated']
            metrics["terminated"] += metadata['terminated']
            metrics["success"] += metadata['success']
    else:
        env_instances_idxs: List[int] = list(range(total_runs))

    for i in tqdm(env_instances_idxs, total=len(env_instances_idxs), desc='Environment instance'):

        logger.info("="*100)
        logger.info(f"Environment Instantiated ({i})")

        # ######################################## Reset the environment ######################################## #
        try:
            state, reward, done, env_metadata = env.reset(inst_idx=i)
        except Exception as e:
            logger.error(f"Environment Reset Exception for env instance {i}: {e}. Skipping!")
            continue
        expert_agent.initialize(env)
        agent_trajectory = {
            'system': env.system,
            'tools': env.tools,
            'interactions': {},
            'tool_availability_policy': env.tool_policy.tool_availability_policy,
            'tool_usage_policy': env.tool_policy.tool_usage_policy,
            "final_answer_policy": env.tool_policy.final_answer_policy
        }

        if expert_agent.trajectory is None and expert_assist.mode in ['ground_truth', 'random', 'informed']:
            logger.warning(f"No G.T. trajectory available to run agent in mode {expert_assist.mode} for env instance {i}. Skipping!")
            continue

        logger.info("Expert trajectory: \n{}".format(json.dumps(expert_agent.trajectory, indent=2)))
        logger.debug("Init State: \n{}".format(json.dumps(state, indent=2)))

        expert_assistance_tracker = []
        t = 0
        expert_help_needed = False
        while not done:
            logger.info(f"Current time step: {t}")
            alternate_trace = []  # List to save data for alternate actions if taken at the same state
            # ######################################## Take Action ######################################## #
            if expert_assist.mode is None:
                # Only take Agentic Actions
                logger.info("Tasking Agent to take the action")
                parsed_response = agent.take_action(
                                    state=state,
                                    include_thoughts=include_thoughts)
                actor = 'agent'

            elif expert_assist.mode == 'ground_truth' or expert_assist.mode == 'ground_truth_non_live':
                # Only take Expert actions
                logger.info("Tasking Expert to take the action")
                parsed_response = expert_agent.take_action(
                                    state=env.curr_turn_history, 
                                    include_thoughts=include_thoughts)
                actor = 'expert'

            elif expert_assist.mode == 'random':
                # Take the action based on epsilon-greedy
                if random.random() < expert_assist.random_epsilon:
                    # Explore using Agentic Actions
                    logger.info("Tasking Agent to take the action")
                    parsed_response = agent.take_action(
                                        state=state,
                                        include_thoughts=include_thoughts)
                    actor = 'agent'
                else:
                    # Exploit using the expert policy
                    logger.info("Tasking Expert to take the action")
                    parsed_response = expert_agent.take_action(
                                        state=env.curr_turn_history,
                                        include_thoughts=include_thoughts)
                    actor = 'expert'

            elif expert_assist.mode == 'informed':
                if expert_help_needed:
                    logger.info("Tasking Expert to take the action")
                    actor = 'expert'
                    parsed_response = expert_agent.take_action(
                                        state=env.curr_turn_history,
                                        include_thoughts=include_thoughts)
                    branching_state = copy.deepcopy(state)  # State on which branching actor takes action i.e. agent

                else:
                    logger.info("Tasking Agent to take the action")
                    actor = 'agent'
                    parsed_response = agent.take_action(
                                        state=state,
                                        include_thoughts=include_thoughts)
                    branching_state = copy.deepcopy(env.curr_turn_history)  # State on which branching actor takes action i.e. expert

                    # For training with expert-assistance in multi-turn, the final answer at the current turn
                    # should be correct before transitioning to the next turn as it will be used for follow-up
                    # question answering
                    if parsed_response["type"] == "FINAL":
                        logger.info("Agent came up with the final answer. We don't know if it is correct and "
                                    "whether agent has gathered sufficient information to come up with it. Tasking"
                                    "expert instead to take action. For multi-turn, future turn's reasoning must"
                                    "be conditioned on the correct answers to past questions for training purposes.")
                        actor = 'expert'
                        parsed_response = expert_agent.take_action(
                                            state=env.curr_turn_history,
                                            include_thoughts=include_thoughts)
                        branching_state = copy.deepcopy(state)

            else:
                raise NotImplementedError(f"Expert Assist mode not yet implemented for {expert_assist.mode}")
            logger.info(f"(t={t}) Action Data: {json.dumps(parsed_response, indent=2)}")

            if actor == 'agent':
                expert_assistance_tracker.append(0)
            else:
                expert_assistance_tracker.append(1)

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
            state, reward, done, env_metadata = env.step(action=parsed_response)

            curr_observation = state[-1]["content"]
            logger.info(f"(t={t}) Observation: {json.dumps(curr_observation, indent=2)}")

            # Add the observation and reward after stepping through the env.
            curr_interaction["metadata"]["observation"] = curr_observation
            curr_interaction["reward"] = reward  # Here the reward placeholder is added, not the actual value

            # Determine if the agent is stuck. Do this independent of expert_assist.mode
            expert_help_needed = env.is_expert_help_needed(last_action_was_experts=actor=='expert')
            if expert_help_needed:
                # Add the stuck penalty to the last action
                curr_interaction["reward"] += '+{REWARD_AGENT_STUCK}'

            # Get the alternate action trace
            if expert_assist.mode == 'informed':
                if actor == 'expert':
                    # [Trigger Branch out for saving the alternate actionic trace] Expert intervention (except final)
                    if config['save_alternate_trace'] and parsed_response["type"] != "FINAL":
                        alternate_trace.extend(
                            get_alternate_action_trace(branching_state, env, agent, 'agent')
                        )
                else:
                    # [Trigger Branch out for saving the alternate actionic trace] Agent mistake (except final)
                    if config['save_alternate_trace'] and parsed_response["type"] != "FINAL":
                        for error in ERRONEOUS_CATEGORIES:
                            if error in curr_interaction["reward"]:
                                alternate_trace.extend(
                                    get_alternate_action_trace(branching_state, env, expert_agent, 'expert')
                                )
                                break
            curr_interaction["alternate_trace"] = alternate_trace
            # Store the current interaction in the agent trajectory
            agent_trajectory['interactions'][t] = curr_interaction

            t += 1

        # ###################################### Compute metrics/Save Metadata ###################################### #
        metrics["truncated"] += env_metadata['truncated']
        metrics["terminated"] += env_metadata['terminated']
        metrics["success"] += env_metadata['success']

        # Let's store other metadata as well
        agent_metadata = {
            "sample_id": env.curr_sample_idx,
            "truncated": env_metadata['truncated'],
            "terminated": env_metadata['terminated'],
            "success": env_metadata['success'],
            "total_time_steps": t,
            "expert_assistance": {
                "mode": expert_assist.mode,
                "init_limit": expert_assist.init_limit,
                "recent_limit": expert_assist.recent_limit,
                "random_epsilon": expert_assist.random_epsilon,
                "tracker": expert_assistance_tracker
            }
        }

        # Save the Agent trajectory
        with open(os.path.join(save_traj_at, f"trajectory_{i}.json"), "w") as f:
            json.dump(agent_trajectory, f, indent=2)
        # Save its metadata
        with open(os.path.join(config['log_dir'], f"metadata_{i}.json"), "w") as f:
            json.dump(agent_metadata, f, indent=2)

    metrics["total_runs"] = total_runs
    logger.info("Metrics: \n{}".format(json.dumps(metrics, indent=2)))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--output_dir', '-o', help="Output directory to save trajectories to")
    args = parser.parse_args()
    run_agent(args)