import copy
import json
import os
import random
import re
import sys
from typing import List, Optional, Tuple

from dotenv import load_dotenv
from loguru import logger
from tqdm import tqdm

from agents.llm import invoke_llm, get_lm
from metrics.plot import plot_freq_dist


def get_step_query_prompt(step_idx: int, sub_question: str, tool_description: str, tool_call: str,
                          tool_response: str) -> str:
    from prompt_thought_generator import QUERY_STEP_PROMPT as step_prompt

    # First replace the step number
    step_prompt = step_prompt.replace("{number}", str(step_idx))
    step_prompt = step_prompt.replace("{sub_question}", sub_question, 1)
    step_prompt = step_prompt.replace("{tool_description}", tool_description, 1)
    step_prompt = step_prompt.replace("{tool_call}", tool_call, 1)
    step_prompt = step_prompt.replace("{tool_response}", tool_response, 1)
    return step_prompt


def get_thought_generator_prompt(previous_dialogue: List[Tuple[str, str]], user_query: str, step_prompts: List[str]) -> \
List[dict]:
    from prompt_thought_generator import SYSTEM_PROMPT, QUERY_PROMPT
    prompt = []
    system_prompt = SYSTEM_PROMPT
    prompt.append(
        {
            "role": "system",
            "content": system_prompt,
        }
    )
    if len(previous_dialogue) > 0:
        for q, a in previous_dialogue:
            prompt.extend(
                [
                    {
                        "role": "user",
                        "content": q,
                    },
                    {
                        "role": "assistant",
                        "content": a if isinstance(a, str) else json.dumps(a),
                    },
                ]
            )

    # Form the prompt for current user turn
    query_prompt = QUERY_PROMPT.format(user_query=user_query)
    query_prompt: str = query_prompt + "\n" + "\n".join(step_prompts)
    prompt.append(
        {
            "role": "user",
            "content": query_prompt,
        }
    )
    return prompt


def get_answer_generator_prompt(user_query: str, trajectory: str, additional_instruction: str = '') -> List[dict]:
    from prompt_final_answer import SYSTEM_PROMPT, QUERY_PROMPT
    prompt = []
    system_prompt = SYSTEM_PROMPT
    system_prompt = system_prompt.replace("{additional_instruction}", additional_instruction, 1)
    prompt.append(
        {
            "role": "system",
            "content": system_prompt,
        }
    )

    query_prompt = QUERY_PROMPT.format(user_query=user_query, agent_trajectory=trajectory)
    prompt.append(
        {
            "role": "user",
            "content": query_prompt,
        }
    )
    return prompt


def parse_thought_generator_response(response: str, num_steps: int) -> Optional[dict]:
    for step_idx in range(num_steps):
        try:
            assert f"thought_{step_idx + 1}" in response.lower()
        except AssertionError:
            logger.error(f"    Thought not found for step {step_idx + 1}")
            return None

    pattern = re.compile(r"Thought_(\d+):\s*(.*?)(?=\nThought_\d+:|$)", re.DOTALL)
    matches = pattern.findall(response)

    result = {}
    for step_str, thought in matches:
        step = int(step_str)
        if 1 <= step <= num_steps:
            result[step - 1] = thought.strip()
    return result


def parse_answer_generator_response(response: str, ) -> Optional[dict]:
    thought_match = re.search(r"<think>(.*?)</think>", response, re.DOTALL)
    final_match = re.search(r"<FINAL>(.*?)</FINAL>", response, re.DOTALL)

    if not thought_match or not final_match:
        logger.error(f"    Thought/Answer not found")
        return None

    return {
        "thought": thought_match.group(1).strip(),
        "answer": final_match.group(1).strip()
    }


def wrap_hop_data_into_trajectory_str(hops) -> str:
    trajectory_str = ""
    for hop_idx, (item_0, item_1) in enumerate(hops):
        trajectory_str += (f"Agent:\n"
                           f"    <think>{item_0['plan']}</think>\n"
                           f"    <tool_call>{json.dumps(item_0['output'], indent=2)}</tool_call>\n"
                           f"Environment:\n"
                           f"    <tool_response>{item_1['response']}</tool_response>\n")
    return trajectory_str


def create_and_inject_thoughts(
        parsed_data_dir: str,
        _save_data_at: str,
        model_name_or_path: str,
        range_tool_resp_cut_off: Tuple[int, int] = (5, 10),
        max_tool_resp_cut_off: int = 1024,
) -> dict:
    """Must run after parsing raw multi-turn data.

       TODO: Inject scenarios here and control thought and final answers. Not implemented Yet!

       ** How Final answers are generated? **
       Final answer is generated based on final tool's response, where the idea is to wrap the return structured object
       from the tool call into a English sentence. However, sometimes the tool can return many items as a list.
       In such cases, we prompt the final answer generator to randomly pick some and construct the surrounding answer.
       For every sample, we save the resp_cutoff_thresh used, the corresponding resp_cutoff_inst, the wrapped final
       answer and also the raw response as a json string.

       ** Instructions for Environment/training pipeline **
       > Task the model to always generate the wrapped final answer.
       > But condition on the raw response of past turns if it has been compressed for reasoning in future turns.
        (need all entities to reason over)


    :param parsed_data_dir:
    :param _save_data_at:
    :param model_name_or_path:
    :param range_tool_resp_cut_off: range within which a tool's response is brought to in the final answer
    :param max_tool_resp_cut_off: max value beyond which if tool's response exists, the sample is discarded
    :return: training data statistics as a dictionary
    """

    llm_parameters = {
        "model_name_or_path": model_name_or_path,
        "max_new_tokens": 4096,
        "temperature": 0.0,
        "stop_sequences": ["User Query"],
    }
    llm = get_lm(model_name_or_path, parameters=llm_parameters)

    domain_files = os.listdir(parsed_data_dir)
    domain_files = [f for f in domain_files if f.endswith('.json')]

    training_data_stats = {
        'num_domains': len(domain_files),
        'train_samples_per_domain': [],
        'total_samples_per_domain': [],
        'train_samples': 0,
        'total_samples': 0
    }

    for domain_file in domain_files:
        with open(os.path.join(parsed_data_dir, domain_file)) as f:
            domain_data = json.load(f)

        logger.info(f"\n# ===================================== {domain_file} ===================================== #")
        final_domain_data, left_out_domain_data = [], []  # Segregate chosen and rejected (in orig format) samples
        for sample in tqdm(domain_data, total=len(domain_data), desc=f"Generating thoughts for domain {domain_file}"):
            is_valid_sample = True
            orig_sample = copy.deepcopy(sample)

            sample_id = sample['sample_id']
            logger.info(f"    Generating thoughts and final answer for Sample #{sample_id}")

            # # For each sample declare scenarios here
            tool_availability_policy = "both_api_rag"  # We support 'only_rag', 'only_api', 'both_api_rag', 'neither_api_rag'
            tool_usage_policy = ""  # This should be the instruction in english to control which tools need to be used
            sample['tool_availability_policy']: str = tool_availability_policy
            sample['tool_usage_policy']: str = tool_usage_policy

            # # Spawn on-the-go additional instr. to compress tool response into the final answer. This will go into the
            # # agentic system prompt to be used for all turns (only during generation not during conditioning on context-response pairs)
            # Spawn a random integer between min to max
            resp_cutoff_thresh = random.randint(range_tool_resp_cut_off[0], range_tool_resp_cut_off[1])
            from envs.constants import COMPRESS_TOOL_RESPONSE_INSTRUCTION
            answer_generator_additional_instr = COMPRESS_TOOL_RESPONSE_INSTRUCTION.format(
                curr_resp_cutoff=resp_cutoff_thresh)
            sample['resp_cutoff_thresh']: int = resp_cutoff_thresh
            sample['resp_cutoff_inst']: str = answer_generator_additional_instr

            previous_dialogue = []
            turns = []  # For storing turn-level query-answer pairs
            for turn_idx, curr_turn_trajectory in enumerate(sample['trajectory']):
                curr_user_query: str = curr_turn_trajectory[0]['input']
                curr_raw_answer = copy.deepcopy(curr_turn_trajectory[-1]['answer'])
                logger.info(f"\nQuery [turn #{turn_idx}]: {curr_user_query}")
                logger.info(f"Final Raw Response [turn #{turn_idx}]: {curr_raw_answer}")

                if isinstance(curr_raw_answer, list) and len(curr_raw_answer) > resp_cutoff_thresh:
                    # Ignore samples for which tool responses are tool long
                    if len(curr_raw_answer) > max_tool_resp_cut_off:
                        logger.info(
                            f"    Discarding the sample since length of its tool response at end of turn #{turn_idx} {len(curr_raw_answer)} > {max_tool_resp_cut_off}")
                        is_valid_sample = False
                        break

                else:
                    # For the current turn's thought generation, we discard the instruction if the agent can pick all
                    # the objects and construct the final answer around it. (No Compression/Truncation)
                    resp_cutoff_thresh = None
                    answer_generator_additional_instr = ''

                # Collect hop-level data
                current_turn_tool_call_trajectory = curr_turn_trajectory[1:-1]  # Exclude user query and final answer
                hops = [(current_turn_tool_call_trajectory[i], current_turn_tool_call_trajectory[i + 1]) for i in
                        range(0, len(current_turn_tool_call_trajectory), 2)]

                # Create step-level information for the thought-generator prompt
                step_prompts = []
                for hop_idx, (item_0, item_1) in enumerate(hops):

                    # 1. Create the tool-call-str
                    tool_call_str = "\n" + json.dumps(item_0['output'], indent=2) if isinstance(item_0['output'],
                                                                                                dict) else item_0[
                        'output']
                    prefix = "            "  # constant indentation from the prompt
                    tool_call_str = "\n".join(prefix + line for line in tool_call_str.splitlines())
                    logger.info(f"        \nTool Call {hop_idx + 1}: {tool_call_str}")

                    # 2. Find and create the matching tool-description-str
                    curr_tool_name: str = item_0['output']['name']
                    tool_description_str = ''
                    for tool in sample['tools']:
                        if curr_tool_name == tool['name']:
                            tool_description_str = json.dumps(tool, indent=2)
                            break
                    assert len(tool_description_str) > 0  # Check to make sure the used tool is in tool universe
                    prefix = "            "  # constant indentation from the prompt
                    tool_description_str = "\n".join(prefix + line for line in tool_description_str.splitlines())

                    # 3. Create the tool-response-str
                    tool_response_str = item_1['response']

                    curr_step_prompt: str = get_step_query_prompt(
                        hop_idx + 1,
                        item_0['question'],
                        tool_description_str,
                        tool_call_str,
                        tool_response_str,
                    )
                    step_prompts.append(curr_step_prompt)

                # ############################ Generate reasoning for tool calls ############################ #
                thought_generator_prompt = get_thought_generator_prompt(
                    previous_dialogue=previous_dialogue,
                    user_query=curr_user_query,
                    step_prompts=step_prompts,
                )
                response = invoke_llm(llm, llm_parameters, thought_generator_prompt)
                logger.info(f"\nThought generator response [turn #{turn_idx}]:\n{response}")
                parsed_response = parse_thought_generator_response(response, len(hops))
                if parsed_response is None:
                    is_valid_sample = False
                    logger.info(
                        f"    Parsing error (intermediate thoughts) for sample {sample['sample_id']} failed. Ignoring!")
                    break

                # Fill in the thought
                for hop_idx, (item_0, item_1) in enumerate(hops):
                    curr_step_thought: str = parsed_response[hop_idx]
                    item_0['plan'] = curr_step_thought

                # # ############################ Generate final answer and its thought ############################ #
                # TODO: Inject the previous_dialogue if the final answer of the current turn depends on it
                trajectory_str = wrap_hop_data_into_trajectory_str(hops)
                answer_generator_prompt = get_answer_generator_prompt(
                    user_query=curr_user_query,
                    trajectory=trajectory_str,
                    additional_instruction=answer_generator_additional_instr
                )
                response = invoke_llm(llm, llm_parameters, answer_generator_prompt)
                logger.info(f"\nFinal answer generator response[turn #{turn_idx}]:\n{response}")
                parsed_response = parse_answer_generator_response(response)
                if parsed_response is None:
                    is_valid_sample = False
                    logger.info(f"    Parsing error (final answer) for sample {sample['sample_id']} failed. Ignoring!")
                    break

                # # Fill in the answer
                curr_turn_trajectory[-1]['plan'] = parsed_response['thought']
                curr_turn_trajectory[-1]['answer'] = parsed_response['answer']
                curr_turn_trajectory[-1]['raw_answer'] = json.dumps(curr_raw_answer)
                turns.append(
                    {
                        'query': curr_user_query,
                        'answer': parsed_response['answer'],
                        'raw_answer': json.dumps(curr_raw_answer),
                        'was_raw_answer_compress': True if resp_cutoff_thresh is not None else False,
                        # Could be Useful to determine turn-level final answer match rewards
                    }
                )

                # M3 Data considers the response from the final tool call as the final answer.
                previous_dialogue.append(
                    (curr_user_query, curr_raw_answer)
                )

            sample['turns'] = turns
            if not is_valid_sample:
                left_out_domain_data.append(orig_sample)
            else:
                final_domain_data.append(sample)

        # Update the statistics with domain's
        training_data_stats['total_samples'] += len(domain_data)
        training_data_stats['total_samples_per_domain'].append(len(domain_data))
        training_data_stats['train_samples_per_domain'].append(len(final_domain_data))
        training_data_stats['train_samples'] += len(final_domain_data)

        # Save the new data
        with open(os.path.join(_save_data_at, domain_file.replace(".json", "_final.json")), 'w') as f:
            json.dump(final_domain_data, f, indent=4)

        # Save the data that got left out
        with open(os.path.join(_save_data_at, domain_file.replace(".json", "_left_out.json")), 'w') as f:
            json.dump(left_out_domain_data, f, indent=4)

    return training_data_stats


def create_multi_turn_data(raw_data_dir, save_data_at, plot_dir):
    """This function simply parses the raw data. Do not inject scenarios here!"""

    if not os.path.exists(plot_dir):
        os.makedirs(plot_dir, exist_ok=True)

    domain_files = os.listdir(raw_data_dir)
    domain_files = [f for f in domain_files if f.endswith('.json')]

    final_tool_response_dist = dict()
    total_samples = 0
    for domain_file in domain_files:
        with open(os.path.join(raw_data_dir, domain_file)) as f:
            domain_data = json.load(f)

        logger.info(f"\n# =================== {domain_file} =================== #")
        final_tool_response_dist_domain = dict()
        rejected_samples_no_tools, rejected_turns_no_tool_call, rejected_turns_non_unique_query, retrieve_type_samples = 0, 0, 0, 0
        unique_queries, parsed_data = [], []
        for sample in tqdm(domain_data, total=len(domain_data),
                           desc=f"Creating turn-level data for domain {domain_file}"):
            sample_id = sample['sample_id']
            logger.info(f"Creating turn level data sample #{sample_id}")

            # If there is a retrieve document tool call for this sample
            if 'RAG' in sample['type']:
                retrieve_type_samples += 1

            # Get the available tools. Will add retrieval tool later in M3-train
            if 'tools' in sample:
                tools = sample['tools']
                # Remove the path from each tool
                for tool in tools:
                    if 'path' in tool:
                        del tool['path']
            else:
                logger.info(
                    f"    Ignoring current sample. No tools available for sample {sample_id} in file {domain_file}")
                rejected_samples_no_tools += 1
                continue

            # Get the available document collections and add the retriever tool to the tools list
            doc_collections = list(set(sample['retrievers']))  # To avoid repeats
            # TODO: Add the description for each collection here as a metadata field in the below

            from envs.constants import RETRIEVE_FUNCTION_NAME
            retriever_tool = {
                "name": RETRIEVE_FUNCTION_NAME,
                "description": "Retrieve document(s) from the collection that best matches the query.",
                "parameters": {
                    "type": "object",
                    "required": ["collection", "query"],
                    "properties": {
                        "collection": {
                            "description": "Name of the collection to retrieve documents from.",
                            "type": "string",
                            "enum": doc_collections
                        },
                        "query": {
                            "description": "Query for retrieving the document(s).",
                            "type": "string"
                        }
                    }
                }
            }
            tools.append(retriever_tool)

            parsed_sample = {
                'sample_id': f"{sample_id}",
                'tools': tools,
                'doc_collections': doc_collections,
                'num_turns': sample['num_turns'],
                'num_hops': sample['num_hops'],
                'type': sample['type'],
                'trajectory': [],
            }

            is_valid_sample: bool = True
            for turn_idx, turn in enumerate(sample['turns']):

                # ################################ Create trajectory (per turn) ################################ #

                # Initialisation
                user_query = turn['query']

                # # [Old Logic]
                # if user_query not in unique_queries:
                #     unique_queries.append(user_query)
                # else:
                #     rejected_turns_non_unique_query += 1
                #     logger.info(f"    Ignoring current turn. Duplicate query {user_query} in turn {turn_idx}")
                #     continue

                # Add n_0 = user query
                single_turn_trajectory = [
                    {
                        'input': user_query,
                    }
                ]

                # Get the current turn's (golden) tool calls and responses at hop-level
                turn_gold_seq = turn['gold_sequence']
                for hop_idx, hop in enumerate(turn_gold_seq):

                    # 1. Get the question at hop-level
                    hop_question = hop['question']

                    # 2. Get the tool call and tool response
                    if hop['question_type'] == "API":
                        hop_agent = 'api_agent'  # We define this to adhere to M3-Train's parsing logic
                        if 'output' in hop:
                            if len(hop['output']) == 1:
                                hop_tool_call = hop['output'][0]
                            else:
                                raise ValueError("Multiple outputs not supported at hop level!")
                        else:
                            logger.info(
                                f"    Ignoring current turn. No output(tool-call) present at hop {hop_idx} in turn {turn_idx} for sample {sample_id} in file {domain_file}!")
                            is_valid_sample = False
                            rejected_turns_no_tool_call += 1
                            break
                        hop_response = hop[
                            'OUTPUT_AFTER_EXECUTING_API']  # We use this field for tool response as it is json structured compared to hop['answer']

                    else:
                        hop_agent = 'rag_agent'
                        from envs.constants import RETRIEVE_FUNCTION_NAME
                        # For a document retrieval, we define the function
                        collection = 'clapnq-' + hop['db_id']
                        try:
                            assert collection in doc_collections
                        except AssertionError:
                            logger.error(
                                f"    Collection {collection} not found for sample {sample_id} in available collections: {doc_collections}")
                            raise AssertionError(
                                f"Collection {collection} not found in available collections: {doc_collections}")

                        hop_tool_call = {
                            'name': RETRIEVE_FUNCTION_NAME,
                            'arguments': {
                                'collection': collection,
                                'query': hop_question,
                            }
                        }
                        if isinstance(hop['rag_doc'], list) and len(hop['rag_doc']) > 1:
                            logger.info(
                                "    Ground-truth response contains multiple documents. Combining them into one.")
                            hop_response = "\n".join(hop['rag_doc'])
                        else:
                            assert isinstance(hop['rag_doc'], str)
                            hop_response = hop['rag_doc']

                    # Add n_t = tool call
                    single_turn_trajectory.append(
                        {
                            'plan': None,
                            'question': hop_question,
                            'agent': hop_agent,
                            'output': hop_tool_call
                        }
                    )
                    single_turn_trajectory.append(  # Add n_t+1 = tool call response
                        {
                            'response': hop_response,
                        }
                    )

                # Add n_T = final answer
                single_turn_trajectory.append(
                    {
                        'plan': None,
                        'answer': turn['answer'],
                        # For now use what is in the turn's json object, later generate this using LLM
                    }
                )

                if not is_valid_sample:
                    # Discard the current sample
                    break

                # Add turn level datum to the trajectory
                parsed_sample['trajectory'].append(single_turn_trajectory)

                len_answer = len(turn['answer']) if isinstance(turn['answer'], list) else 1
                if str(len_answer) not in final_tool_response_dist:
                    final_tool_response_dist[str(len_answer)] = 1
                else:
                    final_tool_response_dist[str(len_answer)] += 1

                if str(len_answer) not in final_tool_response_dist_domain:
                    final_tool_response_dist_domain[str(len_answer)] = 1
                else:
                    final_tool_response_dist_domain[str(len_answer)] += 1

            # Add the parsed sample
            if is_valid_sample:
                parsed_data.append(parsed_sample)

        logger.info(f"====================== x =====================")
        logger.info(f"[Metrics] Total samples: {len(domain_data)}")
        logger.info(f"[Metrics] Total samples with retrieve tool call: {retrieve_type_samples}")
        logger.info(f"[Metrics] Number of rejected samples because of no tools available: {rejected_samples_no_tools}")
        # logger.info(f"[Metrics] Number of rejected turns because of non-unique queries: {rejected_turns_non_unique_query}")
        logger.info(
            f"[Metrics] Number of rejected turns because of no tool call present: {rejected_turns_no_tool_call}")
        logger.info(f"[Metrics] Total number of parsed samples at domain-level: {len(parsed_data)}")
        logger.info(f"====================== x =====================")

        total_samples += len(parsed_data)
        # Save current domain's parsed data as json
        with open(os.path.join(save_data_at, domain_file), 'w') as f:
            json.dump(parsed_data, f, indent=4)

        # [Auxiliary] Plot the length of final tool responses per domain
        final_tool_response_dist_domain = dict(
            sorted(final_tool_response_dist_domain.items(), key=lambda x: int(x[0]), reverse=False))
        plot_freq_dist(final_tool_response_dist_domain,
                       os.path.join(plot_dir, f"final_tool_response_dist_{domain_file.split('.')[0]}.png"))
        with open(os.path.join(plot_dir, f"final_tool_response_len_dist_{domain_file.split('.')[0]}.json"), 'w') as f:
            json.dump(final_tool_response_dist_domain, f, indent=4)

    # [Auxiliary] Plot the length of final tool responses across domains
    final_tool_response_dist = dict(sorted(final_tool_response_dist.items(), key=lambda x: int(x[0]), reverse=False))
    plot_freq_dist(final_tool_response_dist, os.path.join(plot_dir, 'final_tool_response_dist.png'))
    with open(os.path.join(plot_dir, 'final_tool_response_len_dist.json'), 'w') as f:
        json.dump(final_tool_response_dist, f, indent=4)

    logger.info(f"\n[Overall Metrics] Total number of parsed samples: {total_samples}")


if __name__ == "__main__":
    cwd = os.path.dirname(os.path.abspath(__file__))

    # # CCC Paths
    # _raw_data_dir = '/proj/m3benchmark/bird-train/multi_turn/train_rest_v8_0730'
    # _save_parsed_data_at = '/proj/m3benchmark/bird-train/multi_turn/train_parsed'
    # _plot_dir = os.path.join(cwd, 'bird/plots')
    # dotenv_path=os.path.join(cwd, "../.env")

    # # Local Paths
    _raw_data_dir = os.path.join(cwd, '../../raw-data/bird-train/multi_turn/train_rest_v6_0730')
    _log_dir = os.path.join(cwd, 'bird')
    _save_parsed_data_at = os.path.join(cwd, 'bird/parsed')
    _save_final_data_at = os.path.join(cwd, 'bird/final')
    dotenv_path = os.path.join(cwd, "../.env")

    load_dotenv(dotenv_path=os.path.join(cwd, "../.env"))

    logger.remove()
    logger.add(sys.stdout, colorize=True, level="INFO", enqueue=True)
    logger.add(os.path.join(_log_dir, 'logs_{time}.log'), level="INFO", enqueue=True)

    # # 1. Parse the raw data
    # create_multi_turn_data(_raw_data_dir, _save_parsed_data_at, os.path.join(_log_dir, 'plots') )

    # # 2. Create the final training data
    _model_name_or_path = "mistralai/mixtral-8x22B-instruct-v0.1"
    _created_training_data_stats = create_and_inject_thoughts(_save_parsed_data_at, _save_final_data_at,
                                                              _model_name_or_path)
    logger.info(json.dumps(_created_training_data_stats, indent=4))
    with open(os.path.join(_log_dir, 'final_training_data_stats.json'), 'w') as f:
        json.dump(_created_training_data_stats, f, indent=4)
