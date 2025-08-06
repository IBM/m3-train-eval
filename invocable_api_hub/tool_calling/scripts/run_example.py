
import argparse
from collections import Counter
import json
import os

from invocable_api_hub.tool_calling.sql_to_api.sql_dataset_builder import SqlDatasetBuilder
from invocable_api_hub.tool_calling.sql_to_api.sql_sequencing_dataset_builder import SqlSequencingDatasetBuilder
from invocable_api_hub.tool_calling.sql_to_api.sql_slot_filling_dataset_builder import SqlSlotFillingDatasetBuilder
from invocable_api_hub.tool_calling.sql_to_api.utils import validate_api_output, check_equality_without_order

"""

Usage: 

PYTHONPATH=. python invocable_api_hub/tool_calling/scripts/run_example.py --source_file sql_translation_output/dev/sequencing_bird_superhero.json -d superhero --database_directory db/dev_databases

Commmand line params: 
    - source_file: input data
    - database_directory: location of database files
    - d: dataset name

"""

def setup(input_file: str, db_path: str):
    # cache_path is where a temporary copy of the db will be located
    # This is where we write temp tables from joins, and with cleaned table/column names
    # Currently this path needs to be an absolute path. 
    cache_path = os.path.join(db_path, 'cache')
    os.makedirs(cache_path, exist_ok=True)

    if not os.path.isabs(cache_path):
        cache_path = os.path.join(os.getcwd(), cache_path)

    # assert os.path.isfile(input_file), f"Missing file {input_file}"
    filename = os.path.basename(input_file)

    if "_sparc_" in filename:
        source = "sparc"
        database_name = filename.split("_sparc_")[1].replace(".json", "")
    elif "_bird_" in filename:
        source = "bird"
        database_name = filename.split("_bird_")[1].replace(".json", "")
    else:
        raise Exception(f"File has bad source label {filename}")

    if filename.startswith("sequencing"):
        builder = SqlSequencingDatasetBuilder(database_name, db_path, cache_path, source_dataset_name=source)
    elif filename.startswith("slot_filling"):
        builder = SqlSlotFillingDatasetBuilder(database_name, db_path, cache_path, source_dataset_name=source)
    else:
        raise Exception(f"File has bad prefix {filename}")
    builder.build()

    return builder, cache_path


def inference_call(payload: dict, prompt_template: str, builder: SqlDatasetBuilder, runnable):

    api_pool, _ = builder.set_query_specific_api_pool(
            [payload["initialization_step"]]
        )
    
    API_name_list = []
    api_descriptions = {}

    # Format the tool specifications part of the prompt
    tools_info = payload["tools"]
    for tool_spec in tools_info:
        tool_name = tool_spec["name"]
        api_descriptions[tool_name] = json.dumps(tool_spec)
        API_name_list.append(tool_name)

    # Get the extra data describing the valid key_names parameters (column names)
    key_names_and_descriptions = payload['key_values_and_descriptions']
    key_names_and_desc_str = '\n'.join([k['key_name'] + ": " + k['description'] for k in key_names_and_descriptions])
    key_names_str = ', '.join([k['key_name'] for k in key_names_and_descriptions])
    
    # Fill in the prompt template
    prompt_str = prompt_template.format(
        tools=api_descriptions,
        tool_names="\n".join(API_name_list),
        input=payload["input"],
        previousruns="",
        agent_scratchpad="",
        key_enum=key_names_and_desc_str,
        key_names=key_names_str, 
    )
    conversation = [
        {"content": "you are a helpful assistant", "role": "system"},
        {"content": prompt_str, "role": "user"},
    ]

    agent_trajectory = []
    
    first_tao_step = {}
    # Run the initialization step by hand. If running a tao loop, fill in the inital loop with the data point init function like so: 
    first_tao_step["action"] = payload["initialization_step"]["name"]
    first_tao_step["action_input"] = payload["initialization_step"][
        "arguments"
    ]
    first_tao_step['model_response'] = f"Thought: I need to get the data first, Action: {first_tao_step['action']}, Action Input: {first_tao_step['action_input']}"
    agent_trajectory.append(first_tao_step)

    for i in range(5):  # Loop over tao iterations
        # Stub
        # response = runnable.invoke(conversation)
        response = {}

        # The model response should include a choice of api name ("action") and the associated parameters ("action_input")
        # the chosen api must be available in api_pool
        chosen_api = api_pool.get(response.get("action"), None)
        if chosen_api is None:
            observation = ""  # Stub for now, but chosen_api must be in the api_pool to be a valid choice
        else:
            observation = chosen_api(**response.get("action_input"))
        response["API_response"] = observation

        agent_trajectory.append(response)
    return agent_trajectory


def evaluate_win_rate(payloads: list[dict], builder: SqlDatasetBuilder):

    unmatched_lengths = 0
    unmatched_intents = 0
    unmatched_slots = 0
    failed_execution = 0
    unknown_error = 0
    valid = []
    for idx, p in enumerate(payloads):
        if idx % 5 == 0:
            print("Payload number: ", idx)

        # Set the database path to the cache file of the initialized builder. 
        # Otherwise it will point to the cache location from the data generation run. 
        p['initialization_step']['arguments']['database_path'] = builder.loader.cache_file
        p['initialization_step']['label'] = 'starting_table_var'  # 'var_0'
        try:
            for pred in p['predicted_output']:
                if 'name' not in pred:
                    pred['name'] = ''
                if 'arguments' not in pred:
                    pred['arguments'] = {}
                if 'label' not in pred:
                    pred['label'] = pred['name']
        except:
            unknown_error += 1
            valid.append(False)
            continue
        # If we used the original output sequence here, instead of the model output, it would just check the correctness of the data point
        required_api_calls = [p['initialization_step']]
        required_api_calls.extend(p['predicted_output'])  # ie. change 'model_output' to just 'output', and the win_rate should be 1.0

        try:
            if len(p['predicted_output']) != len(p['output']):
                unmatched_lengths += 1
            elif Counter([pred['name'] for pred in p['predicted_output']]) != Counter([o['name'] for o in p['output']]):
                unmatched_intents += 1
            else:
                found_unmatched_slot = False
                for pred, o in zip(p['predicted_output'], p['output']):
                    for k, v in o['arguments'].items():
                        if pred['arguments'].get(k) != v:
                            unmatched_slots += 1
                            found_unmatched_slot = True
                            break
                    if found_unmatched_slot:
                        break
                if not found_unmatched_slot:
                    unknown_error += 1
        except Exception as e:
            unknown_error += 1
            print(f"There was an error counting error stats: {e}")
            print("PREDICTIONS = ")
            print(p['predicted_output'])


        # This dictionary has [key, value] == [tool_name, tool (executable python function)]
        # It is needed for evaluating the win rate, it is NOT consumed by the tool calling model
        api_pool, _ = builder.set_query_specific_api_pool([p['initialization_step']])
        try:
            api_result = validate_api_output(required_api_calls, api_pool)
            validated = check_equality_without_order(api_result, p['gold_answer'])
        except:
            failed_execution += 1
            validated = False
        # try:
        #     api_result = validate_api_output(required_api_calls, api_pool)
        #     validated = check_equality_without_order(api_result, p['gold_answer'])
        # except:
        #     try:
        #         required_api_calls[0]['label']= "var_1"
        #         api_result = validate_api_output(required_api_calls, api_pool)
        #         validated = check_equality_without_order(api_result, p['gold_answer'])
        #     except:
        #         validated = False
        valid.append(validated)
    builder.cleanup_temp_files()
    
    win_rate = sum(valid) / len(valid)
    result = {
        'win_rate': win_rate, 
        'unmatched_lengths': unmatched_lengths, 
        'unmatched_intents': unmatched_intents, 
        'unmatched_slots': unmatched_slots,
        'failed_execution': failed_execution, 
        'unknown_errors': unknown_error,
        'total_data_points': len(payloads)
    }
    return result



if __name__ == "__main__":
    data_files = []
    data_filenames = []
    parse_dir = "~/ai4ba/tool-response-reflection/parsed_agent_output_v2"
    for _, _, files in os.walk(parse_dir):
        for f in files:
            print(f)
            data_filenames.append(f)
            with open(os.path.join(parse_dir, f)) as infile:
                d = json.load(infile)
            data_files.append(d)

    for filename, payloads in zip(data_filenames, data_files):
        db_path = "~/ai4ba/invocable-api-hub/db/dev_databases/"

        # if os.path.exists("win_rate_calculations.json"):
        #     print("Loading win rates")
        #     with open("win_rate_calculations.json") as f:
        #         win_rate_logs = json.load(f)
        # else:
        #     print("Initializing win rates")
        #     win_rate_logs = []

        clean_filename = os.path.basename(filename).split("_agent_")[0]
        model_name = filename.split("_model-")[1].replace(".json", "")
        if filename.startswith("sequencing"):
            style = "sequencing"
        elif filename.startswith("slot_filling"):
            style = "slot_filling"
        else:
            raise Exception
        
        dataset = clean_filename.split("_bird_")[1]
        if dataset != 'superhero' or model_name != "mixtral-22b-parsed" or style != "slot_filling":
            print("skipping: ", clean_filename, model_name)
            continue
        tag = style + "-" + dataset + "-" + model_name
        # if tag in [r['tag'] for r in win_rate_logs]:
        #     print("already have: ", tag, ", skipping")
        #     continue

        full_file = os.path.join(parse_dir, clean_filename)
        builder, cache_file = setup(full_file, db_path)

        for p in payloads:
            p['initialization_step']['arguments']['database_path'] = os.path.join(cache_file, dataset+".sqlite")

        win_rate = evaluate_win_rate(payloads, builder)
        print()
        print("=================================================")
        print({'dataset': dataset, "model": model_name, "style": style, "win_rate": win_rate, "tag": tag})
        print("=================================================")
        print()
        # win_rate_logs.append({'dataset': dataset, "model": model_name, "style": style, "win_rate": win_rate, "tag": tag})
        # with open("win_rate_calculations.json", "w") as f:
        #     json.dump(win_rate_logs, f)
        print(f"FILE: {filename}, WIN RATE = ", win_rate)
