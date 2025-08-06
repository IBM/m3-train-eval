
from copy import deepcopy
import json
import os

from invocable_api_hub.tool_calling.sql_to_api.sql_dataset_builder import SqlDatasetBuilder
from invocable_api_hub.tool_calling.sql_to_api.sql_sequencing_dataset_builder import SqlSequencingDatasetBuilder
from invocable_api_hub.tool_calling.sql_to_api.sql_slot_filling_dataset_builder import SqlSlotFillingDatasetBuilder
from invocable_api_hub.tool_calling.sql_to_api.utils import validate_api_output, check_equality_without_order
from invocable_api_hub.tool_calling.tools.docstring_parsing_utils import translate_data_type

        


def setup(input_file: str, db_path: str):
    # cache_path is where a temporary copy of the db will be located
    # This is where we write temp tables from joins, and with cleaned table/column names
    # This path needs to be correct. 
    cache_path = os.path.join(db_path, 'cache')

    assert os.path.isfile(input_file), f"Missing file {input_file}"
    filename = os.path.basename(input_file)

    if "_sparc_" in filename:
        source = "sparc"
        database_name = filename.split("_sparc_")[1].replace(".json", "")
    elif "_bird_" in filename:
        source = "bird"
        database_name = filename.split("_bird_")[1].replace(".json", "")
    else:
        raise Exception("Bad file")
    cache_file = os.path.join(cache_path, database_name + ".sqlite")

    if filename.startswith("sequencing"):
        builder = SqlSequencingDatasetBuilder(database_name, db_path, cache_path, source_dataset_name=source)
    elif filename.startswith("slot_filling"):
        builder = SqlSlotFillingDatasetBuilder(database_name, db_path, cache_path, source_dataset_name=source)
    else:
        raise Exception("Bad file")
    builder.build()

    return builder, cache_file


def generate_dataset_payload(input_file: str, db_path: str):

    builder, cache_file = setup(input_file, db_path)

    with open(input_file) as f:
        data = json.load(f)['data']

    payloads = []
    for data_point in data:
        payload = generate_single_query_payload(data_point, builder, cache_file)
        payloads.append(payload)

    return payloads



def generate_single_query_payload(data_point: dict, builder: SqlDatasetBuilder, cache_file: str) -> dict:
    payload = {
        'input': data_point['input'], 
        'output': data_point['output'][1:], # Skipping the first step, this is only needed when we invoke
        'gold_answer': data_point['gold_answer'],
        'original_output': data_point['output'],
        'sample_id': data_point['sample_id'],
        'dataset_name': data_point['dataset_name'], 
        'query': data_point['query']

    }
    init_step = data_point['output'][0]
    init_step['arguments']['database_path'] = cache_file
    payload['initialization_step'] = init_step

    tools = []
    for t in data_point['tools']:
        if t['name'] == 'initialize_active_data':
            continue
        t2 = {
            'description': t['description'],
            'name': t['name'],
            'arguments': deepcopy(t['parameters']['properties'])
        }
        tools.append(t2)
    payload['tools'] = tools

    # Get the query-specific values and descriptions for the `key_name` argument
    key_names_and_descriptions = builder.set_query_specific_columns_and_descriptions(data_point['output'])
    for k in key_names_and_descriptions:
        k['dtype'], _ = translate_data_type(str(k['dtype']))
    payload['key_values_and_descriptions'] = key_names_and_descriptions
    return payload



def evaluate_win_rate(payloads: list[dict], input_file: str, db_path: str):

    builder, _ = setup(input_file, db_path)
    valid = []
    for p in payloads:
        # If we used the original output sequence here, instead of the model output, it would just check the correctness of the data point
        required_api_calls = [p['initialization_step']]
        required_api_calls.extend(p['model_output'])  # ie. change 'model_output' to just 'output', and the win_rate should be 1.0

        # This dictionary has [key, value] == [tool_name, tool (executable python function)]
        # It is needed for evaluating the win rate, it is NOT consumed by the tool calling model
        api_pool = builder.set_query_specific_api_pool(p['original_output'])

        api_result = validate_api_output(required_api_calls, api_pool)
        validated = check_equality_without_order(api_result, p['gold_answer'])
        valid.append(validated)
    
    return sum(valid) / len(valid)


if __name__ == "__main__":

    filepath = "~/ai4ba/invocable-api-hub/sql_translation_output/dev_2-14/"

    # Sparc dev set
    db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../db_sparc')
    seq_files = []
    sf_files = []
    sparcpath = os.path.join(filepath, "sparc")
    #sparcpath=filepath
    for _, _, files in os.walk(sparcpath):
        for f in files:
            if f.startswith("sequencing"):
                seq_files.append(f)
            elif f.startswith("slot_filling"):
                sf_files.append(f)
    
    for fileset in [seq_files, sf_files]:
        for base_file in fileset:
            try:
                # TODO: fix descriptions in world_1
                if "world_1" in base_file:
                    continue
                # Don't try to overwrite the new files you're creating
                if "_reformatted_" in base_file:
                    continue
                input_file = os.path.join(sparcpath, base_file)
                payloads = generate_dataset_payload(input_file, db_path)

                # Dummy model inference results
                for p in payloads:
                    p['model_output'] = ""

                win_rate = evaluate_win_rate(payloads, input_file, db_path)
                print(f"FILE: {base_file}, WIN RATE = ", win_rate)

                new_filename = base_file.replace("_sparc_", "_reformatted_sparc_")
                with open(os.path.join(sparcpath, new_filename), "w") as f:
                    json.dump(payloads, f)
            except Exception as e:
                print()
                print(f"Dataset {base_file} failed")
                print("Exception = ", e)
                print()

    # Bird dev set
    db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../db/dev_databases/')
    seq_files = []
    sf_files = []
    birdpath = os.path.join(filepath, "bird")
    for _, _, files in os.walk(birdpath):
        for f in files:
            if f.startswith("sequencing"):
                seq_files.append(f)
            elif f.startswith("slot_filling"):
                sf_files.append(f)
    
    for fileset in [seq_files, sf_files]:
        for base_file in fileset:
            input_file = os.path.join(birdpath, base_file)
            payloads = generate_dataset_payload(input_file, db_path)

            # Dummy model inference results
            for p in payloads:
                p['model_output'] = ""

            win_rate = evaluate_win_rate(payloads, input_file, db_path)
            print(f"FILE: {base_file}, WIN RATE = ", win_rate)

            new_filename = base_file.replace("_bird_", "_reformatted_bird_")
            with open(os.path.join(birdpath, new_filename), "w") as f:
                json.dump(payloads, f)
