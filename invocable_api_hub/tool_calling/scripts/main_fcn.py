
from copy import deepcopy
from collections import defaultdict
import json
import os

from invocable_api_hub.tool_calling.sql_to_api.sql_dataset_builder import SqlDatasetBuilder
from invocable_api_hub.tool_calling.sql_to_api.utils import validate_output, update_dynamic_pool


def format_payloads(data_point):
    payload_model = deepcopy(data_point)
    init_step = payload_model['output'][0]
    payload_model['output'] = payload_model['output'][1:] # Skipping the first step, this is only needed when we invoke
    payload_model['initialization_step'] = init_step
    tools = []  # Format tools to match model input and also drop init
    for t in payload_model['tools']:
        if t['name'] == 'initialize_active_data':
            continue
        t2 = {
            'description': t['description'],
            'name': t['name'],
            'arguments': deepcopy(t['parameters']['properties'])
        }
        tools.append(t2)
    payload_model['tools'] = tools

    payload_agent = deepcopy(data_point)  # Already formatted for agent experiment
    return payload_model, payload_agent
    


def main(database: str, output_file: str, queries: list[str], questions: list[str], dataset_builder: SqlDatasetBuilder, cache_location: str):

    failure_count = pass_count = correct_count = 0
    sql_to_api_translations = []
    dynamic_api_pool = {}
    api_names_set = set()
    errors = defaultdict(list)
    for idx, (question, query) in enumerate(zip(questions, queries)):
        try:
            if database == 'codebase_community' and idx == 40:
                raise Exception("This data point kills the run. Skip it. ")
            required_api_calls = dataset_builder.translate_query_from_sql_tree(query)
            api_pool, key_names_and_descriptions = dataset_builder.set_query_specific_api_pool(required_api_calls)
            database_file = cache_location + "/" + database + ".sqlite"
            correct, _, api_result = validate_output(database_file, query, required_api_calls, api_pool)
            if correct:
                payload = {
                    "query": query,
                    "input": question,
                    "gold_answer": api_result,
                    "output": [{'name': r['name'], 'arguments': r['arguments'], 'label': r['label']} for r in required_api_calls],  # Leave off callable 'fcn'
                    'tools': api_pool
                }
                payload, dynamic_api_pool, api_names_set = update_dynamic_pool(payload, dynamic_api_pool, api_names_set, key_names_and_descriptions)
                payload['dataset_name'] = database
                payload['sample_id'] = idx
                payload['tools'] = [dynamic_api_pool[tool] for tool in payload['tools']]
                payload['key_values_and_descriptions'] = key_names_and_descriptions

                sql_to_api_translations.append(payload)
                correct_count += 1
            else:
                pass_count += 1
            print(f"DATA POINT {idx} -> SCORE = {correct}")
        except Exception as e:
            failure_count += 1
            print("=====================")
            print("DATAPOINT NUMBER: ", idx)
            print("QUESTION = ", question)
            print("QUERY = ", query)
            print("FAILED: ", e)
            try:
                es = str(e)
                errors[es].append(idx)
            except:
                pass

    model_formatted_payloads = []
    agent_formatted_payloads = []
    for data_point in sql_to_api_translations:
        model_data, agent_data = format_payloads(data_point)
        model_formatted_payloads.append(model_data)
        agent_formatted_payloads.append(agent_data)

    # Combine overall dynamic_api_pool and translations
    output = {
        'data': model_formatted_payloads,
        'agent_data': agent_formatted_payloads
    }

    print()
    print()
    print("***************************************")
    print()
    print()
    print(f"There were {correct_count} correct answers, {pass_count} incorrect answers and {failure_count} failures out of a total of {len(queries)} queries. ")
    print()
    print()
    print("***************************************")
    print()
    print()
    dataset_builder.cleanup_temp_files()
    report = {
        "total_queries": len(queries),
        "correct_queries": correct_count,
        "incorrect_queries": pass_count,
        "errored_queries": failure_count,
        "errors": errors
    }

    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    print(f"Dumping to {output_file}")
    with open(output_file, 'w') as f:
        json.dump(output, f)
    
    return report