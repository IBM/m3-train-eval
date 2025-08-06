
import argparse
import json




"""

Script to generate ICL examples for model prompts.

USAGE:

PYTHONPATH=./ python invocable_api_hub.tool_calling.scripts.generate_icl_examples.py --source_file_path PATH_TO_DIRECTORY_CONTAINING_SOURCE_FILE

It is assumed that the directory pointed to by the source_file_path argument will contain both a slot-filling and sequencing source file. 

"""



def create_icl_example(data_point, tools_list):
    tools = data_point['tools']
    tools = [t for t in tools if t['name'] in tools_list]
    icl_example = {
        'sample_id': data_point['sample_id'],
        'dataset_name': data_point['dataset_name'],
        'input': data_point['input'],
        'output': data_point['output'],
        'tools': tools,
        'gold_answer': data_point['gold_answer']
    }
    return icl_example


def main(source_file_path: str):
    # Sequencing

    with open(source_file_path+"sequencing_bird_disney.json") as f:
        sequencing_data = json.load(f)['data']


    sequencing_train_indices = [2, 16, 20]
    sequencing_tools_lists = [
        ['select_data_not_equal_to', 'sort_data_ascending', 'get_movies_total_gross_release_dates', 'truncate', 'select_data_equal_to', 'select_data_contains', 'get_movies_total_gross_movie_titles', 'sort_data_descending'], 
        
        ['select_data_not_equal_to', 'get_director_names', 'select_data_equal_to', 'transform_data_to_substring', 'compute_data_mean', 'select_data_greater_than', 'compute_data_sum', 'compute_data_count'], 
        
        ['select_data_not_equal_to', 'select_unique_values', 'select_data_equal_to', 'get_directors', 'get_names', 'compute_data_sum', 'compute_data_count', 'select_data_like'],
        ]
    sequencing_icl = []
    for index, toolset in zip(sequencing_train_indices, sequencing_tools_lists):
        sequencing_icl.append(create_icl_example(sequencing_data[index], toolset))


    sequencing_icl = {'data': sequencing_icl}
    with open("sequencing_icl_examples.json", "w") as f:
        json.dump(sequencing_icl, f)

    # Slot filling

    with open(source_file_path+"slot_filling_bird_disney.json") as f:
        slot_filling_data = json.load(f)['data']


    slot_filling_train_indices = [2, 16, 20]
    slot_filling_tools_lists = [
        ['sort_data', 'filter_data', 'retrieve_data', 'transform_data'], 
        ['sort_data', 'filter_data', 'retrieve_data', 'aggregate_data'], 
        ['select_unique_values', 'filter_data', 'retrieve_data', 'aggregate_data'],
        ]

    slot_filling_icl = []
    for index, toolset in zip(slot_filling_train_indices, slot_filling_tools_lists):
        slot_filling_icl.append(create_icl_example(slot_filling_data[index], toolset))

    slot_filling_icl = {'data': slot_filling_icl}
    with open("slot_filling_icl_examples.json", "w") as f:
        json.dump(slot_filling_icl, f)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--source_file_path', type=str, required=True)
    args = parser.parse_args()
    main(source_file_path=args.source_file_path)
