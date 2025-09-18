import os
import json
import json
import random
from collections import defaultdict
from tqdm import tqdm

AGENT_DIRS = [
    ('/proj/m3benchmark/m3data/0905/m3_train_test_ood_rest_v2_chunked_none_scenarios/','/proj/m3benchmark/m3data/0905/m3_train_test_ood_rest_v2_chunked_scenarios/'),
    ('/proj/m3benchmark/m3data/0905/m3_train_test_ood_rest_v2_single_turn_chunked/','/proj/m3benchmark/m3data/0905/m3_train_test_ood_rest_v2_single_turn_chunked_scenarios/'),
    ('/proj/m3benchmark/m3data/0905/m3_train_test_ood_rest_v2/test_chunked/','/proj/m3benchmark/m3data/0905/m3_train_test_ood_rest_v2/test_scenarios_chunked/'),
    ]
LABELS = [
    'train_multi_turn_mixed',
    'train_single_turn_mixed',
    'test_multi_turn_mixed',
    ]
CHANGE_STATS=[
    "/proj/m3benchmark/m3data/0905/auxiliary_data/change_stat_train_multiturn.json", # Train Multi-turn
    "/proj/m3benchmark/m3data/0905/auxiliary_data/change_stat_train_single_turn.json", # Train Single Turn
    "", # Test Multi-turn
]
save_grpo_data_at=""
probability_of_using_original=0.5
OUTPUT_FOLDERNAME="/proj/m3benchmark/m3data/0905/grpo"

def load_metadata(file_path):
    with open(file_path, "r") as f:
        return json.load(f)
    
def convert_to_dict(data: list[dict]):
    """
    Convert dataset to dict format.
    """
    data_dict={}
    for item in data:
        if str(item["sample_id"]) not in data_dict.keys():
            data_dict[str(item["sample_id"])] = item
    return data_dict

def group_by_original_sample_id(data: list[dict]):
    grouped_data = defaultdict(list)
    for d in data:
        original_sample_id = str(d['sample_id']).split("_sc_")[0]
        grouped_data[original_sample_id].append(d)
    return grouped_data

def get_mixed_data(data_no_scenarios, data_with_scenarios, changed_ids):
    data_with_scenarios_dict=convert_to_dict(data_with_scenarios)
    grouped_with_scenario=group_by_original_sample_id(data_with_scenarios)

    keep_scenarios = []
    for _, item in enumerate(data_no_scenarios):
        item_with_sceanrio = grouped_with_scenario[str(item["sample_id"])]
        intersection_set = list(set([str(i["sample_id"]) for i in item_with_sceanrio]) & set(changed_ids))
        if len(intersection_set) != 0:
            for id in intersection_set:
                scenario_to_keep=data_with_scenarios_dict[id]
                scenario_to_keep["guid"] = scenario_to_keep["dataset_name"]+"_"+scenario_to_keep["sample_id"]
                keep_scenarios.append(scenario_to_keep)
        else:
            # If there are any samples where the scenario did not change the ground truth
            # 1. flip a coin to decide if we keep the sample with scenario or the original, then
            # 2. if we keep the scenario and there's more than one, pick one at random
            keep_original = 1 if random.random() < probability_of_using_original else 0
            if keep_original==1:
                item["guid"] = str(item["dataset_name"])+"_"+str(item["sample_id"])
                keep_scenarios.append(item)
            else:
                scenario_to_keep=random.choice(item_with_sceanrio)
                scenario_to_keep["guid"]=scenario_to_keep["dataset_name"]+"_"+str(scenario_to_keep["sample_id"])
                keep_scenarios.append(scenario_to_keep)
    return keep_scenarios

def main():
    for (foldername_no_scenario, foldername_with_scenario), label, change_stat_filename in zip(AGENT_DIRS,LABELS,CHANGE_STATS):
        if change_stat_filename != "":
            with open(change_stat_filename, 'r') as f:
                change_dict=json.load(f)
        else:
            continue
        
        domain_names=[i.split("_multiturn_")[0] for i in os.listdir(foldername_no_scenario)]
        mixed_data=[]
        for domain in tqdm(domain_names):
            data_no_scenarios=load_metadata(f"{foldername_no_scenario}/{domain}_multiturn_bird_chunked.json")
            data_with_scenario=load_metadata(f"{foldername_with_scenario}/{domain}_multiturn_bird_chunked.json")
            change_stat_domain=change_dict[domain]
            mixed_data.extend(get_mixed_data(data_no_scenarios,data_with_scenario,change_stat_domain))
        print(f"{label}, {foldername_no_scenario}, {foldername_with_scenario}, {len(mixed_data)}")
        with open(f"{OUTPUT_FOLDERNAME}/{label}.json", 'w') as f:
            json.dump(mixed_data, f)

if __name__=="__main__":
    main()