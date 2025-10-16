import os
import re
import json
import json
import random
from collections import defaultdict
from tqdm import tqdm

AGENT_DIRS = [
    ('/proj/m3benchmark/m3data/0923/data/ood/multi/no_scenarios/generate/final/','/proj/m3benchmark/m3data/0923/data/ood/multi/scenarios/generate/final/'), # Multi Turn OOD
    ('/proj/m3benchmark/m3data/0923/data/ood/single/no_scenarios/generate/final/','/proj/m3benchmark/m3data/0923/data/ood/single/scenarios/generate/final/'), # Single Turn OOD
    ('/proj/m3benchmark/m3data/0923/data/test/multi/no_scenarios/generate/final/','/proj/m3benchmark/m3data/0923/data/test/multi/scenarios/generate/final/'), # Multi Turn Test
    ]
LABELS = [
    'ood_multi_turn_mixed',
    'ood_single_turn_mixed',
    'test_multi_turn_mixed',
    ]
CHANGE_STATS=[
    "/proj/m3benchmark/m3data/0923/auxiliary_data/change_stat_ood_multi_turn.json", # OOD Multi-turn
    "/proj/m3benchmark/m3data/0923/auxiliary_data/change_stat_ood_single_turn.json", # OOD Single Turn
    "/proj/m3benchmark/m3data/0923/auxiliary_data/change_stat_test_multi_turn.json", # Test Multi-turn
]

scenario_mixing_probability=0.5
OUTPUT_FOLDERNAME="/proj/m3benchmark/m3data/0923/evaluation_data/v5"
NUM_SAMPLES=["400","800","1200", "1600", "2000", "2400", "2800","3200","3600","4000","4400","4800","5200","ALL"]

def create_context_response_pair(item, create_pairs=True):
    """
    Returns list of trajectory objects.
    """
    final_samples=[]
    # To avoid duplication. Context-Response pairs created from base sample and the complete sample kept for scenarios.
    if (item["num_turns"] == 1) or (not create_pairs):
        item["guid"]=item["guid"]+"_"+str(0)
        item["sample_id"]=item["sample_id"]+"_"+str(0)
        return [item]
    else:
        question_types=re.findall(r"\([^()]*\)", item["type"]) # Splits "(API-API)(RAG)(API)(API-RAG-API)" into ['(API-API)', '(RAG)', '(API)', '(API-RAG-API)']
        for traj_idx in range(0,len(item["trajectory"])):
            # Construct the item
            new_item={
                "guid": item["guid"]+"_"+str(traj_idx),
                "sample_id": item["sample_id"]+"_"+str(traj_idx),
                "doc_collections":item["doc_collections"],
                "domain":item["domain"],
                "tools":item["tools"],
                "scenarios":item["scenarios"],
                "resp_cutoff_thresh":item["resp_cutoff_thresh"],
                "resp_cutoff_inst":item["resp_cutoff_inst"],
            }
            new_item["turns"]=item["turns"][0:(traj_idx+1)]
            new_item["trajectory"]=item["trajectory"][0:(traj_idx+1)]
            new_item["type"]="".join(question_types[0:(traj_idx+1)])
            new_item["num_turns"]=len(new_item["turns"])
            new_item["num_hops"]=item["num_hops"][0:(traj_idx+1)]
            final_samples.append(new_item)
        return final_samples

def get_scenario_types(data):
    """
    Get list of changed scenarios.
    """
    scenario_type=[]
    for filename in CHANGE_STATS:
        if os.path.isfile(filename):
            with open(filename,'r') as f:
                data=json.load(f)
            for k, v in data.items():
                for i in v:
                   scenario_type.append(i.split("_sc_")[1])
    return set(scenario_type)

def create_stat(data):
    """Create the change stats file for Single Turn OOD."""
    scenario_type=get_scenario_types(data=data)
    chanegd_scenarios_lst = []
    for item in data:
        if item["sample_id"].split("_sc_")[1] in scenario_type:
            chanegd_scenarios_lst.append(item["sample_id"])
    return chanegd_scenarios_lst

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
        if ("ONLY_API" not in d['sample_id']) and ("ONLY_RAG" not in d['sample_id']) and ("EXCLUDE_GT" not in d['sample_id']):
            continue
        else:
            grouped_data[original_sample_id].append(d)
    return grouped_data

def get_mixed_data(data_no_scenarios, data_with_scenarios, changed_ids):
    data_with_scenarios_dict=convert_to_dict(data_with_scenarios)
    grouped_with_scenario=group_by_original_sample_id(data_with_scenarios)

    keep_scenarios = []
    for _, item in enumerate(data_no_scenarios):
        item_with_sceanrio = grouped_with_scenario[str(item["sample_id"])]
        intersection_set = list(set([str(i["sample_id"]) for i in item_with_sceanrio]) & set(changed_ids))
        item["guid"] = str(item["domain"])+"_"+str(item["sample_id"]) # Context-Response pair ID will get added below
        if len(intersection_set) != 0:

            # Add base data point of scenario
            updated_base_pairs=create_context_response_pair(item, create_pairs=True)
            keep_scenarios.extend(updated_base_pairs)

            # Add scenarios which have changed
            for id in intersection_set:
                scenario_to_keep=data_with_scenarios_dict[id]
                scenario_to_keep["guid"] = scenario_to_keep["domain"]+"_"+scenario_to_keep["sample_id"]
                updated_pairs=create_context_response_pair(scenario_to_keep,create_pairs=False)
                keep_scenarios.extend(updated_pairs)
        else:
            # If there are any samples where the scenario did not change the ground truth
            # 1. flip a coin to decide if we keep the sample with scenario or the original, then
            # 2. if we keep the scenario and there's more than one, pick one at random
            keep_original = 1 if random.random() < scenario_mixing_probability else 0
            # if (keep_original==1) or (len(item_with_sceanrio)==0): # If we don't have any relevant scenarios to keep
            if (keep_original==1):
                updated_pairs=create_context_response_pair(item,create_pairs=True)
                for updated_pair_item in updated_pairs:
                    keep_scenarios.append(updated_pair_item)
            else:
                scenario_to_keep=random.choice(item_with_sceanrio)
                scenario_to_keep["guid"]=scenario_to_keep["domain"]+"_"+str(scenario_to_keep["sample_id"])
                updated_pairs=create_context_response_pair(scenario_to_keep, create_pairs=True) # As the base sample is not being included for this scenario we create context-response pairs
                for updated_pair_item in updated_pairs:
                    keep_scenarios.append(updated_pair_item)
    return keep_scenarios

def check_dataset(data):
    # All sample IDs are unique as well as num_hops and num_turns represent the recorded stats
    sample_id_lst=[]
    for item in data:
        sample_id_lst.append(item["domain"]+"_"+item["sample_id"])        
        if item["guid"] != (item["domain"]+"_"+item["sample_id"]):
            import pdb
            pdb.set_trace()
        assert item["num_turns"] == len(item["trajectory"])
    assert len(sample_id_lst)==len(set(sample_id_lst))
    return True

def get_data_stats(data):
    pattern = r"^([A-Za-z_]+)_([A-Za-z0-9_]+)_(\d+)$" # guid should have this pattern
    scenarios=defaultdict(int)
    question_types=defaultdict(int)
    num_turns=defaultdict(int)
    for item in data:
        match = re.match(pattern, item["guid"])
        hops_matches = re.findall(r'\(([^)]+)\)', item["type"])
        domain, sample_id, traj_id = match.groups()

        # Scenario Distribution
        if "_sc_" in sample_id:
            scenarios[sample_id.split("_sc_")[1]]+=1
        else:
            scenarios["base"]+=1
        
        # Turns Distribution
        num_turns[item["num_turns"]]+=1

        # Question Distribution
        for hop_type in hops_matches:
            question_types[hop_type]+=1
    assert sum(num_turns.values())==len(data)
    assert sum(scenarios.values())==len(data)
    return scenarios, question_types, num_turns


def write_data_splits(data,label,num_samples_per_split=400):
    data_split=None
    num_splits=len(data)//num_samples_per_split
    # All but last split created
    for i in range(1,num_splits+1):
        data_split=data[400*max(0,i-1):i*400]
        output_filename=f"{OUTPUT_FOLDERNAME}/{label}_{NUM_SAMPLES[i-1]}.json"
        with open(output_filename, 'w') as f:
            json.dump(data_split, f)
        print(f"File {output_filename} was written with {len(data_split)} samples.")
    
    # Last split
    data_split=data[i*400:]
    output_filename=f"{OUTPUT_FOLDERNAME}/{label}_{NUM_SAMPLES[-1]}.json"
    with open(output_filename, 'w') as f:
        json.dump(data_split, f)
    print(f"File {output_filename} was written with {len(data_split)} samples.")

def main():
    for (foldername_no_scenario, foldername_with_scenario), label, change_stat_filename in zip(AGENT_DIRS,LABELS,CHANGE_STATS):
        if change_stat_filename != "":
            with open(change_stat_filename, 'r') as f:
                change_dict=json.load(f)
        else:
            change_dict=None
        
        domain_names=[i.split("_multiturn_")[0] for i in os.listdir(foldername_no_scenario) if "_final.json" in i]
        mixed_data=[]
        for domain in tqdm(domain_names,desc="[DOMAIN]"):
            data_no_scenarios=load_metadata(f"{foldername_no_scenario}/{domain}_multiturn_bird_chunked_final.json")
            data_with_scenario=load_metadata(f"{foldername_with_scenario}/{domain}_multiturn_bird_chunked_final.json")
            if change_dict:
                change_stat_domain=change_dict[domain]
            else:
                change_stat_domain=create_stat(data_with_scenario)
            mixed_data.extend(get_mixed_data(data_no_scenarios,data_with_scenario,change_stat_domain))
        print(f"{label}, {foldername_no_scenario}, {foldername_with_scenario}, {len(mixed_data)}")
        assert check_dataset(mixed_data)
        scenarios, question_types, num_turns = get_data_stats(mixed_data)
        print(scenarios)
        print(question_types)
        print(num_turns)
        write_data_splits(mixed_data,label=label)

if __name__=="__main__":
    main()