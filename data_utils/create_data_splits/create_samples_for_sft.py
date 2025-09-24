import os
import json
import json
import random
import argparse
from collections import defaultdict
from tqdm import tqdm

"""
Create the training dataset.
"""
CHANGE_FILES = {
    "multi": "./change_stat_train_multiturn.json",
    "single": "./change_stat_train_single_turn.json"
}

IGNORE_FILES = {
    "multi": "./inconsistency_by_domain.json", 
    "single": "./inconsistency_by_domain_single_turn.json", 
}

scenario_mixing_probability = 0.5

OOD_DOMAINS = ["video_games", "chicago_crime", "simpson_episodes",
               "public_review_platform", "movie","movie_3", "movielens", "movies_4"]
DIRECTORY_MULTI_TURN={
    "ground_truth":{
        "no_scenarios":"/proj/m3benchmark/m3data/0923/m3_train_test_ood_rest_v2_expert/trajectories",
        "scenarios":"/proj/m3benchmark/m3data/0923/m3_train_test_ood_rest_v2_scenarios_expert/trajectories",
    },
    "exploratory":{
        "no_scenarios":"/proj/m3benchmark/m3data/0905/balanced_rest_v4_exploratory_trajectory/trajectories",
        "scenarios":"/proj/m3benchmark/m3data/0905/m3_train_test_ood_rest_v2_chunked_scenarios_exp/trajectories",
    }
}

DIRECTORY_SINGLE_TURN={
    "ground_truth":{
        "no_scenarios":"/proj/m3benchmark/m3data/0923/m3_train_test_ood_rest_v2_single_turn_expert/trajectories",
        "scenarios":"/proj/m3benchmark/m3data/0923/m3_train_test_ood_rest_v2_single_turn_scenarios_expert/trajectories",
    },
    "exploratory":{
        "no_scenarios":"/proj/m3benchmark/m3data/0905/m3_train_test_ood_rest_v2_single_turn_exploratory_trajectory/trajectories",
        "scenarios":"/proj/m3benchmark/m3data/0905/m3_train_test_ood_rest_v2_single_turn_chunked_scenarios_st_exp/trajectories",
    }
}

DOMAIN_KEY="domain"

def load_files(foldername):
    print(f"Loading data from folder - {foldername}")
    filenames=os.listdir(foldername)
    data=defaultdict(list)
    num_samples=0
    for filename in tqdm(filenames,desc="[LOAD]"):
        with open(os.path.join(foldername,filename),'r') as f:
            trajectory=json.load(f)
        if trajectory["domain"] not in OOD_DOMAINS:
            data[trajectory["domain"]].append(trajectory)
            num_samples+=1
    print(f"{num_samples} trajectories loaded for foldername {foldername}")
    print("------------------------------------------------------------------------------------------------------------------------------------------------------------------------------")
    return data

def convert_to_dict(data: list[dict]):
    """
    Convert dataset to dict format.
    """
    data_dict={}
    for item in data:
        if str(item["sample_id"]) not in data_dict.keys():
            data_dict[str(item["sample_id"])] = item
    return data_dict

def group_by_original_sample_id(data: list[dict], inconsistent_sample: list):
    grouped_data = defaultdict(list)
    for d in data:
        sample_id=d["sample_id"]
        original_sample_id = str(d['sample_id']).split("_sc_")[0]
        if sample_id not in inconsistent_sample:
            grouped_data[original_sample_id].append(d)
    return grouped_data

def get_mixed_data(data_no_scenarios, data_with_scenarios, changed_ids, inconsistency_ids=None, format=None):
    data_with_scenarios_dict=convert_to_dict(data_with_scenarios)
    grouped_with_scenario=group_by_original_sample_id(data_with_scenarios,inconsistency_ids) # Removes scenario data which is inconsistent list

    keep_scenarios = [] 
    for idx, item in enumerate(tqdm(data_no_scenarios,desc="[MIXING]")):
        item_with_scenario = grouped_with_scenario[str(item["sample_id"])] # Inconsistent data points are already removed
        intersection_set = list(set([str(i["sample_id"]) for i in item_with_scenario]) & set(changed_ids))
        if len(intersection_set) != 0:
            for id in intersection_set:
                scenario_to_keep=data_with_scenarios_dict[id]
                scenario_to_keep["guid"] = str(format)+"_"+scenario_to_keep[DOMAIN_KEY]+"_"+scenario_to_keep["sample_id"]
                scenario_to_keep["format"]=format # Either single or multi
                keep_scenarios.append(scenario_to_keep)
        else:
            # If there are any samples where the scenario did not change the ground truth
            # 1. flip a coin to decide if we keep the sample with scenario or the original, then
            # 2. if we keep the scenario and there's more than one, pick one at random
            keep_original = 1 if random.random() < scenario_mixing_probability else 0
            if (keep_original==1) or (len(item_with_scenario) == 0):
                item["guid"] = str(format)+"_"+str(item[DOMAIN_KEY])+"_"+str(item["sample_id"])
                item["format"]=format # Either single or multi
                keep_scenarios.append(item)
            else:
                scenario_to_keep=random.choice(item_with_scenario)
                scenario_to_keep["guid"]=str(format)+"_"+scenario_to_keep[DOMAIN_KEY]+"_"+str(scenario_to_keep["sample_id"])
                scenario_to_keep["format"]=format # Either single or multi                
                keep_scenarios.append(scenario_to_keep)
    if len(keep_scenarios) < len(data_no_scenarios):
        import pdb
        pdb.set_trace()
    return keep_scenarios

def process_data(args=None, format="single"):
    change_stat_filename=os.path.join(args.change_stats_dir,CHANGE_FILES[format])
    if format == "single":
        foldername_no_scenario = DIRECTORY_SINGLE_TURN[args.type]["no_scenarios"]
        foldername_with_scenario = DIRECTORY_SINGLE_TURN[args.type]["scenarios"]
        assert format in foldername_no_scenario
        assert format in foldername_with_scenario
    elif format == "multi":
        foldername_no_scenario = DIRECTORY_MULTI_TURN[args.type]["no_scenarios"]
        foldername_with_scenario = DIRECTORY_MULTI_TURN[args.type]["scenarios"]
        assert "single" not in foldername_no_scenario
        assert "single" not in foldername_with_scenario

    data_no_scenarios_dict, data_with_scenario_dict = load_files(foldername_no_scenario), load_files(foldername_with_scenario)
    print(f"Datasets Loaded for {format} turn data.")
    print("------------------------------------------------------------------------------------------------------------------------------------------------------------------------------")
    assert set(data_no_scenarios_dict.keys()) == set(data_with_scenario_dict.keys())
    ignore_data_filename=os.path.join(args.inconsistency_dir,CHANGE_FILES[format])
    domain_names=list(data_no_scenarios_dict.keys())
    filename=os.path.join(args.output_dir,f"{format}_{args.output_filename}")

    with open(change_stat_filename, 'r') as f:
        change_dict=json.load(f)

    with open(ignore_data_filename, 'r') as f:
        ignore_dict=json.load(f)        
    
    mixed_data=[]
    for domain in tqdm(domain_names):
        if domain in OOD_DOMAINS:
            continue
        data_no_scenarios, data_with_scenario = data_no_scenarios_dict[domain], data_with_scenario_dict[domain]
        change_stat_domain=change_dict[domain]
        ignore_domain=ignore_dict[domain]
        mixed_data.extend(get_mixed_data(data_no_scenarios,data_with_scenario,change_stat_domain,ignore_domain))
    with open(filename, 'w') as f:
        json.dump(mixed_data, f)
    print(f"{filename}, {foldername_no_scenario}, {foldername_with_scenario}, {len(mixed_data)}")
    print("------------------------------------------------------------------------------------------------------------------------------------------------------------------------------")    
    print("------------------------------------------------------------------------------------------------------------------------------------------------------------------------------")    
    return mixed_data

def run(args):
    # Data mixing between single and multi-turn can be taken care here
    single_turn_data=process_data(args=args,format="single")
    multi_turn_data=process_data(args=args,format="multi")
    output_data=multi_turn_data+single_turn_data
    with open(os.path.join(args.output_dir,"mixed_"+args.output_filename), 'w') as f:
        json.dump(output_data, f)
    print(f"mixed_{args.output_filename}, {len(output_data)}")
    print("------------------------------------------------------------------------------------------------------------------------------------------------------------------------------")    
    print("------------------------------------------------------------------------------------------------------------------------------------------------------------------------------")            
    return output_data

if __name__=="__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--type', '-t', required=True, choices=["ground_truth", "exploratory"], help="Create SFT data from ground truth trajectories or exploratory trajectories.")
    parser.add_argument("--change_stats_dir", "-cs", default="/proj/m3benchmark/m3data/0923/auxiliary_data", help="The directory which change stats file.")
    parser.add_argument("--inconsistency_dir", "-s", default="/proj/m3benchmark/m3data/0923/auxiliary_data", help="The directory which contains inconsistent ground truth samples ids.")
    parser.add_argument("--output_dir",'-od', default="/proj/m3benchmark/m3data/0923/train_data/sft_exploratory", help="Directory to save files.")
    parser.add_argument('--output_filename', '-of', help="Filename to save trajectories.")
    args = parser.parse_args()
    run(args=args)