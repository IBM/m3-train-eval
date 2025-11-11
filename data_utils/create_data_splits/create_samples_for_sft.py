import os
import re
import json
import random
import argparse
from collections import defaultdict, Counter
from tqdm import tqdm
from data_utils.create_data_splits.utils import scenario_based_sample, downsample_data


"""
Create the training dataset.
Usage:
PYTHONPATH=./ python data_utils/create_data_splits/create_samples_for_sft.py -t ground_truth -od /proj/m3benchmark/m3data/0923/train_data/sft_ground_truth_v4/ -of sft_data.json -sd 0.05 -sn 0.03
"""
CHANGE_FILES = {
    "multi": "./change_stat_train_multiturn.json",
    "single": "./change_stat_train_single_turn.json"
}

IGNORE_FILES = {
    "multi": "./inconsistency_multiturn_train.json", 
    "single": "./inconsistency_single_turn.json", 
}

scenario_mixing_probability = 0.5

OOD_DOMAINS = ["video_games", "chicago_crime", "simpson_episodes",
               "public_review_platform", "movie","movie_3", "movielens", "movies_4"]
DIRECTORY_MULTI_TURN={
    "ground_truth":{
        "no_scenarios":"/proj/m3benchmark/m3data/0923/m3_train_test_ood_rest_v2_expert/trajectories",
        # "scenarios":"/proj/m3benchmark/m3data/0923/m3_train_test_ood_rest_v2_scenarios_expert/trajectories",
        "scenarios": "/proj/m3benchmark/m3data/0923/data/train/multi/scenarios/run_gt_non_live/trajectories"
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

DIRECTORY_BASE={
    "multi":{
        "no_scenarios":"/proj/m3benchmark/m3data/0905/m3_train_test_ood_rest_v2_chunked",
        "scenarios":"/proj/m3benchmark/m3data/0905/m3_train_test_ood_rest_v2_chunked_scenarios",
    },
    "single":{
        "no_scenarios":"/proj/m3benchmark/m3data/0905/m3_train_test_ood_rest_v2_single_turn_chunked",
        "scenarios":"/proj/m3benchmark/m3data/0905/m3_train_test_ood_rest_v2_single_turn_chunked_scenarios",
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
    assert len(data)==len(data_dict.keys()), f"Sample IDs of this dataset are not unique as {len(data)} just created {len(data_dict.keys())} elements in the dictionary."
    return data_dict


def create_context_response_pair(item, change_type=None):
    """
    Returns list of trajectory objects.
    """

    final_samples=[]
    if change_type in ["base","nondisruptor"]:
        item["change_state"]=change_type
    
    for (t, timestep) in item["interactions"].items():
        crp_item={
            "idx": t,
            "guid": item["guid"],
            "sample_id": item["sample_id"],
            "domain": item["domain"],
            "format": item["format"],
            "type": item["type"],
            "num_turns": len([input for input in timestep["input"] if input["role"]=="user"]), # num_turns is equal to the number of user turns in the context            
            "system":item["system"],
            "tools": item["tools"],
            "input": timestep["input"],
            "output": timestep["output"],
            "interactions": [{t:timestep}],
            "scenarios": item["scenarios"],
            "tool_availability_policy": item["tool_availability_policy"],
            "tool_usage_policy": item["tool_usage_policy"],
            "final_answer_policy": item["final_answer_policy"],
        }

        if change_type in ["base","nondisruptor"]:
            crp_item["change_state"]= item["change_state"]
        elif (change_type=="disruptor"):
            if crp_item["num_turns"]!= item["num_turns"]: # Interaction is not the last interaction
                crp_item["change_state"]="nondisruptor"
            else:
                crp_item["change_state"]="disruptor"

        final_samples.append(crp_item)
    assert len(final_samples)==len(item["interactions"].keys())
    return final_samples

def group_by_original_sample_id(data: list[dict], inconsistent_sample: list):
    grouped_data = defaultdict(list)
    for d in data:
        sample_id=d["sample_id"]
        original_sample_id = str(d['sample_id']).split("_sc_")[0]
        if sample_id not in inconsistent_sample:
            if ("ONLY_API" not in d['sample_id']) and ("ONLY_RAG" not in d['sample_id']):
                continue
            else:
                grouped_data[original_sample_id].append(d)
    return grouped_data

def get_base_data(domain):
    BASE_DATA=defaultdict(dict)
    for format in ["multi","single"]:
        for type in ["scenarios","no_scenarios"]:
            foldername=DIRECTORY_BASE[format][type]
            filename=f"{foldername}/{domain}_multiturn_bird_chunked.json"
            with open(filename,'r') as f:
                BASE_DATA[format][type]=json.load(f)            
    return BASE_DATA

def get_base_item(base_data,domain: str,sample_id: str,format: str,type: str):
    """
    Get base dataset using
    domain : str Domain name
    sample_id : str sample ID
    format: str multi turn or single turn
    type: str with scenarios or without scenarios
    """
    data=base_data[format][type]
    
    for item in data:
        if str(item["sample_id"])==str(sample_id):
            return item
    return None

def get_mixed_data(data_no_scenarios, data_with_scenarios, changed_ids, inconsistency_ids=None, format=None, domain=None):
    data_with_scenarios_dict=convert_to_dict(data_with_scenarios)
    grouped_with_scenario=group_by_original_sample_id(data_with_scenarios,inconsistency_ids) # Removes scenario data which is inconsistent list
    base_data=get_base_data(domain)

    keep_scenarios = [] 
    for _, item in enumerate(tqdm(data_no_scenarios,desc="[MIXING]")):
        item_with_scenario = grouped_with_scenario[str(item["sample_id"])] # Inconsistent data points are already removed
        intersection_set = list(set([str(i["sample_id"]) for i in item_with_scenario]) & set(changed_ids))
        non_changed_ids = list(set([id["sample_id"] for id in item_with_scenario if id["sample_id"] not in intersection_set]))
        assert (len(intersection_set)+len(non_changed_ids)) == len(item_with_scenario)

        # Add base sample
        item["guid"] = str(format)+"_"+str(item[DOMAIN_KEY])+"_"+str(item["sample_id"])
        item["format"]=format # Either single or multi
        item["domain"] = domain        

        # Get base item to get question metadata
        base_item=get_base_item(base_data,domain=item["domain"],sample_id=item["sample_id"],format=format,type="no_scenarios")
        assert base_item, f"Base item not found for sample id {item['sample_id']} and domain {item['domain']} without scenarios."
        if base_item:
            item["type"]=base_item["type"]
            item["num_turns"]=base_item["num_turns"]
            item["num_hops"]=base_item["num_hops"]
        crp_items=create_context_response_pair(item=item,change_type="base")
        keep_scenarios.extend(crp_items)
        del crp_items
        del base_item

        if len(intersection_set) != 0:
            for id in intersection_set:
                scenario_to_keep=data_with_scenarios_dict[id]
                scenario_to_keep["guid"] = str(format)+"_"+scenario_to_keep[DOMAIN_KEY]+"_"+scenario_to_keep["sample_id"]
                scenario_to_keep["format"]=format # Either single or multi
                scenario_to_keep["domain"] = domain                

                # Get base item to get question metadata
                base_item=get_base_item(base_data,domain=scenario_to_keep["domain"],sample_id=scenario_to_keep["sample_id"],format=format,type="scenarios")
                assert base_item, f"Base item not found for sample id {scenario_to_keep['sample_id']} and domain {scenario_to_keep['domain']} with scenarios."
                if base_item:
                    scenario_to_keep["type"]=base_item["type"]
                    scenario_to_keep["num_turns"]=base_item["num_turns"]
                    scenario_to_keep["num_hops"]=base_item["num_hops"]
                crp_items=create_context_response_pair(item=scenario_to_keep,change_type="disruptor")
                keep_scenarios.extend(crp_items)
                del crp_items
                del base_item
                del scenario_to_keep

        for id in non_changed_ids:
            scenario_to_keep=data_with_scenarios_dict[id]
            scenario_to_keep["guid"]=str(format)+"_"+scenario_to_keep[DOMAIN_KEY]+"_"+str(scenario_to_keep["sample_id"])
            scenario_to_keep["format"]=format # Either single or multi
            scenario_to_keep["domain"] = domain                            

            # Get base item to get question metadata               
            base_item=get_base_item(base_data,domain=scenario_to_keep["domain"],sample_id=scenario_to_keep["sample_id"],format=format,type="scenarios")
            assert base_item, f"Base item not found for sample id {scenario_to_keep['sample_id']} and domain {scenario_to_keep['domain']} with scenarios." 
            if base_item:
                scenario_to_keep["type"]=base_item["type"]
                scenario_to_keep["num_turns"]=base_item["num_turns"]
                scenario_to_keep["num_hops"]=base_item["num_hops"]
            crp_items=create_context_response_pair(item=scenario_to_keep,change_type="nondisruptor")
            keep_scenarios.extend(crp_items)
            del crp_items
            del base_item            
            del scenario_to_keep

    # assert len(keep_scenarios) >= len(data_no_scenarios), f"Total samples kept for this file are {len(keep_scenarios)} which is lesser than base samples without scenarions {len(data_no_scenarios)}"
    return keep_scenarios

def split_sample_ids_random(counter, ratio=0.9, seed=None):
    """
    Randomly split unique sample_ids into two sets so that the first set covers
    approximately `ratio` (default 0.9) of the total instances.
    """
    if seed is not None:
        random.seed(seed)

    sample_ids = list(counter.keys())
    random.shuffle(sample_ids)

    total = sum(counter.values())
    running_total = 0

    set1, set2 = set(), set()

    for sample_id in sample_ids:
        if running_total / total < ratio:
            set1.add(sample_id)
            running_total += counter[sample_id]
        else:
            set2.add(sample_id)

    return set1, set2

def get_unique_id(datapoint: dict) -> str:
    uid = datapoint['domain']+"_"+datapoint["sample_id"].split("_sc_")[0]
    return uid

def perform_train_val_split(json_path: str, ratio=0.9, seed=42):
    
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise ValueError("Expected the JSON file to contain a list of objects.")

    sample_ids = [get_unique_id(row) for row in data]
    counts = Counter(sample_ids)
    
    # Random split (set a seed for reproducibility if desired)
    train_ids, val_ids = split_sample_ids_random(counts, ratio=ratio, seed=seed)
    
    # Print summary
    print(f"Total unique sample_ids: {len(counts)}")
    print(f"Train set (~90%): {len(train_ids)} unique IDs, {sum([counts[t] for t in train_ids])} C-R pairs. ")
    print(f"Val set (~10%): {len(val_ids)} unique IDs, {sum([counts[t] for t in val_ids])} C-R pairs. \n")

    train_set = []
    val_set = []
    for d in data:
        uid = get_unique_id(d)
        if uid in val_ids:
            val_set.append(d)
        else:
            assert uid in train_ids
            train_set.append(d)

    train_file = json_path.replace(".json", "_train_split.json")
    with open(train_file, "w") as f:
        json.dump(train_set, f)
    print(f"Wrote {len(train_set)} / {len(data)} rows to {train_file}")

    val_file = json_path.replace(".json", "_val_split.json")
    with open(val_file, "w") as f:
        json.dump(val_set, f)
    print(f"Wrote {len(val_set)} / {len(data)} rows to {val_file}")
            

def get_data_stats(data):
    pattern = r"^([A-Za-z_]+)_([A-Za-z0-9_]+)$" # guid should have this pattern
    scenarios=defaultdict(int)
    num_turns=defaultdict(int)
    change_state=defaultdict(int)
    for item in data:
        match = re.match(pattern, item["guid"])
        domain, sample_id = match.groups()

        # Scenario Distribution
        if "_sc_" in item["sample_id"]:
            scenarios[sample_id.split("_sc_")[1]]+=1
        else:
            scenarios["base"]+=1
        
        # Turns Distribution
        num_turns[item["num_turns"]]+=1
        change_state[item["change_state"]]+=1        

    assert sum(num_turns.values())==len(data)
    assert sum(scenarios.values())==len(data)
    return scenarios, num_turns, change_state

def process_data(args=None, format="single"):
    change_stat_filename=os.path.join(args.change_stats_dir,CHANGE_FILES[format])
    if format == "single":
        foldername_no_scenario = DIRECTORY_SINGLE_TURN[args.traj_type]["no_scenarios"]
        foldername_with_scenario = DIRECTORY_SINGLE_TURN[args.traj_type]["scenarios"]
        assert format in foldername_no_scenario
        assert format in foldername_with_scenario
    elif format == "multi":
        foldername_no_scenario = DIRECTORY_MULTI_TURN[args.traj_type]["no_scenarios"]
        foldername_with_scenario = DIRECTORY_MULTI_TURN[args.traj_type]["scenarios"]
        assert "single" not in foldername_no_scenario
        assert "single" not in foldername_with_scenario

    data_no_scenarios_dict, data_with_scenario_dict = load_files(foldername_no_scenario), load_files(foldername_with_scenario)
    print(f"Datasets Loaded for {format} turn data.")
    print("------------------------------------------------------------------------------------------------------------------------------------------------------------------------------")
    assert set(data_no_scenarios_dict.keys()) == set(data_with_scenario_dict.keys())
    ignore_data_filename=os.path.join(args.inconsistency_dir,IGNORE_FILES[format])
    domain_names=list(data_no_scenarios_dict.keys())
    filename=os.path.join(args.output_dir,f"{format}_{args.output_filename}")

    with open(change_stat_filename, 'r') as f:
        change_dict=json.load(f)

    with open(ignore_data_filename, 'r') as f:
        ignore_dict=json.load(f)        
    
    mixed_data=[]
    for domain in tqdm(domain_names, desc="[DOMAIN]"):
        if domain in OOD_DOMAINS:
            continue
        data_no_scenarios, data_with_scenario = data_no_scenarios_dict[domain], data_with_scenario_dict[domain]
        change_stat_domain=change_dict[domain]
        ignore_domain=ignore_dict[domain]
        mixed_data.extend(get_mixed_data(data_no_scenarios,data_with_scenario,change_stat_domain,ignore_domain,format,domain))
    with open(filename, 'w') as f:
        json.dump(mixed_data, f)
    scenarios, num_turns, change_state = get_data_stats(mixed_data)
    print(scenarios)
    print(num_turns)
    print(change_state)
    print(f"{filename}, {foldername_no_scenario}, {foldername_with_scenario}, {len(mixed_data)}")
    print("------------------------------------------------------------------------------------------------------------------------------------------------------------------------------")    
    print("------------------------------------------------------------------------------------------------------------------------------------------------------------------------------")    
    return mixed_data

def run(args):
    os.makedirs(args.output_dir, exist_ok=True)

    # Data mixing between single and multi-turn can be taken care here
    single_turn_data=process_data(args=args,format="single")
    multi_turn_data=process_data(args=args,format="multi")
    output_data=multi_turn_data+single_turn_data
    with open(os.path.join(args.output_dir,"mixed_"+args.output_filename), 'w') as f:
        json.dump(output_data, f)
    print(f"mixed_{args.output_filename}, {len(output_data)}")

    # Downsample based on percentage
    if args.sample_disruptor or args.sample_nondisruptor:
        os.makedirs(os.path.join(args.output_dir,"downsampled/"), exist_ok=True)

        print("------------------------------------------------------------------------------------------------------------------------------------------------------------------------------")    
        print("------------------------------------------------------------------------------------------------------------------------------------------------------------------------------")

        # # Downsample Single Turn
        single_turn_downsampled_data=downsample_data(single_turn_data,sample_disruptor=args.sample_disruptor,sample_nondisruptor=args.sample_nondisruptor)
        filename=os.path.join(args.output_dir,"downsampled",f"single_{args.output_filename}")
        with open(filename, 'w') as f:
            json.dump(single_turn_downsampled_data, f)
        scenarios, num_turns, change_state = get_data_stats(single_turn_downsampled_data)
        print(scenarios)
        print(num_turns)
        print(change_state)
        print(f"{filename}, {len(single_turn_downsampled_data)}")

        # Downsample Multi Turn
        multi_turn_downsampled_data=downsample_data(multi_turn_data,sample_disruptor=args.sample_disruptor,sample_nondisruptor=args.sample_nondisruptor)
        filename=os.path.join(args.output_dir,"downsampled","multi_"+args.output_filename)
        with open(filename, 'w') as f:
            json.dump(multi_turn_downsampled_data, f)
        scenarios, num_turns, change_state = get_data_stats(multi_turn_downsampled_data)
        print(scenarios)
        print(num_turns)
        print(change_state)
        print(f"{filename}, {len(multi_turn_downsampled_data)}")


        # Combined file
        output_data_sampled=multi_turn_downsampled_data+single_turn_downsampled_data        
        filename=os.path.join(args.output_dir,"downsampled","mixed_"+args.output_filename)
        with open(filename, 'w') as f:
            json.dump(output_data_sampled, f)
        scenarios, num_turns, change_state = get_data_stats(output_data_sampled)
        print(scenarios)
        print(num_turns)
        print(change_state)
        print(f"{filename}, {len(output_data_sampled)}")

        # Also save separate train and validation splits
        perform_train_val_split(filename)

    print("------------------------------------------------------------------------------------------------------------------------------------------------------------------------------")    
    print("------------------------------------------------------------------------------------------------------------------------------------------------------------------------------")            
    return output_data

if __name__=="__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--traj_type', '-t', required=True, choices=["ground_truth", "exploratory"], help="Create SFT data from ground truth trajectories or exploratory trajectories.") # exploratory
    parser.add_argument("--change_stats_dir", "-cs", default="/proj/m3benchmark/m3data/0923/auxiliary_data", help="The directory which change stats file.")
    parser.add_argument("--inconsistency_dir", "-id", default="/proj/m3benchmark/m3data/0923/auxiliary_data", help="The directory which contains inconsistent ground truth samples ids.") # /proj/m3benchmark/m3data/0923/train_data/sft_exploratory/
    parser.add_argument("--output_dir",'-od', required=True, help="Directory to save files.") # "/proj/m3benchmark/m3data/0923/train_data/sft_exploratory"
    parser.add_argument('--output_filename', '-of', required=True, help="Filename to save trajectories.") # sft_data.json
    parser.add_argument('--sample_disruptor', '-sd', type=float, default=None, help="Percentage of disruptor to sample. No sampling if set to None.") # 0.05
    parser.add_argument('--sample_nondisruptor', '-sn', type=float, default=None, help="Percentage of nondisruptor to sample. No sampling if set to None.") # 0.03   
    args = parser.parse_args()
    run(args=args)