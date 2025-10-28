import os
import re
import json
import random
import argparse
from tqdm import tqdm
from collections import defaultdict
from data_utils.create_data_splits.utils import downsample_data


"""
Usage : Create balanced test set
PYTHONPATH=./ python data_utils/create_data_splits/create_samples_for_test.py -sn 1.0 -sd 1.0 -od /proj/m3benchmark/m3data/0923/evaluation_data/v9/unbalanced/

Usage : Downsample created test set
PYTHONPATH=./ python data_utils/create_data_splits/create_samples_for_test.py -du 0.1 -uf /proj/m3benchmark/m3data/0923/evaluation_data/v9/balanced/pct100/complete/ood_multi_turn_mixed.json -od /proj/m3benchmark/m3data/0923/evaluation_data/v9/balanced/pct10
"""


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

OUTPUT_FOLDERNAME="/proj/m3benchmark/m3data/0923/evaluation_data/v8"
NUM_SAMPLES=list(range(400,12001,400))
NUM_SAMPLES=[str(i) for i in NUM_SAMPLES]
NUM_SAMPLES.append("ALL")

def create_context_response_pair(item, create_pairs=True,change_type=None,label=None):
    """
    Returns list of trajectory objects.
    """
    final_samples=[]
    if ("single_turn" in label) or (not create_pairs) or (item["num_turns"]==1):
        item["guid"]=item["guid"]+"_"+str(0)
        item["sample_id"]=item["sample_id"]+"_"+str(0)
        item["change_state"]=change_type
        return [item]
    else:
        question_types=re.findall(r"\([^()]*\)", item["type"]) # Splits "(API-API)(RAG)(API)(API-RAG-API)" into ['(API-API)', '(RAG)', '(API)', '(API-RAG-API)']
        assert len(item["trajectory"]) == len(item["turns"])
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
            assert new_item["num_turns"] == (traj_idx+1) == len(new_item["turns"]) == len(new_item["trajectory"])
            if change_type=="base":
                new_item["change_state"]="base"
            elif (traj_idx+1)!=len(item["trajectory"]):
                new_item["change_state"]="nondisruptor"
            elif (traj_idx+1)==len(item["trajectory"]):
                new_item["change_state"]=change_type
            final_samples.append(new_item)
        assert len(final_samples) == item["num_turns"], f"Created {len(final_samples)} context response pairs but num turns of the dialogue were {item['num_turns']}."
        assert final_samples[-1]["num_turns"] == item['num_turns'], f"Last context response pair is not as long as num turns."
        return final_samples

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

def get_mixed_data(data_no_scenarios, data_with_scenarios, changed_ids,label):
    data_with_scenarios_dict=convert_to_dict(data_with_scenarios)
    grouped_with_scenario=group_by_original_sample_id(data_with_scenarios)

    keep_scenarios = []
    for _, item in enumerate(data_no_scenarios):
        item_with_scenario = grouped_with_scenario[str(item["sample_id"])]
        intersection_set = list(set([str(i["sample_id"]) for i in item_with_scenario]) & set(changed_ids))
        non_changed_ids = list(set([id["sample_id"] for id in item_with_scenario if id["sample_id"] not in intersection_set]))
        assert len(intersection_set)+len(non_changed_ids)==len(item_with_scenario) != 0

        # Add base data point of scenario
        item["guid"] = str(item["domain"])+"_"+str(item["sample_id"]) # Context-Response pair ID will get added below
        updated_base_pairs=create_context_response_pair(item, create_pairs=True,change_type="base",label=label)
        keep_scenarios.extend(updated_base_pairs)

        # Create CRP from scenarios
        if len(intersection_set) != 0:

            # Add scenarios which have changed
            for id in intersection_set:
                scenario_to_keep=data_with_scenarios_dict[id]
                scenario_to_keep["guid"] = scenario_to_keep["domain"]+"_"+scenario_to_keep["sample_id"]
                updated_pairs=create_context_response_pair(scenario_to_keep,create_pairs=True,change_type="disruptor",label=label)
                keep_scenarios.extend(updated_pairs)

        # Create CRP from non-changed scenarios
        for id in non_changed_ids:
            scenario_to_keep=data_with_scenarios_dict[id]
            scenario_to_keep["guid"]=scenario_to_keep["domain"]+"_"+str(scenario_to_keep["sample_id"])
            updated_pairs=create_context_response_pair(scenario_to_keep, create_pairs=True,change_type="nondisruptor",label=label)
            keep_scenarios.extend(updated_pairs)
    return keep_scenarios

def check_mixed_dataset(data,data_no_scenarios,data_with_scenarios):
    # All sample IDs are unique as well as num_hops and num_turns represent the recorded stats
    sample_id_lst=[]
    for item in data:
        sample_id_lst.append(item["domain"]+"_"+item["sample_id"])        
        assert item["guid"] == (item["domain"]+"_"+item["sample_id"])
        assert item["num_turns"] == len(item["trajectory"])
    assert len(sample_id_lst)==len(set(sample_id_lst))

    # # Total number of samples in data is sum of turns from no_scenarios and with_scenarios
    # TODO: Following assertion is not complete as we don't have all scenario support yet. Some data points are being dropped from scenarios.
    # updated_data_with_senarios=[d for d in data_with_scenarios if ("ONLY_API" in d['sample_id']) or ("ONLY_RAG" in d['sample_id']) or ("EXCLUDE_GT" not in d['sample_id'])]
    # if (sum([i["num_turns"] for i in data_no_scenarios])+sum([i["num_turns"] for i in updated_data_with_senarios])) != len(data):
    #     import pdb
    #     pdb.set_trace()
    # assert (sum([i["num_turns"] for i in data_no_scenarios])+sum([i["num_turns"] for i in updated_data_with_senarios])) == len(data)
    return True

def get_data_stats(data):
    pattern = r"^([A-Za-z_]+)_([A-Za-z0-9_]+)_(\d+)$" # guid should have this pattern
    scenarios=defaultdict(int)
    question_types=defaultdict(int)
    num_turns=defaultdict(int)
    change_state=defaultdict(int)
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
        change_state[item["change_state"]]+=1

        # Question Distribution
        for hop_type in hops_matches:
            question_types[hop_type]+=1
    assert sum(num_turns.values())==len(data)
    assert sum(scenarios.values())==len(data)
    return scenarios, question_types, num_turns, change_state


def write_data_splits(data,label,num_samples_per_split=400,complete=True,output_foldername=None):
    if complete:
        os.makedirs(os.path.join(output_foldername,"complete/"), exist_ok=True)
        output_filename=f"{output_foldername}/complete/{label}.json"
        with open(output_filename, 'w') as f:
            json.dump(data, f)
        print(f"File {output_filename} was written with {len(data)} samples.")
    data_split=None
    num_splits=len(data)//num_samples_per_split
    # All but last split created
    for i in range(1,num_splits+1):
        data_split=data[400*max(0,i-1):i*400]
        output_filename=f"{output_foldername}/{label}_{NUM_SAMPLES[i-1]}.json"
        with open(output_filename, 'w') as f:
            json.dump(data_split, f)
        print(f"File {output_filename} was written with {len(data_split)} samples.")
    
    # Last split
    start_idx = num_splits * num_samples_per_split
    data_split=data[start_idx:]
    if len(data_split)!=0:
        output_filename=f"{output_foldername}/{label}_{NUM_SAMPLES[-1]}.json"
        with open(output_filename, 'w') as f:
            json.dump(data_split, f)
        print(f"File {output_filename} was written with {len(data_split)} samples.")

def main(args=None):
    if args.downsample_universe:
        assert args.universe_filename, f"Mention the filename to downsample using downsample percent {args.downsample_universe}"
        with open(args.universe_filename,'r') as f:
            data=json.load(f)
        num_samples=int(args.downsample_universe*len(data))
        label=args.universe_filename.split("_mixed")[0].split("/")[-1]
        final_data=random.sample(data,num_samples)
        os.makedirs(args.output_dir, exist_ok=True)
        print(f"Downsample dataset file {args.universe_filename} to {len(final_data)}. Writing the split files to directory {args.output_dir}.")
        write_data_splits(final_data,label=label,complete=True,output_foldername=args.output_dir)
        scenarios, question_types, num_turns, change_state = get_data_stats(final_data)
        print(scenarios)
        print(question_types)
        print(num_turns)
        print(change_state)            
        print("------------------------------------------------------------------------------------------------------------------------------------------------------------------------------")    
        print("------------------------------------------------------------------------------------------------------------------------------------------------------------------------------")        
    else:
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
                change_stat_domain=change_dict[domain]
                mixed_label_data=get_mixed_data(data_no_scenarios,data_with_scenario,change_stat_domain,label)
                mixed_data.extend(mixed_label_data)
                assert check_mixed_dataset(mixed_label_data,data_no_scenarios,data_with_scenario)
            print(f"{label}, {foldername_no_scenario}, {foldername_with_scenario}, {len(mixed_data)}")

            scenarios, question_types, num_turns, change_state = get_data_stats(mixed_data)
            print(scenarios)
            print(question_types)
            print(num_turns)
            print(change_state)
            write_data_splits(mixed_data,label=label,complete=True,output_foldername=os.path.join(args.output_dir,"unbalanced/"))

            if args.sample_disruptor or args.sample_nondisruptor:
                print("------------------------------------------------------------------------------------------------------------------------------------------------------------------------------")    
                print(f"---------------------------------------------------------------------------[DOWNSAMPLING]------------------------------------------------------------------------------------")
                os.makedirs(os.path.join(args.output_dir,"balanced/pct100/"), exist_ok=True)
                final_data=downsample_data(mixed_data=mixed_data,sample_disruptor=args.sample_disruptor,sample_nondisruptor=args.sample_nondisruptor)
                write_data_splits(final_data,label=label,complete=True,output_foldername=os.path.join(args.output_dir,"balanced/pct100"))
                scenarios, question_types, num_turns, change_state = get_data_stats(final_data)
                print(scenarios)
                print(question_types)
                print(num_turns)
                print(change_state)            
                print("------------------------------------------------------------------------------------------------------------------------------------------------------------------------------")    
                print("------------------------------------------------------------------------------------------------------------------------------------------------------------------------------")

if __name__=="__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output_dir",'-od', default=OUTPUT_FOLDERNAME, help="Directory to save files.") # "/proj/m3benchmark/m3data/0923/evaluation_data/v6"
    parser.add_argument('--sample_disruptor', '-sd', type=float, default=None, help="Percentage of disruptor to sample. No sampling if set to None.") # 1.0
    parser.add_argument('--sample_nondisruptor', '-sn', type=float, default=None, help="Percentage of nondisruptor to sample. No sampling if set to None.") # 1.0
    parser.add_argument("--downsample_universe", "-du",default=None,type=float,help="Downsample a file based on percentage.") # 0.1
    parser.add_argument("--universe_filename","-uf",default=None,type=str,help="Set if downsample_universe is set.") # /proj/m3benchmark/m3data/0923/evaluation_data/v9/balanced/pct100/complete/ood_multi_turn_mixed.json
    args = parser.parse_args()
    main(args=args)