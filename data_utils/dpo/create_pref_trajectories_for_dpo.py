
import argparse
from collections import defaultdict
from copy import deepcopy
import json
import os
import random

save_pref_data_at = './data/pairwise_pref'
CHANGE_FILES = [
    "./data/change_stat_train_multiturn.json",
    "./data/change_stat_train_single_turn.json"
]
IGNORE_FILES = [
    "./data/inconsistency_by_domain.json", 
    "./data/inconsistency_by_domain.json", 
    "./data/inconsistency_by_domain_single_turn.json", 
    "./data/inconsistency_by_domain_single_turn.json", 
]
scenario_mixing_probability = 0.5

OOD_DOMAINS = ["video_games", "chicago_crime", "simpson_episodes",
               "public_review_platform", "movie","movie_3", "movielens", "movies_4"]

EXPLORATORY_TRAJECTORY_DIRS = [
    '/proj/m3benchmark/m3data/0905/balanced_rest_v4_exploratory_trajectory',
    '/proj/m3benchmark/m3data/0905/m3_train_test_ood_rest_v2_chunked_scenarios_exp',
    '/proj/m3benchmark/m3data/0905/m3_train_test_ood_rest_v2_single_turn_exploratory_trajectory',
    '/proj/m3benchmark/m3data/0905/m3_train_test_ood_rest_v2_single_turn_chunked_scenarios_st_exp',
    '/proj/m3benchmark/m3data/0905/m3_train_test_ood_rest_v2/test_chunked_gt_exp',
    '/proj/m3benchmark/m3data/0905/m3_train_test_ood_rest_v2/test_scenarios_chunked_exp'
    ]

GROUND_TRUTH_TRAJECTORY_DIRS = [
    '/proj/m3benchmark/m3data/0905/m3_train_test_ood_rest_v2_expert',
    '/proj/m3benchmark/m3data/0905/m3_train_test_ood_rest_v2_scenarios_expert',
    '/proj/m3benchmark/m3data/0905/m3_train_test_ood_rest_v2_single_turn_expert',
    '/proj/m3benchmark/m3data/0905/m3_train_test_ood_rest_v2_single_turn_scenarios_expert',
    # '/proj/m3benchmark/m3data/0905/m3_train_test_ood_rest_v2/test_chunked_gt',
    # 'proj/m3benchmark/m3data/0905/m3_train_test_ood_rest_v2/test_chunked_scenarios'
    ]

DATASET_SPLIT_LABELS = [
    'train_multi_turn_no_scenarios',
    'train_multi_turn_with_scenarios',
    'train_single_turn_no_scenarios',
    'train_single_turn_with_scenarios',
    # 'test_multi_turn_no_scenarios',
    # 'test_multi_turn_with_scenarios'
    ]
MIXED_SCENARIO_LABELS = [
    "train_multi_turn", 
    "train_single_turn", 
    # "test_multi_turn"
    ]

if not os.path.exists(save_pref_data_at):
    os.makedirs(save_pref_data_at)

def load_metadata(file_path):
    with open(file_path, "r") as f:
        return json.load(f)


def load_trajectories(file_path):
    with open(file_path, "r") as f:
        return json.load(f)


def get_turn_wise_data(trajectory, with_alt: bool = True):
    time_steps = list(trajectory['interactions'].keys())
    time_steps = sorted(time_steps, key=lambda x: int(x))
    turn_wise_data = []
    for t in time_steps:
        turn = {
            "input": trajectory['interactions'][t]['input'][-1],
            "output": trajectory['interactions'][t]['output'],
            "reward": trajectory['interactions'][t]['reward'],
            "actor": trajectory['interactions'][t]['actor']
        }
        if with_alt:
            turn["alternate_output"] = trajectory['interactions'][t]['alternate_trace']
        turn_wise_data.append(turn)
    return turn_wise_data


def collect_agent_data(agent_dir, ignore_list, with_alt: bool = True):
    sample_to_traj = {}
    files = list(os.listdir(os.path.join(agent_dir, "trajectories")))
    print(f"Found {len(files)} trajectory files, starting with {files[0]}")
    skipped_count = 0
    ood_skipped = 0
    for file in files:
        if file.startswith("trajectory_") and file.endswith(".json"):
            idx = file.split("_", 1)[1].split(".")[0]
            if idx in ignore_list:
                skipped_count += 1
                continue
            trajectory_path = os.path.join(agent_dir, "trajectories", file)
            trajectory = load_trajectories(trajectory_path)
            if trajectory['domain'] in OOD_DOMAINS:
                ood_skipped += 1
                continue
            sample_to_traj[idx] = {
                "sample_id": trajectory['sample_id'], 
                "domain": trajectory["domain"], 
                "tool_availability_policy": trajectory["tool_availability_policy"],
                "tool_usage_policy": trajectory["tool_usage_policy"],
                "turn_wise_interactions": get_turn_wise_data(trajectory, with_alt=with_alt), 
                "tools": trajectory["tools"], 
                "system": trajectory["system"]
            }
        else:
            print(f"WARNING, FOUND ANOMALOUS FILE: {file}")
    print(f"We loaded {len(sample_to_traj)} files, and skipped {skipped_count} inconsistent files and {ood_skipped} OOD files. ")
    return sample_to_traj

def split_trajectories_at_interventions(actor_agent_dir, ignore_list):

    agent_data = collect_agent_data(actor_agent_dir, ignore_list)

    # Match common sample_ids
    grouped = []
    alternate_outputs = 0
    samples_with_traces = 0
    for trajectory_id in agent_data.keys():
        active_trajectory = []
        sample_has_alternate_trace = False
        for idx, turn in enumerate(agent_data[trajectory_id]["turn_wise_interactions"]):
            if turn['alternate_output'] != []:
                alternate_outputs += 1
                sample_has_alternate_trace = True
                # Create a DPO pair
                online_data = {'input': deepcopy(turn['input']), 'output': deepcopy(turn['output'])}
                offline_data = {
                    'input': deepcopy(turn['input']), 
                    'output': deepcopy(turn['alternate_output'][0]['output'])
                    }
                chosen = deepcopy(active_trajectory)
                rejected = deepcopy(active_trajectory)
                if turn['actor'] == 'agent':
                    # The actor is the agent, so the correct answer is in the alternate_trace
                    chosen.append(offline_data)
                    rejected.append(online_data)
                elif turn['actor'] == 'expert':
                    # The actor is the expert, so the wrong answer is in the alternate_trace
                    chosen.append(online_data)
                    rejected.append(offline_data)
                dpo_guid = agent_data[trajectory_id]["domain"] + "_" + agent_data[trajectory_id]["sample_id"] + f"_turn_{idx}"
                new_pair = {
                    "dpo_guid": dpo_guid, 
                    "sample_id": agent_data[trajectory_id]["sample_id"],
                    "domain": agent_data[trajectory_id]["domain"], 
                    "tool_availability_policy": agent_data[trajectory_id]['tool_availability_policy'],
                    "tool_usage_policy": agent_data[trajectory_id]['tool_usage_policy'],
                    "tools": agent_data[trajectory_id]["tools"],
                    "system": agent_data[trajectory_id]["system"], 
                    "chosen": chosen,
                    "rejected": rejected
                }
                grouped.append(new_pair)
            turn_data = {'input': deepcopy(turn['input']), 'output': deepcopy(turn['output'])}
            active_trajectory.append(turn_data)
        samples_with_traces += int(sample_has_alternate_trace)
    print(f"We found {alternate_outputs} alternate traces, and {samples_with_traces} out of {len(agent_data.keys())} samples had at least one. ")
    return grouped


def group_by_original_sample_id(data: list[dict]):
    grouped_data = defaultdict(list)
    for d in data:
        original_sample_id = d['domain'] + "_" + d['sample_id'].split("_sc_")[0]
        grouped_data[original_sample_id].append(d)
    return grouped_data

def get_mixed_data(label, changed_ids, suffix: str):
    with open(os.path.join(save_pref_data_at, label+"_no_scenarios" + suffix + ".json")) as f:
        no_scenarios = json.load(f)
    grouped_no_scenarios = group_by_original_sample_id(no_scenarios)

    with open(os.path.join(save_pref_data_at, label+"_with_scenarios" + suffix + ".json")) as f:
        with_scenarios = json.load(f)
    grouped_with_scenarios = group_by_original_sample_id(with_scenarios)

    keep_scenarios = []
    changed_count = 0
    unchanged_count = 0
    for scen_id, scenes in grouped_no_scenarios.items():
        if scen_id in grouped_with_scenarios:
            matches = [(g['domain'] + '_' + g['sample_id'], g) for g in grouped_with_scenarios[scen_id]]
            unchanged_matches = defaultdict(list)
            # Keep all the trajectories where the scenario altered the ground truth
            for idx, idx_scene in matches:
                if idx in changed_ids:
                    keep_scenarios.append(idx_scene)
                    changed_count +=1
                else:
                    unchanged_matches[idx].append(idx_scene)
                    unchanged_count +=1
            
            # If there are any trajectories where the scenario did not change the ground truth
            # 1. flip a coin to decide if we keep the trajectory with scenario or the original, then
            # 2. if we keep the scenario and there's more than one, pick one at random
            keep_original = 1 if random.random() < scenario_mixing_probability else 0
            if keep_original or len(unchanged_matches) == 0:
                keep_scenarios.extend(scenes)
            else:
                scenario_to_keep_key = random.choice(list(unchanged_matches.keys()))
                keep_scenarios.extend(unchanged_matches[scenario_to_keep_key])
                
        else:
            keep_scenarios.extend(scenes)

    print(f"Created a total of {len(keep_scenarios)} DPO pairs for label {label} after filtering. Starting from {len(grouped_no_scenarios)} without scenarios and {len(grouped_with_scenarios)} with scenarios.  ")
    print(f"Total of {changed_count} changed and {unchanged_count} unchanged trajectories. ")
    return keep_scenarios


def format_overseer_pairs(mix_scenarios: bool):

    # Read the trajectories of pair of agents and create preference data
    for agent_dir, dataset, ignore_file in zip(EXPLORATORY_TRAJECTORY_DIRS, DATASET_SPLIT_LABELS, IGNORE_FILES):
        ignore_list = []
        if ignore_file is not None:
            assert os.path.isfile(ignore_file), f"File not found: {ignore_file}"
            with open(ignore_file) as f:
                ignore = json.load(f)
            for domain, domain_list in ignore.items():
                ignore_list.extend([domain+"_"+i for i in domain_list])
        print(f'\n\n Creating DPO data for dataset: {dataset}\n\n')
        grouped_data = split_trajectories_at_interventions(
            agent_dir, ignore_list
        )
        print(f"Created {len(grouped_data)} DPO trajectory pairs. ")
        with open(os.path.join(save_pref_data_at, f"{dataset}.json"), "w") as f:
            json.dump(grouped_data, f, indent=4)


    if mix_scenarios:
        for label in DATASET_SPLIT_LABELS:
            assert os.path.isfile(os.path.join(save_pref_data_at, label+".json")), f"File not found: {os.path.join(save_pref_data_at, label+'.json')}"

        for label, change_file in zip(MIXED_SCENARIO_LABELS, CHANGE_FILES):
            assert os.path.isfile(change_file), f"File not found: {change_file}"
            with open(change_file) as f:
                changes = json.load(f)
        
            changed_ids = []
            for domain, domain_list in changes.items():
                changed_ids.extend([domain+"_"+i for i in domain_list])
            mixed_data = get_mixed_data(label, changed_ids, "")

            with open(os.path.join(save_pref_data_at, label+"_mixed.json"), "w") as f:
                json.dump(mixed_data, f)


def format_ground_truth_pairs(mix_scenarios: bool):

    def group_pref_data_by_sample_id(chosen_agent_dir, rejected_agent_dir, ignore_file):

        assert os.path.isfile(ignore_file), f"File not found: {ignore_file}"
        with open(ignore_file) as f:
            ignore = json.load(f)
        ignore_list = []
        for domain, domain_list in ignore.items():
            ignore_list.extend([domain+"_"+i for i in domain_list])
    
        chosen_agent_data = collect_agent_data(chosen_agent_dir, ignore_list, with_alt=False)
        rejected_agent_data = collect_agent_data(rejected_agent_dir, ignore_list, with_alt=False)

        # Match common sample_ids
        common_sample_ids = set(chosen_agent_data.keys()) & set(rejected_agent_data.keys())
        grouped = []

        match_failures = 0
        for trajectory_id in common_sample_ids:

            try:
                assert chosen_agent_data[trajectory_id]["system"] == rejected_agent_data[trajectory_id]["system"]
                assert chosen_agent_data[trajectory_id]["tools"] == rejected_agent_data[trajectory_id]["tools"]
                assert chosen_agent_data[trajectory_id]["tool_availability_policy"] == rejected_agent_data[trajectory_id]["tool_availability_policy"]
                assert chosen_agent_data[trajectory_id]["tool_usage_policy"] == rejected_agent_data[trajectory_id]["tool_usage_policy"]
            except:
                match_failures += 1
                continue

            dpo_guid = chosen_agent_data[trajectory_id]["domain"] + "_" + chosen_agent_data[trajectory_id]["sample_id"] + "_gt_vs_exp"
            grouped.append({
                "dpo_guid": dpo_guid, 
                "sample_id": chosen_agent_data[trajectory_id]["sample_id"],
                "domain": chosen_agent_data[trajectory_id]["domain"], 
                "system": chosen_agent_data[trajectory_id]["system"],
                "tools": chosen_agent_data[trajectory_id]["tools"],
                "tool_availability_policy": chosen_agent_data[trajectory_id]['tool_availability_policy'],
                "tool_usage_policy": chosen_agent_data[trajectory_id]['tool_usage_policy'],
                "chosen_trajectory": chosen_agent_data[trajectory_id]['turn_wise_interactions'],
                "rejected_trajectory": rejected_agent_data[trajectory_id]['turn_wise_interactions'],
            })
        print(f"FAILED TO MATCH FIELDS FOR {match_failures} SAMPLES")
        return grouped

    # # Read the trajectories of pair of agents and create preference data
    # for agent_dir, gt_dir, dataset, ignore in zip(EXPLORATORY_TRAJECTORY_DIRS, GROUND_TRUTH_TRAJECTORY_DIRS, DATASET_SPLIT_LABELS, IGNORE_FILES):
    #     grouped_data = group_pref_data_by_sample_id(
    #         gt_dir, 
    #         agent_dir,
    #         ignore
    #     )
    #     print(f"Created {len(grouped_data)} DPO trajectory pairs from ground truth vs expert trajectories. ")
    #     with open(os.path.join(save_pref_data_at, f"{dataset}_gt_vs_expert.json"), "w") as f:
    #         json.dump(grouped_data, f, indent=4)
        
    if mix_scenarios:
        for label in DATASET_SPLIT_LABELS:
            assert os.path.isfile(os.path.join(save_pref_data_at, label+"_gt_vs_expert.json")), f"File not found: {os.path.join(save_pref_data_at, label+'_gt_vs_expert.json')}"

        for label, change_file in zip(MIXED_SCENARIO_LABELS, CHANGE_FILES):
            assert os.path.isfile(change_file), f"File not found: {change_file}"
            with open(change_file) as f:
                changes = json.load(f)
        
            changed_ids = []
            for domain, domain_list in changes.items():
                changed_ids.extend([domain+"_"+i for i in domain_list])
            mixed_data = get_mixed_data(label, changed_ids, "_gt_vs_expert")

            with open(os.path.join(save_pref_data_at, label+"_gt_vs_expert_mixed.json"), "w") as f:
                json.dump(mixed_data, f)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--style', '-s', required=True, choices=['overseer', 'ground_truth'])
    args = parser.parse_args()
    
    style = args.style

    # if not args.no_format_overseer:
    if style == "overseer":
        format_overseer_pairs(True)
    elif style == "ground_truth":
        format_ground_truth_pairs(True)
    else:
        raise Exception("Which one do we do?")
