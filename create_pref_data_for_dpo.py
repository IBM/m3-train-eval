"""
Details for generating preference trajectories (chosen v/s reject):
    > This script accepts trajectory data from two agents of which one is chosen and other one is rejected.
    > The implemented parsing logic is based on how the trajectories are saved while executing run.py
    > The agents must run on the same tasks so that they share a common prompt, tool universe, tool policy.
    > The script saves the preference data in turn-wise format (input, output, reward) to allow finer-control over
      which part of agent's execution trace should be used for training.
"""
import json
import os

# [Here] Provide the paths to agent collected trajectories
# Expected format is what the run.py saves the data in
chosen_agent_data_dir = './data/bird-dev/train/sft_expert'
rejected_agent_data_dir = './data/bird-dev/train/sft_agent_informed_assist'

# [Here] Provide the dataset names (scenario-specific)
datasets = ['both_api_rag-api_before_rag', 'both_api_rag-rag_before_api']

save_pref_data_at = './data/bird-dev/train/pairwise_pref'
if not os.path.exists(save_pref_data_at):
    os.makedirs(save_pref_data_at)

def load_metadata(file_path):
    with open(file_path, "r") as f:
        return json.load(f)


def load_trajectories(file_path):
    with open(file_path, "r") as f:
        return json.load(f)


def get_turn_wise_data(trajectory):
    time_steps = list(trajectory['interactions'].keys())
    time_steps = sorted(time_steps, key=lambda x: int(x))
    turn_wise_data = []
    for t in time_steps:
        turn_wise_data.append({
            "input": trajectory['interactions'][t]['input'][-1],
            "output": trajectory['interactions'][t]['output'],
            "reward": trajectory['interactions'][t]['reward'],
        })
    return turn_wise_data


def group_pref_data_by_sample_id(chosen_agent_dir, rejected_agent_dir):
    def collect_agent_data(agent_dir):
        sample_to_traj = {}
        for file in os.listdir(agent_dir):
            if file.startswith("metadata_") and file.endswith(".json"):
                idx = file.split("_")[1].split(".")[0]
                metadata_path = os.path.join(agent_dir, f"metadata_{idx}.json")
                trajectory_path = os.path.join(agent_dir, f"trajectory_{idx}.json")
                metadata = load_metadata(metadata_path)
                trajectory = load_trajectories(trajectory_path)

                sample_id = metadata["sample_id"]

                sample_to_traj[sample_id] = {
                    "system": trajectory["system"],
                    "tools": trajectory["tools"],
                    "turn_wise_interactions": get_turn_wise_data(trajectory),
                    # Determine the Tool Policy [TODO: Adapt this how tool policies are being incorporated into G.T. Data]
                    "tool_availability_policy": "both_api_rag",
                    "tool_usage_policy": ""
                }
        return sample_to_traj

    chosen_agent_data = collect_agent_data(chosen_agent_dir)
    rejected_agent_data = collect_agent_data(rejected_agent_dir)

    # Match common sample_ids
    common_sample_ids = set(chosen_agent_data.keys()) & set(rejected_agent_data.keys())
    grouped = []

    for sample_id in common_sample_ids:

        assert chosen_agent_data[sample_id]["system"] == rejected_agent_data[sample_id]["system"]
        assert chosen_agent_data[sample_id]["tools"] == rejected_agent_data[sample_id]["tools"]
        assert chosen_agent_data[sample_id]["tool_availability_policy"] == rejected_agent_data[sample_id]["tool_availability_policy"]
        assert chosen_agent_data[sample_id]["tool_usage_policy"] == rejected_agent_data[sample_id]["tool_usage_policy"]

        grouped.append({
            "sample_id": sample_id,
            "system": chosen_agent_data[sample_id]["system"],
            "tools": chosen_agent_data[sample_id]["tools"],
            "tool_availability_policy": chosen_agent_data[sample_id]['tool_availability_policy'],
            "tool_usage_policy": chosen_agent_data[sample_id]['tool_usage_policy'],
            "chosen_trajectory": chosen_agent_data[sample_id]['turn_wise_interactions'],
            "rejected_trajectory": rejected_agent_data[sample_id]['turn_wise_interactions'],
        })

    return grouped


# Read the trajectories of pair of agents and create preference data
for dataset in datasets:
    grouped_data = group_pref_data_by_sample_id(
        os.path.join(chosen_agent_data_dir, dataset),
        os.path.join(rejected_agent_data_dir, dataset)
    )
    with open(os.path.join(save_pref_data_at, f"{dataset}.json"), "w") as f:
        json.dump(grouped_data, f, indent=4)



