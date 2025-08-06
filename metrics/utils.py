import json
import os
from collections import defaultdict, Counter
from typing import Dict, List, Literal

import numpy as np

Scenario = Literal['both_api_rag-api_before_rag', 'both_api_rag-rag_before_api']
predefined_action_types = {'API', 'RETRIEVE', 'FINAL'}
rewards_for_valid_actions = {
    "REWARD_SUCCESS_API_CALL",
    "REWARD_SUCCESS_RETRIEVAL_CALL",
    "REWARD_FINAL_ANSWER_MATCH",
    "REWARD_FINAL_ANSWER_NO_MATCH"
}


def get_error_profile(result_dir: str) -> Dict[str, int]:
    files = os.listdir(result_dir)
    traj_files = [f for f in files if f.startswith("trajectory")]
    traj_files = sorted(traj_files, key=lambda x: int(x.split(".")[0].split("_")[-1]))

    trajectories = []
    for f in traj_files:
        with open(os.path.join(result_dir, f), "r") as f:
            trajectories.append(json.load(f))

    error_dict = defaultdict(int)
    for traj in trajectories:
        for t in traj["interactions"].keys():
            obs: str = traj["interactions"][t]["metadata"]["observation"]
            # obs = obs.split("<OBSERVATION>")[-1].split("</OBSERVATION>")[0]
            if 'error' in obs.split(":")[0].strip().lower():
                error = obs.split(":")[0].strip()
                error_dict[error] += 1

    print(json.dumps(error_dict, indent=4))
    return error_dict


def get_action_type_trajectory(trajectory_data: dict) -> List[str]:
    interactions = trajectory_data["interactions"]
    time_steps = list(interactions.keys())
    time_steps = sorted(time_steps, key=lambda x: int(x))
    actions = []
    for t in time_steps:
        action = interactions[t]["metadata"]["action"]
        actions.append(action)

    return actions


def get_reward_type_trajectory(trajectory_data: dict) -> List[str]:
    interactions = trajectory_data["interactions"]
    time_steps = list(interactions.keys())
    time_steps = sorted(time_steps, key=lambda x: int(x))
    reward_types = []
    for t in time_steps:
        reward_type = interactions[t]["reward"]
        # assert reward_type.startswith("{") and reward_type.endswith("}")
        # reward_type = reward_type[1:-1]
        reward_types.append(reward_type)

    return reward_types


def analyze_agent_actions(
        action_trajectories: List[List[str]],
        successes: List[bool],
        scenario: Scenario = None
) -> dict:
    """
    For each trajectory, compute whether a scenario was followed and frequency of each action type (including invalids)
    :param action_trajectories: List of action_type at each step for every trajectory
    :param successes: List of whether each trajectory was successful
    :param scenario: Scenario to follow
    """
    def check_scenario(traj, scenario):
        if scenario is None:
            return True

        elif scenario.endswith("api_before_rag"):
            """No API calls before any RETRIEVE call"""
            if "API" not in traj or "RETRIEVE" not in traj:
                return False

            seen_rag = False
            for a in traj:
                if a == "API":
                    if seen_rag: return False # API call after RAG
                elif a == "RETRIEVE":
                    seen_rag = True
            return True

        elif scenario.endswith("rag_before_api"):
            """No RETRIEVE calls after any API call"""
            if "API" not in traj or "RETRIEVE" not in traj:
                return False

            seen_api = False
            for a in traj:
                if a == "RETRIEVE":
                    if seen_api: return False # RAG call after API
                elif a == "API":
                    seen_api = True
            return True

        elif scenario.endswith("rag_only"):
            """Only RETRIEVE calls"""
            return all(a != "API" for a in traj)

        elif scenario.endswith("api_only"):
            """Only API calls"""
            return all(a != "RETRIEVE" for a in traj)

        else:
            raise ValueError(f"Unknown scenario: {scenario}")

    # Sanity check
    assert len(action_trajectories) == len(successes), "Length mismatch between actions and successes."

    # === Scenario-specific evaluation ===
    scenario_flags = [check_scenario(traj, scenario) for traj in action_trajectories]
    scenario_flags_successful = [flag for flag, succ in zip(scenario_flags, successes) if succ]

    scenario_avg_all = np.mean(scenario_flags)
    scenario_avg_success = np.mean(scenario_flags_successful) if scenario_flags_successful else float('nan')

    # === Action count stats ===
    def count_actions(traj):
        counter = Counter(traj)
        counts = {atype: counter.get(atype, 0) for atype in predefined_action_types}
        counts['OTHER'] = sum(counter[a] for a in counter if a not in predefined_action_types)
        return counts

    def compute_stats(counts_list):
        keys = counts_list[0].keys()
        stats = {}
        for key in keys:
            values = [c[key] for c in counts_list]
            stats[key] = {
                'mean': np.mean(values),
                'std': np.std(values),
            }
        return stats

    # Compute stats
    all_counts = [count_actions(traj) for traj in action_trajectories]
    success_counts = [count_actions(traj) for traj, succ in zip(action_trajectories, successes) if succ]

    stats_all = compute_stats(all_counts)
    stats_success = compute_stats(success_counts) if success_counts else {}

    # === Return summary ===
    return {
        # 'scenario_followed': scenario_flags,
        'scenario_followed': {
            'all_trajectories': scenario_avg_all,
            'successful_trajectories': scenario_avg_success
        },
        'action_stats': {
            'all_trajectories': stats_all,
            'successful_trajectories': stats_success
        }
    }


def analyse_expert_assistance(expert_assistance_data: List[List[int]]) -> dict:
    """
    :param expert_assistance_data: List of whether expert assistance was provided at each step for every trajectory
    :return:
    """
    if not expert_assistance_data:
        raise ValueError("No expert assistance run data found.")

    first_help_timesteps = []
    total_helps = []
    non_help_stretches = []
    for curr_exp_assistance in expert_assistance_data:
        total_helps.append(sum(curr_exp_assistance))

        # First help timestep
        try:
            first_help_timesteps.append(curr_exp_assistance.index(1))
        except ValueError:
            first_help_timesteps.append(None)  # Never sought help

        # Compute stretches of consecutive 0s
        count = 0
        for help_provided in curr_exp_assistance + [1]:  # Sentinel to flush the last segment
            if help_provided == 0:
                count += 1
            else:
                if count > 0:
                    non_help_stretches.append(count)
                    count = 0

    avg_first_help = (
        sum(t for t in first_help_timesteps if t is not None) / sum(1 for t in first_help_timesteps if t is not None)
        if any(t is not None for t in first_help_timesteps)
        else None
    )
    avg_total_helps = sum(total_helps) / len(expert_assistance_data)
    avg_non_help_stretch = (
        sum(non_help_stretches) / len(non_help_stretches) if non_help_stretches else 0
    )

    return {
        "avg_first_help_timestep": avg_first_help,
        "avg_total_helps": avg_total_helps,
        "avg_non_help_stretch": avg_non_help_stretch,
    }


def analyse_error_recovery(
    reward_trajectories: List[List[str]],
    successes: List[bool],
) -> dict:
    """
    For each trajectory, compute how many steps it took to recover from an error after a valid action.
    :param reward_trajectories: List of reward_type at each step for every trajectory
    :param successes: List of whether each trajectory was successful
    """

    assert len(reward_trajectories) == len(successes), "Mismatch between rewards and success flags"

    def extract_recovery_lengths(rewards):
        recovery_lengths = []
        in_error = False
        steps_since_error = 0
        last_was_valid = True  # init t=-1 is a valid step

        for r in rewards:

            valid_action = False
            for valid_r in rewards_for_valid_actions:
                if valid_r in r:  # The env given reward can be of multiple types at once
                    valid_action = True
                    break

            if valid_action:
                if in_error:
                    # Recovered from error
                    recovery_lengths.append(steps_since_error)
                    in_error = False
                    steps_since_error = 0
                last_was_valid = True
            else:
                if last_was_valid:
                    # Start of a new error streak after a valid
                    in_error = True
                    steps_since_error = 1
                    last_was_valid = False
                elif in_error:
                    steps_since_error += 1
                else:
                    # Error at beginning or following another error without prior valid
                    continue

        return recovery_lengths

    # Gather all recovery lengths
    all_recoveries = []
    success_recoveries = []

    for rewards, succ in zip(reward_trajectories, successes):
        recs = extract_recovery_lengths(rewards)

        all_recoveries.extend(recs)
        if succ:
            success_recoveries.extend(recs)

    def summarize(data):
        return {
            'num_recoveries': len(data),
            'steps_taken': {
                'mean': np.mean(data) if data else float('nan'),
                'std': np.std(data) if data else float('nan')
            }
        }

    return {
        'all_trajectories': summarize(all_recoveries),
        'successful_trajectories': summarize(success_recoveries)
    }
