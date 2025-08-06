import json
import os
from collections import defaultdict

from metrics.utils import get_error_profile, get_action_type_trajectory, get_reward_type_trajectory, \
    analyze_agent_actions, analyse_expert_assistance, analyse_error_recovery
from metrics.plot import plot_error_histogram, plot_error_frequencies_comparison


def analyze_single_scenario_perf(result_dir: str, scenario_to_follow=None):

    # Analyse Error Profile
    error_profile = get_error_profile(result_dir)
    plot_error_histogram(error_profile, f"{result_dir}/error_freq_agent_no_expert_assist.png")

    files = os.listdir(result_dir)
    metadata_files = [f for f in files if f.startswith("metadata")]
    traj_files = [f for f in files if f.startswith("trajectory")]
    metadata_files = sorted(metadata_files, key=lambda x: int(x.split(".")[0].split("_")[-1]))
    traj_files = sorted(traj_files, key=lambda x: int(x.split(".")[0].split("_")[-1]))
    assert len(metadata_files) == len(traj_files)

    expert_assistance_data = []
    agent_reward_data = []
    agent_run_length, success_tracker, termination_tracker, truncation_tracker = [], [], [], []
    actionic_data = []
    for t_f, m_f in zip(traj_files, metadata_files):
        with open(os.path.join(result_dir, t_f), "r") as f:
            trajectory = json.load(f)
        actions = get_action_type_trajectory(trajectory)
        actionic_data.append(actions)

        rewards = get_reward_type_trajectory(trajectory)
        agent_reward_data.append(rewards)

        with open(os.path.join(result_dir, m_f), "r") as f:
            metadata = json.load(f)

        expert_assistance_data.append(metadata['expert_assistance']['tracker'])
        agent_run_length.append(metadata['total_time_steps'])
        success_tracker.append(1 if metadata['success'] else 0)
        termination_tracker.append(1 if metadata['terminated'] else 0)
        truncation_tracker.append(1 if metadata['truncated'] else 0)

    # Analyse Agent Performance
    agent_performance_stats = {
            "avg_agent_run_length": sum(agent_run_length) / len(agent_run_length),
            "completion_rate":  sum(success_tracker) / len(success_tracker),
            "truncation_rate": sum(truncation_tracker) / len(truncation_tracker),
            "termination_rate": sum(termination_tracker) / len(termination_tracker),
            "perf_str": f"Total={len(success_tracker)}, Success={sum(success_tracker)}, Truncated={sum(truncation_tracker)}, Terminated={sum(termination_tracker)}",
    }

    # Analyse Actions & Scenario-following
    actionic_performance_stats = analyze_agent_actions(actionic_data, success_tracker, scenario_to_follow)

    # Analyse expert assistance
    expert_assistance_stats = analyse_expert_assistance(expert_assistance_data)

    # Analyse error recovery
    error_recovery_stats = analyse_error_recovery(agent_reward_data, success_tracker)

    analysis = {
        "agent_performance": agent_performance_stats,
        "actionic_performance": actionic_performance_stats,
        "expert_assistance": expert_assistance_stats,
        "error_recovery": error_recovery_stats
    }

    print(json.dumps(analysis, indent=4))
    with open(os.path.join(result_dir, "agent_performance_analysis.json"), "w") as f:
        json.dump(analysis, f, indent=4)


def analyse_aggregated_perf(result_dir: str):
    scenarios = ["both_api_rag-api_before_rag", "both_api_rag-rag_before_api"]

    aggregated_error_profile = defaultdict(int)
    aggregated_expert_assistance_data = []
    aggregated_agent_reward_data = []
    aggregated_actionic_performance_stats = {}
    aggregated_agent_run_length, aggregated_success_tracker, aggregated_termination_tracker, aggregated_truncation_tracker = [], [], [], []
    for scenario in scenarios:
        # Analyse Error Profile
        error_profile = get_error_profile(os.path.join(result_dir, scenario))
        for key in error_profile.keys():
            aggregated_error_profile[key] += error_profile[key]

        scenario_result_dir = os.path.join(result_dir, scenario)
        files = os.listdir(scenario_result_dir)
        metadata_files = [f for f in files if f.startswith("metadata")]
        traj_files = [f for f in files if f.startswith("trajectory")]
        metadata_files = sorted(metadata_files, key=lambda x: int(x.split(".")[0].split("_")[-1]))
        traj_files = sorted(traj_files, key=lambda x: int(x.split(".")[0].split("_")[-1]))
        assert len(metadata_files) == len(traj_files)

        actionic_data = []
        agent_run_length, success_tracker, termination_tracker, truncation_tracker = [], [], [], []
        for t_f, m_f in zip(traj_files, metadata_files):
            with open(os.path.join(scenario_result_dir, t_f), "r") as f:
                trajectory = json.load(f)
            actions = get_action_type_trajectory(trajectory)
            actionic_data.append(actions)

            rewards = get_reward_type_trajectory(trajectory)
            aggregated_agent_reward_data.append(rewards)

            with open(os.path.join(scenario_result_dir, m_f), "r") as f:
                metadata = json.load(f)

            aggregated_expert_assistance_data.append(metadata['expert_assistance']['tracker'])
            agent_run_length.append(metadata['total_time_steps'])
            success_tracker.append(1 if metadata['success'] else 0)
            termination_tracker.append(1 if metadata['terminated'] else 0)
            truncation_tracker.append(1 if metadata['truncated'] else 0)

        aggregated_agent_run_length.extend(agent_run_length)
        aggregated_success_tracker.extend(success_tracker)
        aggregated_termination_tracker.extend(termination_tracker)
        aggregated_truncation_tracker.extend(truncation_tracker)

        # Analyse Actions & Scenario-following
        actionic_performance_stats = analyze_agent_actions(actionic_data, success_tracker, scenario)
        aggregated_actionic_performance_stats[scenario] = actionic_performance_stats

    # Analyse expert assistance
    expert_assistance_stats = analyse_expert_assistance(aggregated_expert_assistance_data)

    # Analyse error recovery
    error_recovery_stats = analyse_error_recovery(aggregated_agent_reward_data, aggregated_success_tracker)

    # Analyse Agent Performance
    agent_performance_stats = {
        "avg_agent_run_length": sum(aggregated_agent_run_length) / len(aggregated_agent_run_length),
        "completion_rate": sum(aggregated_success_tracker) / len(aggregated_success_tracker),
        "truncation_rate": sum(aggregated_truncation_tracker) / len(aggregated_truncation_tracker),
        "termination_rate": sum(aggregated_termination_tracker) / len(aggregated_termination_tracker),
        "perf_str": f"Total={len(aggregated_success_tracker)}, Success={sum(aggregated_success_tracker)}, Truncated={sum(aggregated_truncation_tracker)}, Terminated={sum(aggregated_termination_tracker)}",
    }

    analysis = {
        "agent_performance": agent_performance_stats,
        "actionic_performance": aggregated_actionic_performance_stats,
        "expert_assistance": expert_assistance_stats,
        "error_recovery": error_recovery_stats
    }

    print(json.dumps(analysis, indent=4))
    with open(os.path.join(result_dir, "aggregated_agent_performance_analysis.json"), "w") as f:
        json.dump(analysis, f, indent=4)

    plot_error_histogram(aggregated_error_profile, f"{result_dir}/aggregated_error_freq_agent_no_expert_assist.png")


def compare_error_profiles(result_dir: str):

    training_methods = ['dpo_agent_informed_assist', 'sft', 'base']
    scenarios = ["both_api_rag-api_before_rag", "both_api_rag-rag_before_api"]

    training_error_profile = {}
    for training_method in training_methods:
        training_method_result_dir = os.path.join(result_dir, training_method)

        # Collect aggregated error_profile
        aggregated_error_profile = defaultdict(int)
        for scenario in scenarios:
            # Analyse Error Profile
            error_profile = get_error_profile(os.path.join(training_method_result_dir, scenario))
            for key in error_profile.keys():
                aggregated_error_profile[key] += error_profile[key]

        training_error_profile[training_method] = aggregated_error_profile

    plot_error_frequencies_comparison(training_error_profile, f"{result_dir}/comparison_error_freq_{'_'.join(training_methods)}.png")


if __name__ == "__main__":
    # # For single scenario specific analysis
    # scenario_to_follow = "both_api_rag-api_before_rag"
    # result_dir = f"./results/bird-dev_test/granite-3.3-8b-instruct/sft/{scenario_to_follow}"
    # analyze_single_scenario_perf(result_dir=result_dir, scenario_to_follow=scenario_to_follow)

    # # For aggregated scenarios analysis
    result_dir = "./results/bird-dev_test/Mistral-7B-Instruct-v0.3/dpo_agent_informed_assist"
    analyse_aggregated_perf(result_dir)

    # # For comparison of error profiles between training methods
    # result_dir = "./results/bird-dev_test/granite-3.3-8b-instruct"
    # compare_error_profiles(result_dir)