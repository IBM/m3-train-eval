import os
import json
import argparse

CHANGE_STATS_DIR="/proj/m3benchmark/m3data/0923/auxiliary_data"

def add_avgs(d: dict) -> dict:
    assert "total" in d
    assert "successes" in d
    assert "total_steps" in d

    d["success_rate"] = 0 if d["total"] == 0 else d["successes"] / d["total"]
    d["avg_steps"] = 0 if d["total"] == 0 else d["total_steps"] / d["total"]
    return d


def compute_success_fraction(directory_path: str, change_file: str):
    # List all files matching the pattern
    matching_files = [
        f for f in os.listdir(directory_path)
        if f.startswith("metadata_") and f.endswith(".json")
    ]

    with open(change_file) as f:
        changes = json.load(f)

    summary = {}
    s_keys = ["no_scenarios", "changed_scenarios", "unchanged_scenarios"]
    for k in s_keys:
        summary[k] = {"total": 0, "successes": 0, "total_steps": 0}

    for filename in matching_files:
        filepath = os.path.join(directory_path, filename)
        try:
            with open(filepath, 'r') as f:
                data = json.load(f)
            sample_id = data['sample_id']
            domain = data['domain']
            if "_sc_" in sample_id:
                if domain in changes.keys() and sample_id in changes[domain]:
                    # Change scenario
                    summary["changed_scenarios"]["total"] += 1
                    summary["changed_scenarios"]["total_steps"] += data["total_time_steps"]
                    if data.get("success") is True:
                        summary["changed_scenarios"]["successes"] += 1
                else:
                    # Unchange scenario
                    summary["unchanged_scenarios"]["total"] += 1
                    summary["unchanged_scenarios"]["total_steps"] += data["total_time_steps"]
                    if data.get("success") is True:
                        summary["unchanged_scenarios"]["successes"] += 1
            else:
                # No scenario
                summary["no_scenarios"]["total"] += 1
                summary["no_scenarios"]["total_steps"] += data["total_time_steps"]
                if data.get("success") is True:
                    summary["no_scenarios"]["successes"] += 1
        except (json.JSONDecodeError, IOError) as e:
            print(f"⚠️ Failed to load {filename}: {e}")

    overall = {"total": 0, "successes": 0, "total_steps": 0}
    for k in s_keys:
        overall['total'] += summary[k]['total']
        overall['successes'] += summary[k]['successes']
        overall['total_steps'] += summary[k]['total_steps']
        summary[k] = add_avgs(summary[k])
        print(f"Summary for {k}")
        print(summary[k])
        print('\n\n')
    summary['overall'] = add_avgs(overall)
    return summary


# Example usage
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--split_type","-s", nargs='+', type=str, help='List of types')
    parser.add_argument('--input_metadata_dir', '-i', default="/proj/m3benchmark/m3data/0923/m3_test_evaluation/baseline/v3/", help="Input Directory Name.")
    parser.add_argument('--model','-m',default="granite38b",help="Model name to evaluate")
    parser.add_argument('--output_file', '-of')
    args = parser.parse_args()
    print(args.split_type,args.model)

    global_summary = {
        "all_eval_sets": {"total": 0, "successes": 0, "total_steps": 0}
    }
    for s in args.split_type:
        change_file = os.path.join(CHANGE_STATS_DIR,f"change_stat_{s}.json")
        input_file = os.path.join(args.input_metadata_dir,f"{args.model}/{s}_mixed/metadata")
        summary = compute_success_fraction(input_file, change_file)
        global_summary[s] = summary
        global_summary["all_eval_sets"]['total'] += summary['overall']['total']
        global_summary["all_eval_sets"]['successes'] += summary['overall']['successes']
        global_summary["all_eval_sets"]['total_steps'] += summary['overall']['total_steps']
    global_summary["all_eval_sets"] = add_avgs(global_summary["all_eval_sets"])

    with open(args.output_file, "w") as f:
        json.dump(global_summary, f)
