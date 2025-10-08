import os
import json

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
                    if data.get("success") is True:
                        summary["changed_scenarios"]["successes"] += 1
                        summary["changed_scenarios"]["total_steps"] += data["total_time_steps"]
                else:
                    # Unchange scenario
                    summary["unchanged_scenarios"]["total"] += 1
                    if data.get("success") is True:
                        summary["unchanged_scenarios"]["successes"] += 1
                        summary["unchanged_scenarios"]["total_steps"] += data["total_time_steps"]
            else:
                # No scenario
                summary["no_scenarios"]["total"] += 1
                if data.get("success") is True:
                    summary["no_scenarios"]["successes"] += 1
                    summary["no_scenarios"]["total_steps"] += data["total_time_steps"]
        except (json.JSONDecodeError, IOError) as e:
            print(f"⚠️ Failed to load {filename}: {e}")


    for k in s_keys:
        summary[k]["success_rate"] = 0 if summary[k]["total"] == 0 else summary[k]["successes"] / summary[k]["total"]
        summary[k]["avg_steps"] = 0 if summary[k]["total"] == 0 else summary[k]["total_steps"] / summary[k]["total"]
        print(f"Summary for {k}")
        print(summary[k])
        print('\n\n')


# Example usage
if __name__ == "__main__":
    change_file = "/proj/m3benchmark/m3data/0923/auxiliary_data/change_stat_test_chunked_scenarios.json"
    input_file = "/proj/m3benchmark/siyu/m3-train-eval/ccc_eval/g4-gt_dummy_all/metadata"
    compute_success_fraction(input_file, change_file)
