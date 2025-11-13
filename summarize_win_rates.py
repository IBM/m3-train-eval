import os
import json
import argparse
import re

#CHANGE_STATS_DIR="/Users/siyuhuo/Desktop/GithubRepo/m3-train-eval/data/changed_state"
CHANGE_STATS_DIR="/proj/m3benchmark/m3data/0923/auxiliary_data"
def decode_toolcall(result):
    tool_jsons = re.findall(r'<tool_call>\s*(.*?)\s*</tool_call>', result, flags=re.DOTALL)

    # Convert JSON strings to dicts
    tool_calls = [json.loads(j) for j in tool_jsons]
    return tool_calls

def add_avgs(d: dict) -> dict:
    assert "total" in d
    assert "successes" in d
    assert "total_steps" in d

    d["success_rate"] = 0 if d["total"] == 0 else d["successes"] / d["total"]
    d["avg_steps"] = 0 if d["total"] == 0 else d["total_steps"] / d["total"]
    if d["total_tool_call"] == 0:
        d["tool_call_with_thought_ratio"] = 0
        d["tool_call_hallucination_ratio"] = 0
    else:
        d["tool_call_with_thought_ratio"] = d["tool_call_with_thought"]/ d["total_tool_call"]
        d["tool_call_hallucination_ratio"] = d["tool_call_hallucination"]/ d["total_tool_call"]
    return d
ood_domains =["chicago_crime","movie", "simpson_episodes", "movie_3",  "movies_4","video_games","movielens","public_review_platform"]

def compute_success_fraction(directory_path: str, change_file: str):
    # List all files matching the pattern


    matching_files = [
        f for f in os.listdir(directory_path)
        if f.startswith("trajectory_") and f.endswith(".json")
    ]

    if len(matching_files) == 0:
        print(f"COULDN'T FIND ANY FILES AT LOCATION: {directory_path}. ")
        print(f"Directory contents: {os.listdir(directory_path)}")
        raise Exception("No files found")

    with open(change_file) as f:
        print("changed data", change_file)
        changes = json.load(f)

    summary = {}
    s_keys = ["no_scenarios", "changed_scenarios", "unchanged_scenarios"]
    for k in s_keys:
        summary[k] = {"total": 0, "successes": 0, "total_steps": 0, "first_tool_call_with_thought":0, "tool_call_with_thought":0, "total_tool_call":0, "tool_call_hallucination":0 }

    for filename in matching_files:
        filepath = os.path.join(directory_path, filename)
        try:
            with open(filepath, 'r') as f:
                data = json.load(f)

            sample_id = data['sample_id']
            sample_id = sample_id.split("_")[:-1]
            sample_id = "_".join(sample_id)
            domain = data['domain']
            if (data["num_turns"] == 1 and domain in ood_domains and "change_stat_ood_single_turn.json" in change_file)\
                    or (data["num_turns"] >= 1 and domain not in ood_domains and "change_stat_test_multi_turn.json" in change_file) or\
                    (data["num_turns"] != 1 and domain in ood_domains and "change_stat_ood_multi_turn.json" in change_file) :
                pass
            else:
                continue

            ## tool call status
            first_tool_call_with_thought = []
            tool_call_with_thought = []
            total_tool_call = []
            tool_call_hallucination = []
            api_names = [ ap["function"]["name"] for ap in json.loads(data['tools'])]

            for j in range(len(data['interactions'])):
                output = data['interactions'][str(j)]['output']
                if '<tool_call>' in output["content"]:
                    thought = output["content"].split("<tool_call>")[0]
                    tool_call_text = output["content"][len(thought):]
                    tool_call = decode_toolcall(tool_call_text)
                    total_tool_call.extend(tool_call)
                    if len(thought) > 10:
                        tool_call_with_thought.extend(tool_call)
                        if j == 0:
                            first_tool_call_with_thought.extend(tool_call)
                    for tc in tool_call:
                        if tc['name'] not in api_names:
                            tool_call_hallucination.append(tc)
            ##
            if "_sc_" in sample_id:
                if domain in changes.keys() and sample_id in changes[domain]:
                    # Change scenario
                    summary["changed_scenarios"]["total"] += 1
                    summary["changed_scenarios"]["total_steps"] += data["metadata"]["total_time_steps"]
                    if data["metadata"].get("success") is True:
                        summary["changed_scenarios"]["successes"] += 1
                    summary["changed_scenarios"]["first_tool_call_with_thought"] += len(first_tool_call_with_thought)
                    summary["changed_scenarios"]["tool_call_with_thought"] +=  len(tool_call_with_thought)
                    summary["changed_scenarios"]["total_tool_call"] += len(total_tool_call)
                    summary["changed_scenarios"]["tool_call_hallucination"] += len(tool_call_hallucination)


                else:
                    # Unchange scenario
                    summary["unchanged_scenarios"]["total"] += 1
                    summary["unchanged_scenarios"]["total_steps"] += data["metadata"]["total_time_steps"]
                    if data["metadata"].get("success") is True:
                        summary["unchanged_scenarios"]["successes"] += 1
                    summary["unchanged_scenarios"]["first_tool_call_with_thought"] += len(first_tool_call_with_thought)
                    summary["unchanged_scenarios"]["tool_call_with_thought"] += len(tool_call_with_thought)
                    summary["unchanged_scenarios"]["total_tool_call"] += len(total_tool_call)
                    summary["unchanged_scenarios"]["tool_call_hallucination"] += len(tool_call_hallucination)
            else:
                # No scenario
                summary["no_scenarios"]["total"] += 1
                summary["no_scenarios"]["total_steps"] += data["metadata"]["total_time_steps"]
                if data["metadata"].get("success") is True:
                    summary["no_scenarios"]["successes"] += 1
                summary["no_scenarios"]["first_tool_call_with_thought"] += len(first_tool_call_with_thought)
                summary["no_scenarios"]["tool_call_with_thought"] += len(tool_call_with_thought)
                summary["no_scenarios"]["total_tool_call"] += len(total_tool_call)
                summary["no_scenarios"]["tool_call_hallucination"] += len(tool_call_hallucination)
        except (json.JSONDecodeError, IOError) as e:
            print(f"⚠️ Failed to load {filename}: {e}")


    overall = {"total": 0, "successes": 0, "total_steps": 0, "first_tool_call_with_thought":0, "tool_call_with_thought":0, "total_tool_call":0, "tool_call_hallucination":0}
    for k in s_keys:
        overall['total'] += summary[k]['total']
        overall['successes'] += summary[k]['successes']
        overall['total_steps'] += summary[k]['total_steps']
        overall["first_tool_call_with_thought"] += summary[k]["first_tool_call_with_thought"]
        overall["tool_call_with_thought"] += summary[k]["tool_call_with_thought"]
        overall["total_tool_call"] += summary[k]["total_tool_call"]
        overall["tool_call_hallucination"] += summary[k]["tool_call_hallucination"]
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
    parser.add_argument('--input_metadata_dir', '-i', default="/Users/benjaminelder/reward_models/m3-train-eval/overnight/granite4/", help="Input Directory Name.")
    parser.add_argument('--model','-m',default="granite3",help="Model name to evaluate")
    parser.add_argument('--output_file', '-of')
    args = parser.parse_args()
    print(args.split_type,args.model)

    global_summary = {
        "all_eval_sets": {"total": 0, "successes": 0, "total_steps": 0,
                          "first_tool_call_with_thought":0,
                          "tool_call_with_thought":0,
                          "total_tool_call":0,
                          "tool_call_hallucination":0
                          }
    }

    for s in args.split_type:
        change_file = os.path.join(CHANGE_STATS_DIR,f"change_stat_{s}.json")
        # input_file = os.path.join(args.input_metadata_dir,f"{args.model}/{s}_mixed/metadata")
        input_file = args.input_metadata_dir
        summary = compute_success_fraction(input_file, change_file)
        global_summary[s] = summary
        global_summary["all_eval_sets"]['total'] += summary['overall']['total']
        global_summary["all_eval_sets"]['successes'] += summary['overall']['successes']
        global_summary["all_eval_sets"]['total_steps'] += summary['overall']['total_steps']

        global_summary["all_eval_sets"]["first_tool_call_with_thought"] += summary['overall']["first_tool_call_with_thought"]
        global_summary["all_eval_sets"]["tool_call_with_thought"] += summary['overall']["tool_call_with_thought"]
        global_summary["all_eval_sets"]["total_tool_call"] += summary['overall']["total_tool_call"]
        global_summary["all_eval_sets"]["tool_call_hallucination"] += summary['overall']["tool_call_hallucination"]


    global_summary["all_eval_sets"] = add_avgs(global_summary["all_eval_sets"])

    with open(args.output_file, "w") as f:
        json.dump(global_summary, f)
    print("sumarize: global ood_multi multi ood_single")
    print(f"{args.input_metadata_dir.split('/')[-2]},{global_summary['all_eval_sets']['success_rate']},{global_summary['ood_multi_turn']['overall']['success_rate']},{global_summary['test_multi_turn']['overall']['success_rate']},{global_summary['ood_single_turn']['overall']['success_rate']}")
