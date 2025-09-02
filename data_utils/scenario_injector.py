import copy
import copyreg
import json
from abc import ABC, abstractmethod
import json
from enum import Enum
from typing import List
import argparse
from pathlib import Path
import yaml
import random

class QuestionMergedType(Enum):
    """
    reasoning type of the question, based on RAG/API condition
    """
    RAG_BEFORE_API = "(RAG-API)"
    API_BEFORE_RAG = "(API-RAG)"
    #API_AND_RAG = "API_AND_RAG"
    ONLY_RAG = "(RAG)"
    ONLY_API = "(API)"
    #NOT_API_RAG = "NOT_API_RAG"
    RAG_API_RAG = "(RAG-API-RAG)"
    API_RAG_API = "(API-RAG-API)"
    API_API = "(API-API)"
    API_API_API = "(API-API-API)"
    RAG_RAG = "(RAG-RAG)"
    RAG_RAG_RAG = "(RAG-RAG-RAG)"

class ToolUsePolicy(Enum):
    """
    Guide/ask agent how to use RAG/API
    """
    ONLY_API = "Do not use RAG, only use API."
    ONLY_RAG = "Do not use API, only use RAG."
    RAG_FIRST = "Must use RAG tool first"
    API_FIRST = "Must use API tool first."


class ToolAvailability(Enum):
    """
    configure if RAG/API is available in environment, or if they function properly.
    """
    ONLY_RAG = "Only RAG is available, API is unavailable."
    ONLY_API = "Only API is unavailable, RAG is unavailable."
    BOTH_API_RAG = "Both API and RAG are available."
    NEITHER_API_RAG = "Neither API nor RAG is available."



class APIsAvailability(Enum):
    """
    configure if ground truth api is available for answer the api sub-question.
    """
    INCLUDE_GT = "API list include ground truth API."
    EXCLUDE_GT = "API list does not include ground truth API."


class OperationType(Enum):
    SET_POLICY_DOMAIN = "set policy domain"
    SET_TOOL_USE_POLICY = "set tool use policy"
    SET_TOOL_AVAILABILITY = "set tool availability"
    SET_API_AVAILABILITY = "set api availability"


class Operation(ABC):
    """Abstract base class for JSON operations."""

    @abstractmethod
    def apply(self, v, json_data: dict) -> dict:
        """Apply the operation to the JSON data."""
        pass


class SetPolicyDomain(Operation):
    """
    choose domain of processed datapoint.
    """
    def apply(self,domain, json_data):
        json_data_copy = copy.deepcopy(json_data)
        json_data_copy["scenarios"]['policy_domain'] = domain
        return json_data_copy


# class SetQuestionMergedType(Operation):
#     """
#     choose question type  processed datapoint
#     """
#     def __init__(self, value: List[QuestionMergedType]):
#         self.value = value
#
#     def apply(self,v, json_data):
#         res = []
#         for v in self.value:
#             for jd in json_data:
#                 json_data_copy = copy.deepcopy(jd)
#                 json_data_copy["scenarios"]['question_merged_type'] = v.value
#                 res.append(json_data_copy)
#         json_data[:] = res


class SetToolUsePolicy(Operation):
    """
    set tool policy to answer a question
    """

    def apply(self,v, json_data):
      
        json_data_copy = copy.deepcopy(json_data)
        json_data_copy["scenarios"]['tool_use_policy'] = v.name
        data_domain = json_data_copy['dataset_name']
        for turn in json_data_copy["turns"]:
            question_merge_type = turn["type"]
            policy_domain = json_data_copy["scenarios"]["policy_domain"]
            if v == ToolUsePolicy.ONLY_API:
                if "RAG" in question_merge_type and policy_domain == data_domain:
                    turn["answer"] = "I can not answer."
                    rag_api_slots = question_merge_type.replace("(","").replace(")","").split("-")
                    if len(rag_api_slots) >= 2:
                        idx = rag_api_slots.index('RAG') #loc first RAG and mute the rest
                        for i in range(idx, len(rag_api_slots)):
                            turn["gold_sequence"][i]["answer"] = "I can not answer."

            elif v == ToolUsePolicy.ONLY_RAG:
                if "API" in question_merge_type and policy_domain == data_domain:
                    turn["answer"] = "I can not answer."
                    rag_api_slots = question_merge_type.replace("(", "").replace(")", "").split("-")
                    if len(rag_api_slots) >= 2:
                        idx = rag_api_slots.index('API') #loc first API and mute the rest
                        for i in range(idx,len(rag_api_slots)):
                            turn["gold_sequence"][i]["answer"] = "I can not answer."

            elif v == ToolUsePolicy.RAG_FIRST:
                rag_api_slots = question_merge_type.replace("(", "").replace(")", "").split("-")
                if rag_api_slots[0] != "RAG" and policy_domain == data_domain:
                    turn["answer"] = "I can not answer."
                    if len(rag_api_slots) >= 2:
                        for i in range(len(rag_api_slots)):
                            turn["gold_sequence"][i]["answer"] = "I can not answer."

            elif v == ToolUsePolicy.API_FIRST:
                rag_api_slots = question_merge_type.replace("(", "").replace(")", "").split("-")
                if rag_api_slots[0] != "API" and policy_domain == data_domain:
                    turn["answer"] = "I can not answer."
                    if len(rag_api_slots) >= 2:
                        for i in range(len(rag_api_slots)):
                            turn["gold_sequence"][i]["answer"] = "I can not answer."
        return json_data_copy

class SetToolAvailability(Operation):
    """
    set tool availability for the quesition, it is about environmental faiture
    """

    def apply(self, v, json_data):
        json_data_copy = copy.deepcopy(json_data)
        for turn in json_data_copy["turns"]:
            question_merge_type = turn["type"]
            json_data_copy["scenarios"]['tool_availability'] = v.name
            if v == ToolAvailability.ONLY_API:
                if "RAG" in question_merge_type:
                    turn["answer"] = "I can not answer."
                    rag_api_slots = question_merge_type.replace("(", "").replace(")", "").split("-")
                    if len(rag_api_slots) >= 2:
                        idx = rag_api_slots.index('RAG')  # loc first RAG and mute the rest
                        for i in range(idx, len(rag_api_slots)):
                            turn["gold_sequence"][i]["answer"] = "I can not answer"

            elif v == ToolAvailability.ONLY_RAG:
                if "API" in question_merge_type:
                    turn["answer"] = "I can not answer."
                    rag_api_slots = question_merge_type.replace("(", "").replace(")", "").split("-")
                    if len(rag_api_slots) >= 2:
                        idx = rag_api_slots.index('API')  # loc first API and mute the rest
                        for i in range(idx, len(rag_api_slots)):
                            turn["gold_sequence"][i]["answer"] = "I can not answer."

            elif v == ToolAvailability.NEITHER_API_RAG:
                turn["answer"] = "I can not answer."
                rag_api_slots = question_merge_type.replace("(", "").replace(")", "").split("-")
                if len(rag_api_slots) >= 2:
                    for i in range(len(rag_api_slots)):
                        turn["gold_sequence"][i]["answer"] = "I can not answer."

        return json_data_copy

class SetAPIsAvailability(Operation):
    """
    set api availability for each api sub-question.
    """
    def apply(self,v, json_data):
        # sample a missing api
        gd_api = []
        for turn in json_data["turns"]:
            for gs in turn['gold_sequence']:
                if 'output' in gs:
                    gd_api.extend([g["name"] for g in gs['output'] ])
        if len(gd_api) == 0:
            return {}
        missing_api = random.choice(gd_api)
        json_data_copy = copy.deepcopy(json_data)
        json_data_copy['tools'] = [item for item in json_data_copy["tools"] if item["name"] != missing_api]
        json_data_copy["scenarios"]['missing_api'] = missing_api
        for turn in json_data_copy["turns"]:
            question_merge_type = turn["type"]
            rag_api_slots = question_merge_type.replace("(", "").replace(")", "").split("-")
            if len(rag_api_slots) == 1:
                if "output" in turn['gold_sequence'][0]:
                    output_apis = [op["name"] for op in turn['gold_sequence'][0]["output"]]
                    if missing_api in output_apis:
                        turn["answer"] = "I can not answer."
            else:
                for i in range(len(rag_api_slots)):
                    if "output" in turn['gold_sequence'][i]:
                        output_apis = [op["name"] for op in turn['gold_sequence'][i]["output"]]
                        if missing_api in output_apis:
                            turn["answer"] = "I can not answer."
                        if "I can not answer" in turn["answer"]:
                            turn['gold_sequence'][i]["answer"] = "I can not answer."
        return json_data_copy


class InjectionPipeline:
    """
    apply scenarios injection to each datapoint, based on dictionary of operations. 
    """
    def __init__(self, json_file):
        with open(json_file) as f:
            self.json_data_list = json.load(f)

            for d in self.json_data_list:
                d["scenarios"] = {
                    "tool_use_policy" : None,
                    "policy_domain" : None,
                    "missing_api" : None,
                    "tool_availability" : None
                }
                for turn in d['turns']:
                    for gd in turn["gold_sequence"]:
                        if 'tools' in gd:
                           gd.pop("tools")


            self.set_domain = SetPolicyDomain()
            self.set_policy = SetToolUsePolicy()
            self.set_tool_avail = SetToolAvailability()
            self.set_api_avail = SetAPIsAvailability()

    # def insert_scenarios(self, json_data):
    #     for d in json_data:
    #         scenarios = d["scenarios"]
    #         if "tool_use_policy" in scenarios:
    #             tool_use_policy_name = scenarios["tool_use_policy"]
    #             policy = ToolUsePolicy[tool_use_policy_name].value
    #             scenarios_prompt = f"Below is the policy to use API/RAG tool:\n{policy}\n"
    #             d["scenarios_prompt"] = scenarios_prompt
    #         if "tool_availability" in scenarios:
    #             tool_availability_name = scenarios["tool_availability"]
    #             ta = ToolAvailability[tool_availability_name].value
    #             d["scenarios_env"] = []
    #             d["scenarios_env"].append(ta)
    #     return None
    def apply_operations(self, operations_dict: dict, n=-1):
        res = []
        if n > 0:
            json_data_copy = copy.deepcopy(self.json_data_list)[:n]
        else:
            json_data_copy = copy.deepcopy(self.json_data_list)

        for json_data in json_data_copy:
            # if json_data['sample_id']!= 7748:
            #     continue
            #rand_domain = random.choice(operations_dict.get(OperationType.SET_POLICY_DOMAIN.name))
            in_domain = json_data["dataset_name"]
            out_domain = random.choice([ dm for dm in operations_dict.get(OperationType.SET_POLICY_DOMAIN.name) if dm != in_domain])
            #rand_policy = random.choice(operations_dict.get(OperationType.SET_TOOL_USE_POLICY.name))
            rand_policy = ToolUsePolicy.RAG_FIRST
            sd_in = self.set_domain.apply(in_domain, json_data)
            sp_in = self.set_policy.apply(rand_policy, sd_in)  # depends on policy domain

            sd_out = self.set_domain.apply(out_domain, json_data)
            sp_out = self.set_policy.apply(rand_policy, sd_out)

            rand_tool_avail = random.choice(operations_dict.get(OperationType.SET_TOOL_AVAILABILITY.name))
            rand_api_avail = random.choice(operations_dict.get(OperationType.SET_API_AVAILABILITY.name))
            st = self.set_tool_avail.apply(rand_tool_avail, json_data)
            sa = self.set_api_avail.apply(rand_api_avail, json_data)
            res.extend( [x for x in [json_data,sp_in,sp_out,st,sa] if x is not None])
        return res



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--data_file', type=str, default="/Users/siyuhuo/Desktop/GithubRepo/agentic-middleware-playground/data/bird/car_retails_multiturn_bird.json",
                        help='api data file')
    parser.add_argument('--cluster_file', type=str,
                        default="/Users/siyuhuo/Desktop/GithubRepo/agentic-middleware-playground/agentic_bench/generated/domain_cluster_N.json",
                        help='api data file')
    parser.add_argument('--output_file', type=str,
                        default="generated/airline_multiturn_bird_scenarios.json", help='output file')
    parser.add_argument('--n', type=int, default=-1, help='inject scenarios based on n samples.')

    config = parser.parse_args()
    data_file = Path(config.data_file)
    output_file = Path(config.output_file)
    n = config.n
    ip = InjectionPipeline(data_file)
    operation_dict = {}

    # all domains
    domain_names = ['food_inspection', 'world_development_indicators', 'car_retails', 'shipping', 'card_games', 'software_company', 'bike_share_1', 'regional_sales', 'retail_world', 'shakespeare', 'european_football_1', 'works_cycles', 'olympics', 'image_and_language', 'craftbeer', 'book_publishing_company', 'toxicology', 'student_loan', 'retail_complains', 'financial', 'movielens', 'university', 'social_media', 'sales', 'world', 'codebase_community', 'codebase_comments', 'citeseer', 'cars', 'movie_3', 'superstore', 'chicago_crime', 'soccer_2016', 'synthea', 'thrombosis_prediction', 'european_football_2', 'app_store', 'college_completion', 'video_games', 'law_episode', 'professional_basketball', 'menu', 'shooting', 'trains', 'hockey', 'genes', 'movie', 'restaurant', 'computer_student', 'superhero', 'address', 'human_resources', 'simpson_episodes', 'talkingdata', 'public_review_platform', 'student_club', 'food_inspection_2', 'california_schools', 'movies_4', 'language_corpus', 'retails', 'music_platform_2', 'formula_1', 'ice_hockey_draft', 'sales_in_weather', 'donor', 'books', 'legislator', 'coinmarketcap', 'airline', 'beer_factory', 'mental_health_survey', 'disney', 'mondial_geo', 'movie_platform', 'authors', 'cs_semester', 'music_tracker', 'debit_card_specializing', 'cookbook']
    #question_merged_type = [QuestionMergedType.API_BEFORE_RAG]
    tool_use_policy = [e for e in ToolUsePolicy]
    tool_availability = [e for e in ToolAvailability]
    apis_availability = [e for e in APIsAvailability]

    operations_dict = {
        OperationType.SET_POLICY_DOMAIN.name: domain_names,
        OperationType.SET_TOOL_USE_POLICY.name: tool_use_policy,
        OperationType.SET_TOOL_AVAILABILITY.name: tool_availability,
        OperationType.SET_API_AVAILABILITY.name: apis_availability
    }
    pipeline = InjectionPipeline(data_file)
    output_json = pipeline.apply_operations(operations_dict, n)
    with open(output_file, 'w') as f:
        json.dump(output_json, f, indent=4)
    #print(output_json)
