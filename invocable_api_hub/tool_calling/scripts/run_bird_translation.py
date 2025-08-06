
import argparse
import json
import os

from invocable_api_hub.tool_calling.sql_to_api.sql_sequencing_dataset_builder import SqlSequencingDatasetBuilder
from invocable_api_hub.tool_calling.sql_to_api.sql_slot_filling_dataset_builder import SqlSlotFillingDatasetBuilder
from invocable_api_hub.tool_calling.scripts.main_fcn import main


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Load source nl2sql data and generate API-sequence data")
    parser.add_argument('-m', '--mode', type=str, choices=['train', 'dev'], default='dev', help='run on the train or dev set', required=False)
    parser.add_argument('-s', '--size', type=str, choices=['small', 'large'], default='small', help='run all databases or a subset', required=False)
    args = parser.parse_args()

    mode = args.mode
    size = args.size

    db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../../db')
    cache_path = os.path.join(db_path, 'cache')

    if mode == 'train' and size == 'large':
        datasets = ['app_store', 'law_episode', 'citeseer', 'retail_world', 
                    'college_completion', 'coinmarketcap', 'human_resources', 
                    'beer_factory', 'food_inspection_2', 'authors', 'cars', 
                    'book_publishing_company', 'codebase_comments', 'synthea', 
                    'european_football_1', 'movielens', 'world_development_indicators', 
                    'craftbeer', 'address', 'bike_share_1', 'mental_health_survey', 
                    'food_inspection', 'mondial_geo', 'legislator', 'cookbook', 
                    'olympics', 'soccer_2016', 'public_review_platform', 
                    'movies_4', 'airline', 'video_games', 'university', 
                    'movie_platform', 'sales', 'genes', 'software_company', 
                    'hockey', 'menu', 'retail_complains', 'restaurant', 
                    'car_retails', 'donor', 'talkingdata', 'cs_semester', 
                    'language_corpus', 'ice_hockey_draft', 'world', 'regional_sales', 
                    'retails', 'shakespeare', 'superstore', 'sales_in_weather', 
                    'works_cycles', 'movie_3', 'social_media', 'chicago_crime', 
                    'disney', 'books', 'image_and_language', 'professional_basketball', 
                    'simpson_episodes', 'music_platform_2', 'student_loan', 
                    'computer_student', 'shooting', 'music_tracker', 'trains', 
                    'shipping', 'movie']
    elif mode == 'dev' and size == 'large':
        datasets = ['debit_card_specializing', 'formula_1', 'thrombosis_prediction', 
                    'student_club', 'california_schools', 'european_football_2', 
                    'card_games', 'toxicology', 'financial', 
                    'codebase_community', 'superhero']
    elif mode == 'train' and size == 'small':
        datasets = [
            "disney",
        ]
    elif mode == 'dev' and size == 'small':
        datasets = ['california_schools']

    for dataset in datasets:

        # Search for queries file. If not found, extract from global queries file. 
        if mode == 'train':
            qpath = os.path.join(db_path, 'train_queries', f"train_{dataset}.json")
            backup_qpath = os.path.join(db_path, "train.json")
            print(f"Loaded database {dataset} from train_queries")
        elif mode == 'dev':
            qpath = os.path.join(db_path, 'dev_queries', f"dev_{dataset}.json")
            backup_qpath = os.path.join(db_path, "dev.json")
            print(f"Loaded database {dataset} from dev_queries")

        try:
            with open(qpath) as f:
                query_data = json.load(f)
            print(f"Loaded {len(query_data)} prefiltered queries from database {dataset} in mode {mode}")
        except:
            try:
                with open(backup_qpath) as f:
                    query_data = json.load(f)
                total_lth = len(query_data)
                query_data = [q for q in query_data if q['db_id'] == dataset]
                print(f"Loaded {total_lth} queries in mode {mode} and filtered {len(query_data)} from database {dataset}.")
                with open(qpath, 'w') as f:
                    json.dump(query_data, f)
            except:
                print(f"FAILED TO LOAD: {dataset} IN MODE {mode}")
                print(f"{backup_qpath} exists: {os.path.isfile(backup_qpath)}")

        questions = [q['question'] for q in query_data]
        queries = [q['SQL'] for q in query_data]
        if mode == 'train':
            db_path_full = db_path + "/train_databases/"
        elif mode == 'dev':
            db_path_full = db_path + "/dev_databases/"
        output_filename_slot_filling = f"sql_translation_output/{mode}/slot_filling_bird_{dataset}.json"
        ds_builder_slot_filling = SqlSlotFillingDatasetBuilder(dataset, db_path_full, cache_location=cache_path, source_dataset_name="bird")
        ds_builder_slot_filling.build()
        sf_report = main(dataset, output_filename_slot_filling, queries, questions, ds_builder_slot_filling, cache_path)

        output_filename_sequencing = f"sql_translation_output/{mode}/sequencing_bird_{dataset}.json"
        ds_builder_sequencing = SqlSequencingDatasetBuilder(dataset, db_path_full, cache_location=cache_path, source_dataset_name="bird")
        ds_builder_sequencing.build()
        seq_report = main(dataset, output_filename_sequencing, queries, questions, ds_builder_sequencing, cache_path)
    
