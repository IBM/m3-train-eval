#!/bin/bash


my_list=(
    authors
    restaurant
    codebase_comments
    computer_student
    european_football_1
    beer_factory
    cars
    university
    olympics
    shakespeare
    olympics
    citeseer
    talkingdata
    music_tracker
    university
    genes
    student_loan
    ice_hockey_draft
    bike_share_1
    student_loan
    image_and_language
    menu
    book_publishing_company
    law_episode
    world
    mental_health_survey
    book_publishing_company
    cookbook
    genes
    hockey
    citeseer
    soccer_2016
    hockey
    cars
    address
    image_and_language
    music_tracker
    sales_in_weather
    books
    ice_hockey_draft
    law_episode
    beer_factory
    world_development_indicators
    restaurant
    app_store
    food_inspection
    disney
    computer_student
    trains
    books
    airline
    airline
    menu
    talkingdata
    professional_basketball
    cookbook
    soccer_2016
    shakespeare
    bike_share_1
    mondial_geo
    mental_health_survey
    authors
    college_completion
    college_completion
    coinmarketcap
    address
    mondial_geo
    trains
    coinmarketcap
    european_football_1
    professional_basketball
    world
    app_store
    food_inspection
    disney
    world_development_indicators
    sales_in_weather
    codebase_comments
    )


src_dirs=(
    "/proj/m3benchmark/m3data/0905/m3_train_test_ood_rest_v2_after_generate/final/" # multi-turn no scenarios
    "/proj/m3benchmark/danish/m3data/0905/m3_train_test_ood_rest_v2_chunked_scenarios_gt/final/" # multi-turn with scenarios
    "/proj/m3benchmark/m3data/0905/m3_train_test_ood_rest_v2_single_turn_after_generate/final/" # single-turn no scenarios
    "/proj/m3benchmark/m3data/0905/m3_train_test_ood_rest_v2_single_turn_chunked_scenarios/final/" # single-turn with scenarios
    )
dst_dirs=(
    "/proj/m3benchmark/m3data/0923/m3_train_test_ood_rest_v2_expert/"
    "/proj/m3benchmark/m3data/0923/m3_train_test_ood_rest_v2_scenarios_expert/" 
    "/proj/m3benchmark/m3data/0923/m3_train_test_ood_rest_v2_single_turn_expert/"
    "/proj/m3benchmark/m3data/0923/m3_train_test_ood_rest_v2_single_turn_scenarios_expert/" 
    )
labels=(
    "mt_ns"
    "mt_ws" 
    "st_ns"
    "st_ws" 
    )

# Check if lengths match
if [ ${#src_dirs[@]} -ne ${#dst_dirs[@]} ]; then
  echo "Error: source and destination lists are not the same length."
  exit 1
fi

# Loop through the arrays
for i in "${!src_dirs[@]}"; do
    input_file="${src_dirs[$i]}"
    output_dir="${dst_dirs[$i]}"
    label="${labels[$i]}"

    echo "Processing pair: $src -> $dst"
    for item in "${my_list[@]}"; do
        echo "Running dataset ${item}"
        input_file=${input_dir}${item}_multiturn_bird_chunked_final.json
        bsub -oo logging/${label}_${item}.log -eo logging/${label}_${item}.log -J ${label}_${item} -n 1 PYTHONPATH=./ python run.py --output_dir ${output_dir} --input_filename ${input_file}
    done

done

