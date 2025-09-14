#!/bin/bash
log () {
    d=`date -u '+%Y-%m-%dT%H:%M:%S.%3NZ'`
    echo $d 'run_exploratory.sh [INFO ]:' $*
}
logLine () {
    log '---------------------------------------------------------------------------'
}
logHeader () {
    logLine
    log $*
    logLine
}

logHeader 'Started running'
runStart=$(date +%s)

set -e
set -x

export PYTHONPATH=.

RUN_DATE=$(date "+%m%d")
mkdir -p runs/${RUN_DATE}

# Multi-turn Domains
# DB_ID=( "address" "airline" "app_store" "authors" "beer_factory" "bike_share_1" "book_publishing_company" "books" "cars" "chicago_crime" "citeseer" "codebase_comments" "coinmarketcap" "college_completion" "computer_student" "cookbook" "disney" "european_football_1" "food_inspection" "genes" "hockey" "ice_hockey_draft" "image_and_language" "law_episode" "mental_health_survey" "menu" "mondial_geo" "movie_3" "movielens" "movie" "movies_4" "music_tracker" "olympics" "professional_basketball" "public_review_platform" "restaurant" "sales_in_weather" "shakespeare" "simpson_episodes" "soccer_2016" "student_loan" "talkingdata" "trains" "university" "video_games" "world_development_indicators" "world" )
# INPUT_DIR="/proj/m3benchmark/m3data/0905/m3_train_test_ood_rest_v2_after_generate/final/"
# OUTPUT_DIR="/proj/m3benchmark/m3data/0905/balanced_rest_v4_exploratory_trajectory/"

# Single Turn Domains
# DB_ID=( "restaurant"  "simpson_episodes"  "computer_student"  "european_football_1"  "video_games"  "university"  "movielens"  "olympics"  "music_tracker"  "ice_hockey_draft"  "student_loan"  "image_and_language"  "menu"  "book_publishing_company"  "chicago_crime"  "cookbook"  "movies_4"  "genes"  "hockey"  "public_review_platform"  "citeseer"  "soccer_2016"  "cars"  "address"  "sales_in_weather"  "books"  "law_episode"  "beer_factory"  "movie"  "app_store"  "food_inspection"  "airline"  "talkingdata"  "professional_basketball"  "shakespeare"  "bike_share_1"  "mondial_geo"  "mental_health_survey"  "authors"  "movie_3"  "college_completion"  "trains"  "coinmarketcap"  "world"  "disney"  "world_development_indicators"  "codebase_comments" )
# INPUT_DIR="/proj/m3benchmark/m3data/0905/m3_train_test_ood_rest_v2_single_turn_after_generate/final/"
# OUTPUT_DIR="/proj/m3benchmark/m3data/0905/m3_train_test_ood_rest_v2_single_turn_exploratory_trajectory/"

mkdir -p ${OUTPUT_DIR}

CCC_CMD_NO_GPU='bsub -n 1 -U infusion -R "rusage[mem=20GB, cpu=4]"'
CCC_CMD_GPU='bsub -n 1 -U infusion -R "rusage[mem=20GB, cpu=4]" -gpu num=1'
GENRATION_CMD="PYTHONPATH=./ python run.py"

for id in "${DB_ID[@]}"; do
    gen_cmd_args="-i ${INPUT_DIR}/${id}_multiturn_bird_chunked_final.json -o ${OUTPUT_DIR}"
    g_cmd="${CCC_CMD_NO_GPU} -o runs/${RUN_DATE}/%J_${id}.log ${GENRATION_CMD} ${gen_cmd_args}"
    logHeader "Running: ${g_cmd}"
    eval "$g_cmd"
done