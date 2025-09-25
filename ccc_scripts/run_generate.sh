#!/bin/bash
log () {
    d=`date -u '+%Y-%m-%dT%H:%M:%S.%3NZ'`
    echo $d 'run_generate.sh [INFO ]:' $*
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

# # TRAIN DOMAINS
# DB_ID=( "address" "airline" "app_store" "authors" "beer_factory" "bike_share_1" "book_publishing_company" "books" "cars" "chicago_crime" "citeseer" "codebase_comments" "coinmarketcap" "college_completion" "computer_student" "cookbook" "disney" "european_football_1" "food_inspection" "genes" "hockey" "ice_hockey_draft" "image_and_language" "law_episode" "mental_health_survey" "menu" "mondial_geo" "movie_3" "movielens" "movie" "movies_4" "music_tracker" "olympics" "professional_basketball" "public_review_platform" "restaurant" "sales_in_weather" "shakespeare" "simpson_episodes" "soccer_2016" "student_loan" "talkingdata" "trains" "university" "video_games" "world_development_indicators" "world" )

# INPUT_DIR="data/chunked_balanced_rest_v2"
# OUTPUT_DIR="ground_truth/balanced_rest_v2/"

# TEST DOMAINS with and without scenarios
DB_ID=( "world" "disney" "cars" "authors" "computer_student" "music_tracker" "menu" "sales_in_weather" "student_loan" "address" "professional_basketball" "citeseer" "coinmarketcap" "books" "mondial_geo" "shakespeare" "book_publishing_company" "soccer_2016" "talkingdata" "beer_factory" "restaurant" "cookbook" "mental_health_survey" "app_store" "bike_share_1" "world_development_indicators" "european_football_1" "law_episode" "genes" "food_inspection" "university" "ice_hockey_draft" "airline" "image_and_language" "college_completion" "hockey" "olympics" "codebase_comments" "trains" )
INPUT_DIR="/proj/m3benchmark/m3data/0923/data/test/multi/no_scenarios/chunked"
OUTPUT_DIR="/proj/m3benchmark/m3data/0923/data/test/multi/no_scenarios/generate"

# # OOD MULTI DOMAINS with and without scenarios
# DB_ID=( "movie_3" "movie" "movielens" "video_games" "chicago_crime" "simpson_episodes" "public_review_platform" "movies_4" )
# INPUT_DIR="/proj/m3benchmark/m3data/0923/data/ood/multi/no_scenarios/chunked"
# OUTPUT_DIR="/proj/m3benchmark/m3data/0923/data/ood/multi/no_scenarios/chunked"

# # OOD SINGLE DOMAINS with and without scenarios
# DB_ID=( "movie_3" "movie" "movielens" "video_games" "chicago_crime" "simpson_episodes" "public_review_platform" "movies_4" )
# INPUT_DIR="data/chunked_balanced_rest_v2"
# OUTPUT_DIR="ground_truth/balanced_rest_v2/"

CCC_CMD_NO_GPU='bsub -n 1 -U infusion -R "rusage[mem=20GB, cpu=4]"'
CCC_CMD_GPU='bsub -n 1 -U infusion -R "rusage[mem=20GB, cpu=4]" -gpu num=1'
GENRATION_CMD="PYTHONPATH=./ python ground_truth/generate.py --no_thoughts true"

for id in "${DB_ID[@]}"; do
    gen_cmd_args="-i ${INPUT_DIR} -o ${OUTPUT_DIR} --domain ${id}"
    g_cmd="${CCC_CMD_NO_GPU} -o runs/${RUN_DATE}/%J_${id}.log ${GENRATION_CMD} ${gen_cmd_args}"
    logHeader "Running: ${g_cmd}"
    eval "$g_cmd"
done