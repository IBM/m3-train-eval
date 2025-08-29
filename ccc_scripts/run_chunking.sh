#!/bin/bash
log () {
    d=`date -u '+%Y-%m-%dT%H:%M:%S.%3NZ'`
    echo $d 'run_chunking.sh [INFO ]:' $*
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

DB_ID=( "address" "airline" "app_store" "authors" "beer_factory" "bike_share_1" "book_publishing_company" "books" "cars" "chicago_crime" "citeseer" "codebase_comments" "coinmarketcap" "college_completion" "computer_student" "cookbook" "disney" "european_football_1" "food_inspection" "genes" "hockey" "ice_hockey_draft" "image_and_language" "law_episode" "mental_health_survey" "menu" "mondial_geo" "movie_3" "movielens" "movie" "movies_4" "music_tracker" "olympics" "professional_basketball" "public_review_platform" "restaurant" "sales_in_weather" "shakespeare" "simpson_episodes" "soccer_2016" "student_loan" "talkingdata" "trains" "university" "video_games" "world_development_indicators" "world" )

INPUT_DIR="data/balanced_rest_v2"
OUTPUT_DIR="data/test_chunked_balanced_rest_v2"
TOPK=5

CCC_CMD_NO_GPU='bsub -n 1 -U infusion -R "rusage[mem=20GB, cpu=4]"'
CCC_CMD_GPU='bsub -n 1 -U infusion -R "rusage[mem=20GB, cpu=4]" -gpu num=1'
GENRATION_CMD="PYTHONPATH=./ python -m ground_truth.chunk"

for id in "${DB_ID[@]}"; do
    gen_cmd_args="-i ${INPUT_DIR} -o ${OUTPUT_DIR} --topk ${TOPK} --domain ${id}"
    g_cmd="${CCC_CMD_NO_GPU} -o runs/${RUN_DATE}/%J_${id}.log ${GENRATION_CMD} ${gen_cmd_args}"
    logHeader "Running: ${g_cmd}"
    eval "$g_cmd"
done