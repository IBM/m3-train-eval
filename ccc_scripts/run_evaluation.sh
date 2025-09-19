#!/bin/bash
log () {
    d=`date -u '+%Y-%m-%dT%H:%M:%S.%3NZ'`
    echo $d 'run_evaluation.sh [INFO ]:' $*
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
DB_ID=( "restaurant" "computer_student" "european_football_1" "university" "olympics" "music_tracker" "ice_hockey_draft" "student_loan" "image_and_language" "menu" "book_publishing_company" "cookbook" "genes" "hockey" "citeseer" "soccer_2016" "cars" "address" "sales_in_weather" "books" "law_episode" "beer_factory" "app_store" "food_inspection" "airline" "talkingdata" "professional_basketball" "shakespeare" "bike_share_1" "mondial_geo" "mental_health_survey" "authors" "college_completion" "trains" "coinmarketcap" "world" "disney" "world_development_indicators" "codebase_comments" )
INPUT_DIR="/proj/m3benchmark/m3data/0905/m3_train_test_ood_rest_v2/test_chunked_gt/final/"
OUTPUT_DIR="/proj/m3benchmark/m3data/0905/m3_test_evaluation/mixtral22b"

mkdir -p ${OUTPUT_DIR}

CCC_CMD_NO_GPU='bsub -n 1 -U infusion -R "rusage[mem=20GB, cpu=4]"'
CCC_CMD_GPU='bsub -n 1 -U infusion -gpu "num=1:mode=exclusive_process:gmodel=NVIDIAA100_SXM4_80GB" -R "rusage[mem=64GB, cpu=4]"'
GENRATION_CMD="PYTHONPATH=./ python run.py"

for id in "${DB_ID[@]}"; do
    gen_cmd_args="-i ${INPUT_DIR}/${id}_multiturn_bird_chunked_final.json -o ${OUTPUT_DIR}"
    g_cmd="${CCC_CMD_GPU} -o runs/${RUN_DATE}/%J_${id}.log ${GENRATION_CMD} ${gen_cmd_args}"
    logHeader "Running: ${g_cmd}"
    eval "$g_cmd"
done