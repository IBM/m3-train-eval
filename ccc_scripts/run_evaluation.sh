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


# Multi-turn Domains
# DB_ID=( "restaurant" "computer_student" "european_football_1" "university" "olympics" "music_tracker" "ice_hockey_draft" "student_loan" "image_and_language" "menu" "book_publishing_company" "cookbook" "genes" "hockey" "citeseer" "soccer_2016" "cars" "address" "sales_in_weather" "books" "law_episode" "beer_factory" "app_store" "food_inspection" "airline" "talkingdata" "professional_basketball" "shakespeare" "bike_share_1" "mondial_geo" "mental_health_survey" "authors" "college_completion" "trains" "coinmarketcap" "world" "disney" "world_development_indicators" "codebase_comments" )
# CONFIG_FILE="config_files/infer_agent_${MODEL_NAME}"
MODEL_NAME=( "mixtral22b" "mistral7b" "granite38b" )
INPUT_TYPE=( "ood_multi_turn_mixed" "ood_single_turn_mixed" "test_multi_turn_mixed")
# INPUT_DIR="/proj/m3benchmark/m3data/0923/evaluation_data/${INPUT_TYPE}.json"
# OUTPUT_DIR="/proj/m3benchmark/m3data/0923/m3_test_evaluation/${MODEL_NAME}/${INPUT_TYPE}/"

# mkdir -p ${OUTPUT_DIR}

CCC_CMD_NO_GPU='bsub -n 1 -U infusion -R "rusage[mem=20GB, cpu=4]"'
CCC_CMD_GPU='bsub -n 1 -U infusion -gpu "num=1:mode=exclusive_process:gmodel=NVIDIAA100_SXM4_80GB" -R "rusage[mem=64GB, cpu=4]"'
GENRATION_CMD="PYTHONPATH=./ python run.py"

for mn in "${MODEL_NAME[@]}"; do
    for it in "${INPUT_TYPE[@]}"; do
        mkdir -p runs/${RUN_DATE}/${mn}/${it}
        gen_cmd_args="-i /proj/m3benchmark/m3data/0923/evaluation_data/${it}.json -o /proj/m3benchmark/m3data/0923/m3_test_evaluation/${mn}/${it}/ -ic /dccstor/arnaik_data/routing/m3-train-eval/config_files/evaluation/infer_agent_${mn}.json"
        g_cmd="${CCC_CMD_GPU} -o runs/${RUN_DATE}/${mn}/${it}/%J_${id}.log ${GENRATION_CMD} ${gen_cmd_args}"
        logHeader "Running: ${g_cmd}"
        eval "$g_cmd"
    done
done