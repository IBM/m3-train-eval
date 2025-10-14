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

# Setup the following locations and paramters for the run
MODEL_NAME=( "granite4" "granite3" "mistral" )
INPUT_DIR="/proj/m3benchmark/m3data/0923/m3_test_evaluation/sft/"
GENRATION_CMD="PYTHONPATH=./ python summarize_win_rates.py"

for mn in "${MODEL_NAME[@]}"; do
    if [ -e "$INPUT_DIR" ]; then
        export HF_HOME='/dccstor/belder1/cache/'
        export HF_CACHE='/dccstor/belder1/cache/'
        g_cmd="bsub -n 1 -J ${RUN_MODE}_${mn} -o logging/win_rate_${mn}.log ${GENRATION_CMD} -i ${INPUT_DIR} -s "ood_multi_turn" "ood_single_turn" "test_multi_turn" --model ${mn} -of logging/eval_result_${mn}.json"
        logHeader "Running: ${g_cmd}"
        eval "$g_cmd"
    else
        echo "${INPUT_DIR} does not exist."
    fi
done