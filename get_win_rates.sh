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
MODEL_NAME=("granite4-micro-sft" "granite4-tiny-sft")
GENRATION_CMD="PYTHONPATH=./ python summarize_win_rates.py"
TRAJ_PATH = "your trajectories path"
for mn in "${MODEL_NAME[@]}"; do
    INPUT_DIR="${TRAJ_PATH}/${mn}/trajectories"
    if [ -e "$INPUT_DIR" ]; then
        # g_cmd="bsub -n 1 -J ${RUN_MODE}_${mn} -o logging/win_rate_${mn}.log ${GENRATION_CMD} -i ${INPUT_DIR} -s "ood_multi_turn" "ood_single_turn" "test_multi_turn" --model ${mn} -of logging/m3_eval_win_rates/eval_result_${mn}.json"
        g_cmd="${GENRATION_CMD} -i ${INPUT_DIR} -s "ood_multi_turn" "ood_single_turn" "test_multi_turn" --model ${mn} -of ${TRAJ_PATH}/${mn}/eval_result_${mn}.json"
        logHeader "Running: ${g_cmd}"
        eval "$g_cmd"
    else
        echo "${INPUT_DIR} does not exist."
    fi
done