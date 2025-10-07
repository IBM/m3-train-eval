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
RUN_TYPE="dummy" # Update to the required folder
RUN_MODE="run"
# MODEL_NAME=( "mixtral22b" "mistral7b" "granite38b" "granite4" )
MODEL_NAME=( "mistral7b" "granite38b" )
INPUT_TYPE=( "ood_multi_turn_mixed" "ood_single_turn_mixed" "test_multi_turn_mixed" )
NUM_SAMPLES=( "400" "800" "1200" "ALL")

INPUT_DIR="/proj/m3benchmark/m3data/0923/evaluation_data/v2"
OUTPUT_DIR="/proj/m3benchmark/m3data/0923/m3_test_evaluation/${RUN_TYPE}"
CONFIG_DIR="/dccstor/arnaik_data/routing/m3-train-eval/config_files/evaluation"

CCC_CMD_NO_GPU='bsub -n 1 -U infusion -R "rusage[mem=20GB, cpu=4]"'
CCC_CMD_GPU='bsub -n 1 -U infusion -gpu "num=1:mode=exclusive_process:gmodel=NVIDIAA100_SXM4_80GB" -R "rusage[mem=64GB, cpu=4]"'
GENRATION_CMD="PYTHONPATH=./ python run.py"

for mn in "${MODEL_NAME[@]}"; do
    for it in "${INPUT_TYPE[@]}"; do
        mkdir -p runs/${RUN_DATE}/${mn}/${it}
        mkdir -p ${OUTPUT_DIR}/${mn}/${it}/
        for ns in "${NUM_SAMPLES[@]}"; do
            INPUT_FILE_NAME=${INPUT_DIR}/${it}_${ns}.json
            if [ -e "$INPUT_FILE_NAME" ]; then
                gen_cmd_args="-i ${INPUT_FILE_NAME} -o ${OUTPUT_DIR}/${mn}/${it}/ -ic ${CONFIG_DIR}/infer_agent_${mn}.json"
                g_cmd="${CCC_CMD_GPU} -J ${RUN_MODE}_${mn}_${it}_${ns} -o runs/${RUN_DATE}/${mn}/${it}/%J_${ns}.log ${GENRATION_CMD} ${gen_cmd_args}"
                logHeader "Running: ${g_cmd}"
                eval "$g_cmd"
            else
                echo "$INPUT_FILE_NAME does not exist."
            fi
        done
    done
done