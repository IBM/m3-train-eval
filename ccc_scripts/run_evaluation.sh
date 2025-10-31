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
export HF_HOME="/proj/m3benchmark/ben/cache/"
export HF_CACHE="/proj/m3benchmark/ben/cache/"


export CUDA_HOME=/opt/share/cuda-12.6
export CUDA_PATH=/opt/share/cuda-12.6/
export PATH=${CUDA_HOME}/bin:${PATH}
export LD_LIBRARY_PATH=${CUDA_HOME}/lib64:${LD_LIBRARY_PATH}
export PYTORCH_CUDA_ALLOC_CONF=expandable_segments:True
export DS_ENABLE_MEMORY_TRACKER=1
export CUDA_LAUNCH_BLOCKING=1

RUN_DATE=$(date "+%m%d")

# Setup the following locations and paramters for the run
RUN_TYPE="sft-10-31" # Update to the required folder
MODEL_NAME=( 
    "granite4-tiny-sft" 
    "granite4-micro-sft" 
    )
MODEL_PATH=(
    "ibm-granite/granite-4.0-h-tiny"
    "ibm-granite/granite-4.0-micro"
)

INPUT_DIR="/proj/m3benchmark/m3data/0923/evaluation_data/v9/balanced/pct1"
OUTPUT_DIR="/proj/m3benchmark/m3data/0923/m3_test_evaluation/${RUN_TYPE}"
CONFIG_FILE="/u/belder/m3-train-eval/config_files/m3_evaluation.json"

GPU_STR="num=1:mode=exclusive_process:gmodel=NVIDIAA100_SXM4_80GB"
for i in "${!MODEL_NAME[@]}"; do
    mn="${MODEL_NAME[$i]}"
    mfile="${MODEL_PATH[$i]}"
    mkdir -p runs/${RUN_DATE}/${mn}
    mkdir -p ${OUTPUT_DIR}/${mn}/trajectories/

    for INPUT_FILE_NAME in "${INPUT_DIR}"/*.json; do
        if [ -e "$INPUT_FILE_NAME" ]; then
            FILENAME=$(basename "$INPUT_FILE_NAME")
            FILENAME_NO_EXT="${FILENAME%.json}"
            LOGFILE="runs/${RUN_DATE}/${mn}/${FILENAME_NO_EXT}.log"
            bsub -n 1 -U infusion -gpu ${GPU_STR} -R "select[hname != cccxc606] rusage[mem=64GB, cpu=4]" -J "${mn}_${FILENAME_NO_EXT}" -o ${LOGFILE} ./ccc_scripts/single_evaluation_command.sh ${mfile} ${OUTPUT_DIR}/${mn} ${INPUT_FILE_NAME} ${CONFIG_FILE}
        else
            echo "$INPUT_FILE_NAME does not exist."
        fi
    done
done