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
RUN_TYPE="sft-10-27" # Update to the required folder
MODEL_NAME=( 
    "granite4-tiny-sft" 
    "granite4-micro-sft" 
    # "granite3-base" 
    # "granite4-micro-base" 
    )
MODEL_PATH=(
    "ibm-granite/granite-4.0-h-tiny"
    "ibm-granite/granite-4.0-micro"
)
CONFIG_FILES=( 
    "tool_calling_granite4-tiny.json" 
    "tool_calling_granite4-micro.json" 
    # "tool_calling_granite3-base.json" 
    # "tool_calling_granite4-base.json" 
    )
INPUT_TYPE=( "ood_multi_turn_mixed" "ood_single_turn_mixed" "test_multi_turn_mixed" )
# INPUT_TYPE=( "test_multi_turn_mixed" )
# NUM_SAMPLES=( "400" "800" "1200" "1600" "2000" "2400" "2800" "3200" "3600" "4000" "4400" "4800" "5200" "ALL")

INPUT_DIR="/proj/m3benchmark/m3data/0923/evaluation_data/v9/balanced/pct10"
OUTPUT_DIR="/proj/m3benchmark/m3data/0923/m3_test_evaluation/${RUN_TYPE}"
CONFIG_DIR="/u/belder/m3-train-eval/config_files/tool_calling/"

GPU_STR="num=1:mode=exclusive_process:gmodel=NVIDIAA100_SXM4_80GB"
R_STR="select[hname != cccxc606] rusage[mem=64GB, cpu=4]"
for i in "${!MODEL_NAME[@]}"; do
    mn="${MODEL_NAME[$i]}"
    mfile="${MODEL_PATH[$i]}"
    config="${CONFIG_FILES[$i]}"
    for it in "${INPUT_TYPE[@]}"; do
        mkdir -p runs/${RUN_DATE}/${mn}/${it}
        mkdir -p ${OUTPUT_DIR}/${mn}/${it}/trajectories/

        # for ns in "${NUM_SAMPLES[@]}"; do
        for INPUT_FILE_NAME in "${INPUT_DIR}"/*.json; do
            # INPUT_FILE_NAME=${INPUT_DIR}/${it}_${ns}.json
            if [ -e "$INPUT_FILE_NAME" ]; then
                FILENAME=$(basename "$INPUT_FILE_NAME")
                FILENAME_NO_EXT="${FILENAME%.json}"
                LOGFILE="runs/${RUN_DATE}/${mn}/${it}/${FILENAME_NO_EXT}.log"
                bsub -n 1 -U infusion -gpu ${GPU_STR} -R "select[hname != cccxc606] rusage[mem=64GB, cpu=4]" -J "${mn}_${FILENAME_NO_EXT}" -o ${LOGFILE} ./ccc_scripts/single_evaluation_command.sh ${mfile} ${OUTPUT_DIR}/${mn}/${INPUT_TYPE} ${INPUT_FILE_NAME} ${CONFIG_DIR}/${config}
            else
                echo "$INPUT_FILE_NAME does not exist."
            fi
        done
    done
done