#!/bin/bash
log () {
    d=`date -u '+%Y-%m-%dT%H:%M:%S.%3NZ'`
    echo $d 'run_evaluation.sh [INFO ]:' $*
}
logLine () {
    log '---------------------------------------------------------------------------'
}


logHeader 'Started running'
runStart=$(date +%s)

set -e
set -x

export PYTHONPATH=.
export HF_HOME="YOUR HF HOME"

export CUDA_HOME=/opt/share/cuda-12.6
export CUDA_PATH=/opt/share/cuda-12.6/
export PATH=${CUDA_HOME}/bin:${PATH}
export LD_LIBRARY_PATH=${CUDA_HOME}/lib64:${LD_LIBRARY_PATH}
export PYTORCH_CUDA_ALLOC_CONF=expandable_segments:True
export DS_ENABLE_MEMORY_TRACKER=1
export CUDA_LAUNCH_BLOCKING=1

RUN_DATE=$(date "+%m%d")
RUN_TIME=$(date "+%d%H%M%S")
# This is just a tag that will be used for logfiles and output directories.
# You can make it whatever you want.
MODEL_NAME=(
    #"granite4-tiny-sft"
    #"granite4-micro-sft"
    #"granite4-micro-sft-nothought"
    #"granite4-micro-sft-nothought_1103"
    #"g4_micro_1104_lre4_1800_thought"
    #"g4_tiny_1104_lre4_1800_thought"
   "g4_tiny"
   "g4_micro"
#  "granite_4.0_micro_lr_e4_no_thoughts_rank64_epoch1_model_step_1200"
#  "granite_4.0_micro_lr_e5_no_thoughts_rank64_epoch1_model_step_1800"
#  "granite_4.0_micro_lr_e5_no_thoughts_rank64_epoch1_final"
#  "granite_4.0_micro_lr_e5_thoughts_rank16_epoch3_model_step_1800"
#  "granite_4.0_micro_lr_3e5_no_thoughts_rank32_alpha64_epoch3_model_step_1200"
#  "granite_4.0_micro_lr_3e5_no_thoughts_rank32_alpha64_epoch3_model_step_1800"
#  "granite_4.0_micro_lr_3e5_thoughts_rank32_alpha64_epoch3_model_step_1200"
#  "granite_4.0_micro_lr_3e5_thoughts_rank32_alpha64_epoch3_model_step_1800"
)
# This needs to be a valid huggingface name or a path to a local checkpoint
MODEL_PATH=(
#"/proj/m3benchmark/siyu/downloaded_models/granite_4.0_micro_lr_e4_no_thoughts_rank64_epoch1_model_step_1200"
#"/proj/m3benchmark/siyu/downloaded_models/granite_4.0_micro_lr_e5_no_thoughts_rank64_epoch1_model_step_1800"
#"/proj/m3benchmark/siyu/downloaded_models/granite_4.0_micro_lr_e5_no_thoughts_rank64_epoch1_final"
#"/proj/m3benchmark/siyu/downloaded_models/granite_4.0_micro_lr_e5_thoughts_rank16_epoch3_model_step_1800"
#"/proj/m3benchmark/siyu/downloaded_models/granite_4.0_micro_lr_3e5_no_thoughts_rank32_alpha64_epoch3_model_step_1200"
#"/proj/m3benchmark/siyu/downloaded_models/granite_4.0_micro_lr_3e5_no_thoughts_rank32_alpha64_epoch3_model_step_1800"
#"/proj/m3benchmark/siyu/downloaded_models/granite_4.0_micro_lr_3e5_thoughts_rank32_alpha64_epoch3_model_step_1200"
#"/proj/m3benchmark/siyu/downloaded_models/granite_4.0_micro_lr_3e5_thoughts_rank32_alpha64_epoch3_model_step_1800"
 "ibm-granite/granite-4.0-h-tiny"
 "ibm-granite/granite-4.0-micro"
)
# Input data loaded from here
INPUT_DIR="/proj/m3benchmark/m3data/0923/evaluation_data/v9/balanced/pct1"
# Output files (trajectories) will go in here.
OUTPUT_DIR="YOUR OUTPUT DIR"
# Only one config file should be required for all models.
CONFIG_FILE="/u/belder/m3-train-eval/config_files/m3_evaluation.json"

GPU_STR="num=1:mode=exclusive_process:gmodel=NVIDIAA100_SXM4_80GB"
port=7000
for i in "${!MODEL_NAME[@]}"; do
    mn="${MODEL_NAME[$i]}"
    mfile="${MODEL_PATH[$i]}"
    mkdir -p runs/${RUN_DATE}/${mn}
    mkdir -p ${OUTPUT_DIR}/${mn}/trajectories/
    for INPUT_FILE_NAME in "${INPUT_DIR}"/*.json; do
        if [ -e "$INPUT_FILE_NAME" ]; then
            let port=port+1
            FILENAME=$(basename "$INPUT_FILE_NAME")
            FILENAME_NO_EXT="${FILENAME%.json}"
            LOGFILE="runs/${RUN_DATE}/${mn}/${FILENAME_NO_EXT}_${RUN_TIME}.log"
            bsub -n 1 -U infusion -gpu ${GPU_STR} -R "select[hname != cccxc606] rusage[mem=64GB, cpu=4]" -J "${mn}_${FILENAME_NO_EXT}" -o ${LOGFILE} ./ccc_scripts/single_evaluation_command.sh "${mfile}" ${OUTPUT_DIR}/${mn} ${INPUT_FILE_NAME} ${CONFIG_FILE} "${port}"
        else
            echo "$INPUT_FILE_NAME does not exist."
        fi
        
for i in "${!MODEL_NAME[@]}"; do
    mn="${MODEL_NAME[$i]}"
    mfile="${MODEL_PATH[$i]}"
    mkdir -p runs/${RUN_DATE}/${mn}
    mkdir -p ${OUTPUT_DIR}/${mn}/trajectories/

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
