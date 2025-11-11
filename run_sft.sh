# Print hostname (useful to know which node you're on)
echo "Running on host: $(hostname)"

# Optional: Show CUDA-visible devices from environment
echo "CUDA_VISIBLE_DEVICES: $CUDA_VISIBLE_DEVICES"

# Assign arguments to variables
ENV_NAME="/dccstor/belder1/cache/conda/envs/g4"
ROOT_DIR="/dccstor/arnaik_data/routing/m3-train-eval/"
HF_CACHE_DIR="/proj/m3benchmark/ankita/cache/"
VERSION="v6"

# Activate your environment
# Load conda (adjust path if necessary)
# CONDA_BASE="/dccstor/belder1/cache/conda/envs/"
# CONDA_BASE=$(conda info --base)
# source "$CONDA_BASE/etc/profile.d/conda.sh"
# conda activate "$ENV_NAME"
# echo "Activated conda environment: $ENV_NAME"

# Move to your project directory
cd "$ROOT_DIR" || { echo "Directory $ROOT_DIR not found"; exit 1; }
echo "Changed directory to $ROOT_DIR"


# Set Hugging Face cache directory
# export HF_HOME="$HF_CACHE_DIR"
# echo "Exported HF_HOME=$HF_HOME"


export CUDA_HOME=/opt/share/cuda-12.6
export CUDA_PATH=/opt/share/cuda-12.6/
export PATH=${CUDA_HOME}/bin:${PATH}
export LD_LIBRARY_PATH=${CUDA_HOME}/lib64:${LD_LIBRARY_PATH}
export PYTORCH_CUDA_ALLOC_CONF=expandable_segments:True
export DS_ENABLE_MEMORY_TRACKER=1
export CUDA_LAUNCH_BLOCKING=1
# nvcc --version
# Run training using accelerate


configs=(
    # "config_files/sft/train_lora_granite4-micro.json"
    # "config_files/sft/train_lora_granite4-tiny.json"
    # "config_files/sft/train_lora_granite4-micro-v2.json"
    # "config_files/sft/train_lora_granite4-tiny-v2.json"
    "config_files/sft/${VERSION}/train_lora_granite4-micro_${RUN_NAME}.json"
)
labels=(
    # "granite4-micro-l64"
    # "granite4-tiny-l64"
    # "granite4-micro-l32"
    # "granite4-tiny-l32"
    "granite4-micro-${RUN_NAME}"    
)

RUN_NAMES=(
    # "lr_e5_no_thoughts_rank64_epoch3"
    # "lr_e4_no_thoughts_rank64_epoch3"
    # # "lr_e5_no_thoughts_rank16_epoch3"
    "lr_e5_thoughts_rank16_epoch3"
    "lr_3e5_thoughts_rank32_alpha64_epoch3"
    "lr_3e5_no_thoughts_rank32_alpha64_epoch3"
)

# Loop through the arrays
for i in "${!RUN_NAMES[@]}"; do
    runname="${RUN_NAMES[$i]}"
    mkdir -p ${ROOT_DIR}/logging/${VERSION}/${runname}
    config_file="config_files/sft/${VERSION}/train_lora_granite4-micro_${runname}.json"
    ds_config="config_files/training/multi_gpu_ds_stage3.yml"
    # bs_config=' -gpu "num=4:mode=exclusive_process:gmodel=NVIDIAH10080GBHBM3" -R "select[hname != cccxc604]" -R "rusage[mem=256GB, cpu=8]" '
    # label="${labels[$i]}"

    echo "Processing job: ${config_file}"
    echo "Using label: ${runname}"
    # base="-oo logging/tune_${label}.log -eo logging/tune_${label}.log -J tune_${label} -U infusion -q normal -n 1"
    # bcommand=${base}${bs_config}
    bsub -oo logging/${VERSION}/${runname}/tune_${runname}.log -eo logging/${VERSION}/${runname}/tune_${runname}.log -J tune_${runname} -U infusion -q normal -n 1 -gpu "num=4:mode=exclusive_process:gmodel=NVIDIAA100_SXM4_80GB" -R "select[hname != cccxc604]" -R "rusage[mem=256GB, cpu=8]" accelerate launch --config_file ${ds_config} tune.py --tune_config ${config_file}
done
