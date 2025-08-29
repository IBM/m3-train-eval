#!/bin/bash
# BSUB -J train_single_gpu
# BSUB -n 1                                 # 1 task
# BSUB -R "rusage[mem=64GB, cpu=4]"         # 64 GB RAM, 4 CPUs
# BSUB -gpu "num=1:mode=exclusive_process:gmodel=NVIDIAA100_SXM4_80GB"                         # 1 GPU
# BSUB -o logging/single_%J.out
# BSUB -e logging/single_%J.err

# bsub -J train_single_gpu -n 1 -R "rusage[mem=64GB, cpu=4]" -gpu "num=1:mode=exclusive_process:gmodel=NVIDIAA100_SXM4_80GB" -o logging/single_%J.out -e logging/single_%J.err


# Print hostname (useful to know which node you're on)
echo "Running on host: $(hostname)"

# Show GPU info
echo "Checking GPU availability:"
nvidia-smi

# Optional: Show CUDA-visible devices from environment
echo "CUDA_VISIBLE_DEVICES: $CUDA_VISIBLE_DEVICES"

# Assign arguments to variables
ENV_NAME="g4"
ROOT_DIR="/u/belder/m3-train-eval/"
HF_CACHE_DIR="/dccstor/belder1/cache/"

# Activate your environment
# Load conda (adjust path if necessary)
CONDA_BASE=$(conda info --base)
source "$CONDA_BASE/etc/profile.d/conda.sh"
conda activate "$ENV_NAME"
echo "Activated conda environment: $ENV_NAME"

# Move to your project directory
cd "$ROOT_DIR" || { echo "Directory $ROOT_DIR not found"; exit 1; }
echo "Changed directory to $ROOT_DIR"

# Set Hugging Face cache directory
export HF_HOME="$HF_CACHE_DIR"
echo "Exported HF_HOME=$HF_HOME"

# Check if CUDA binaries are available at the default path (otherwise accelerate launch will give error)
if ! command -v /usr/local/cuda/bin/nvcc &> /dev/null; then
    echo "CUDA not found at /usr/local/cuda/bin. Manually setting CUDA paths..."

    export CUDA_HOME=/opt/share/cuda-12.6
    export CUDA_PATH=/opt/share/cuda-12.6/
    export PATH=${CUDA_HOME}/bin:${PATH}
    export LD_LIBRARY_PATH=${CUDA_HOME}/lib64:${LD_LIBRARY_PATH}
    export PYTORCH_CUDA_ALLOC_CONF=expandable_segments:True
    export DS_ENABLE_MEMORY_TRACKER=1
    # export NCCL_DEBUG=INFO
    # export NCCL_DEBUG_SUBSYS=ALL

    # Optional: Verify
    if command -v nvcc &> /dev/null; then
        echo "CUDA manually loaded from ${CUDA_HOME}"
    else
        echo "Error: CUDA binaries still not found. Please check installation."
        exit 1
    fi
else
    echo "CUDA found at /usr/local/cuda/bin"
fi

# Run training using accelerate w deepspeed
# accelerate launch --config_file config_files/training/ds_stage2.yml tune.py

## Run training using python
# accelerate launch tune.py