#!/bin/bash
#BSUB -J train_multi_gpu
#BSUB -n 1                                 # One launcher task
#BSUB -R "rusage[mem=256GB, cpu=8]"        # More RAM & CPUs
#BSUB -gpu "num=4:mode=exclusive_process:gmodel=NVIDIAA100_SXM4_80GB"     # Request 4 exclusive(default=shared) 80GBxA100 GPUs
#BSUB -o logging/multi_%J.out
#BSUB -e logging/multi_%J.err

# Print hostname (useful to know which node you're on)
echo "Running on host: $(hostname)"

# Show GPU info
echo "Checking GPU availability:"
nvidia-smi

# Optional: Show CUDA-visible devices from environment
echo "CUDA_VISIBLE_DEVICES: $CUDA_VISIBLE_DEVICES"

# Assign arguments to variables
ENV_NAME="<YOUR_ENV_NAME>"
ROOT_DIR="<PATH_TO_YOUR_PROJECT_ROOT_DIR>"
HF_CACHE_DIR="<PATH_TO_YOUR_HF_CACHE_DIR>"

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

    export CUDA_HOME=/usr/local/cuda-12.8
    export PATH=${CUDA_HOME}/bin:${PATH}
    export LD_LIBRARY_PATH=${CUDA_HOME}/lib64:${LD_LIBRARY_PATH}

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

# Run training using accelerate
accelerate launch --config_file config_files/training/multi_gpu_ds_stage3.yml tune.py