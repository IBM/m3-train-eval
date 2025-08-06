#!/bin/bash
#BSUB -J train_single_gpu
#BSUB -n 1                                 # 1 task
#BSUB -R "rusage[mem=64GB, cpu=4]"         # 64 GB RAM, 4 CPUs
#BSUB -gpu "num=1:mode=exclusive_process:gmodel=NVIDIAA100_SXM4_80GB"                         # 1 GPU
#BSUB -o logging/single_%J.out
#BSUB -e logging/single_%J.err

# Print hostname (useful to know which node you're on)
echo "Running on host: $(hostname)"

# Show GPU info
echo "Checking GPU availability:"
nvidia-smi

# Optional: Show CUDA-visible devices from environment
echo "CUDA_VISIBLE_DEVICES: $CUDA_VISIBLE_DEVICES"

# Load conda (adjust path if necessary)
source ~/miniconda3/etc/profile.d/conda.sh

# Activate your environment
conda activate AgenticAI

# Move to your project directory
cd /u/aj05/project/Code

# Set Hugging Face cache directory
export HF_HOME=/u/aj05/project/hf_cache

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

# Run training using accelerate w deepspeed
accelerate launch --config_file config_files/training/ds_stage2.yml tune.py

## Run training using python
#accelerate launch tune.py