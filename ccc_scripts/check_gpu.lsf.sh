#!/bin/bash
#BSUB -J gpu_check
#BSUB -n 1
#BSUB -R "rusage[mem=8GB, cpu=2]"
#BSUB -gpu "num=1"
#BSUB -o gpu_check_%J.out
#BSUB -e gpu_check_%J.err

# Load any module required for CUDA visibility, if applicable
# e.g., module load cuda/11.8
# Or activate your environment if that sets up CUDA

# Print hostname (useful to know which node you're on)
echo "Running on host: $(hostname)"

# Show GPU info
echo "Checking GPU availability:"
nvidia-smi

# Optional: Show CUDA-visible devices from environment
echo "CUDA_VISIBLE_DEVICES: $CUDA_VISIBLE_DEVICES"
