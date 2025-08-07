# Check if all arguments are provided
if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <ENV_NAME> <ROOT_DIR> <HF_CACHE_DIR>"
    exit 1
fi

# Assign arguments to variables
ENV_NAME="$1"
ROOT_DIR="$2"
HF_CACHE_DIR="$3"

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