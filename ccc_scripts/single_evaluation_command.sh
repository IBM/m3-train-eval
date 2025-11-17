

export PYTHONPATH=.
export HF_HOME="YOUR HF HOME"
#export HF_CACHE="/proj/m3benchmark/ben/cache/"


export CUDA_HOME=/opt/share/cuda-12.6
export CUDA_PATH=/opt/share/cuda-12.6/
export PATH=${CUDA_HOME}/bin:${PATH}
export LD_LIBRARY_PATH=${CUDA_HOME}/lib64:${LD_LIBRARY_PATH}
export PYTORCH_CUDA_ALLOC_CONF=expandable_segments:True
export DS_ENABLE_MEMORY_TRACKER=1
export CUDA_LAUNCH_BLOCKING=1
export VLLM_SKIP_ADAPTERS=True
export VLLM_USE_ADAPTERS=0

# --- Input Parameters ---
# $1: model_file (for --model)
# $2: output_dir_base (for --output_dir)
# $3: input_filename (for --input_filename)
# $4: infer_config_file (for --infer_config)

MODEL_FILE=$1
OUTPUT_DIR=$2
INPUT_FILE_NAME=$3
CONFIG_FILE=$4
PORT=$5

VLLM_PID=""

cleanup() {
    echo ""
    echo "Running cleanup..."

    if [[ -n "$VLLM_PID" ]]; then
        if ps -p "$VLLM_PID" > /dev/null 2>&1; then
            echo "Killing vLLM server (PID $VLLM_PID)..."
            kill "$VLLM_PID" 2>/dev/null
            wait "$VLLM_PID" 2>/dev/null
        else
            echo "vLLM server already stopped."
        fi
    fi

    echo "Cleanup done."
}

# Run cleanup on EXIT, Ctrl-C, or termination signals
trap cleanup EXIT INT TERM

# 1. Start the vLLM OpenAI API server in the background
echo "Starting vLLM API server with model: ${MODEL_FILE}"
python -m vllm.entrypoints.openai.api_server \
    --model "${MODEL_FILE}" \
    --trust-remote-code \
    --host 0.0.0.0 \
    --port "${PORT}" &

# 2. Wait until the server is healthy (using a loop)
until curl -s "http://localhost:${PORT}/v1/models" > /dev/null; do
    echo -n "."
    sleep 10;
done;
echo "Server is up!"
sleep 30; # Make sure the server is really up before we start, otherwise the first few samples will fail


VLLM_PID=$!
echo "vLLM server started with PID ${VLLM_PID}"

# 3. Run the evaluation script
python -u evaluation.py \
    --output_dir "${OUTPUT_DIR}" \
    --input_filename "${INPUT_FILE_NAME}" \
    --infer_config "${CONFIG_FILE}" \
    --model "${MODEL_FILE}" \
    --num_workers 4 \
    --port "${PORT}"


EVAL_STATUS=$?

echo "evaluation.py finished with status ${EVAL_STATUS}"

# Cleanup is automatically called now due to trap

exit ${EVAL_STATUS}
