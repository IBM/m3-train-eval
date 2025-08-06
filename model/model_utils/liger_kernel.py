import inspect
from typing import TYPE_CHECKING
from transformers.utils import is_liger_kernel_available

if TYPE_CHECKING:
    from transformers import PretrainedConfig

    from hparams import ModelArguments


from loguru import logger


def apply_liger_kernel(
    config: "PretrainedConfig",
    model_args: "ModelArguments",
    is_trainable: bool,
    require_logits: bool,
) -> None:
    if not is_trainable or not model_args.enable_liger_kernel:
        return

    if not is_liger_kernel_available():
        raise ImportError(
            "You have set `enable_liger_kernel` to `True` but liger-kernel >= 0.3.0 is not available. "
            "Please install it with `pip install liger-kernel`"
        )

    model_type = getattr(config, "model_type", None)
    if model_type == "gemma":
        from liger_kernel.transformers import apply_liger_kernel_to_gemma as apply_liger_kernel
    elif model_type == "gemma2":
        from liger_kernel.transformers import apply_liger_kernel_to_gemma2 as apply_liger_kernel
    elif model_type == "gemma3":
        from liger_kernel.transformers import apply_liger_kernel_to_gemma3 as apply_liger_kernel
    elif model_type == "gemma3_text":
        from liger_kernel.transformers import apply_liger_kernel_to_gemma3_text as apply_liger_kernel
    elif model_type == "glm4":
        from liger_kernel.transformers import apply_liger_kernel_to_glm4 as apply_liger_kernel
    elif model_type == "granite":
        from liger_kernel.transformers import apply_liger_kernel_to_granite as apply_liger_kernel
    elif model_type == "llama":
        from liger_kernel.transformers import apply_liger_kernel_to_llama as apply_liger_kernel
    elif model_type == "llama4" or model_type == "llama4_text":
        from liger_kernel.transformers import apply_liger_kernel_to_llama4 as apply_liger_kernel
    elif model_type == "llava":
        from liger_kernel.transformers import apply_liger_kernel_to_llava as apply_liger_kernel
    elif model_type == "mistral":
        from liger_kernel.transformers import apply_liger_kernel_to_mistral as apply_liger_kernel
    elif model_type == "mixtral":
        from liger_kernel.transformers import apply_liger_kernel_to_mixtral as apply_liger_kernel
    elif model_type == "mllama":
        from liger_kernel.transformers import apply_liger_kernel_to_mllama as apply_liger_kernel
    elif model_type == "olmo2":
        from liger_kernel.transformers import apply_liger_kernel_to_olmo2 as apply_liger_kernel
    elif model_type == "paligemma":
        from liger_kernel.transformers import apply_liger_kernel_to_paligemma as apply_liger_kernel
    elif model_type == "phi3":
        from liger_kernel.transformers import apply_liger_kernel_to_phi3 as apply_liger_kernel
    elif model_type == "qwen2":
        from liger_kernel.transformers import apply_liger_kernel_to_qwen2 as apply_liger_kernel
    elif model_type == "qwen2_vl":
        from liger_kernel.transformers import apply_liger_kernel_to_qwen2_vl as apply_liger_kernel
    elif model_type == "qwen2_5_vl" or model_type == "qwen2_5_vl_text":
        from liger_kernel.transformers import apply_liger_kernel_to_qwen2_5_vl as apply_liger_kernel
    elif model_type == "qwen3":
        from liger_kernel.transformers import apply_liger_kernel_to_qwen3 as apply_liger_kernel
    elif model_type == "qwen3_moe":
        from liger_kernel.transformers import apply_liger_kernel_to_qwen3_moe as apply_liger_kernel
    elif model_type == "smollm3":
        from liger_kernel.transformers import apply_liger_kernel_to_smollm3 as apply_liger_kernel
    else:
        logger.warning("Current model does not support liger kernel.")
        return

    if require_logits and "fused_linear_cross_entropy" in inspect.signature(apply_liger_kernel).parameters:
        logger.info("Current training stage does not support chunked cross entropy.")
        kwargs = {"fused_linear_cross_entropy": False, "cross_entropy": True}
    else:
        kwargs = {}

    apply_liger_kernel(**kwargs)
    logger.info("Liger kernel has been applied to the model.")
