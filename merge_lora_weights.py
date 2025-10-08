


import os
from transformers import AutoTokenizer, AutoModelForCausalLM
from peft import PeftModel
import torch


BASE_MODELS=[
    # "ibm-granite/granite-3.3-8b-instruct", 
    # "mistralai/Mistral-7B-Instruct-v0.3", 
    # "Qwen/Qwen3-8B",
    # "ibm-granite/granite-3.3-8b-instruct", 
    # "mistralai/Mistral-7B-Instruct-v0.3", 
    # "Qwen/Qwen3-8B",
    "ibm-granite/granite-3.3-8b-instruct", 
    "mistralai/Mistral-7B-Instruct-v0.3", 
    "Qwen/Qwen3-8B",
    # "/proj/m3benchmark/granite4_ckpts/granite-4.0-tiny-prerelease-greylock/r250825a", 
    # "/proj/m3benchmark/granite4_ckpts/granite-4.0-tiny-prerelease-greylock/r250825a", 
    # "/proj/m3benchmark/granite4_ckpts/granite-4.0-tiny-prerelease-greylock/r250825a", 
    # "/proj/m3benchmark/ben/checkpoints/granite4-warmup/r250825a/final/PEFT/"
]
PEFT_ADAPTERS=[
    # "/proj/m3benchmark/ben/checkpoints/granite-3.3-8b-instruct/final/PEFT", 
    # "/proj/m3benchmark/ben/checkpoints/Mistral-7B-Instruct-v0.3/final/PEFT", 
    # "/proj/m3benchmark/ben/checkpoints/Qwen3-8B/final/PEFT", 
    # "/proj/m3benchmark/ben/checkpoints/granite3-gt/final/PEFT", 
    # "/proj/m3benchmark/ben/checkpoints/mistral-gt/final/PEFT", 
    # "/proj/m3benchmark/ben/checkpoints/qwen-gt/final/PEFT", 
    "/proj/m3benchmark/ben/checkpoints/granite3-warmup/final/PEFT", 
    "/proj/m3benchmark/ben/checkpoints/mistral-warmup/final/PEFT", 
    "/proj/m3benchmark/ben/checkpoints/qwen-warmup/final/PEFT", 
    # "/proj/m3benchmark/ben/checkpoints/granite4-warmup/r250825a/final/PEFT", 
    # "/proj/m3benchmark/ben/checkpoints/granite4-gt/r250825a/final/PEFT", 
    # "/proj/m3benchmark/ben/checkpoints/granite4-warmup/r250825a/final/PEFT", 
    # "/proj/m3benchmark/siyu/m3-train-eval/g4_wm_dpo/r250825a/final/PEFT"
]
OUTPUT_DIRS=[
    # "/proj/m3benchmark/ben/checkpoints/granite-3.3-8b-instruct/merged", 
    # "/proj/m3benchmark/ben/checkpoints/Mistral-7B-Instruct-v0.3/merged", 
    # "/proj/m3benchmark/ben/checkpoints/Qwen3-8B/merged", 
    # "/proj/m3benchmark/ben/checkpoints/granite3-gt/merged", 
    # "/proj/m3benchmark/ben/checkpoints/mistral-gt/merged", 
    # "/proj/m3benchmark/ben/checkpoints/qwen-gt/merged", 
    "/proj/m3benchmark/ben/checkpoints/granite3-warmup/merged", 
    "/proj/m3benchmark/ben/checkpoints/mistral-warmup/merged", 
    "/proj/m3benchmark/ben/checkpoints/qwen-warmup/merged", 
    # "/proj/m3benchmark/ben/checkpoints/granite4-warmup/r250825a/merged", 
    # "/proj/m3benchmark/ben/checkpoints/granite4-gt/r250825a/merged", 
    # "/proj/m3benchmark/ben/checkpoints/granite4-warmup/r250825a/merged", 
    # "/proj/m3benchmark/ben/checkpoints/g4_wm_dpo/r250825a/merged"
]







def merge_peft_adapter(
    base_model_path: str,
    peft_adapter_path: str,
    output_path: str,
    torch_dtype=torch.float16,
    is_dpo: bool = True,
    dpo_adapter: str = None,
):
    """
    Merges a PEFT adapter (e.g., LoRA) into a base model and saves the merged model.

    Args:
        base_model_path (str): Path to the base model directory.
        peft_adapter_path (str): Path to the PEFT adapter directory.
        output_path (str): Path to save the merged model.
        torch_dtype (torch.dtype, optional): Data type for loading the model. Defaults to torch.float16.

    Returns:
        None
    """
    if is_dpo:
        assert dpo_adapter is not None
        assert os.path.isdir(dpo_adapter)

    # Ensure output directory exists
    os.makedirs(output_path, exist_ok=True)

    print("🔁 Loading tokenizer...")
    try:
        tokenizer = AutoTokenizer.from_pretrained(base_model_path)
    except:
        tokenizer = AutoTokenizer.from_pretrained("ibm-granite/granite-3.3-8b-instruct")

    print(f"🔁 Loading base model: {base_model_path}")
    base_model = AutoModelForCausalLM.from_pretrained(base_model_path, torch_dtype=torch_dtype)
    
    # There will be both warm-up and dpo adapters in this case
    if is_dpo:
        print(f"🔁 Loading SFT PEFT adapter: {peft_adapter_path}")
        model = PeftModel.from_pretrained(base_model, peft_adapter_path, adapter_name="sft")
        model = model.merge_and_unload()
        print(f"🔁 Loading DPO PEFT adapter: {dpo_adapter}")
        model = PeftModel.from_pretrained(model, dpo_adapter, adapter_name="dpo")
    else:
        # Only one adapter for sft
        print(f"🔁 Loading PEFT adapter: {peft_adapter_path}")
        model = PeftModel.from_pretrained(base_model, peft_adapter_path)

    print("🔁 Merging PEFT adapter into base model...")
    merged_model = model.merge_and_unload()

    # Get the actual base model (should strip all PEFT wrapper)
    base_model_merged = merged_model.base_model if hasattr(merged_model, 'base_model') else merged_model

    print(f"💾 Saving merged model to: {output_path}")
    base_model_merged.save_pretrained(output_path)
    tokenizer.save_pretrained(output_path)

    print("✅ Merge complete. Merged model saved.")




if __name__ == "__main__":
    # DPO
    # merge_peft_adapter(
    #     base_model_path="/proj/m3benchmark/ben/checkpoints/granite4-warmup/r250825a/final/PEFT/",
    #     peft_adapter_path="/proj/m3benchmark/ben/checkpoints/granite4-warmup/r250825a/final/PEFT",
    #     output_path="/proj/m3benchmark/ben/checkpoints/g4_wm_dpo/r250825a/merged", 
    #     is_dpo=True,
    #     dpo_adapter="/proj/m3benchmark/siyu/m3-train-eval/g4_wm_dpo/r250825a/final/PEFT"
    # )

    for base, adapter, outdir in zip(BASE_MODELS, PEFT_ADAPTERS, OUTPUT_DIRS):
        merge_peft_adapter(
            base_model_path=base,
            peft_adapter_path=adapter,
            output_path=outdir, 
            is_dpo=False
        )
