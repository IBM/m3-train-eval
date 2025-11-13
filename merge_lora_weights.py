


import os
from transformers import AutoTokenizer, AutoModelForCausalLM
from peft import PeftModel
import torch

STEP="500"
OUTPUT_DIR="/proj/m3benchmark/ben/checkpoints/11-13"
MODELS_TO_MERGE=[ # Base model , adapter, merged path
    # # lr_e5_thoughts_rank16_epoch3
    # ("ibm-granite/granite-4.0-micro","/dccstor/arnaik_data/routing/m3-train-eval/logging/v6/lr_e5_thoughts_rank16_epoch3/granite-4.0-micro/model_step_1200/PEFT","/proj/m3benchmark/ankita/logging/v6/lr_e5_thoughts_rank16_epoch3/granite-4.0-micro/merged/model_step_1200"),
    # ("ibm-granite/granite-4.0-micro","/dccstor/arnaik_data/routing/m3-train-eval/logging/v6/lr_e5_thoughts_rank16_epoch3/granite-4.0-micro/model_step_1800/PEFT","/proj/m3benchmark/ankita/logging/v6/lr_e5_thoughts_rank16_epoch3/granite-4.0-micro/merged/model_step_1800"),
    # # lr_3e5_thoughts_rank32_alpha64_epoch3
    # ("ibm-granite/granite-4.0-micro","/dccstor/arnaik_data/routing/m3-train-eval/logging/v6/lr_3e5_thoughts_rank32_alpha64_epoch3/granite-4.0-micro/model_step_1200/PEFT","/proj/m3benchmark/ankita/logging/v6/lr_3e5_thoughts_rank32_alpha64_epoch3/granite-4.0-micro//merged/model_step_1200"),
    # ("ibm-granite/granite-4.0-micro","/dccstor/arnaik_data/routing/m3-train-eval/logging/v6/lr_3e5_thoughts_rank32_alpha64_epoch3/granite-4.0-micro/model_step_1800/PEFT","/proj/m3benchmark/ankita/logging/v6/lr_3e5_thoughts_rank32_alpha64_epoch3/granite-4.0-micro//merged/model_step_1800"),    
    # # lr_3e5_no_thoughts_rank32_alpha64_epoch3
    # ("ibm-granite/granite-4.0-micro","/dccstor/arnaik_data/routing/m3-train-eval/logging/v6/lr_3e5_no_thoughts_rank32_alpha64_epoch3/granite-4.0-micro/model_step_1200/PEFT","/proj/m3benchmark/ankita/logging/v6/lr_3e5_no_thoughts_rank32_alpha64_epoch3/granite-4.0-micro//merged/model_step_1200"),
    # ("ibm-granite/granite-4.0-micro","/dccstor/arnaik_data/routing/m3-train-eval/logging/v6/lr_3e5_no_thoughts_rank32_alpha64_epoch3/granite-4.0-micro/model_step_1800/PEFT","/proj/m3benchmark/ankita/logging/v6/lr_3e5_no_thoughts_rank32_alpha64_epoch3/granite-4.0-micro//merged/model_step_1800"),        
    # micro-base
    ("ibm-granite/granite-4.0-micro",
     f"/u/belder/m3-train-eval/logging/granite4-micro/granite-4.0-micro/model_step_{STEP}/PEFT",
     f"{OUTPUT_DIR}/granite4-micro/model_step_{STEP}"),
    # lr_3e5_rank32
    ("ibm-granite/granite-4.0-micro",
     f"/u/belder/m3-train-eval/logging/granite4-micro-expl-lr-3e5-r32/granite-4.0-micro/model_step_{STEP}/PEFT",
     f"{OUTPUT_DIR}/granite4-micro-expl-lr-3e5-r32/model_step_{STEP}"),
    # lr_1e4_rank32
    ("ibm-granite/granite-4.0-micro",
     f"/u/belder/m3-train-eval/logging/granite4-micro-expl-lr-1e4-r32/granite-4.0-micro/model_step_{STEP}/PEFT",
     f"{OUTPUT_DIR}/granite4-micro-expl-lr-1e4-r32/model_step_{STEP}"),
    # lr_3e5_rank64
    ("ibm-granite/granite-4.0-micro",
     f"/u/belder/m3-train-eval/logging/granite4-micro-expl-lr-3e5-r64/granite-4.0-micro/model_step_{STEP}/PEFT",
     f"{OUTPUT_DIR}/granite4-micro-expl-lr-3e5-r64/model_step_{STEP}"),
    # lr_3e5_rank64_alpha_64
    ("ibm-granite/granite-4.0-micro",
     f"/u/belder/m3-train-eval/logging/granite4-micro-expl-lr-3e5-r64-alpha64/granite-4.0-micro/model_step_{STEP}/PEFT",
     f"{OUTPUT_DIR}/granite4-micro-expl-lr-3e5-r64-alpha64/model_step_{STEP}"),
]


def merge_peft_adapter(
    base_model_path: str,
    peft_adapter_path: str,
    output_path: str,
    torch_dtype=torch.bfloat16,
    is_dpo: bool = True,
    dpo_adapter: str = None,
):
    """
    Merges a PEFT adapter (e.g., LoRA) into a base model and saves the merged model.

    Args:
        base_model_path (str): Path to the base model directory.
        peft_adapter_path (str): Path to the PEFT adapter directory.
        output_path (str): Path to save the merged model.
        torch_dtype (torch.dtype, optional): Data type for loading the model. Defaults to torch.bfloat16.

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
        try:
            model = PeftModel.from_pretrained(base_model, peft_adapter_path)
        except Exception as e:
            print(f"Merge failed for adapter {peft_adapter_path} due to {e}.")
            return False

    print("🔁 Merging PEFT adapter into base model...")
    merged_model = model.merge_and_unload()

    # Get the actual base model (should strip all PEFT wrapper)
    base_model_merged = merged_model.base_model if hasattr(merged_model, 'base_model') else merged_model

    # Double check the merged weights are cast correctly
    base_model_merged = base_model_merged.to(torch_dtype)

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

    for (base, adapter, outdir) in MODELS_TO_MERGE:
        os.makedirs(outdir, exist_ok=True)
        merge_peft_adapter(
            base_model_path=base,
            peft_adapter_path=adapter,
            output_path=outdir, 
            is_dpo=False
        )
