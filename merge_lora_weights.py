


import os
import json

from safetensors.torch import load_file, save_file
from transformers import AutoTokenizer, AutoModelForCausalLM
from peft import PeftModel
import torch

STEP="3000"
OUTPUT_DIR="/proj/m3benchmark/ben/checkpoints/11-18"
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
    # ("ibm-granite/granite-4.0-micro",
    #  f"/u/belder/m3-train-eval/logging/granite4-micro/granite-4.0-micro/model_step_{STEP}/PEFT",
    #  f"{OUTPUT_DIR}/granite4-micro/final"
     # f"{OUTPUT_DIR}/granite4-micro/model_step_{STEP}"
    # ),
    # lr_3e5_rank32
    ("ibm-granite/granite-4.0-micro",
     f"/u/belder/m3-train-eval/logging/granite4-micro-expl-lr-3e5-r32/granite-4.0-micro/model_step_{STEP}/PEFT",
    # f"{OUTPUT_DIR}/granite4-micro-expl-lr-3e5-r32/model_step_{STEP}"),
     f"{OUTPUT_DIR}/granite4-micro-expl-lr-3e5-r32/final"),
    # # lr_1e4_rank32
    # ("ibm-granite/granite-4.0-micro",
    #  f"/u/belder/m3-train-eval/logging/granite4-micro-expl-lr-1e4-r32/granite-4.0-micro/model_step_{STEP}/PEFT",
    #  f"{OUTPUT_DIR}/granite4-micro-expl-lr-1e4-r32/model_step_{STEP}"),
    # # lr_3e5_rank64
    # ("ibm-granite/granite-4.0-micro",
    #  f"/u/belder/m3-train-eval/logging/granite4-micro-expl-lr-3e5-r64/granite-4.0-micro/model_step_{STEP}/PEFT",
    #  f"{OUTPUT_DIR}/granite4-micro-expl-lr-3e5-r64/model_step_{STEP}"),
    # # lr_3e5_rank64_alpha_64
    # ("ibm-granite/granite-4.0-micro",
    #  f"/u/belder/m3-train-eval/logging/granite4-micro-expl-lr-3e5-r64-alpha64/granite-4.0-micro/model_step_{STEP}/PEFT",
    #  f"{OUTPUT_DIR}/granite4-micro-expl-lr-3e5-r64-alpha64/model_step_{STEP}"),
]


def rename_model_layers(ori_path: str, new_path: str):
    os.makedirs(new_path, exist_ok=True)
    contents = os.listdir(ori_path)
    safe_tensors_n = 0
    for d in contents:
        if "model-0000" in d:
            safe_tensors_n = int(d.split('-')[-1].split(".")[0].replace("0",""))
            break


    for i in range(1,safe_tensors_n+1):
    # Load the original safetensors file
        ori_tensor_path = f"{ori_path}/model-0000{i}-of-0000{safe_tensors_n}.safetensors"
        new_tensor_path = f"{new_path}/model-0000{i}-of-0000{safe_tensors_n}.safetensors"

        # Load the tensor dictionary
        state_dict = load_file(ori_tensor_path)

        # Create a new dictionary with renamed keys
        new_state_dict = {}
        for key, tensor in state_dict.items():
            # Rename 'embed_tokens.weight' to 'model.embed_tokens.weight'
            new_key = f"model.{key}"
        # new_key = f"{key}"
            new_state_dict[new_key] = tensor

        # Save the new state_dict to a new safetensors file
        print("new path:", new_tensor_path)
        save_file(new_state_dict, new_tensor_path)

    original_index_path = f"{ori_path}/model.safetensors.index.json"
    new_index_path = f"{new_path}/model.safetensors.index.json"
    new_json = {}
    with open(original_index_path) as f:
        jd = json.load(f)
    new_json["metadata"] = jd["metadata"]
    new_json["weight_map"] = {}
    for key in jd["weight_map"]:
        new_key = f"model.{key}"
    # new_key = f"{key}"
        new_json["weight_map"][new_key] = jd["weight_map"][key]
    with open(new_index_path, 'w') as f:
        json.dump(new_json, f, indent=4)


    original_config_path = f"{ori_path}/config.json"
    new_config_path = f"{new_path}/config.json"
    with open(original_config_path) as f:
        jd = json.load(f)
    jd["architectures"] = ["GraniteMoeHybridForCausalLM"]
    jd["torch_dtype"] = "bfloat16"
    jd["model_type"] = "granitemoehybrid"

    with open(new_config_path, 'w') as f:
        json.dump(jd, f, indent=4)

    sources = ["chat_template.jinja", "special_tokens_map.json", "vocab.json","tokenizer_config.json", "tokenizer.json","added_tokens.json "]
    for s in sources:
        os.system(f"cp {ori_path}/{s} {new_path}/{s}")

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
        tokenizer = AutoTokenizer.from_pretrained("ibm-granite/granite-4.0-micro")

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

    merged_path = os.path.join(output_path, "merged")
    print(f"💾 Saving merged model to: {merged_path}")
    base_model_merged.save_pretrained(merged_path)
    tokenizer.save_pretrained(merged_path)
    print("✅ Merge complete. Merged model saved.")

    # The layer names and architecture name in the config files are incorrect. 
    # These need to be fixed in order to use model with vllm
    renamed_layers_path = os.path.join(output_path, "renamed_layers")
    rename_model_layers(merged_path, renamed_layers_path)
    print("✅ Model layer and architeture names fixed. Renamed model saved.\n\n")




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
