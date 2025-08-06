import json
import os
from datetime import datetime

from loguru import logger
from transformers.utils import is_flash_attn_2_available
from transformers.utils.generic import strtobool

from hparams import get_train_args
from extras.custom import is_rank_0, set_run_environment, make_json_serializable, gpu_supports_fa2, create_dir

if is_rank_0():
    set_run_environment(dotenv_path="./.env")

# # Login with the hf_token
# from huggingface_hub import login
# login(token=os.environ.get("HF_TOKEN", ""))

def main(debug=False):
    # Load the user-specified training configuration
    if debug:
        path_to_config = './config_files/debug_train.json'
    else:
        path_to_config = './config_files/train_lora.json'
    with open(os.path.join(path_to_config), 'r') as f:
        override_args = json.load(f)

    # Create the output dir
    model_dir = override_args['model_name_or_path'].split('/')[-1]
    override_args['output_dir'] = os.path.join(override_args['logging_dir'], model_dir)
    create_dir(override_args['output_dir'])

    training_method = override_args['stage'].upper()
    # Print a big message for the training method
    logger.info("=" * 60)
    logger.info("\t||\t\t" + f"Training Method: {training_method}" + "\t\t||")
    logger.info("=" * 60)

    current_time = datetime.now().strftime("%Y%m%d-%H%M%S")
    override_args['run_name'] = f'{training_method}/{override_args["model_name_or_path"]}/{current_time}'

    # Load deepspeed config if using within accelerator. HF's training_args implementation loads the deepspeed plugin in __post__init
    if strtobool(os.environ.get("ACCELERATE_USE_DEEPSPEED", "false")):
        with open(override_args['deepspeed'], 'r') as f:
            override_args['deepspeed'] = json.load(f)
        logger.info(f"\tLoading DeepSpeed Config: {override_args['deepspeed']}")
    else:
        override_args['deepspeed'] = None

    # My Check for flash-attention2
    if 'flash_attn' in override_args and override_args['flash_attn'] == 'fa2':
        can_run_fa2 = True
        if not is_flash_attn_2_available():
            can_run_fa2 = False
        if not gpu_supports_fa2():
            can_run_fa2 = False

        if not can_run_fa2:
            del override_args['flash_attn']  # will be later set to Auto after parsing
        else:
            logger.info("Provided args specify using flash-attention 2. Based on preliminary checks it should work.")

    # Load the arguments
    model_args, data_args, training_args, finetuning_args, generating_args = get_train_args(override_args)

    logger.info(
    	f"\n|--------------------------- Model Configuration ---------------------------|\n{json.dumps(make_json_serializable(model_args.to_dict()), indent=4)}")  # FIXME: ModelArguments not serialisable
    logger.info(
        f"\n|--------------------------- Data Configuration ---------------------------|\n{json.dumps(data_args.to_dict(), indent=4)}")
    logger.info(
        f"\n|--------------------------- Training Configuration ---------------------------|\n{json.dumps(training_args.to_dict(), indent=4)}")
    logger.info(
        f"\n|--------------------------- Generating Configuration ---------------------------|\n{json.dumps(finetuning_args.to_dict(), indent=4)}")
    logger.info(
        f"\n|--------------------------- Evaluation Configuration ---------------------------|\n{json.dumps(generating_args.to_dict(), indent=4)}\n")

    if training_method == "SFT":
        # Generic trainer for common PeFT methods like LoRA and PT
        from trainers.sft import Trainer

        trainer = Trainer(
            model_args,
            data_args,
            training_args,
            finetuning_args,
            generating_args,
        )
    elif training_method == "DPO":
        from trainers.dpo import Trainer

        trainer = Trainer(
            model_args,
            data_args,
            training_args,
            finetuning_args,
            generating_args,
        )
    else:
        raise ValueError(f"PEFT method {training_method} currently not supported.")

    trainer.train()


if __name__ == '__main__':
    # To run with accelerate, $ accelerate launch --config_file config_ds_zero_stage2_no_fp16.yaml tune_lora_baseline.py
    main()