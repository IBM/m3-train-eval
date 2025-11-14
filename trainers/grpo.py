import os
import re
import json
from contextlib import nullcontext
from typing import Optional, TYPE_CHECKING

import torch
import torch.nn.functional as F
from loguru import logger
from torch.utils.data import DataLoader
from typing_extensions import override

from data_utils.collator import GRPODataCollatorWith4DAttentionMask
from data_utils.custom_loader import AgentTrajectoryGRPOData
from extras.constants import IGNORE_INDEX
from hparams import ModelArguments, DataArguments, TrainingArguments, FinetuningArguments, GeneratingArguments
from model.loader import load_model, create_ref_model
from trainers.base import BaseTrainer
from envs.tool_call_env import M3GRPOEnv
from trl import GRPOTrainer, GRPOConfig
# from trainers.utils import nested_detach, get_batch_logps
# from extras.custom import create_dir, is_rank_0
# from peft import get_peft_model, LoraConfig
# from transformers import AutoModelForCausalLM
# from envs.loader import get_agent_env
from data_utils.template import Template, TEMPLATES
from evaluation import parse_llm_response

if TYPE_CHECKING:
    from transformers import PreTrainedModel


REWARD_MAPPING = {
    "{REWARD_PARSING_ERROR}": 0.0,
    "{REWARD_BAD_TOOL_CALL}": 0.0,
    "{REWARD_NO_PENALTY}": 0.0,
    "{REWARD_ERROR_NO_CATEGORY}": 0.0,
    "{REWARD_AGENT_STUCK}": 0.0,
    "{REWARD_SUCCESS_TOOL_CALL}": 0.0,
    "{REWARD_SUCCESS_RETRIEVAL_CALL}": 0.0,
    "{REWARD_FINAL_ANSWER_MATCH}": 1.0,
    "{REWARD_FINAL_ANSWER_NO_MATCH}": 0.0,
    "{REWARD_SCENARIO_NOT_FOLLOWED}": 0.0
}

TEMPLATE_MAPPUNG={
    "ibm-granite/granite-4.0-micro": "student_granite4"
}


class Trainer(BaseTrainer):
    """
    Custom Trainer integrating Hugging Face TRL's GRPOTrainer into the BaseTrainer architecture.
    """

    def __init__(            
        self,
        model_args: "ModelArguments",
        data_args: "DataArguments",
        training_args: "TrainingArguments",
        finetuning_args: "FinetuningArguments",
        generating_args: "GeneratingArguments",
        env:"M3GRPOEnv",
        reward_fn=None,
    ):
        super().__init__(model_args, data_args, training_args, finetuning_args, generating_args)

        # Setup Tool Call Environment
        self.env=env
        self.agent_template = TEMPLATES[TEMPLATE_MAPPUNG[model_args.model_name_or_path]]

        # Initialize TRL GRPO config
        self.grpo_config = GRPOConfig(
            output_dir=self.training_args.output_dir,
            learning_rate=self.training_args.learning_rate,
            num_generations=getattr(self.finetuning_args, "num_generations", 1),
            max_prompt_length=None,
            max_completion_length=getattr(self.generating_args, "max_new_tokens", 256),
            temperature=getattr(self.generating_args, "temperature", 1.0),
            top_k=getattr(self.generating_args, "top_k", 0),
            ds3_gather_for_generation=False,
            use_vllm=getattr(self.training_args, "use_vllm", False),
            loss_type="bnpo",
            num_train_epochs=self.training_args.num_train_epochs,
            logging_dir=self.training_args.logging_dir,
            per_device_train_batch_size=self.training_args.per_device_train_batch_size,
            gradient_accumulation_steps=self.training_args.gradient_accumulation_steps,            
        )

        # self.grpo_config = GRPOConfig(
        #     bf16=self.training_args.bf16,
        #     fp16=self.training_args.fp16,
        #     use_vllm=getattr(self.training_args, "use_vllm", False),  # optional acceleration
        # )

        # Store reward function
        self.reward_fn = reward_fn or self.reward_fn

        # Create the Hugging Face GRPO trainer
        self.hf_grpo_trainer = GRPOTrainer(
            model=self.model,
            args=self.grpo_config,
            train_dataset=self.train_dataset,
            eval_dataset=getattr(self, "eval_dataset", None),
            reward_funcs=self.reward_fn,
        )

        logger.success("✅ GRPOTrainer initialized successfully using TRLs GRPOTrainer.")

    def reward_fn_batch(self, prompts, completions, **kwargs) -> list[float]:
        """
        Reward function for GRPO. Produces one reward per completion.
        GRPO requires: len(rewards) = batch_size * num_generations
        """
        num_generations = self.grpo_config.num_generations
        rewards = []

        # Set up tools for the batch
        self.env.setup_tools(kwargs["tools"][0], kwargs["domain"][0])

        # For each generated completion, compute reward
        for i, completion in enumerate(completions):

            # Identify which prompt this completion came from
            input_idx = i // num_generations
            prompt = prompts[input_idx]

            # Extract user query and golden answer for env and setup tools
            self.env.setup_tools(kwargs["tools"][input_idx], kwargs["domain"][input_idx])
            query = prompt.split("<|start_of_role|>user<|end_of_role|>")[-1].split(".<|end_of_text|>")[0]
            self.env.set_curr_query(query)
            self.env.set_curr_golden_answer(kwargs["output"][input_idx])

            # Parse the model-generated tool/action
            action = parse_llm_response(completion, agent_template=self.agent_template)

            # Step env
            observation = self.env.get_observation(action)
            reward_key, success = self.env.get_reward(action, observation)

            # Assign reward
            if success:
                rewards.append(1.0)
            else:
                rewards.append(REWARD_MAPPING[reward_key])

        return rewards

    def reward_fn(self, prompts, completions, **kwargs) -> list[float]:
        """
        TODO : Extend reward function for batch operation of inputs.
        Reward function based on the ToolCallEnv. Two types of rewards
        (1) FunctionCall construction reward.
        (2) Final answer construction reward.
        """
        self.env.setup_tools(kwargs["tools"][0], kwargs["domain"][0])
        self.env.set_curr_query(prompts[0].split("<|start_of_role|>user<|end_of_role|>")[-1].split(".<|end_of_text|>")[0]) # Same user query for a particular data sample
        self.env.set_curr_golden_answer(kwargs["output"][0])
        rewards=[]
        for output, completion in zip(kwargs["output"],completions):
            action=parse_llm_response(completion,agent_template=self.agent_template)
            observation=self.env.get_observation(action)
            reward, success = self.env.get_reward(action, observation)
            if success:
                rewards.append(1.0)
            else:
                rewards.append(REWARD_MAPPING[reward])
        return [rewards]

    def train(self):
        logger.info("🚀 Starting GRPO training with TRL…")

        # This delegates actual GRPO optimization to the TRL trainer
        self.hf_grpo_trainer.train()

        logger.success("🎯 GRPO training complete!")

    def init_foundation_model(self, is_trainable):
        logger.info(f"Initializing foundation model...")
        model = load_model(self.tokenizer, self.model_args, self.finetuning_args, is_trainable, add_valuehead=False)
        return model

    def _build_model(self):
        foundation_model = self.init_foundation_model(is_trainable=True)
        return foundation_model

        # base_model = AutoModelForCausalLM.from_pretrained(
        #     self.model_args.model_name_or_path,
        #     torch_dtype=torch.bfloat16 if self.training_args.bf16 else torch.float16,
        # )
        # lora_cfg = LoraConfig(r=self.finetuning_args.lora_rank, lora_alpha=self.finetuning_args.lora_alpha)
        # model = get_peft_model(base_model, lora_cfg)        
        return model

    @override
    def _build_dataloader(self, setting: str):
        with self.accelerator.main_process_first():
            dataset = AgentTrajectoryGRPOData(
                self.template,
                self.tokenizer,
                self.processor, 
                self.data_args,
                setting,
                dataset=self.data_args.dataset
            )

            # Define the collator.
            collator = GRPODataCollatorWith4DAttentionMask(
                tokenizer=self.tokenizer,
                pad_to_multiple_of=8,
                template=self.template,
                processor=self.processor,
            )
        
        ds_loader = DataLoader(dataset, batch_size=self.training_args.per_device_train_batch_size, collate_fn=collator)
        self.train_dataset, self.train_dataloader = dataset, ds_loader


    def _init_trackers(self):
        # Initialize the trackers
        args = {**vars(self.training_args), **vars(self.data_args), **vars(self.model_args),
                **vars(self.finetuning_args), **vars(self.generating_args)}        

    # def _init_trackers(self):
    #     logger.info("Initializing trackers for GRPOTrainer...")
    #     self.accelerator.init_trackers("grpo_trainer")

