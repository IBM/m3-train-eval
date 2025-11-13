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
from extras.custom import create_dir, is_rank_0
from hparams import ModelArguments, DataArguments, TrainingArguments, FinetuningArguments, GeneratingArguments
from model.loader import load_model, create_ref_model
from trainers.base import BaseTrainer
from trainers.utils import nested_detach, get_batch_logps
from envs.tool_call_env import M3GRPOEnv
from trl import GRPOTrainer, GRPOConfig
from transformers import AutoModelForCausalLM
from envs.loader import get_agent_env

if TYPE_CHECKING:
    from transformers import PreTrainedModel

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
    ):
        super().__init__(model_args, data_args, training_args, finetuning_args, generating_args)

        # Setup Tool Call Environment
        self.env=env

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

    def reward_fn(self, prompts, completions, **kwargs) -> list[float]:
        """
        TODO : Extend reward function for batch operation of inputs.
        Reward function based on the ToolCallEnv. Two types of rewards
        (1) FunctionCall construction reward.
        (2) Final answer construction reward.
        """
        import pdb
        pdb.set_trace()
        # self.env.setup_tools(tools, doc_collections, domain)
        rewards=[]
        for output, completion in zip(kwargs["output"],completions):
            if "<FINAL>" in output:
                # ScorerLLM used to get reward
                rewards.append(1.0)
            elif ("<tool_call>" in output) or ('"name"' in output):
                # Check for prescense of tool call. If present run tool call.
                try:
                    api_func=json.loads(output)
                    rewards.append(1.0)
                except:
                    rewards(0.0)
            else:
                # This technically shouldn't exist.
                import pdb
                pdb.set_trace()

        # import pdb
        # pdb.set_trace()
        # reward = 0.0

        # # Reward if model makes a valid tool call
        
        # if "<tool_call>" in completions and "</tool_call>" in completions:
        #     reward += 1.0

        # # Penalize hallucinated arguments or missing fields
        # if "error" in completions.lower() or "invalid" in completions.lower():
        #     reward -= 0.5

        # # Reward for matching expected output pattern
        # if "success" in completions.lower() or "result" in completions.lower():
        #     reward += 0.5

        return [rewards]

    def train(self):
        logger.info("🚀 Starting GRPO training with TRL…")

        # This delegates actual GRPO optimization to the TRL trainer
        self.hf_grpo_trainer.train()

        logger.success("🎯 GRPO training complete!")

    def _build_model(self):
        """
        Override: load a causal LM for RL fine-tuning.
        """
        model = AutoModelForCausalLM.from_pretrained(
            self.model_args.model_name_or_path,
            torch_dtype=torch.bfloat16 if self.training_args.bf16 else torch.float16,
        )
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

