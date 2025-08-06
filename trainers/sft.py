import os

from loguru import logger
from torch.utils.data import DataLoader
from typing_extensions import override

from data_utils.collator import SFTDataCollatorWith4DAttentionMask
from data_utils.custom_loader import AgentTrajectorySFTData
from extras.constants import IGNORE_INDEX
from extras.custom import create_dir, is_rank_0
from hparams import ModelArguments, DataArguments, TrainingArguments, FinetuningArguments, GeneratingArguments
from model.loader import load_model
from trainers.base import BaseTrainer


class Trainer(BaseTrainer):
    """
    Trainer for SFT with PEFT.
    """

    def __init__(
            self,
            model_args: "ModelArguments",
            data_args: "DataArguments",
            training_args: "TrainingArguments",
            finetuning_args: "FinetuningArguments",
            generating_args: "GeneratingArguments",
    ):
        # Specify here sft hyperparams
        super().__init__(model_args, data_args, training_args, finetuning_args, generating_args)

    @override
    def _build_dataloader(self, setting: str):
        with self.accelerator.main_process_first():
            dataset = AgentTrajectorySFTData(
                self.template,
                self.tokenizer,
                self.processor,
                self.data_args,
                setting,
            )

            # Define the collator.
            collator = SFTDataCollatorWith4DAttentionMask(
                tokenizer=self.tokenizer,
                label_pad_token_id=IGNORE_INDEX if self.data_args.ignore_pad_token_for_loss else self.tokenizer.pad_token_id,
                pad_to_multiple_of=8 if setting.lower() == "supervised" else None,  # for shift short attention <-?
                template=self.template,
                processor=self.processor,
            )

        # # Leave this as is to only read prompt for any type of data
        ds_loader = DataLoader(dataset, batch_size=self.training_args.per_device_train_batch_size, collate_fn=collator)
        self.train_dataset, self.train_dataloader = dataset, ds_loader

    def init_foundation_model(self, is_trainable):
        logger.info(f"Initializing foundation model...")
        model = load_model(self.tokenizer, self.model_args, self.finetuning_args, is_trainable, add_valuehead=False)
        return model

    def _build_model(self):
        foundation_model = self.init_foundation_model(is_trainable=True)
        return foundation_model

    def _init_trackers(self):
        # Initialize the trackers
        # args = {**vars(self.training_args), **vars(self.data_args), **vars(self.model_args),
        #         **vars(self.finetuning_args), **vars(self.generating_args)}
        if 'wandb' in self.training_args.report_to:
            with self.accelerator.main_process_first():
                self.accelerator.init_trackers(
                    project_name='AI4ToolInvocation',
                    # config=args,
                    init_kwargs={"wandb": {"name": self.training_args.run_name}},
                )

    # @override
    # def _save(self, dir_tag: str):
    #     super()._save(dir_tag)
    #
    #     # Create a directory to save the model
    #     save_at = os.path.join(self.training_args.output_dir, dir_tag)
    #     create_dir(save_at)
    #
    #     model = self.accelerator.unwrap_model(self.model)
    #
    #     model.save_pretrained(
    #         save_directory=os.path.join(save_at, "PEFT"),
    #         is_main_process=is_rank_0()
    #     )