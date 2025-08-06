import math
import os
import time
from collections import defaultdict
from typing import Union, Any, Optional, Tuple

import accelerate
import safetensors
import safetensors.torch
import torch
from accelerate import __version__ as accelerate_version
from accelerate.state import AcceleratorState
from accelerate.utils import ProjectConfiguration, DistributedDataParallelKwargs, DataLoaderConfiguration, \
    DistributedType
from loguru import logger
from packaging import version
from peft import PeftModel
from tqdm import tqdm
from transformers import get_scheduler, PreTrainedModel
from transformers.pytorch_utils import ALL_LAYERNORM_LAYERS
from transformers.trainer_pt_utils import get_parameter_names
from transformers.training_args import ParallelMode
from transformers.utils import is_torch_mps_available, is_peft_available

from data_utils.template import get_template_and_fix_tokenizer
from extras.custom import create_dir, is_rank_0
from hparams import ModelArguments, DataArguments, TrainingArguments, FinetuningArguments, GeneratingArguments
from model.loader import load_tokenizer

# Name of the files used for checkpointing
TRAINING_ARGS_NAME = "training_args.bin"
TRAINER_STATE_NAME = "trainer_state.json"
OPTIMIZER_NAME = "optimizer.pt"
SCALER_NAME = "scaler.pt"
OPTIMIZER_NAME_BIN = "optimizer.bin"
SCHEDULER_NAME = "scheduler.pt"
SAFE_WEIGHTS_NAME = "model.safetensors"
WEIGHTS_NAME = "pytorch_model.bin"


class BaseTrainer(object):

    def __init__(
            self,
            model_args: "ModelArguments",
            data_args: "DataArguments",
            training_args: "TrainingArguments",
            finetuning_args: "FinetuningArguments",
            generating_args: "GeneratingArguments",
    ):
        # Set the arguments
        self.training_args = training_args
        self.model_args = model_args
        self.data_args = data_args
        self.finetuning_args = finetuning_args
        self.generating_args = generating_args

        # Log some info
        logger.info("=" * 60)
        logger.info("\t\t||\t\t" + "New training process started." + "\t\t||")
        logger.info("=" * 60)
        logger.info("\n")
        logger.info(f"Experiment directory: {self.training_args.logging_dir}")

        # init counts
        self.starting_epoch = 0
        self.global_step: int = -1
        self.epoch: int = self.starting_epoch

        # Internal variable to count flops in each process
        self.current_flops = 0

        # [1] setup tokenizer
        self._build_tokenizer()

        # [2] setup model
        logger.info("Building model...")
        start = time.monotonic_ns()
        self.model = self._build_model()
        end = time.monotonic_ns()
        logger.debug(self.model)
        logger.success(f"Building model done in {(end - start) / 1e6:.2f}ms")

        # init with accelerate
        self.create_accelerator_and_postprocess()
        self.accelerator.wait_for_everyone()

        # [3] setup data_loader
        with self.accelerator.main_process_first():
            logger.info("Building dataset...")
            start = time.monotonic_ns()
            self._build_dataloader('supervised')
            self._build_eval_dataloader('supervised')
            end = time.monotonic_ns()
            logger.success(f"Building dataset done in {(end - start) / 1e6:.2f}ms")

        # [4] setup optimizer & scheduler
        with self.accelerator.main_process_first():
            logger.info("Building optimizer and scheduler...")
            start = time.monotonic_ns()
            self.optimizer = self._build_optimizer()
            self.lr_scheduler = self._build_scheduler()
            end = time.monotonic_ns()
            logger.success(f"Building optimizer and scheduler done in {(end - start) / 1e6:.2f}ms")

        # [5] accelerate prepare
        logger.info("Initializing accelerate...")
        start = time.monotonic_ns()
        self._accelerator_prepare()
        end = time.monotonic_ns()
        logger.success(f"Initializing accelerate done in {(end - start) / 1e6:.2f}ms")

        # We need to recalculate our total training steps as the size of the training dataloader may have changed after
        # Accelerator's prepare function.
        self._recalculate_training_metrics()

        # # save config file path
        # self.args.device = self.accelerator.device

        # Finally, initialize the trackers. During init of the model we computed new arguments. Thus setting after that.
        self._init_trackers()

        # Initialize training metrics. e.g.
        # metric = evaluate.load("squad_v2" if args.version_2_with_negative else "squad")
        self.metric = None

    def create_accelerator_and_postprocess(self):
        project_config = ProjectConfiguration(
            logging_dir=self.training_args.logging_dir,
        )

        # For allocating accelerator's ddp_handler (handler_class_to_attr = {DistributedDataParallelKwargs: "ddp_handler")..}
        # Can also do this after init self.accelerator as self.accelerator.ddp_handler = DistributedDataParallelKwargs(**kwargs)
        handlers = None
        if self.training_args.parallel_mode == ParallelMode.DISTRIBUTED:
            kwargs = {}
            if self.training_args.ddp_find_unused_parameters is not None:
                kwargs["find_unused_parameters"] = self.training_args.ddp_find_unused_parameters
            elif isinstance(self.model, PreTrainedModel):
                # find_unused_parameters breaks checkpointing as per
                # https://github.com/huggingface/transformers/pull/4659#issuecomment-643356021
                kwargs["find_unused_parameters"] = not self.model.is_gradient_checkpointing
            else:
                kwargs["find_unused_parameters"] = True

            if self.training_args.ddp_bucket_cap_mb is not None:
                kwargs["bucket_cap_mb"] = self.training_args.ddp_bucket_cap_mb

            if self.training_args.ddp_broadcast_buffers is not None:
                kwargs["broadcast_buffers"] = self.training_args.ddp_broadcast_buffers

            handlers = [DistributedDataParallelKwargs(**kwargs)]
            logger.info("Created DistributedDataParallel handler for accelerator with kwargs: {}".format(kwargs))

        # when using DeepSpeed, the `gradient_accumulation_steps` is properly set either
        # > from the DeepSpeed plugin/config
        # > from `accelerate launch` via `--gradient_accumulation_steps`
        # > defaulting to the passed `args.gradient_accumulation_steps` (using this + setting auto in the config file)
        args = {
            "deepspeed_plugin": self.training_args.deepspeed_plugin,
            "gradient_accumulation_steps": self.training_args.gradient_accumulation_steps,
            "project_config": project_config,
            "kwargs_handlers": handlers
        }
        if 'wandb' in self.training_args.report_to:
            args["log_with"] = ["wandb"]

        """             [Here] From the original implementation of HF trainer           """
        # accelerator_config is a subset of arguments relating to the underlying [`accelerate.Accelerator`]
        # implementation utilized in the `Trainer` that can be customized. Mostly relating to data. Can provide it as a
        # loaded dict or path to training args else the default is loaded by the Xformer's training args parser
        grad_acc_kwargs = {}
        if self.training_args.accelerator_config.gradient_accumulation_kwargs is not None:
            grad_acc_kwargs = self.training_args.accelerator_config.gradient_accumulation_kwargs

        # check if num_steps is attempted to be passed in gradient_accumulation_kwargs
        if "num_steps" in grad_acc_kwargs:
            if self.training_args.gradient_accumulation_steps > 1:
                # raise because we do not know which setting is intended.
                raise ValueError(
                    "The `AcceleratorConfig`'s `num_steps` is set but `gradient_accumulation_steps` is greater than 1 in the passed `TrainingArguments`"
                    "If using the passed `AcceleratorConfig` is desired, do not set the `TrainingArguments` `gradient_accumulation_steps`."
                )
            else:
                self.training_args.gradient_accumulation_steps = grad_acc_kwargs["num_steps"]

        accelerator_config = self.training_args.accelerator_config.to_dict()

        # Extract dataloader config params from accelerator config
        dataloader_params = ["split_batches", "dispatch_batches", "even_batches", "use_seedable_sampler"]
        dataloader_config = DataLoaderConfiguration(
            **{param: accelerator_config.pop(param) for param in dataloader_params}
        )
        dataloader_config.data_seed = self.training_args.data_seed

        # dataloader_pin_memory check
        non_blocking = accelerator_config.pop("non_blocking")
        if non_blocking and not self.training_args.dataloader_pin_memory:
            logger.warning(
                "`non_blocking` is enabled but `dataloader_pin_memory` is not. For the best performance, it's recommended to enable both."
            )
        dataloader_config.non_blocking = non_blocking
        accelerator_config.pop("gradient_accumulation_kwargs")

        args["dataloader_config"] = dataloader_config
        # create accelerator object
        self.accelerator = accelerate.Accelerator(**args)

        """             [Here] From the original implementation of HF trainer           """
        # deepspeed and accelerate flags covering both trainer args and accelerate launcher
        self.is_deepspeed_enabled = getattr(self.accelerator.state, "deepspeed_plugin", None) is not None
        self.is_fsdp_enabled = getattr(self.accelerator.state, "fsdp_plugin", None) is not None

        # post accelerator creation setup
        if self.is_fsdp_enabled:
            fsdp_plugin = self.accelerator.state.fsdp_plugin
            for param in ["limit_all_gathers", "activation_checkpointing"]:
                setattr(fsdp_plugin, param, self.training_args.fsdp_config.get(param, getattr(fsdp_plugin, param)))
            if fsdp_plugin.activation_checkpointing and self.training_args.gradient_checkpointing:
                raise ValueError(
                    "The activation_checkpointing in FSDP config and the gradient_checkpointing in training arg "
                    "can't be set to True simultaneously. Please use FSDP's activation_checkpointing logic "
                    "when using FSDP."
                )
        if self.is_deepspeed_enabled and getattr(self.training_args, "hf_deepspeed_config", None) is None:
            logger.info("Using deepspeed plugin but its args have not been set, will set them based on Trainer args")
            self.propagate_args_to_deepspeed()

        # `save_only_model` can't be used with DeepSpeed/FSDP along with `load_best_model_at_end`
        if (
                self.training_args.save_only_model
                and (self.is_deepspeed_enabled or self.is_fsdp_enabled)
                and self.training_args.load_best_model_at_end
        ):
            wrapper = "DeepSpeed" if self.is_deepspeed_enabled else "FSDP"
            raise ValueError(f"{wrapper} can't be used with `save_only_model` along with `load_best_model_at_end`.")

        # `auto_find_batch_size` isn't supported yet with DeepSpeed Zero-3
        if (
                self.is_deepspeed_enabled
                and self.accelerator.state.deepspeed_plugin.zero_stage == 3
                and self.training_args.auto_find_batch_size
        ):
            raise ValueError("`auto_find_batch_size` isn't supported yet with DeepSpeed Zero-3. Please consider using Zero-2, Zero-1, or FSDP")
        if (
                self.training_args.save_only_model
                and self.is_fsdp_enabled
                and "SHARDED_STATE_DICT" in str(self.accelerator.state.fsdp_plugin.state_dict_type)
        ):
            raise ValueError("save_only_model option is not compatible with FSDP state dict type 'SHARDED_STATE_DICT'")

    def propagate_args_to_deepspeed(self, auto_find_batch_size=False):
        """
        Sets values in the deepspeed plugin based on the Trainer args
        """
        from transformers.integrations.deepspeed import HfTrainerDeepSpeedConfig

        ds_plugin = self.accelerator.state.deepspeed_plugin

        ds_plugin.hf_ds_config = HfTrainerDeepSpeedConfig(ds_plugin.hf_ds_config.config)
        ds_plugin.deepspeed_config = ds_plugin.hf_ds_config.config
        ds_plugin.hf_ds_config.trainer_config_process(self.training_args, auto_find_batch_size)

    def _build_tokenizer(self):
        # [1a] Load tokenizer
        logger.info(f"Loading FM tokenizer for {self.model_args.model_name_or_path}")
        tokenizer_module = load_tokenizer(self.model_args)
        self.processor, self.tokenizer = tokenizer_module["processor"], tokenizer_module["tokenizer"]

        # [1b] Load the template
        logger.info(f"Loading template {self.data_args.template}")
        self.template = get_template_and_fix_tokenizer(self.tokenizer, self.data_args)

    def _build_dataloader(self, setting: str):
        raise NotImplementedError()

    def _build_eval_dataloader(self, setting: str):
        self.eval_dataloader = None

    def _build_model(self):
        raise NotImplementedError

    def _ds_prepare(self):
        # [TO AVOID] You must specify a training or evaluation dataloader in accelerate.prepare() when using DeepSpeed
        # Debug: https://github.com/huggingface/accelerate/pull/676
        AcceleratorState().deepspeed_plugin.deepspeed_config[
            "train_micro_batch_size_per_gpu"] = self.training_args.per_device_train_batch_size

    def get_decay_parameter_names(self, model) -> list[str]:
        """
        Get all parameter names that weight decay will be applied to.

        This function filters out parameters in two ways:
        1. By layer type (instances of layers specified in ALL_LAYERNORM_LAYERS)
        2. By parameter name patterns (containing 'bias', 'layernorm', or 'rmsnorm')
        """
        decay_parameters = get_parameter_names(model, ALL_LAYERNORM_LAYERS, ["bias", "layernorm", "rmsnorm"])
        return decay_parameters

    def _build_optimizer(self):
        # Split weights in two groups, one with weight decay and the other not.

        decay_parameters = self.get_decay_parameter_names(self.model)
        optimizer_grouped_parameters = [
            {
                "params": [p for n, p in self.model.named_parameters() if (n in decay_parameters and p.requires_grad)],
                "weight_decay": self.training_args.weight_decay,
            },
            {
                "params": [p for n, p in self.model.named_parameters() if (n not in decay_parameters and p.requires_grad)],
                "weight_decay": 0.0,
            },
        ]

        # Creates Dummy Optimizer if `optimizer` was specified in the config file else creates Adam Optimizer
        optimizer_cls = (
            torch.optim.AdamW  # TODO: Add the logic to create optimiser cls based on the provided training args (ref. HF's Trainers)
            if self.accelerator.state.deepspeed_plugin is None
               or "optimizer" not in self.accelerator.state.deepspeed_plugin.deepspeed_config  # Can also replace this with 'if not self.is_deepspeed_enabled'
            else accelerate.utils.DummyOptim
        )
        optimizer = optimizer_cls(optimizer_grouped_parameters, lr=self.training_args.learning_rate)

        return optimizer

    def _build_scheduler(self):

        # Do this after init dataloaders and before preparing with accelerator
        name = self.training_args.lr_scheduler_type
        len_dataloader = len(self.train_dataloader)
        num_update_steps_per_epoch = math.ceil(len_dataloader / self.training_args.gradient_accumulation_steps)
        num_train_steps = int(self.training_args.num_train_epochs * num_update_steps_per_epoch)
        # If num_train_steps manually provided. Src: https://github.com/huggingface/transformers/blob/main/examples/pytorch/question-answering/run_qa_no_trainer.py#L798
        # num_train_steps = self.args.num_train_steps * self.accelerator.num_processes
        num_warmup_steps = self.training_args.get_warmup_steps(num_train_steps)

        # Creates Dummy Scheduler if `scheduler` was specified in the config file else creates `args.lr_scheduler_type` Scheduler
        if (
                self.accelerator.state.deepspeed_plugin is None
                or "scheduler" not in self.accelerator.state.deepspeed_plugin.deepspeed_config
        ):  # Can also replace this with 'if not self.is_deepspeed_enabled'
            lr_scheduler = get_scheduler(
                name=name,
                optimizer=self.optimizer,
                num_warmup_steps=num_warmup_steps,
                num_training_steps=num_train_steps,
            )
        else:
            def _lr_scheduler_callable(optimizer):
                _lr_scheduler = get_scheduler(
                    name=name,
                    optimizer=optimizer,
                    num_warmup_steps=num_warmup_steps,
                    num_training_steps=num_train_steps,
                )
                return _lr_scheduler
            lr_scheduler = accelerate.utils.DummyScheduler(
                self.optimizer,
                total_num_steps=num_train_steps,
                warmup_num_steps=num_warmup_steps,
                lr_scheduler_callable=_lr_scheduler_callable,
            )
        return lr_scheduler

    def _accelerator_prepare(self):

        if self.eval_dataloader is not None:
            self.train_dataloader, self.eval_dataloader, self.model, self.optimizer, self.lr_scheduler = self.accelerator.prepare(
                self.train_dataloader, self.eval_dataloader, self.model, self.optimizer, self.lr_scheduler)
        else:
            self.train_dataloader, self.model, self.optimizer, self.lr_scheduler = self.accelerator.prepare(
                self.train_dataloader, self.model, self.optimizer, self.lr_scheduler)

    def _recalculate_training_metrics(self):

        len_dataloader = len(self.train_dataloader)
        self.num_update_steps_per_epoch = math.ceil(len_dataloader / self.training_args.gradient_accumulation_steps)
        num_train_steps = self.training_args.num_train_epochs * self.num_update_steps_per_epoch

        # # # After wards we recalculate our number of training epochs.
        # # Keep this. Useful when num_train_steps is to be set manually
        # self.args.num_epochs = math.ceil(num_train_steps / self.num_update_steps_per_epoch)
        self.total_batch_size = self.training_args.per_device_train_batch_size * self.accelerator.num_processes * self.training_args.gradient_accumulation_steps

        logger.info("\n")
        logger.info("***** Running training *****")
        logger.info(f"  Num examples = {len(self.train_dataset)}")
        logger.info(f"  Num Epochs = {self.training_args.num_train_epochs}")
        logger.info(f"  Instantaneous batch size per device = {self.training_args.per_device_train_batch_size}")
        logger.info(f"  Total train batch size (w. parallel, distributed & accumulation) = {self.total_batch_size}")
        logger.info(f"  Gradient Accumulation steps = {self.training_args.gradient_accumulation_steps}")
        logger.info(f"  Total optimization steps = {num_train_steps}")
        logger.info("\n")

    def _init_trackers(self):
        raise NotImplementedError

    def compute_loss(self, batch: dict[str, "torch.Tensor"]):
        """
        How the loss is computed by Trainer. By default, all models return the loss in the first element.
        Subclass and override for custom behavior.
        """
        outputs = self.model(
            input_ids=batch["input_ids"],
            attention_mask=batch["attention_mask"],
            labels=batch["labels"],
            output_hidden_states=True,
        )
        if isinstance(outputs, dict) and "loss" not in outputs:
            raise ValueError(
                "The model did not return a loss from the inputs, only the following keys: "
                f"{','.join(outputs.keys())}. For reference, the inputs it received are {','.join(batch.keys())}."
            )
        # We don't use .loss here since the model may return tuples instead of ModelOutput.
        loss = outputs["loss"] if isinstance(outputs, dict) else outputs[0]

        metrics = {}
        return loss, metrics

    def training_step(self, batch: dict[str, "torch.Tensor"]) -> Tuple[torch.Tensor, dict]:
        r"""Forward step for training. This function is called
        in ``_train_step`` & ``_test_step`` function.
        """
        # with self.accelerator.accumulate(self.model):  # HF Trainer (4.52.4) explicitly avoids relying on `accelerator.accumulate`
        loss, metrics = self.compute_loss(batch)

        del batch
        if self.training_args.torch_empty_cache_steps is not None and self.global_step % self.training_args.torch_empty_cache_steps == 0:
            # This can help avoid CUDA OOM errors by lowering peak VRAM usage at a cost of about [10% slower performance]
            # (https://github.com/huggingface/transformers/issues/31372).
            if is_torch_mps_available():
                torch.mps.empty_cache()
            else:
                torch.cuda.empty_cache()

        # 'n_gpu' will only be greater than one when you have multiple GPUs available but are not using distributed training.
        # For distributed training torch.nn.DistributedDataParallel (several GPUs, each having its own process), it will always be 1.
        # For not-distributed training torch.nn.DataParallel (several GPUs in one single process), it will be equal to number of GPUs available
        if self.training_args.n_gpu > 1:
            loss = loss.mean()  # mean() to average on multi-gpu parallel training

        loss = loss / self.training_args.gradient_accumulation_steps

        kwargs = {}
        # Turning off loss scaling w.r.t. gradient accumulation when DeepSpeed is enabled
        # https://github.com/huggingface/transformers/pull/35808
        if self.accelerator.distributed_type == DistributedType.DEEPSPEED:
            kwargs["scale_wrt_gas"] = False

        self.accelerator.backward(loss, **kwargs)

        return loss.detach(), metrics

    def _train_epoch(self, resume_step=None):
        r"""Training epoch. Should return average loss of a batch (sample) over
                one epoch. See ``train_loop`` for usage.
        """
        self.model.train()

        # Skip first few batches if resuming from a checkpoint
        if self.epoch == self.starting_epoch and resume_step is not None:
            # We skip the first `n` batches in the dataloader when resuming from a checkpoint
            active_dataloader = self.accelerator.skip_first_batches(self.train_dataloader, resume_step)
        else:
            active_dataloader = self.train_dataloader

        epoch_metrics: dict = defaultdict(float)
        steps_in_epoch = len(active_dataloader)
        grad_norm: Optional[float] = None
        learning_rate = None
        for batch in tqdm(
                active_dataloader,
                desc=f"Training Epoch {self.epoch}",
                unit="batch",
                colour="GREEN",
                leave=False,
                dynamic_ncols=True,
                smoothing=0.04,
                disable=not self.accelerator.is_main_process,
        ):
            self.global_step += 1
            do_sync_step = (self.global_step + 1) % self.training_args.gradient_accumulation_steps == 0 or (self.global_step + 1) == steps_in_epoch
            # Since we perform prefetching, we need to manually set sync_gradients
            self.accelerator.gradient_state._set_sync_gradients(do_sync_step)

            tr_loss_step, tr_metrics = self.training_step(batch)

            self.current_flops += float(self.floating_point_ops(batch))
            if do_sync_step:
                # Since we perform prefetching, we need to manually set sync_gradients to True
                self.accelerator.gradient_state._set_sync_gradients(True)

                # Gradient clipping
                if self.training_args.max_grad_norm is not None and self.training_args.max_grad_norm > 0:

                    # Clip
                    _grad_norm = self.accelerator.clip_grad_norm_(
                        self.model.parameters(),
                        self.training_args.max_grad_norm,
                    )

                    # Get the grad norm
                    if self.accelerator.distributed_type == DistributedType.DEEPSPEED:
                        grad_norm = self.model.get_global_grad_norm()
                        # In some cases the grad norm may not return a float
                        if hasattr(grad_norm, "item"):
                            grad_norm = grad_norm.item()
                    else:
                        if hasattr(_grad_norm, "item"):
                            grad_norm = _grad_norm.item()
                        else:
                            grad_norm = _grad_norm

                self.optimizer.step()

                # Get learning rate before update
                learning_rate = self._get_learning_rate()
                if not self.accelerator.optimizer_step_was_skipped:
                    # Delay optimizer scheduling until metrics are generated
                    if not isinstance(self.lr_scheduler, torch.optim.lr_scheduler.ReduceLROnPlateau):
                        self.lr_scheduler.step()
                self.optimizer.zero_grad()

            # Log
            if 'wandb' in self.training_args.report_to:
                log_dict = {
                        "Step/Loss": tr_loss_step.cpu().item(),
                        # "Step/Learning Rate": self.optimizer.param_groups[0]["lr"],
                        "Step/Learning Rate": learning_rate,
                        "Step/Gradient Norm": grad_norm,
                        "FLOPS": self.current_flops,
                }
                if tr_metrics:
                    for k, v in tr_metrics.items():
                        log_dict[f"Step/{k}"] = v
                self.accelerator.log(log_dict, step=self.global_step)

            epoch_metrics['loss'] += tr_loss_step.cpu().item()
            if tr_metrics:
                for k, v in tr_metrics.items():
                    epoch_metrics[f"{k}"] += v

        self.accelerator.wait_for_everyone()
        # Compute the average losses for the epoch
        for key in epoch_metrics.keys():
            epoch_metrics[key] = (
                    epoch_metrics[key] / len(self.train_dataloader) * self.training_args.gradient_accumulation_steps
            )

        return epoch_metrics

    def _eval_epoch(self):
        raise NotImplementedError

    def train(self):
        r"""Training loop. The public entry of training process."""

        # # For Debugging
        # if self.accelerator.is_main_process:
        #     self._save("init")

        # Potentially load in the weights and states from a previous save
        resume_step = None
        if self.training_args.resume_from_checkpoint:
            checkpoint_path = self.training_args.resume_from_checkpoint
            path = os.path.basename(checkpoint_path)

            self.accelerator.print(f"Resumed from checkpoint: {checkpoint_path}")
            self.accelerator.load_state(checkpoint_path)
            # Extract `epoch_{i}` or `step_{i}`
            training_difference = os.path.splitext(path)[0]

            if "epoch" in training_difference:
                starting_epoch = int(training_difference.replace("epoch_", "")) + 1
                resume_step = None
                completed_steps = starting_epoch * self.num_update_steps_per_epoch
            else:
                # need to multiply `gradient_accumulation_steps` to reflect real steps
                resume_step = int(
                    training_difference.replace("step_", "")) * self.training_args.gradient_accumulation_steps
                starting_epoch = resume_step // len(self.train_dataloader)
                completed_steps = resume_step // self.training_args.gradient_accumulation_steps
                resume_step -= starting_epoch * len(self.train_dataloader)

        self.accelerator.wait_for_everyone()
        while self.epoch < self.training_args.num_train_epochs:
            logger.info("\n")
            logger.info("-" * 32)
            logger.info("Epoch {}: ".format(self.epoch))

            # Do training epoch
            if self.training_args.do_train:
                train_metrics = self._train_epoch(resume_step)

                if isinstance(train_metrics, dict):
                    for key, value in train_metrics.items():
                        logger.info("  |- Train/{}: {:.6f}".format(key, value))

                        if 'wandb' in self.training_args.report_to:
                            self.accelerator.log({"Epoch/{}".format(key): value}, step=self.global_step)

            # Do evaluation epoch
            if self.training_args.do_eval:
                logger.info("***** Running Evaluation *****")
                eval_metrics = self._eval_epoch()
                for keys, values in eval_metrics.items():
                    logger.info("  |- Eval/{}: {:.6f}".format(keys, values))
                    if 'wandb' in self.training_args.report_to:
                        # if is_rank_0():
                        #     print("epoch: ", self.epoch)
                        #     print("epoch valid_losses[loss]: ", values)
                        self.accelerator.log(
                            {"Epoch/Eval/{}".format(keys): values},
                            step=self.global_step,
                        )

            # Update info for each epoch
            self.epoch += 1

            if self.training_args.save_steps > 0 and self.epoch % self.training_args.save_steps == 0:
                self.accelerator.wait_for_everyone()
                # if self.accelerator.is_main_process:
                self._save(f"model_epoch_{self.epoch}")

        # Finish training and save final checkpoint
        self.accelerator.wait_for_everyone()
        self._save(f"final")

        self.accelerator.end_training()

    def _save(self, dir_tag: str):
        # Create a directory to save the model
        save_at = os.path.join(self.training_args.output_dir, dir_tag)
        create_dir(save_at)
        logger.info(f"Saving model checkpoint to {save_at}")

        if self.is_deepspeed_enabled or self.is_fsdp_enabled:
            state_dict = self.accelerator.get_state_dict(self.model)  # must be called at all ranks
        else:
            state_dict = self.model.state_dict()

        supported_classes = (PreTrainedModel,) if not is_peft_available() else (PreTrainedModel, PeftModel)
        if not isinstance(self.model, supported_classes):
            if isinstance(self.accelerator.unwrap_model(self.model, keep_torch_compile=False), supported_classes):
                self.accelerator.unwrap_model(self.model, keep_torch_compile=False).save_pretrained(
                    save_directory=os.path.join(save_at, "PEFT"),
                    state_dict=state_dict,
                    safe_serialization=self.training_args.save_safetensors,
                    # is_main_process=is_rank_0()
                )
            else:
                logger.info("Trainer.model is not a `PreTrainedModel`, only saving its state dict.")
                if self.training_args.save_safetensors:
                    safetensors.torch.save_file(
                        state_dict, os.path.join(save_at, SAFE_WEIGHTS_NAME), metadata={"format": "pt"}
                    )
                else:
                    torch.save(state_dict, os.path.join(save_at, WEIGHTS_NAME))
        else:
            self.model.save_pretrained(
                save_directory=os.path.join(save_at, "PEFT"),
                state_dict=state_dict,
                safe_serialization=self.training_args.save_safetensors,
                # is_main_process=is_rank_0()
            )


        if not self.training_args.save_only_model:
            raise NotImplementedError("Logic for saving optimizer state etc. is not implemented. Refer HF's trainer for implementing it.")

        # model = self.accelerator.unwrap_model(self.model)
        # model.save_pretrained(
        #     save_directory=os.path.join(save_at, "PEFT"),
        #     is_main_process=is_rank_0()
        # )

        # Good practice: save your training arguments together with the trained model
        torch.save(self.training_args, os.path.join(save_at, TRAINING_ARGS_NAME))

    def _get_learning_rate(self):
        if self.is_deepspeed_enabled:
            # with deepspeed's fp16 and dynamic loss scale enabled the optimizer/scheduler steps may
            # not run for the first few dozen steps while loss scale is too large, and thus during
            # that time `get_last_lr` will fail if called during that warm up stage, so work around it:
            try:
                last_lr = self.lr_scheduler.get_last_lr()[0]
            except AssertionError as e:
                if "need to call step" in str(e):
                    logger.warning("tried to get lr value before scheduler/optimizer started stepping, returning lr=0")
                    last_lr = 0
                else:
                    raise
        else:
            if isinstance(self.lr_scheduler, torch.optim.lr_scheduler.ReduceLROnPlateau):
                last_lr = self.optimizer.param_groups[0]["lr"]
            else:
                last_lr = self.lr_scheduler.get_last_lr()[0]

        if torch.is_tensor(last_lr):
            last_lr = last_lr.item()
        return last_lr

    def floating_point_ops(self, inputs: dict[str, Union[torch.Tensor, Any]]):
        """
        For models that inherit from [`PreTrainedModel`], uses that method to compute the number of floating point
        operations for every backward + forward pass. If using another model, either implement such a method in the
        model or subclass and override this method.

        Args:
            inputs (`Dict[str, Union[torch.Tensor, Any]]`):
                The inputs and targets of the model.

        Returns:
            `int`: The number of floating-point operations.
        """
        unwrapped_model = self.accelerator.unwrap_model(self.model)
        if hasattr(self.model, "floating_point_ops"):
            return self.model.floating_point_ops(inputs)
        elif hasattr(unwrapped_model, "floating_point_ops"):
            return unwrapped_model.floating_point_ops(inputs)
        else:
            return 0