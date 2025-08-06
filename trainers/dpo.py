import os
from contextlib import nullcontext
from typing import Optional, TYPE_CHECKING

import torch
import torch.nn.functional as F
from loguru import logger
from torch.utils.data import DataLoader
from trl import FDivergenceType, FDivergenceConstants
from trl.trainer.utils import cap_exp, RunningMoments
from typing_extensions import override

from data_utils.collator import PairwiseDataCollatorWithPadding
from data_utils.custom_loader import AgentTrajectoryPreferenceData
from extras.constants import IGNORE_INDEX
from extras.custom import create_dir, is_rank_0
from hparams import ModelArguments, DataArguments, TrainingArguments, FinetuningArguments, GeneratingArguments
from model.loader import load_model, create_ref_model
from trainers.base import BaseTrainer
from trainers.utils import nested_detach, get_batch_logps
from trl.models.utils import prepare_fsdp, prepare_deepspeed

if TYPE_CHECKING:
    from transformers import PreTrainedModel


class Trainer(BaseTrainer):
    """
    Trainer for DPO with PEFT.
    """

    def __init__(
            self,
            model_args: "ModelArguments",
            data_args: "DataArguments",
            training_args: "TrainingArguments",
            finetuning_args: "FinetuningArguments",
            generating_args: "GeneratingArguments",
    ):

        self.ref_model = None  # Default. It will get assigned in _build_model if finetuning_args.use_ref_model is true

        # Specify here dpo hyperparams. Ref [https://github.com/huggingface/trl/blob/main/trl/trainer/dpo_config.py]
        self.f_divergence_type = "reverse_kl"  # Type of f-divergence regularization function to compute divergence between policy and reference
        self.f_alpha_divergence_coef = 1.0  # α coefficient in the α-divergence u^-α regularization function for DPO loss.
        self.f_divergence_params = {FDivergenceConstants.ALPHA_DIVERGENCE_COEF_KEY: self.f_alpha_divergence_coef}
        self.reference_free = False  # Whether to ignore the provided reference model and implicitly use a reference model that assigns equal probability to all responses.
        self.discopop_tau = 0.05  # τ/temperature parameter from the DiscoPOP paper, which controls the shape of log ratio modulated loss. 0.05 is the recommended value

        self.beta = finetuning_args.pref_beta
        self.loss_type = finetuning_args.pref_loss
        self.ftx_gamma = finetuning_args.pref_ftx
        self.label_smoothing = finetuning_args.dpo_label_smoothing
        self.simpo_gamma = finetuning_args.simpo_gamma
        self.ld_alpha = finetuning_args.ld_alpha

        super().__init__(model_args, data_args, training_args, finetuning_args, generating_args)

        # Init after accelerator is initialised
        if self.loss_type == "bco_pair":
            self.running = RunningMoments(self.accelerator)

        # Prepare ref model
        if self.ref_model is not None:
            if self.is_deepspeed_enabled:
                if not (
                        getattr(self.ref_model, "is_loaded_in_8bit", False) or getattr(self.ref_model, "is_loaded_in_4bit", False)
                ):  # quantized models are already set on the correct device
                    self.ref_model = prepare_deepspeed(self.ref_model, self.accelerator)
            elif self.is_fsdp_enabled:
                self.ref_model = prepare_fsdp(self.ref_model, self.accelerator)
            else:
                self.ref_model = self.accelerator.prepare_model(self.ref_model, evaluation_mode=True)
                self.ref_model.eval()

    @override
    def _build_dataloader(self, setting: str):
        with self.accelerator.main_process_first():
            dataset = AgentTrajectoryPreferenceData(
                self.template,
                self.tokenizer,
                self.processor,
                self.data_args,
                setting,
            )

            # Define the collator.
            collator = PairwiseDataCollatorWithPadding(
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

        # Create reference model
        if self.finetuning_args.use_ref_model:
            if self.finetuning_args.ref_model is None and (not self.training_args.do_train):  # use the model itself
                self.ref_model = foundation_model
            else:
                self.ref_model = create_ref_model(self.model_args, self.finetuning_args)

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

    def dpo_loss(
            self,
            chosen_logps: "torch.Tensor",
            rejected_logps: "torch.Tensor",
            ref_chosen_logps: "torch.Tensor",
            ref_rejected_logps: "torch.Tensor",
    ) -> tuple["torch.Tensor", "torch.Tensor", "torch.Tensor"]:
        """From [trl] library's implementation of DPO
        Compute the DPO loss for a batch of policy and reference model log probabilities.

        Args:
            chosen_logps (`torch.FloatTensor`):
                Log probabilities of the model for the chosen responses. Shape: `(batch_size,)`.
            rejected_logps (`torch.FloatTensor`):
                Log probabilities of the model for the rejected responses. Shape: `(batch_size,)`.
            ref_chosen_logps (`torch.FloatTensor`):
                Log probabilities of the reference model for the chosen responses. Shape: `(batch_size,)`.
            ref_rejected_logps (`torch.FloatTensor`):
                Log probabilities of the reference model for the rejected responses. Shape: `(batch_size,)`.

        Returns:
            A tuple of three tensors: `(losses, chosen_rewards, rejected_rewards)`. The losses tensor contains the DPO
            loss for each example in the batch. The `chosen_rewards` and `rejected_rewards` tensors contain the rewards
            for the chosen and rejected responses, respectively.
        """
        device = self.accelerator.device

        # Get the log ratios for the chosen and rejected responses
        chosen_logratios = chosen_logps.to(device) - (not self.reference_free) * ref_chosen_logps.to(device)
        rejected_logratios = rejected_logps.to(device) - (not self.reference_free) * ref_rejected_logps.to(device)

        if self.f_divergence_type == FDivergenceType.ALPHA_DIVERGENCE.value:
            # The alpha-divergence formula: (1 - u^-alpha) / alpha
            # The divergence difference between the chosen and rejected sample is:
            #     (1 - u[w]^-alpha) / alpha - (1 - u[l]^-alpha) / alpha
            #        = (u[l]^-alpha - u[w]^-alpha) / alpha
            # where u[w] and u[l] are the policy/reference probability ratios
            # for the chosen and rejected samples, respectively.
            alpha_coef = FDivergenceConstants.ALPHA_DIVERGENCE_COEF_DEFAULT
            if self.f_divergence_params and FDivergenceConstants.ALPHA_DIVERGENCE_COEF_KEY in self.f_divergence_params:
                alpha_coef = float(self.f_divergence_params[FDivergenceConstants.ALPHA_DIVERGENCE_COEF_KEY])
            logits = (cap_exp(rejected_logratios * -alpha_coef) - cap_exp(chosen_logratios * -alpha_coef)) / alpha_coef
        else:
            logratios = chosen_logps - rejected_logps
            if self.reference_free:
                ref_logratios = torch.tensor([0], dtype=logratios.dtype, device=logratios.device)
            else:
                ref_logratios = ref_chosen_logps - ref_rejected_logps

            logratios = logratios.to(self.accelerator.device)
            ref_logratios = ref_logratios.to(self.accelerator.device)
            logits = logratios - ref_logratios

            if self.f_divergence_type == FDivergenceType.JS_DIVERGENCE.value:
                # The js-divergence formula: log(2 * u / (1 + u))
                # The divergence difference between the chosen and rejected sample is:
                #     log(2 * u[w] / (1 + u[w])) - log(2 * u[l] / (1 + u[l]))
                #       = log(u[w]) - log(u[l]) - (log(1 + u[w]) - log(1 + u[l]))
                # where u[w] and u[l] are the policy/reference probability ratios
                # for the chosen and rejected samples, respectively.
                logits -= F.softplus(chosen_logratios) - F.softplus(rejected_logratios)

        # The beta is a temperature parameter for the DPO loss, typically something in the range of 0.1 to 0.5.
        # We ignore the reference model as beta -> 0. The label_smoothing parameter encodes our uncertainty about the
        # labels and calculates a conservative DPO loss.
        if self.loss_type == "sigmoid":
            losses = (
                    -F.logsigmoid(self.beta * logits) * (1 - self.label_smoothing)
                    - F.logsigmoid(-self.beta * logits) * self.label_smoothing
            )

        elif self.loss_type == "robust":
            losses = (
                             -F.logsigmoid(self.beta * logits) * (1 - self.label_smoothing)
                             + F.logsigmoid(-self.beta * logits) * self.label_smoothing
                     ) / (1 - 2 * self.label_smoothing)

        elif self.loss_type == "exo_pair":
            # eqn (16) of the EXO paper: https://huggingface.co/papers/2402.00856
            import math

            if self.label_smoothing == 0:
                self.label_smoothing = 1e-3
            losses = (self.beta * logits).sigmoid() * (
                    F.logsigmoid(self.beta * logits) - math.log(1 - self.label_smoothing)
            ) + (-self.beta * logits).sigmoid() * (F.logsigmoid(-self.beta * logits) - math.log(self.label_smoothing))

        elif self.loss_type == "hinge":
            losses = torch.relu(1 - self.beta * logits)

        elif self.loss_type == "ipo":
            # eqn (17) of the paper where beta is the regularization parameter for the IPO loss, denoted by tau in the paper.
            losses = (logits - 1 / (2 * self.beta)) ** 2

        elif self.loss_type == "bco_pair":
            chosen_logratios = chosen_logps - ref_chosen_logps
            rejected_logratios = rejected_logps - ref_rejected_logps
            chosen_rewards = self.beta * chosen_logratios
            rejected_rewards = self.beta * rejected_logratios
            rewards = torch.cat((chosen_rewards, rejected_rewards), 0).mean().detach()
            self.running.update(rewards)
            delta = self.running.mean
            losses = -F.logsigmoid((self.beta * chosen_logratios) - delta) - F.logsigmoid(
                -(self.beta * rejected_logratios - delta)
            )

        elif self.loss_type == "sppo_hard":
            # In the paper (https://huggingface.co/papers/2405.00675), SPPO employs a soft probability approach,
            # estimated using the PairRM score. The probability calculation is conducted outside of the trainer class.
            # The version described here is the hard probability version, where P in Equation (4.7) of Algorithm 1 is
            # set to 1 for the winner and 0 for the loser.
            a = chosen_logps - ref_chosen_logps
            b = rejected_logps - ref_rejected_logps
            losses = (a - 0.5 / self.beta) ** 2 + (b + 0.5 / self.beta) ** 2

        elif self.loss_type == "nca_pair":
            chosen_rewards = (chosen_logps - ref_chosen_logps) * self.beta
            rejected_rewards = (rejected_logps - ref_rejected_logps) * self.beta
            losses = (
                    -F.logsigmoid(chosen_rewards)
                    - 0.5 * F.logsigmoid(-chosen_rewards)
                    - 0.5 * F.logsigmoid(-rejected_rewards)
            )

        elif self.loss_type == "aot_pair":
            chosen_logratios = chosen_logps - ref_chosen_logps
            rejected_logratios = rejected_logps - ref_rejected_logps
            chosen_logratios_sorted, _ = torch.sort(chosen_logratios, dim=0)
            rejected_logratios_sorted, _ = torch.sort(rejected_logratios, dim=0)
            delta = chosen_logratios_sorted - rejected_logratios_sorted
            losses = (
                    -F.logsigmoid(self.beta * delta) * (1 - self.label_smoothing)
                    - F.logsigmoid(-self.beta * delta) * self.label_smoothing
            )

        elif self.loss_type == "aot":
            logratios = chosen_logps - rejected_logps
            ref_logratios = ref_chosen_logps - ref_rejected_logps
            logratios_sorted, _ = torch.sort(logratios, dim=0)
            ref_logratios_sorted, _ = torch.sort(ref_logratios, dim=0)
            delta = logratios_sorted - ref_logratios_sorted
            losses = (
                    -F.logsigmoid(self.beta * delta) * (1 - self.label_smoothing)
                    - F.logsigmoid(-self.beta * delta) * self.label_smoothing
            )

        elif self.loss_type == "apo_zero":
            # Eqn (7) of the APO paper (https://huggingface.co/papers/2408.06266)
            # Use this loss when you believe the chosen outputs are better than your model's default output
            losses_chosen = 1 - F.sigmoid(self.beta * chosen_logratios)  # Increase chosen likelihood
            losses_rejected = F.sigmoid(self.beta * rejected_logratios)  # Decrease rejected likelihood
            losses = losses_chosen + losses_rejected

        elif self.loss_type == "apo_down":
            # Eqn (8) of the APO paper (https://huggingface.co/papers/2408.06266)
            # Use this loss when you believe the chosen outputs are worse than your model's default output.
            # Decrease chosen likelihood and decrease rejected likelihood more
            losses_chosen = F.sigmoid(self.beta * chosen_logratios)
            losses_rejected = 1 - F.sigmoid(self.beta * (chosen_logratios - rejected_logratios))
            losses = losses_chosen + losses_rejected

        elif self.loss_type == "discopop":
            # Eqn (5) of the DiscoPOP paper (https://huggingface.co/papers/2406.08414)
            # This loss was discovered with LLM discovery
            logratios = chosen_logps - rejected_logps
            ref_logratios = ref_chosen_logps - ref_rejected_logps
            logits = logratios - ref_logratios
            logits = logits * self.beta
            # Modulate the mixing coefficient based on the log ratio magnitudes
            log_ratio_modulation = torch.sigmoid(logits / self.discopop_tau)
            logistic_component = -F.logsigmoid(logits)
            exp_component = torch.exp(-logits)
            # Blend between logistic and exponential component based on log ratio modulation
            losses = logistic_component * (1 - log_ratio_modulation) + exp_component * log_ratio_modulation

        else:
            raise ValueError(
                f"Unknown loss type: {self.loss_type}. Should be one of ['sigmoid', 'hinge', 'ipo', 'exo_pair', "
                "'nca_pair', 'robust', 'bco_pair', 'sppo_hard', 'aot', 'aot_pair', 'discopop', 'apo_zero', 'apo_down']"
            )

        chosen_rewards = self.beta * (chosen_logps.to(device) - ref_chosen_logps.to(device)).detach()
        rejected_rewards = self.beta * (rejected_logps.to(device) - ref_rejected_logps.to(device)).detach()

        return losses, chosen_rewards, rejected_rewards

    def odds_ratio_loss(self, chosen_logps: "torch.Tensor", rejected_logps: "torch.Tensor") -> "torch.Tensor":
        r"""Compute ORPO's odds ratio (OR) loss for batched log probabilities of the policy model."""
        log_odds = (chosen_logps - rejected_logps) - (
                torch.log1p(-torch.exp(chosen_logps)) - torch.log1p(-torch.exp(rejected_logps))
        )
        sft_loss = -chosen_logps
        odds_ratio_loss = -F.logsigmoid(log_odds)
        orpo_loss = sft_loss + self.beta * odds_ratio_loss
        return orpo_loss

    def simpo_loss(self, chosen_logps: "torch.Tensor", rejected_logps: "torch.Tensor") -> "torch.Tensor":
        r"""Compute SimPO loss for batched log probabilities of the policy model."""
        pi_logratios = chosen_logps - rejected_logps
        gamma_logratios = self.simpo_gamma / self.beta
        logits = pi_logratios - gamma_logratios
        simpo_loss = -F.logsigmoid(self.beta * logits)
        return simpo_loss

    def compute_preference_loss(
            self,
            policy_chosen_logps: "torch.Tensor",
            policy_rejected_logps: "torch.Tensor",
            reference_chosen_logps: Optional["torch.Tensor"],
            reference_rejected_logps: Optional["torch.Tensor"],
    ) -> tuple["torch.Tensor", "torch.Tensor", "torch.Tensor"]:
        r"""Compute loss for preference learning."""
        if not self.finetuning_args.use_ref_model:
            # These are losses implemented by llama-factory in addition to trl's
            if self.loss_type == "orpo":
                losses = self.odds_ratio_loss(policy_chosen_logps, policy_rejected_logps)
            elif self.loss_type == "simpo":
                losses = self.simpo_loss(policy_chosen_logps, policy_rejected_logps)
            else:
                raise NotImplementedError(f"Unknown loss type: {self.loss_type}.")

            chosen_rewards = self.beta * policy_chosen_logps.to(self.accelerator.device).detach()
            rejected_rewards = self.beta * policy_rejected_logps.to(self.accelerator.device).detach()
        else:
            # This is the call to trl implemented DPO losses
            losses, chosen_rewards, rejected_rewards = self.dpo_loss(
                policy_chosen_logps, policy_rejected_logps, reference_chosen_logps, reference_rejected_logps
            )

        return losses, chosen_rewards, rejected_rewards

    def concatenated_forward(
            self, model: "PreTrainedModel", batch: dict[str, "torch.Tensor"], is_ref_model: bool = False
    ) -> tuple["torch.Tensor", "torch.Tensor", "torch.Tensor", "torch.Tensor", "torch.Tensor"]:
        r"""Compute the sum log probabilities of the labels under
                given logits if loss_type is not IPO or ORPO or SimPO, otherwise, the average log probabilities.
        """
        if self.finetuning_args.use_ref_model:
            batch = nested_detach(batch, clone=True)  # avoid error

        all_logits: torch.Tensor = model(
            input_ids=batch["input_ids"],
            attention_mask=batch["attention_mask"],
            labels=batch["labels"],
            return_dict=True,
            use_cache=False
        ).logits.to(torch.float32)

        all_logps, valid_length = get_batch_logps(
            logits=all_logits, labels=batch["labels"], ld_alpha=(self.ld_alpha if not is_ref_model else None)
        )

        if self.loss_type in ["ipo", "orpo", "simpo"]:
            all_logps = all_logps / valid_length

        batch_size = batch["input_ids"].size(0) // 2
        chosen_logps, rejected_logps = all_logps.split(batch_size, dim=0)
        chosen_logits, rejected_logits = all_logits.split(batch_size, dim=0)
        chosen_length, _ = valid_length.split(batch_size, dim=0)

        if self.loss_type in ["ipo", "orpo", "simpo"]:
            return chosen_logps, rejected_logps, chosen_logits, rejected_logits, chosen_logps
        else:
            return chosen_logps, rejected_logps, chosen_logits, rejected_logits, chosen_logps / chosen_length

    def compute_reference_log_probs(
            self, batch: dict[str, "torch.Tensor"]
    ) -> tuple[Optional["torch.Tensor"], Optional["torch.Tensor"]]:
        r"""Compute log probabilities of the reference model."""
        if not self.finetuning_args.use_ref_model:
            return None, None

        if self.ref_model is None:
            ref_model = self.model
            ref_context = self.accelerator.unwrap_model(self.model).disable_adapter()
        else:
            ref_model = self.ref_model
            ref_context = nullcontext()

        with torch.no_grad(), ref_context:
            reference_chosen_logps, reference_rejected_logps, *_ = self.concatenated_forward(
                ref_model, batch, is_ref_model=True
            )

        return reference_chosen_logps, reference_rejected_logps

    @override
    def compute_loss(self, batch: dict[str, "torch.Tensor"]):
        # Get the current policy specific log probs of chosen and rejected samples
        (
            policy_chosen_logps,
            policy_rejected_logps,
            policy_chosen_logits,
            policy_rejected_logits,
            policy_chosen_logps_avg,
        ) = self.concatenated_forward(self.model, batch)

        # Get the reference policy specific log probs of chosen and rejected samples
        reference_chosen_logps, reference_rejected_logps = self.compute_reference_log_probs(batch)

        # Compute loss
        losses, chosen_rewards, rejected_rewards = self.compute_preference_loss(
            policy_chosen_logps,
            policy_rejected_logps,
            reference_chosen_logps,
            reference_rejected_logps,
        )
        sft_loss = -policy_chosen_logps_avg
        if self.ftx_gamma > 1e-6:
            losses += self.ftx_gamma * sft_loss

        metrics = {f"rewards/chosen": chosen_rewards.mean().item(),
                   f"rewards/rejected": rejected_rewards.mean().item(),
                   f"rewards/accuracies": (chosen_rewards > rejected_rewards).float().mean().item(),
                   f"rewards/margins": (chosen_rewards - rejected_rewards).mean().item(),
                   f"logps/chosen": policy_chosen_logps.mean().item(),
                   f"logps/rejected": policy_rejected_logps.mean().item(),
                   f"logits/chosen": policy_chosen_logits.mean().item(),
                   f"logits/rejected": policy_rejected_logits.mean().item()}
        if self.loss_type == "orpo":
            metrics[f"sft_loss"] = sft_loss.mean().item()
            metrics[f"odds_ratio_loss"] = ((losses - sft_loss) / self.beta).mean().item()

        loss = losses.mean()
        # # Check if this is needed
        # loss = loss.to(self.accelerator.device)

        return loss, metrics