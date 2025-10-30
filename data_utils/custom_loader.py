import json
import os
import random
import sys
from typing import Any, List, TYPE_CHECKING

from loguru import logger
from torch.utils.data import Dataset as TorchDataset
from tqdm import tqdm

from data_utils.processor.processor_utils import infer_seqlen
from data_utils.utils import Role, downsample_tools, update_retrieval_tools
from envs.tool_call_env import ERRONEOUS_CATEGORIES
from extras.constants import IGNORE_INDEX, PREFERENCE_KEYS
from data_utils.tool_utils import create_ToolPolicy
from envs.base_env import ToolPolicy
from extras.custom import make_json_serializable

from data_utils.utils import load_data_files
if TYPE_CHECKING:
    from data_utils.template import Template
    from hparams import DataArguments
    from data_utils.mm_plugin import MMProcessor


class BaseDataset(TorchDataset):
    def __init__(
            self,
            template: "Template",
            tokenizer: Any,
            processor: Any,
            data_args: "DataArguments",
            train_setting: str,
            for_pref_modelling: bool = False,

    ):
        self.template = template
        self.tokenizer = tokenizer
        self.processor = processor

        self._name = data_args.dataset
        self.data_args = data_args
        self.train_setting = train_setting
        self.for_pref_modelling = for_pref_modelling

        # Read Data
        self.task_idxs, self.processed_samples = self.read_data()

        # Process samples
        self.print_data_sample()

        # # Convert samples to features
        if train_setting.lower() == "supervised":

            if not for_pref_modelling:
                self.inputs, self.labels, self.attn_masks, self.input_lens = self.convert_samples_to_supervised_features()
            else:
                self.inputs, self.labels, self.attn_masks, self.input_lens = self.convert_samples_to_supervised_preference_features()

        else:
            raise ValueError(f"Unknown training setting: {train_setting}")

        if isinstance(self.input_lens[0], dict):
            input_lens = []
            for input_len in self.input_lens:
                for k, v in input_len.items():
                    input_lens.append(v)
        else:
            input_lens = self.input_lens
        logger.info(f"Input Length (system + history + context + query) Stats: Min = {min(input_lens)}, Max = {max(input_lens)}, Avg = {sum(input_lens) / len(input_lens)}")

    def read_data(self) -> List[dict]:
        """Read the raw data from the dataset.
        """
        raise NotImplementedError("read_data() method not implemented!")


    def convert_samples_to_supervised_features(self):
        """Convert the processed samples to features that can be used for supervised training."""

        inputs, labels, attn_masks, input_lens = [], [], [], []
        pbar = tqdm(total=len(self.processed_samples), ncols=0, desc=f"Converting examples to features: ")
        stats = []
        skipped_points = 0
        for i in range(len(self.processed_samples)):

            sample = self.processed_samples[i]
            if sample is None:
                continue

            messages = sample['input'] + [sample['output']]

            # Remove tags
            for m in messages:
                if m['role'] == Role.FUNCTION.value:
                    m['role'] = Role.ASSISTANT.value
                    parsed = m['content'].split('</think>{"name"')
                    print("m['content']:", m['content'])
                    m['content'] = parsed[0] + "</think>" + "<tool_call>\n" + '{"name"' + parsed[1] + "\n</tool_call>"
                elif m['role'] == Role.OBSERVATION.value:
                    m['role'] = Role.USER.value
                m['content'] = m['content'].replace("<FINAL>", "").replace("</FINAL>", "")
                if not self.data_args.include_thinking:
                    t1 = m['content'].find("<think>")
                    t2 = m['content'].find("</think>")
                    if t1 != -1 and t2 != -1:
                        t1 = t1 + len("<think>")
                        m['content'] = m['content'][:t1] + m['content'][t2:]

                    t1 = m['content'].find("<thought>")
                    t2 = m['content'].find("</thought>")
                    if t1 != -1 and t2 != -1:
                        t1 = t1 + len("<thought>")
                        m['content'] = m['content'][:t1] + m['content'][t2:]
                if not self.data_args.enable_thinking:
                    m['content'] = m['content'].replace("<think>", "").replace("</think>", "")
                    m['content'] = m['content'].replace("<thought>", "").replace("</thought>", "")


            system_prompt = sample['system']
            tools = sample['tools']
            tool_policy = sample['tool_policy']
            if tool_policy.tool_usage_policy is not None:
                system_prompt += " " + tool_policy.tool_usage_policy
            if tool_policy.final_answer_policy is not None:
                system_prompt += tool_policy.final_answer_policy

            # TODO: maybe try putting the tool_usage_policy and final_answer_policy at the end of the prompt (after the tool specs)
            formatted_text = self.tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=False, tools=json.loads(tools), system=system_prompt)
            print("final prompt:\n ", formatted_text)
            # formatted_text is the final text before tokenization. check this to see what's actually getting passed into the model
            tokens = self.tokenizer(formatted_text, return_tensors="pt", padding=True, truncation=True, return_attention_mask=True)

            input_ids = tokens['input_ids'].squeeze(0)
            label_ids = tokens["input_ids"].squeeze(0).clone()
            attn_mask = tokens['attention_mask'].squeeze(0)
            if len(input_ids) > self.data_args.cutoff_len:
                logger.info(f"Ignoring data point of with token length: {len(input_ids)}")
                skipped_points+=1
                continue
            inputs.append(input_ids)
            labels.append(label_ids)
            attn_masks.append(attn_mask)
            input_lens.append(len(input_ids))
            pbar.update()
            stats.append({
                'input_length': len(input_ids), 
                'domain': sample['domain'], 
                'sample_id': sample['sample_id'],
                # 'formatted_text': formatted_text

            })
        
        logger.info(f"Skipped {skipped_points} long context points out of a total of {len(input_lens)}. ")
        with open(f"stats.json", "w") as f:
            json.dump(stats, f)
        return inputs, labels, attn_masks, input_lens

    def convert_samples_to_supervised_preference_features(self):

        inputs, labels, attn_masks, input_lens = [], [], [], []  # This will now be a list of dicts

        pbar = tqdm(total=len(self.processed_samples), ncols=0, desc=f"Converting examples to preference features: ")
        for i in range(len(self.processed_samples)):

            sample = self.processed_samples[i]
            if sample is None:
                continue

            pref_inputs, pref_labels, pref_attn_masks, pref_input_lens = {}, {}, {}, {}
            for key in PREFERENCE_KEYS:
                prompt = sample[f"{key}_prompt"]
                response = sample[f"{key}_response"]
                # The sample must have a response for supervised training
                try:
                    assert len(response) == 1
                except AssertionError:
                    logger.error(f"Sample {sample} has no response")
                    sys.exit(-1)

                messages = prompt + response
                input_ids, label_ids = []

                # Encode the messages. Here, we get encoded_pairs for the input_ids and labels turn-wise
                encoded_pairs = self.template.encode_multiturn(self.tokenizer, messages, sample['_system'], sample['_tools'], tool_policy=sample['_tool_policy'])
                # We will start with the last turn and move towards the first turn until max_context_length is reached.
                if self.data_args.mask_history:
                    encoded_pairs = encoded_pairs[::-1]  # high priority for last turns. Earlier turns likely to be masked out.

                total_length = len(input_ids) + (1 if self.template.efficient_eos else 0)
                for turn_idx, (source_ids, target_ids) in enumerate(encoded_pairs):

                    # Check. Older turns/conversations are likely to be masked out as they are towards the end
                    if total_length > self.data_args.cutoff_len:
                        break
                    # Get the source and target lengths
                    source_len, target_len = infer_seqlen(
                        len(source_ids), len(target_ids), self.data_args.cutoff_len - total_length
                    )
                    # Truncate the source and target ids
                    source_ids = source_ids[:source_len]
                    target_ids = target_ids[:target_len]
                    # Update the total length
                    total_length += source_len + target_len

                    # Creat input ids and target labels
                    if self.data_args.train_on_prompt:
                        source_label = source_ids
                    elif self.template.efficient_eos:
                        source_label = [self.tokenizer.eos_token_id] + [IGNORE_INDEX] * (source_len - 1)
                    else:
                        source_label = [IGNORE_INDEX] * source_len

                    if self.data_args.mask_history and turn_idx != 0:  # train on the last turn only
                        target_label = [IGNORE_INDEX] * target_len
                    else:
                        if f"{key}_turn_mask" in sample.keys() and sample[f"{key}_turn_mask"][turn_idx]:
                            # If the turn mask is provided and current turn has to be masked (untrainable)
                            target_label = [IGNORE_INDEX] * target_len
                        else:
                            target_label = target_ids

                    if self.data_args.mask_history:  # reversed sequences. trainable part pushed to the end
                        input_ids = source_ids + target_ids + input_ids
                        label_ids = source_label + target_label + label_ids
                    else:
                        input_ids += source_ids + target_ids
                        label_ids += source_label + target_label

                if self.template.efficient_eos:
                    input_ids += [self.tokenizer.eos_token_id]
                    label_ids += [self.tokenizer.eos_token_id]
                attn_mask = [1] * len(input_ids)

                pref_inputs[key] = input_ids
                pref_labels[key] = label_ids
                pref_attn_masks[key] = attn_mask
                pref_input_lens[key] = len(input_ids)

            if any([traj_lth > 17000 for traj_lth in pref_input_lens.values()]):
                # label_ids = [IGNORE_INDEX] * len(label_ids)
                logger.info(f"Ignoring data point of with token length: {len(input_ids)}")
                continue
            inputs.append(pref_inputs)
            labels.append(pref_labels)
            attn_masks.append(pref_attn_masks)
            input_lens.append(pref_input_lens)
            pbar.update()

        return inputs, labels, attn_masks, input_lens

    def get_processed_sample(self, i: int):
        """Get the (processed) sample at index i.
        :param i:
        :return:
        """
        return self.processed_samples[i]

    def __len__(self):
        return len(self.inputs)


    def __getitem__(self, i):
        if not self.for_pref_modelling:
            sample = {
                "idx": self.task_idxs[i],
                "input_ids": self.inputs[i],
                "attention_mask": self.attn_masks[i],
                "labels": self.labels[i],
                "input_lens": self.input_lens[i]
            }
            assert not all(element == IGNORE_INDEX for element in sample["labels"].tolist()), f"Sample {i} has all labels set to {IGNORE_INDEX}"
        else:
            sample = {
                "idx": self.task_idxs[i],
            }
            for key in PREFERENCE_KEYS:
                sample[f"{key}_input_ids"] = self.inputs[i][key]
                sample[f"{key}_attention_mask"] = self.attn_masks[i][key]
                sample[f"{key}_labels"] = self.labels[i][key]
                sample[f"{key}_input_lens"] = self.input_lens[i][key]
                assert not all(element == IGNORE_INDEX for element in sample[f"{key}_labels"]), f"Sample {i} has all {key} labels set to {IGNORE_INDEX}"

        return sample

    def __iter__(self):
        for i in range(len(self)):
            yield self[i]

    def print_data_sample(self):
        for _ in range(2):
            # Get a random sample from the dataset
            random_sample = random.choice(self.processed_samples)
            if random_sample is None:
                random_sample = random.choice(self.processed_samples)

            logger.info(
                "\n|======================================================================================|\n"
                "|============================== Random Processed Sample ===============================|\n"
                "|======================================================================================|\n"
                f"{json.dumps(make_json_serializable(random_sample), indent=4)}"
                "\n|======================================================================================|\n"
            )


def get_turn_level_masks_for_training():
    pass


class AgentTrajectorySFTData(BaseDataset):
    def __init__(
            self,
            template: "Template",
            tokenizer: Any,
            processor: "MMProcessor",
            data_args: "DataArguments",
            train_setting: str
    ):
        # Use this to control if you want to train on interactions from expert or agent or both
        self.accept_interactions_from = 'expert'  #  Possible values: expert, agent, both
        logger.info("Will train with interactions from '{}'".format(self.accept_interactions_from))

        super().__init__(
            template=template,
            tokenizer=tokenizer,
            processor=processor,
            data_args=data_args,
            train_setting=train_setting
        )

    def read_data(self):

        # Collect trajectories
        trajectories = load_data_files(self.data_args.dataset_dir, self.data_args.dataset)
        total_trajectories = len(trajectories)
        sample_idx = 0
        task_idxs, data = [], []
        skipped_missing_gt = 0

        # Collect interactions from trajectories
        for traj in trajectories:
            # Skip exclude GT samples
            if "EXCLUDE_GT" in traj['sample_id']:
                skipped_missing_gt += 1
                continue
            
            system, tools = traj['system'], traj['tools']
            tool_policy=create_ToolPolicy(scenarios=traj["scenarios"],current_domain=traj["domain"])
            tool_policy.tool_usage_policy = traj['tool_usage_policy']
            tool_policy.final_answer_policy = traj['final_answer_policy']
            cr_pair = {
                        "idx": sample_idx,
                        "input": traj["input"],
                        "output": traj["output"],
                        "system": system,
                        "tools": tools,
                        "tool_policy": tool_policy,
                        'domain': traj['domain'], 
                        'sample_id': traj['sample_id'], 
                    }
            data.append(cr_pair)
            task_idxs.append(sample_idx)
            sample_idx += 1

            # time_steps = list(traj['interactions'].keys())
            # time_steps = sorted(time_steps, key=lambda x: int(x))
            
            # for t in time_steps:
            #     cr_pair = {
            #             "idx": sample_idx,
            #             "input": traj['interactions'][t]["input"],
            #             "output": {"role": traj['interactions'][t]["output"]['role'], "content": traj['interactions'][t]["output"]['content']},
            #             "thought": traj['interactions'][t]['metadata']['thought'],
            #             "action_type": traj['interactions'][t]['metadata']['action'], 
            #             "action": traj['interactions'][t]['metadata']['action_arguments'], 
            #             "system": system,
            #             "tools": tools,
            #             "tool_policy": tool_policy,
            #             'domain': traj['domain'], 
            #             'sample_id': traj['sample_id'], 
            #         }
                    
            #     data.append(cr_pair)
            #     task_idxs.append(sample_idx)
            #     sample_idx += 1

        try:
            assert len(data) > 0
        except AssertionError:
            raise AssertionError("No training data parsed!")

        if self.data_args.max_samples is not None and self.data_args.max_samples > 0:
            task_idxs = task_idxs[:self.data_args.max_samples]
            data = data[:self.data_args.max_samples]
            logger.info(f"Max samples set to {self.data_args.max_samples}")

        logger.info("Selected {} training samples from {} trajectories".format(len(data), total_trajectories))
        logger.info("Skipped {} samples in MISSING GT scenario".format(skipped_missing_gt))
        return task_idxs, data


class AgentTrajectoryPreferenceData(BaseDataset):
    def __init__(
            self,
            template: "Template",
            tokenizer: Any,
            processor: "MMProcessor",
            data_args: "DataArguments",
            train_setting: str
    ):
        # self.preference_granularity: str = 'step'  # 'step' (collected using single agent) or 'trajectory' (collected using multiple agents)
        # self.mask_suboptimal_traces: bool = True  # Used with trajectory-level preference granularity

        super().__init__(
            template=template,
            tokenizer=tokenizer,
            processor=processor,
            data_args=data_args,
            train_setting=train_setting,
            for_pref_modelling=True
        )

    def read_step_preferences(self):
        """Preferences collected from a single agent's run"""
        # Collect trajectories
        trajectories = load_data_files(self.data_args.dataset_dir, self.data_args.dataset)
        total_trajectories = len(trajectories)
        sample_idx = 0
        task_idxs, data = [], []

        # Collect interactions from trajectories
        for traj in trajectories:

            system, tools = traj['system'], traj['tools']
            tool_policy=create_ToolPolicy(scenarios=traj["scenarios"],current_domain=traj["domain"])
            tool_policy.tool_usage_policy = traj['tool_usage_policy']
            tool_policy.final_answer_policy = traj['final_answer_policy']

            time_steps = list(traj['interactions'].keys())
            time_steps = sorted(time_steps, key=lambda x: int(x))
            # For every partial (/full) traj., train on the last action given the state when history is masked
            for t in time_steps:

                # Collect preferred (expert) and dispreferred (actor) actions.
                # Filter out samples where the expert intervened at the final step but the final answer was not correct
                if (len(traj['interactions'][t]['alternate_trace']) > 0
                        and traj['interactions'][t]["reward"] != "{REWARD_FINAL_ANSWER_NO_MATCH}"):

                    curr_actor = traj['interactions'][t]['actor']
                    alt_actor = traj['interactions'][t]['alternate_trace'][0]['actor']
                    if curr_actor == 'agent' and alt_actor == 'expert':
                        data.append(
                            {
                                "idx": sample_idx,
                                "chosen_input": traj['interactions'][t]["input"],
                                "rejected_input": traj['interactions'][t]["input"],
                                "chosen_output": traj['interactions'][t]['alternate_trace'][0]["output"],
                                "rejected_output":traj['interactions'][t]["output"],
                                "system": system,
                                "tools": tools,
                                "tool_policy": tool_policy,
                            }
                        )
                        task_idxs.append(sample_idx)
                        sample_idx += 1

                    elif curr_actor == 'expert' and alt_actor == 'agent':
                        data.append(
                            {
                                "idx": sample_idx,
                                "chosen_input": traj['interactions'][t]["input"],
                                "rejected_input": traj['interactions'][t]["input"],
                                "chosen_output": traj['interactions'][t]["output"],
                                "rejected_output": traj['interactions'][t]['alternate_trace'][0]["output"],
                                "system": system,
                                "tools": tools,
                                "tool_policy": tool_policy,
                            }
                        )
                        task_idxs.append(sample_idx)
                        sample_idx += 1

        return total_trajectories, task_idxs, data

    def read_trajectory_preferences(self):
        """Preferences collected from chosen/rejected agent's run
        Expected format for preference data:
            Each sample's chosen/rejected traj must be a list of dict with i/p, o/p, reward at turn-level
       """

        def split_traj_input_output(traj, is_chosen):
            """[Here] implement the logic to filter preference samples based on turn-level rewards"""
            flatten_traj = []
            _mask_turn = []  # Set to turn if training on particular turn is not desired
            for curr_turn in traj:
                flatten_traj.append(curr_turn["input"])
                flatten_traj.append(curr_turn["output"])
                if not self.data_args.mask_suboptimal_traces:
                    _mask_turn.append(False)
                else:
                    turn_penalised = False
                    for error in ERRONEOUS_CATEGORIES:
                        if error in curr_turn["reward"]:
                            turn_penalised = True
                            break
                    if turn_penalised:
                        _mask_turn.append(True if is_chosen else False)
                    else:
                        _mask_turn.append(False if is_chosen else True)

            _input = flatten_traj[:-1]
            _output = flatten_traj[-1]
            return _input, _output, _mask_turn

        # Collect trajectories
        datasets = [self.data_args.dataset] if isinstance(self.data_args.dataset, str) else self.data_args.dataset
        sample_idx = 0
        total_trajectories = 0
        task_idxs, data = [], []
        for dataset in datasets:

            logger.info(f"Reading dataset '{dataset}' from {self.data_args.dataset_dir}")
            with open(os.path.join(self.data_args.dataset_dir, f"{dataset}.json"), "r") as f:
                preference_data = json.load(f)
            total_trajectories += len(preference_data)

            for sample in preference_data:
                chosen_trajectory: List[dict] = sample['chosen_trajectory']
                rejected_trajectory: List[dict] = sample['rejected_trajectory']

                # TODO: Extend the following for scenario grounded cases (where final reward = REWARD_SCENARIO_NOT_FOLLOWED)
                # Filter 1: Check if chosen trajectory successfully completed the task
                if chosen_trajectory[-1]["reward"] != "{REWARD_FINAL_ANSWER_MATCH}":
                    logger.info("Ignoring the preference sample since the chosen trajectory has the final reward "
                                "= {}".format(chosen_trajectory[-1]["reward"]))
                    continue
                # Filter 2: Check if rejected trajectory failed the task
                if rejected_trajectory[-1]["reward"] == "{REWARD_FINAL_ANSWER_MATCH}":
                    logger.info("Ignoring the preference sample since the rejected trajectory has the final reward "
                                "= {}".format(rejected_trajectory[-1]["reward"]))
                    continue

                chosen_input, chosen_output, chosen_turn_mask = split_traj_input_output(chosen_trajectory, True)
                rejected_input, rejected_output, rejected_turn_mask = split_traj_input_output(rejected_trajectory, False)
                tool_policy = ToolPolicy(
                    tool_availability_policy = sample["scenarios"]["tool_availability_policy"],
                    tool_use_policy=sample["scenarios"]["tool_use_policy"],
                    tool_usage_policy = sample["scenarios"]["tool_usage_policy"],
                    final_answer_policy = sample["scenarios"]["final_answer_policy"],
                )
                data.append(
                    {
                        "idx": sample_idx,
                        "chosen_input": chosen_input,
                        "chosen_output": chosen_output,
                        "chosen_turn_mask": chosen_turn_mask,
                        "rejected_input": rejected_input,
                        "rejected_output": rejected_output,
                        "rejected_turn_mask": rejected_turn_mask,
                        "system": sample['system'],
                        "tools": sample['tools'],
                        "tool_policy":tool_policy,
                    }
                )
                task_idxs.append(sample_idx)
                sample_idx += 1

        return total_trajectories, task_idxs, data

    def read_data(self):
        if self.data_args.preference_granularity == 'step':
            try:
                assert self.data_args.mask_history == True
            except AssertionError:
                raise ValueError("For step level DPO training, mask_history must be set to True.")
            total_trajectories, task_idxs, data = self.read_step_preferences()

        elif self.data_args.preference_granularity == 'trajectory':
            try:
                assert self.data_args.mask_history == False
            except AssertionError:
                raise ValueError("For trajectory level DPO training, mask_history must be set to False.")
            total_trajectories, task_idxs, data = self.read_trajectory_preferences()

        else:
            raise NotImplementedError("Parsing of preference granularity must be set to 'step' or 'trajectory'. "
                                      "It is currently set to {}".format(self.data_args.preference_granularity))

        try:
            assert len(data) > 0
        except AssertionError:
            raise AssertionError("No training data parsed!")

        if self.data_args.max_samples is not None and self.data_args.max_samples > 0:
            task_idxs = task_idxs[:self.data_args.max_samples]
            data = data[:self.data_args.max_samples]
            logger.info(f"Max samples set to {self.data_args.max_samples}")

        logger.info("Selected {} training samples from {} trajectories".format(len(data), total_trajectories))

        return task_idxs, data

    def process_samples(self) -> List[dict]:
        processed_samples = []
        for sample in tqdm(self.data, total=len(self.data), desc=f"Processing samples: "):
            idx = sample["idx"]

            processed_sample = {
                "idx": idx,
                "_system": sample['system'] if 'system' in sample.keys() and sample['system'] else "",
                # For calling external tools
                "_tools": sample['tools'] if 'tools' in sample.keys() and sample['tools'] else "",
                "_tool_policy": sample['tool_policy'] if 'tool_policy' in sample.keys() else None
            }

            for key in PREFERENCE_KEYS:
                # Get the prompt
                processed_sample[f"{key}_prompt"] = sample[f"{key}_input"]

                # Get the turn mask [will not be present in step-level preference granularity]
                if f"{key}_turn_mask" in sample.keys() and sample[f"{key}_turn_mask"]:
                    processed_sample[f"{key}_turn_mask"] = sample[f"{key}_turn_mask"]

                # Get the response
                output = sample[f"{key}_output"]
                if output is not None:
                    if isinstance(output, str):
                        response = [{"role": Role.ASSISTANT.value, "content": output}]
                    elif isinstance(output, list) and isinstance(output[0], dict):
                        response = output
                    elif isinstance(output, dict):
                        response = [output]
                else:
                    response = []
                processed_sample[f"{key}_response"] = response

            processed_samples.append(processed_sample)

        return processed_samples