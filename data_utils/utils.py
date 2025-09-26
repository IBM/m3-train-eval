# Copyright 2025 the LlamaFactory team.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from copy import deepcopy
import json
import os
from enum import Enum, unique
import random
from typing import TYPE_CHECKING, Any, Optional, TypedDict, Union

import fsspec
from datasets import DatasetDict, concatenate_datasets, interleave_datasets

if TYPE_CHECKING:
    from datasets import Dataset, IterableDataset

    from hparams import DataArguments

from loguru import logger


SLOTS = list[Union[str, set[str], dict[str, str]]]
RETRIEVERS_TO_IGNORE = ["retriever_clapnq_california_schools", "retriever_clapnq_card_games", "retriever_clapnq_codebase_community", "retriever_clapnq_european_football_2" # BIRD Train
                        , "retriever_clapnq_formula_1", "retriever_clapnq_superhero", "retriever_clapnq_toxicology", "retriever_clapnq_debit_card_specializing" # BIRD Train
                        , "retriever_clapnq_financial", "retriever_clapnq_student_club", "retriever_clapnq_thrombosis_prediction" # BIRD Train
                        , "retriever_clapnq_car_retails", "retriever_clapnq_synthea", "retriever_clapnq_shipping", "retriever_clapnq_cs_semester", "retriever_clapnq_food_inspection_2" # RED Domains
                        , "retriever_clapnq_sales", "retriever_clapnq_software_company", "retriever_clapnq_social_media", "retriever_clapnq_human_resources", "retriever_clapnq_regional_sales" # RED Domains
                        , "retriever_clapnq_works_cycles", "retriever_clapnq_retails", "retriever_clapnq_retail_world", "retriever_clapnq_retail_complains", "retriever_clapnq_shooting", "retriever_clapnq_superstore" # RED Domains
                        ]

@unique
class Role(str, Enum):
    USER = "user"
    ASSISTANT = "assistant"
    SYSTEM = "system"
    FUNCTION = "function"
    OBSERVATION = "observation"


class DatasetModule(TypedDict):
    train_dataset: Optional[Union["Dataset", "IterableDataset"]]
    eval_dataset: Optional[Union["Dataset", "IterableDataset", dict[str, "Dataset"]]]


def merge_dataset(
    all_datasets: list[Union["Dataset", "IterableDataset"]], data_args: "DataArguments", seed: int
) -> Union["Dataset", "IterableDataset"]:
    r"""Merge multiple datasets to a unified dataset."""
    if len(all_datasets) == 1:
        return all_datasets[0]

    elif data_args.mix_strategy == "concat":
        if data_args.streaming:
            logger.warning("The samples between different datasets will not be mixed in streaming mode.")

        return concatenate_datasets(all_datasets)

    elif data_args.mix_strategy.startswith("interleave"):
        if not data_args.streaming:
            logger.warning("We recommend using `mix_strategy=concat` in non-streaming mode.")

        return interleave_datasets(
            datasets=all_datasets,
            probabilities=data_args.interleave_probs,
            seed=seed,
            stopping_strategy="first_exhausted" if data_args.mix_strategy.endswith("under") else "all_exhausted",
        )

    else:
        raise ValueError(f"Unknown mixing strategy: {data_args.mix_strategy}.")


def split_dataset(
    dataset: Optional[Union["Dataset", "IterableDataset"]],
    eval_dataset: Optional[Union["Dataset", "IterableDataset", dict[str, "Dataset"]]],
    data_args: "DataArguments",
    seed: int,
) -> "DatasetDict":
    r"""Split the dataset and returns a dataset dict containing train set and validation set.

    Support both map dataset and iterable dataset.
    """
    if eval_dataset is not None and data_args.val_size > 1e-6:
        raise ValueError("Cannot specify `val_size` if `eval_dataset` is not None.")

    dataset_dict = {}
    if dataset is not None:
        if data_args.streaming:
            dataset = dataset.shuffle(buffer_size=data_args.buffer_size, seed=seed)

        if data_args.val_size > 1e-6:
            if data_args.streaming:
                dataset_dict["validation"] = dataset.take(int(data_args.val_size))
                dataset_dict["train"] = dataset.skip(int(data_args.val_size))
            else:
                val_size = int(data_args.val_size) if data_args.val_size > 1 else data_args.val_size
                dataset_dict = dataset.train_test_split(test_size=val_size, seed=seed)
                dataset = dataset.train_test_split(test_size=val_size, seed=seed)
                dataset_dict = {"train": dataset["train"], "validation": dataset["test"]}
        else:
            dataset_dict["train"] = dataset

    if eval_dataset is not None:
        if isinstance(eval_dataset, dict):
            dataset_dict.update({f"validation_{name}": data for name, data in eval_dataset.items()})
        else:
            if data_args.streaming:
                eval_dataset = eval_dataset.shuffle(buffer_size=data_args.buffer_size, seed=seed)

            dataset_dict["validation"] = eval_dataset

    return DatasetDict(dataset_dict)


def get_dataset_module(dataset: Union["Dataset", "DatasetDict"]) -> "DatasetModule":
    r"""Convert dataset or dataset dict to dataset module."""
    dataset_module: DatasetModule = {}
    if isinstance(dataset, DatasetDict):  # dataset dict
        if "train" in dataset:
            dataset_module["train_dataset"] = dataset["train"]

        if "validation" in dataset:
            dataset_module["eval_dataset"] = dataset["validation"]
        else:
            eval_dataset = {}
            for key in dataset.keys():
                if key.startswith("validation_"):
                    eval_dataset[key[len("validation_") :]] = dataset[key]

            if len(eval_dataset):
                dataset_module["eval_dataset"] = eval_dataset

    else:  # single dataset
        dataset_module["train_dataset"] = dataset

    return dataset_module


def setup_fs(path: str, anon: bool = False) -> "fsspec.AbstractFileSystem":
    r"""Set up a filesystem object based on the path protocol."""
    storage_options = {"anon": anon} if anon else {}
    if path.startswith("s3://"):
        fs = fsspec.filesystem("s3", **storage_options)
    elif path.startswith(("gs://", "gcs://")):
        fs = fsspec.filesystem("gcs", **storage_options)
    else:
        raise ValueError(f"Unsupported protocol in path: {path}. Use 's3://' or 'gs://'.")

    if not fs.exists(path):
        raise ValueError(f"Path does not exist: {path}.")

    return fs


def _read_json_with_fs(fs: "fsspec.AbstractFileSystem", path: str) -> list[Any]:
    r"""Helper function to read JSON/JSONL files using fsspec."""
    with fs.open(path, "r") as f:
        if path.endswith(".jsonl"):
            return [json.loads(line) for line in f if line.strip()]
        else:
            return json.load(f)


def read_cloud_json(cloud_path: str) -> list[Any]:
    r"""Read a JSON/JSONL file from cloud storage (S3 or GCS).

    Args:
        cloud_path: str
            Cloud path in the format:
            - 's3://bucket-name/file.json' for AWS S3
            - 'gs://bucket-name/file.jsonl' or 'gcs://bucket-name/file.jsonl' for Google Cloud Storage
    """
    try:
        fs = setup_fs(cloud_path, anon=True)  # try with anonymous access first
    except Exception:
        fs = setup_fs(cloud_path)  # try again with credentials

    # filter out non-JSON files
    files = [x["Key"] for x in fs.listdir(cloud_path)] if fs.isdir(cloud_path) else [cloud_path]
    files = filter(lambda file: file.endswith(".json") or file.endswith(".jsonl"), files)
    if not files:
        raise ValueError(f"No JSON/JSONL files found in the specified path: {cloud_path}.")

    return sum([_read_json_with_fs(fs, file) for file in files], [])


# =============================
# Downsample tool/API pool to fit in context
# =============================

def downsample_tools(tool_pool: Union[str, list], max_tools: int = 50,  required_tools: list[str] = None, keep_retrievers: bool = True) -> list[dict]:

    pool = deepcopy(tool_pool) # Don't modify the original pool
    if isinstance(pool, str):
        pool = json.loads(pool)

    # pool = {p['function']['name']: p for p in pool} # Uncomment this if we need to run cusyom_loader.py
    pool = {p['name']: p for p in pool}
    downsampled_tools = []
    if required_tools:
        for req in required_tools:
            if req in pool: # TODO : Temporary bug fix for retrieval tools wherein BIRD Train domains are added by mistake.
                tool = pool.pop(req)
                downsampled_tools.append(tool)

    if keep_retrievers:
        retrievers = [k for k in pool.keys() if k.startswith("retriever_")]
        retriever_list = [pool.pop(t) for t in retrievers]
        downsampled_tools.extend(retriever_list)
    
    assert len(downsampled_tools) <= max_tools, f"The list of {len(downsampled_tools)} is longer than the specified length: {max_tools}"

    distractor_count = max_tools - len(downsampled_tools)

    if distractor_count >= len(pool):
        # Just return the original pool
        return tool_pool
    
    distractors = random.sample(list(pool.values()), distractor_count)
    downsampled_tools.extend(distractors)
    random.shuffle(downsampled_tools)
    return downsampled_tools


def update_retrieval_tools(tool_pool: Union[str, list]) -> list[dict]:
    pool = deepcopy(tool_pool) # Don't modify the original pool
    if isinstance(pool, str):
        pool = json.loads(pool)

    # pool = {p['function']['name']: p for p in pool} # Uncomment this if we need to run cusyom_loader.py
    pool = {p['name']: p for p in pool}
    retrievers_present = [k for k in pool.keys() if (k.startswith("retriever_"))]
    retrievers = [k for k in pool.keys() if (k.startswith("retriever_")) and (k not in RETRIEVERS_TO_IGNORE)]
    if len(retrievers_present) != 0: # Single turn dataset don't have retrievers at all by design.
        assert len(retrievers) != 0, "No retrivers left after removing RED domain and BIRD Train retrivers."
    updated_tool = [v for k, v in pool.items() if k not in RETRIEVERS_TO_IGNORE]
    return updated_tool

def load_data_files(dataset_dir: str, dataset: str) -> list[dict]:
    # Collect trajectories
    datasets = [dataset] if isinstance(dataset, str) else dataset
    trajectories = []
    for dataset in datasets:

        logger.info(f"Reading dataset '{dataset}' from {dataset_dir}")
        if dataset.endswith(".json"):
            with open(os.path.join(dataset_dir, dataset), "r") as f:
                new_trajectories = json.load(f)
            logger.info(f"Located single training file containing {len(new_trajectories)} trajectories. ")
            trajectories.extend(new_trajectories)

        else:
            single_dataset_dir = os.path.join(dataset_dir, dataset)
            files = os.listdir(single_dataset_dir)
            files = [f for f in files if f.startswith("trajectory")]
            files = sorted(files)
            logger.info(f"Located {len(files)} individual trajectory files in {single_dataset_dir}")
            for f in files:
                with open(os.path.join(single_dataset_dir, f), "r") as f:
                    trajectories.append(json.load(f))

    assert len(trajectories) > 0, f"Failed to find any trajectories files in {dataset_dir}"
    logger.info(f"Returning a total of {len(trajectories)} trajectories from {len(datasets)} datasets in {dataset_dir}")
    return trajectories