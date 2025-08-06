import csv
import importlib.metadata
import importlib.util
import json
import multiprocessing
import os
import pathlib
import random
import sys
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import List, Union, Iterable, Any
from typing import TYPE_CHECKING

import torch
import transformers
from loguru import logger
from packaging import version
from transformers.dynamic_module_utils import get_relative_imports
from transformers.utils import (
    is_torch_cuda_available,
    is_torch_mps_available,
    is_torch_npu_available,
    is_torch_xpu_available,
    is_torch_bf16_gpu_available
)
from transformers.utils.versions import require_version

if TYPE_CHECKING:
    from packaging.version import Version

_is_fp16_available = is_torch_npu_available() or is_torch_cuda_available()
try:
    _is_bf16_available = is_torch_bf16_gpu_available() or (is_torch_npu_available() and torch.npu.is_bf16_supported())
except Exception:
    _is_bf16_available = False


def skip_check_imports() -> None:
    r"""Avoid flash attention import error in custom model files."""
    if not is_env_enabled("FORCE_CHECK_IMPORTS"):
        transformers.dynamic_module_utils.check_imports = get_relative_imports


def _is_package_available(name: str) -> bool:
    return importlib.util.find_spec(name) is not None


def _get_package_version(name: str) -> "Version":
    try:
        return version.parse(importlib.metadata.version(name))
    except Exception:
        return version.parse("0.0.0")


def is_rank_0() -> bool:
    # Can also use accelerator.is_local_main_process if using Accelerator
    return int(os.environ.get("RANK", "0")) == 0


def create_dir(path: str):
    if not is_rank_0():
        return
    _dir = pathlib.Path(path)
    _dir.mkdir(parents=True, exist_ok=True)


def log_dist(
        message: str,
        ranks: List[int],
        level: int
) -> None:
    import logging
    logger = logging.getLogger(__name__)
    """Log messages for specified ranks only"""
    my_rank = int(os.environ.get("RANK", "0"))
    if my_rank in ranks:
        if level == logging.INFO:
            logger.info(f'[Rank {my_rank}] {message}')
        if level == logging.ERROR:
            logger.error(f'[Rank {my_rank}] {message}')
        if level == logging.DEBUG:
            logger.debug(f'[Rank {my_rank}] {message}')


def get_device_count() -> int:
    r"""Get the number of available GPU or NPU devices."""
    if is_torch_xpu_available():
        return torch.xpu.device_count()
    elif is_torch_npu_available():
        return torch.npu.device_count()
    elif is_torch_cuda_available():
        return torch.cuda.device_count()
    else:
        return 0


def get_current_device() -> "torch.device":
    r"""Get the current available device."""
    if is_torch_xpu_available():
        device = "xpu:{}".format(os.environ.get("LOCAL_RANK", "0"))
    elif is_torch_npu_available():
        device = "npu:{}".format(os.environ.get("LOCAL_RANK", "0"))
    elif is_torch_mps_available():
        device = "mps:{}".format(os.environ.get("LOCAL_RANK", "0"))
    elif is_torch_cuda_available():
        device = "cuda:{}".format(os.environ.get("LOCAL_RANK", "0"))
    else:
        device = "cpu"

    return torch.device(device)


def set_run_environment(verbosity="INFO", dotenv_path=None, log_dir=None):
    """
    Helpful Link: https://betterstack.com/community/guides/logging/loguru/
    :param verbosity: the minimum log level for the logger. Can be one of ["TRACE", "DEBUG", "INFO", "SUCCESS",
    "WARNING", "ERROR", "CRITICAL"]. Messages of levels below this level will be ignored.
    :param dotenv_path: path to .env file
    :param log_dir: path to log directory
    :return:
    """
    if dotenv_path is not None:
        from dotenv import load_dotenv
        load_dotenv(dotenv_path=dotenv_path)

    # # Set logger
    # # reset logger
    logger.remove()
    # # To log to stdout. Set enqueue=True to make logging multi-process safe (it is multi-thread safe by default)
    logger.add(sys.stdout, colorize=True, level=verbosity, enqueue=True)
    # # To log to file
    if log_dir is not None:
        logger.add(os.path.join(log_dir, 'logs_{time}.log'), level=verbosity, enqueue=True)
    else:
        logger.add('./logging/logs_{time}.log', level=verbosity, enqueue=True)
    logger.info(f"Verbosity set to {verbosity}")

    # # Set environment variables
    os.environ["VERBOSITY"] = verbosity
    os.environ["TOKENIZERS_PARALLELISM"] = "false"

    # Set seed
    random.seed(42)


def set_dist(args):
    # To train on cpu, set args.no_cuda=True else it will use all available gpus [Recommended use for now]
    if args.local_rank == -1 or args.no_cuda:
        if torch.cuda.is_available():
            device = torch.device("cuda")
            args.n_gpu = torch.cuda.device_count()
        elif torch.backends.mps.is_available():
            device = torch.device("mps")
            args.n_gpu = 1
        else:
            device = torch.device("cpu")
            args.n_gpu = 0
    # To enable distributed training (does it mean multi-node?), set local_rank
    else:
        torch.cuda.set_device(args.local_rank)
        device = torch.device("cuda", args.local_rank)
        # noinspection PyUnresolvedReferences
        torch.distributed.init_process_group(backend='nccl')
        args.local_rank += args.node_index * args.gpu_per_node
        args.n_gpu = 1

    cpu_cont = multiprocessing.cpu_count()  # Gives number of logical CPU cores
    # Do not use all cpu cores for parallel processing. For computationally intensive tasks, recommended usage is
    # to use number of physical CPU cores i.e. = (number of logical CPU cores)/2
    # Recommended reading: https://superfastpython.com/multiprocessing-pool-num-workers/
    args.cpu_cont = cpu_cont - int(cpu_cont / 2)  # Ignore half of the cores
    args.device = device

    logger.info("Process rank: {}, device: {}, n_gpu: {}, distributed training: {}, cpu count(using): {}, "
                "cpu count(available): {}", args.local_rank, device, args.n_gpu, bool(args.local_rank != -1),
                args.cpu_cont, cpu_cont)


def lower_keys(example):
    new_example = {}
    for key, value in example.items():
        if key != key.lower():
            new_key = key.lower()
            new_example[new_key] = value
        else:
            new_example[key] = value
    return new_example

# for chemistry data
def load_csv(file: Union[str, Path]) -> Iterable[Any]:
    with open(file, newline='', encoding='utf-8') as f:
        try:
            reader = csv.DictReader(f)  # yields each row as a dict
            for row in reader:
                yield row
        except:
            print("Error in loading:", file)
            exit()


def load_jsonl(file: Union[str, Path]) -> Iterable[Any]:
    with open(file, "r", encoding="utf-8") as f:
        for line in f:
            try:
                yield json.loads(line)
            except:
                print("Error in loading:", line)
                exit()


def save_jsonl(samples, save_path):
    # ensure path
    folder = os.path.dirname(save_path)
    os.makedirs(folder, exist_ok=True)

    with open(save_path, "w", encoding="utf-8") as f:
        for sample in samples:
            f.write(json.dumps(sample, ensure_ascii=False) + "\n")
    print("Saved to", save_path)


def is_env_enabled(env_var: str, default: str = "0") -> bool:
    r"""Check if the environment variable is enabled."""
    return os.getenv(env_var, default).lower() in ["true", "y", "1"]


def check_version(requirement: str, mandatory: bool = False) -> None:
    r"""Optionally check the package version."""
    if is_env_enabled("DISABLE_VERSION_CHECK") and not mandatory:
        logger.warning("Version checking has been disabled, may lead to unexpected behaviors.")
        return

    if mandatory:
        hint = f"To fix: run `pip install {requirement}`."
    else:
        hint = f"To fix: run `pip install {requirement}` or set `DISABLE_VERSION_CHECK=1` to skip this check."

    require_version(requirement, hint)


def check_dependencies() -> None:
    r"""Check the version of the required packages."""
    check_version("transformers>=4.41.2,<=4.50.0,!=4.46.0,!=4.46.1,!=4.46.2,!=4.46.3,!=4.47.0,!=4.47.1,!=4.48.0")
    check_version("datasets>=2.16.0,<=3.4.1")
    check_version("accelerate>=0.34.0,<=1.5.2")
    check_version("peft>=0.14.0,<=0.15.0")
    check_version("trl>=0.8.6,<=0.9.6")
    if is_transformers_version_greater_than("4.46.0") and not is_transformers_version_greater_than("4.48.1"):
        logger.warning("There are known bugs in transformers v4.46.0-v4.48.0, please use other versions.")


def get_peak_memory() -> tuple[int, int]:
    r"""Get the peak memory usage for the current device (in Bytes)."""
    if is_torch_npu_available():
        return torch.npu.max_memory_allocated(), torch.npu.max_memory_reserved()
    elif is_torch_cuda_available():
        return torch.cuda.max_memory_allocated(), torch.cuda.max_memory_reserved()
    else:
        return 0, 0


def has_tokenized_data(path: "os.PathLike") -> bool:
    r"""Check if the path has a tokenized dataset."""
    return os.path.isdir(path) and len(os.listdir(path)) > 0


def infer_optim_dtype(model_dtype: "torch.dtype") -> "torch.dtype":
    r"""Infer the optimal dtype according to the model_dtype and device compatibility."""
    if _is_bf16_available and model_dtype == torch.bfloat16:
        return torch.bfloat16
    elif _is_fp16_available:
        return torch.float16
    else:
        return torch.float32


@lru_cache
def is_transformers_version_greater_than(content: str):
    return _get_package_version("transformers") >= version.parse(content)


def count_parameters(model: "torch.nn.Module") -> tuple[int, int]:
    r"""Return the number of trainable parameters and number of all parameters in the model."""
    trainable_params, all_param = 0, 0
    for param in model.parameters():
        num_params = param.numel()
        # if using DS Zero 3 and the weights are initialized empty
        if num_params == 0 and hasattr(param, "ds_numel"):
            num_params = param.ds_numel

        # Due to the design of 4bit linear layers from bitsandbytes, multiply the number of parameters by itemsize
        if param.__class__.__name__ == "Params4bit":
            if hasattr(param, "quant_storage") and hasattr(param.quant_storage, "itemsize"):
                num_bytes = param.quant_storage.itemsize
            elif hasattr(param, "element_size"):  # for older pytorch version
                num_bytes = param.element_size()
            else:
                num_bytes = 1

            num_params = num_params * 2 * num_bytes

        all_param += num_params
        if param.requires_grad:
            trainable_params += num_params

    return trainable_params, all_param


def make_json_serializable(obj):
    if isinstance(obj, dict):
        return {k: make_json_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple, set)):
        return [make_json_serializable(v) for v in obj]
    elif isinstance(obj, (str, int, float, bool)) or obj is None:
        return obj
    elif isinstance(obj, datetime):
        return obj.isoformat()
    else:
        return str(obj)


class SafeDict(dict):
    """
     A safe method to partially fill a string with placeholders w/o raising a KeyError if any placeholder is missing
    """
    def __missing__(self, key):
        return f'{{{key}}}'  # leave the placeholder unchanged


def gpu_supports_fa2():
    if not torch.cuda.is_available():
        return False
    name = torch.cuda.get_device_name()
    major, minor = torch.cuda.get_device_capability()
    logger.info(f"{name} - Compute Capability {major}.{minor}")
    # Ampere & Hopper GPUs typically have compute capability >= 8.x  (A100 has 8.0, RTX 30-series (e.g., 3080, 3090) has 8.6, H100 has 9.0)
    return major >= 8
