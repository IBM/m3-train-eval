import json
import os
from dataclasses import dataclass
from enum import Enum
from typing import Any, List, Dict, Optional

import torch
from ibm_watsonx_ai.metanames import GenTextParamsMetaNames
from langchain_core.language_models import BaseLLM
from langchain_ibm import WatsonxLLM
from langchain_ollama import OllamaLLM
from loguru import logger
from openai import OpenAI
from pydantic import SecretStr
from transformers import PreTrainedModel, PreTrainedTokenizer, ProcessorMixin, DataCollatorForSeq2Seq, \
    StoppingCriteriaList
from transformers.utils import is_flash_attn_2_available

from data_utils import get_template_and_fix_tokenizer, Template, Role
from data_utils.processor.processor_utils import infer_seqlen
from hparams import get_infer_args
from model import load_tokenizer, load_model
from extras.custom import gpu_supports_fa2
from model.model_utils.generate import KeywordsStopWordsCriteria, KeyWordsStopWordsIDsCriteria


class LLM_Options(Enum):
    AUTO = (1,)
    WATSONX = (2,)
    BAM = 3
    RITS = 4
    LOCAL = 5

@dataclass
class HFLLM:
    tokenizer: "PreTrainedTokenizer"
    model: "PreTrainedModel"
    template: "Template"
    processor: Optional["ProcessorMixin"]
    collator: Optional["DataCollatorForSeq2Seq"]


DEFAULT_WATSONX_PROJECT = "a415727b-d3c0-45cd-92df-a29626a2c7d9"
DEFAULT_WATSONX_URL = "https://us-south.ml.cloud.ibm.com"

MODEL_NAME_HF_MAP = {
    "meta-llama/llama-3-3-70b-instruct": "meta-llama/llama-3-3-70b-instruct",
    "mistralai/mixtral-8x22B-instruct-v0.1": "mistralai/Mixtral-8x22B-Instruct-v0.1",
    "ibm-granite/granite-3.1-8b-instruct": "ibm-granite/granite-3.1-8b-instruct",
    "deepseek-ai/DeepSeek-V3": "deepseek-ai/DeepSeek-V3",
}

RITS_MODEL_URL_MAPPING = {
    "deepseek-ai/DeepSeek-V3": "https://inference-3scale-apicast-production.apps.rits.fmaas.res.ibm.com/deepseek-v3/v1",

    # "mistralai/mixtral-8x22B-instruct-v0.1": "https://inference-3scale-apicast-production.apps.rits.fmaas.res.ibm.com/mixtral-8x22b-instruct-a100/v1",
    "mistralai/mixtral-8x22B-instruct-v0.1": "https://inference-3scale-apicast-production.apps.rits.fmaas.res.ibm.com/mixtral-8x22b-instruct-v01/v1",
    "mistralai/mixtral-8x7B-instruct-v0.1": "https://inference-3scale-apicast-production.apps.rits.fmaas.res.ibm.com/mixtral-8x7b-instruct-v01/v1",
    "mistralai/mistral-large-instruct-2407": "https://inference-3scale-apicast-production.apps.rits.fmaas.res.ibm.com/mistral-large-instruct-2407/v1",

    "ibm-granite/granite-3.1-8b-instruct": "https://inference-3scale-apicast-production.apps.rits.fmaas.res.ibm.com/granite-3-1-8b-instruct/v1",
    "ibm-granite/granite-3.0-8b-instruct": "https://inference-3scale-apicast-production.apps.rits.fmaas.res.ibm.com/granite-3-0-8b-instruct/v1",
    "ibm-granite/granite-3.2-8b-instruct": "https://inference-3scale-apicast-production.apps.rits.fmaas.res.ibm.com/granite-3-2-8b-instruct/v1",
    "ibm-granite/granite-8b-code-instruct-128k": "https://inference-3scale-apicast-production.apps.rits.fmaas.res.ibm.com/granite-8b-code-instruct-128k/v1",

    "Qwen/Qwen2.5-72B-Instruct": "https://inference-3scale-apicast-production.apps.rits.fmaas.res.ibm.com/qwen2-5-72b-instruct/v1",
    "Qwen/Qwen2.5-Coder-32B-Instruct": "https://inference-3scale-apicast-production.apps.rits.fmaas.res.ibm.com/Qwen/Qwen2-5-Coder-32B-Instruct/v1",
    "Qwen/QwQ-32B": "https://restricted-3scale-apicast-production.apps.rits.fmaas.res.ibm.com/qwen-qwq-32b/v1",

    "meta-llama/llama-3-1-405b-instruct-fp8": "https://inference-3scale-apicast-production.apps.rits.fmaas.res.ibm.com/llama-3-1-405b-instruct-fp8/v1",
    "meta-llama/llama-3-3-70b-instruct": "https://inference-3scale-apicast-production.apps.rits.fmaas.res.ibm.com/llama-3-3-70b-instruct/v1",
    "meta-llama/llama-3-1-70b-instruct": "https://inference-3scale-apicast-production.apps.rits.fmaas.res.ibm.com/llama-3-1-70b-instruct/v1",
    "meta-llama/Llama-3.1-8B-Instruct": "https://inference-3scale-apicast-production.apps.rits.fmaas.res.ibm.com/llama-3-1-8b-instruct/v1",
    "codellama/CodeLlama-34b-Instruct-hf": "https://inference-3scale-apicast-production.apps.rits.fmaas.res.ibm.com/codellama/CodeLlama-34b-Instruct-hf/v1",
}


def get_lm(
    model_id: str | None = None,
    llm_provider: Enum = LLM_Options.AUTO,
    parameters: dict[str, Any] | None = None,
    hf_config: dict[str, Any] | None = None,
) -> Any:
    # returns LLM provider, Auto selects based on env variable, defaults to WatsonX
    provider_env = os.getenv("LLM_PROVIDER", "rits").lower()
    if llm_provider == LLM_Options.AUTO:
        if provider_env == "watsonx":
            return get_lm_watsonx(model_id, parameters=parameters)
        elif provider_env == "rits":
            return get_lm_rits(model_id)  # parameters are passed at the invoke step
        elif provider_env == "local":
            return get_lm_local(model_id, parameters=parameters)
        elif provider_env == "hf":
            return get_lm_hf(hf_config)
        return None
    return None

def get_args_hf(override_args: dict):
    model_args, data_args, finetuning_args, generating_args = get_infer_args(override_args)
    logger.info(f"\n|--------------------------- Model Configuration ---------------------------|\n{json.dumps(model_args.to_dict(), indent=4)}")
    logger.info(f"\n|--------------------------- Data Configuration ---------------------------|\n{json.dumps(data_args.to_dict(), indent=4)}")
    logger.info(f"\n|--------------------------- Finetuning Configuration ---------------------------|\n{json.dumps(finetuning_args.to_dict(), indent=4)}")
    logger.info(f"\n|--------------------------- Generating Configuration ---------------------------|\n{json.dumps(generating_args.to_dict(), indent=4)}")

    return model_args, data_args, finetuning_args, generating_args

def get_lm_hf(hf_config: dict) -> "HFLLM":

    # My Check for flash-attention2
    if 'flash_attn' in hf_config and hf_config['flash_attn'] == 'fa2':
        can_run_fa2 = True
        if not is_flash_attn_2_available():
            can_run_fa2 = False
        if not gpu_supports_fa2():
            can_run_fa2 = False

        if not can_run_fa2:
            del hf_config['flash_attn']  # will be later set to Auto after parsing
        else:
            logger.info("Provided args specify using flash-attention 2. Based on preliminary checks it should work!")

    model_args, data_args, finetuning_args, generating_args = get_args_hf(hf_config)

    # [1a] Load tokenizer
    logger.info(f"Loading FM tokenizer for {model_args.model_name_or_path}")
    tokenizer_module = load_tokenizer(model_args)
    processor, tokenizer = tokenizer_module["processor"], tokenizer_module["tokenizer"]

    # [1b] Load the chat template
    logger.info(f"Loading template {data_args.template}")
    template = get_template_and_fix_tokenizer(tokenizer, data_args)

    # [2] Load the model
    model = load_model(tokenizer, model_args, finetuning_args, is_trainable=False)

    # [Optionally] Put the model on cuda
    if torch.cuda.is_available():
        # Check if model is already on a CUDA device
        if not next(model.parameters()).is_cuda:
            logger.info("CUDA is available. Moving model to CUDA...")
            model = model.cuda()
        else:
            logger.info("Model is already on CUDA.")
    else:
        logger.info("CUDA not available. Model stays on CPU.")

    # [3] Load the collator
    collator = DataCollatorForSeq2Seq(
        tokenizer=tokenizer,
        label_pad_token_id=-100,
        pad_to_multiple_of=None,  # for shift short attention <-?
    )

    hf_model = HFLLM(
        model=model,
        tokenizer=tokenizer,
        processor=processor,
        template=template,
        collator=collator,
    )
    return hf_model


def get_lm_local(model_id: str, parameters: Any) -> BaseLLM:
    llm = OllamaLLM(model=model_id, **parameters)
    return llm


def get_lm_watsonx(model_id: str, parameters: Any) -> WatsonxLLM:
    if not os.getenv("WATSONX_APIKEY"):
        raise ValueError(
            "You need to set the env var WATSONX_APIKEY.  You can get your key"
            " from https://cloud.ibm.com/iam/apikeys"
        )
    project_id = os.getenv("WATSONX_PROJECT", DEFAULT_WATSONX_PROJECT)

    if not parameters:
        params = {
            GenTextParamsMetaNames.MAX_NEW_TOKENS: 250,
            GenTextParamsMetaNames.MIN_NEW_TOKENS: 20,
            GenTextParamsMetaNames.TEMPERATURE: 0,
            GenTextParamsMetaNames.STOP_SEQUENCES: ["\nObservation"],
        }
    else:
        params = {
            GenTextParamsMetaNames.MAX_NEW_TOKENS: parameters["max_new_tokens"],
            GenTextParamsMetaNames.MIN_NEW_TOKENS: parameters["min_new_tokens"],
            GenTextParamsMetaNames.TEMPERATURE: parameters["temperature"],
            GenTextParamsMetaNames.TOP_P: parameters["top_p"],
            GenTextParamsMetaNames.RANDOM_SEED: parameters["random_seed"],
            GenTextParamsMetaNames.STOP_SEQUENCES: parameters["stop_sequences"],
            GenTextParamsMetaNames.DECODING_METHOD: parameters["decoding_method"],
        }

    return WatsonxLLM(
        model_id=model_id,
        url=SecretStr(os.getenv("WATSONX_URL", DEFAULT_WATSONX_URL)),
        project_id=project_id,
        params=params,
    )


def get_lm_rits(model_id: str) -> OpenAI:
    try:
        rits_api_key = os.environ["RITS_API_KEY"]
    except BaseException:
        raise ValueError(
            "You need to set the env var RITS_API_KEY to use a model from RITS."
        )
    return OpenAI(
        api_key=rits_api_key,
        base_url=RITS_MODEL_URL_MAPPING[model_id],
        default_headers={"RITS_API_KEY": rits_api_key},
    )


def invoke_hf_model(llm: "HFLLM", llm_parameters: Dict[str, Any], messages: List[Dict[str, str]]) -> str:

    # During inference, we will infer cutoff length from model's
    try:
        cutoff_len = llm.model.config.max_position_embeddings
    except AttributeError:
        logger.error(f"Couldn't determine cutoff length from model's config: {json.dumps(llm.model.config, indent=2)}")

    assert messages[0]['role'] == Role.SYSTEM.value
    system_prompt: str = messages[0]["content"]
    tools = None  # already part of the system prompt
    prompt: List[Dict[str, str]] = messages[1:]
    messages = prompt + [{"role": Role.ASSISTANT.value, "content": ""}]

    # Multi-modal processing of messages [PLUGIN-SPECIFIC]. Having no mm_plugin will not affect this.
    messages = llm.template.mm_plugin.process_messages(
        messages,[],[],[], llm.processor
    )

    # Multi-modal processing of the input_ids and label_ids [PLUGIN-SPECIFIC].
    # > Having no mm_plugin will not affect this.
    # > If present, it will add prepend ids corresponding to mm input i.e. 'before' the input_ids and label_ids
    # > Ignore the label_ids since in unsupervised training we don't predict labels of the mm inputs
    input_ids, _ = llm.template.mm_plugin.process_token_ids(
        [],[],[],[],[], llm.tokenizer, llm.processor
    )

    # Encode the messages. Here, we get encoded_pairs for the input_ids and labels turn-wise
    encoded_pairs = llm.template.encode_multiturn(llm.tokenizer, messages, system_prompt, None)

    # We will start with the last turn and move towards the first turn until max_context_length is reached.
    encoded_pairs = encoded_pairs[::-1]  # high priority for last turns. Earlier turns likely to be masked out.
    total_length = len(input_ids) + (1 if llm.template.efficient_eos else 0)  # init

    # First, pop the last (most-recent) turn
    source_ids, label_ids = encoded_pairs.pop(0)
    if llm.template.efficient_eos:
        label_ids += [llm.tokenizer.eos_token_id]
    source_len, target_len = infer_seqlen(len(source_ids), len(label_ids), cutoff_len - total_length)
    source_ids = source_ids[:source_len]
    label_ids = label_ids[:target_len]  # Done, don't need to process it further

    # Update the total length
    total_length += source_len + target_len
    input_ids += source_ids  # Add the mm tokens to the beginning of the last turn

    for turn_idx, (source_ids, target_ids) in enumerate(encoded_pairs):
        # Check. Older turns/conversations are likely to be masked out as they are towards the end
        if total_length > cutoff_len:
            break
        # Get the source and target lengths
        source_len, target_len = infer_seqlen(
            len(source_ids), len(target_ids), cutoff_len - total_length
        )
        # Truncate the source and target ids
        source_ids = source_ids[:source_len]
        target_ids = target_ids[:target_len]
        # Update the total length
        total_length += source_len + target_len
        input_ids = source_ids + target_ids + input_ids  # Prepend the past turn to the current turn
    attn_mask = [1] * len(input_ids)
    input_len = len(input_ids)

    input_features = [
        {
            "input_ids": input_ids,
            "attention_mask": attn_mask,
        }
    ]
    batch: dict[str, torch.Tensor] = llm.collator.__call__(input_features)

    # # Define the stopping criterion
    stop_words = llm_parameters["stop_sequences"]
    # # Use Keywords based
    # stop_criteria = KeywordsStopWordsCriteria(stop_words, llm.tokenizer)
    # # Use Token Ids based
    stop_word_ids_list = [llm.tokenizer.encode(w, add_special_tokens=False) for w in stop_words]
    stop_criteria = KeyWordsStopWordsIDsCriteria(stop_word_ids_list)

    prediction = llm.model.generate(
        input_ids=batch["input_ids"].to(llm.model.device),
        attention_mask=batch["attention_mask"].to(llm.model.device),
        max_new_tokens=llm_parameters["max_new_tokens"],
        temperature=0.0,
        do_sample=False,
        stopping_criteria=StoppingCriteriaList([stop_criteria])
    )
    prediction = prediction[0][input_len:]
    outputs: str = llm.tokenizer.decode(prediction, skip_special_tokens=True)
    if len(outputs.strip()) == 0:
        logger.error(f"Model output is empty: {llm.tokenizer.decode(prediction, skip_special_tokens=False)}")
        raise ValueError(f"Model output is empty: {llm.tokenizer.decode(prediction, skip_special_tokens=False)}")
    logger.info(f"HF Model Prediction (w Special Tokens): {llm.tokenizer.decode(prediction, skip_special_tokens=False)}")
    return outputs


def invoke_hf_model_old(prompt: str, model_id: str) -> Any:
    try:
        from transformers import AutoModelForCausalLM, AutoTokenizer
    except ImportError:
        raise ImportError(
            "Install torch and the hugging face transformers library to perform inference using HF models."
        )
    device = "cuda"  # or "cpu"
    model_path = "ibm-granite/granite-20b-functioncalling"
    tokenizer = AutoTokenizer.from_pretrained(model_path)
    # drop device_map if running on CPU
    model = AutoModelForCausalLM.from_pretrained(model_path, device_map=device)
    model.eval()
    payload: Any = None
    # serialize functions and define a payload to generate the input template
    if model_id == "ibm-granite/granite-20b-functioncalling":
        payload = {
            "query": prompt,
        }
    elif model_id == "MadeAgents/Hammer2.0-7b":
        payload = [{"role": "user", "content": prompt}]
    else:
        raise BaseException(
            "The only HF models supported as of now are ibm-granite/granite-20b-functioncalling and MadeAgents/Hammer2.0-7b."
        )

    instruction = tokenizer.apply_chat_template(
        payload, tokenize=False, add_generation_prompt=True
    )

    # tokenize the text
    input_tokens = tokenizer(instruction, return_tensors="pt").to(device)

    # generate output tokens
    outputs = model.generate(**input_tokens, max_new_tokens=100)

    # decode output tokens into text
    outputs = tokenizer.batch_decode(outputs)

    # loop over the batch to print, in this example the batch size is 1
    # for output in outputs:
    # Each function call in the output will be preceded by the token "<function_call>" followed by a
    # json serialized function call of the format {"name": $function_name$, "arguments" {$arg_name$: $arg_val$}}
    # In this specific case, the output will be: <function_call> {"name": "get_current_weather", "arguments": {"location": "New York"}}
    # print(output)


def invoke_llm(llm, llm_parameters: Dict[str, Any], messages: List[Dict[str, str]]) -> Any:
    if isinstance(llm, WatsonxLLM):
        try:
            prompt = messages[-1]["content"]  # TODO: Check if this is what WatsonxLLM requires
            response = llm.invoke(prompt)
        except BaseException as e:
            raise e
        return response  # .content

    elif isinstance(llm, OpenAI):
        try:
            response = llm.chat.completions.create(
                model=llm_parameters['model_name_or_path'],
                messages=messages,
                temperature=llm_parameters['temperature'],
                max_tokens=llm_parameters['max_new_tokens'],
                stop=llm_parameters['stop_sequences'],
            )
        except BaseException as e:
            raise e
        return response.choices[0].message.content

    elif isinstance(llm, HFLLM):
        return invoke_hf_model(llm, llm_parameters, messages)

    elif isinstance(llm, str) and llm == "hf":
        prompt = messages[-1]["content"]  # TODO: Check if this is what HF requires
        return invoke_hf_model_old(prompt, llm_parameters['model_name_or_path'])

    else:
        raise ValueError("Incorrect llm type: {}".format(type(llm)))