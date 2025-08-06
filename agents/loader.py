import json
import os
from typing import List, Optional

from agents.experts.linear_expert import M3Expert, LinearExpert
from agents.llm import get_lm, get_lm_hf
from agents.monolithic_agent import Monolithic as Agent
from data_utils.template import Template, TEMPLATES


def get_agent(
        model_name_or_path: str,
        max_new_tokens: int,
        temperature: float,
        stop_sequences: List[str],
        template_type: str,
        is_hf_agent: bool = False,
        path_to_hf_config: Optional[str] = None,
) -> Agent:
    # Load the template
    agent_template: "Template" = TEMPLATES[template_type]

    # ####################################### Set up the Agent [RITS backend] ####################################### #
    if not is_hf_agent:

        llm_parameters = {
            "model_name_or_path": model_name_or_path,
            "max_new_tokens": max_new_tokens,
            "temperature": temperature,
            "stop_sequences": stop_sequences,
        }
        llm = get_lm(model_id=llm_parameters["model_name_or_path"], parameters=llm_parameters)
        agent = Agent(
            tokenizer_id_hf=None,
            hf_token=os.environ.get("HF_TOKEN", ""),
            llm=llm,
            llm_parameters=llm_parameters,
            agent_template=agent_template
        )

    # ####################################### Set up the Agent [HF backend] ####################################### #
    else:
        with open(path_to_hf_config, 'r') as f:
            hf_config = json.load(f)
        # We will load this into the hf config (so that we declare these vars in one place only)
        hf_config["model_name_or_path"] = model_name_or_path
        hf_config['template'] = template_type
        hf_config["max_new_tokens"] = max_new_tokens
        hf_config["temperature"] = temperature

        llm_parameters = {
            "model_name_or_path": model_name_or_path,
            "max_new_tokens": max_new_tokens,
            "temperature": temperature,
            "stop_sequences": stop_sequences,
            # "cutoff_len": hf_config["cutoff_len"],  # Instead of providing this explicitly, we will infer using the model's config
        }
        llm = get_lm_hf(hf_config=hf_config)
        agent = Agent(
            llm=llm,
            llm_parameters=llm_parameters,
            agent_template=agent_template
        )

    return agent


def get_expert(
        model_name_or_path: str,
        max_new_tokens: int,
        temperature: float,
        stop_sequences: List[str],
        template_type: str,
) -> LinearExpert:

    # Load the template
    agent_template: "Template" = TEMPLATES[template_type]

    # ########################################## Define the expert ########################################## #
    llm_parameters = {
        "model_name_or_path": model_name_or_path,
        "max_new_tokens": max_new_tokens,
        "temperature": temperature,
        "stop_sequences": stop_sequences,
    }
    expert_llm = get_lm(model_name_or_path, parameters=llm_parameters)
    expert_agent = M3Expert(
        llm=expert_llm,
        llm_parameters=llm_parameters,
        agent_template=agent_template
    )
    return expert_agent
