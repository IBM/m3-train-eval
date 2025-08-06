import json
from typing import Dict

from agents.llm import get_lm
from data_utils import Template, TEMPLATES
from envs.tool_call_env import ToolCallEnv, M3ToolCallEnv
from envs.base_env import SubDomain, ToolPolicy
from envs.expert_assist import ExpertAssist
from loguru import logger


def get_agent_env(
        path_to_env_data: str,
        db_config: dict,
        api_config: dict,
        expert_assist: "ExpertAssist",
        horizon: int,
        template_type: str,
        overseer_llm_params: Dict,
        scorer_llm_params: Dict,
        env_subdomain_mode: str,
        partial_credit: bool = False,
) -> ToolCallEnv:
    # Load the template
    agent_template: "Template" = TEMPLATES[template_type]

    # ########################################## Set up the Agent environment ########################################## #
    # with open(path_to_db_config) as f:
    #     db_config = json.load(f)
    # with open(path_to_api_config) as f:
    #     api_config = json.load(f)

    logger.info("Loaded the database/document collection config")
    logger.info(json.dumps(db_config, indent=4))
    logger.info("Loaded the api config")
    logger.info(json.dumps(api_config, indent=4))

    # Configure the Final Scoring Agent/Overseer Agent's LLM
    overseer_llm = get_lm(overseer_llm_params["model_name_or_path"], parameters=overseer_llm_params)
    scorer_llm = get_lm(scorer_llm_params["model_name_or_path"], parameters=scorer_llm_params)

    # Configure the environment
    sub_domain = SubDomain(
        mode=env_subdomain_mode,
    )
    env = M3ToolCallEnv(
        path_to_env_data=path_to_env_data,
        es_config=db_config,
        api_config=api_config,  # Local end point: "end_point": "http://127.0.0.1:8000",
        horizon=horizon,
        sub_domain=sub_domain,
        expert_assist=expert_assist,
        agent_template=agent_template,
        overseer_llm=overseer_llm,
        overseer_llm_parameters=overseer_llm_params,
        scorer_llm=scorer_llm,
        scorer_llm_parameters=scorer_llm_params,
        partial_credit=partial_credit
    )
    return env
