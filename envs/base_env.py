from loguru import logger
from typing import Tuple, List, Dict, Optional, Any
from dataclasses import dataclass, field


@dataclass
class ToolPolicy:
    """A dataclass representing different policies that the agent must adhere to while calling tools"""
    tool_availability_policy: str = field(default="both_api_rag")
    tool_usage_policy: str = field(default="")
    final_answer_policy: str = field(default="")

    def __post_init__(self):
        assert self.tool_availability_policy in {'only_rag', 'only_api', 'both_api_rag', 'neither_api_rag'}
        # assert self.tool_usage_policy in {'api_before_rag', 'rag_before_api', 'only_api', 'only_rag', 'no_policy'}

    def __str__(self):
        return f"Tool Availability Policy: {self.tool_availability_policy}\nTool Usage Policy: {self.tool_usage_policy}\nFinal Answer Policy: {self.final_answer_policy}"

    def to_dict(self):
        return {
            "tool_availability_policy": self.tool_availability_policy,
            "tool_usage_policy": self.tool_usage_policy,
            "final_answer_policy": self.final_answer_policy,
        }


@dataclass
class SubDomain:
    """A dataclass representing a sub-domain that categorizes the tool-calling environment"""
    # tool_policy: ToolPolicy
    mode: str = field(default="rest")

    def __post_init__(self):
        assert self.mode in ['rest', 'selection', 'slot_filling']

class BaseEnv:
    def __init__(self, horizon: int, **kwargs):
        # Load data for the environment
        self.data = self.load_env_data()
        self.total_unique_instances = len(self.data)
        logger.info(f"Total unique environment instances found: {self.total_unique_instances}")
        self.curr_instance_idx = -1

        self.curr_state = None
        self.curr_step = 0
        self.horizon = horizon

        # [Optionally] G.T. for the current instantiation of the environment
        self.expert_traj = None

    def load_env_data(self):
        raise NotImplementedError()

    def reset(self) -> Tuple[List[Dict[str, str]], float, bool, Dict[str, bool]]:
        """
        :return: state (chat messages), reward, done, metadata: {terminated, truncated, success}
        """
        raise NotImplementedError()

    def step(self, action: Dict[str, Optional[Any]]) -> Tuple[List[Dict[str, str]], float, bool, Dict[str, bool]]:
        """
        Must call 1. get_observation(), 2. get_reward() and 3. transition()
        :param action: Dict containing parsed agentic response. Must have the following keys:
            :key response, str: original response from the agent
            :key thought, str: thought before taking action
            :key action, str: high-level action (or action_type)
            :key action_arguments, dict: arguments expected by the high-level action
            :key error, str: environment error created within the agent by parsing its response
                        (defined within the agent since parsing is unique to each agent)
        :return: state (chat messages), reward, done, metadata: {terminated, truncated, success}
        """
        raise NotImplementedError()

    def get_observation(self, action: Dict[str, Optional[Any]]) -> Tuple[str, str, bool, bool]:
        """
        :param action: Dict containing parsed agentic response.
        :return:
            env_role: str i.e. One of Role.USER.value (on user-turn) or Role.OBSERVATION.value (response from tool invocation)
            observation: str i.e. observation from the environment (user turn or tool response)
            terminated: bool i.e. whether the environment is terminated or not
            truncated: bool i.e. whether the environment is truncated or not
        """
        raise NotImplementedError()

    def get_reward(self, action: Dict[str, Optional[Any]], observation: str) -> Tuple[float, bool]:
        """
        Call to determine reward and success flag. Can be called at any step.
        :param action:
        :param observation:
        :return: reward (float), success (bool)
        """
        raise NotImplementedError()

    def transition(self, observation: str, action: Dict[str, Optional[Any]], env_role: str):
        """
        :param observation:
        :param action:
        :param env_role:
        :return:
        """
        raise NotImplementedError()

    def close(self):
        raise NotImplementedError()