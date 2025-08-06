from dataclasses import dataclass, field
from typing import Optional

from loguru import logger


@dataclass
class ExpertAssist:
    mode: Optional[str] = field(default=None)  # "random" or "informed" or None
    random_epsilon: float = field(default=0.0)  # Increasing the value increases the likelihood of agent taking the action
    init_limit: int = field(default=2)
    recent_limit: int = field(default=-2)

    def __post_init__(self):
        if self.mode is None:
            logger.info("Expert Assist mode not set. Agent will not get expert help.")
        else:
            assert self.mode in ["informed", "random", "ground_truth"]

        assert self.init_limit > 0
        assert self.recent_limit < 0
        assert 0 <= self.random_epsilon <= 1


def setup_expert_assist(
        mode,
        init_limit,
        recent_limit,
        random_epsilon
) -> ExpertAssist:
    # Configure the Expert Assistance during trajectory generation
    expert_assist = ExpertAssist(
        mode=mode,
        init_limit=init_limit,
        recent_limit=recent_limit,
        random_epsilon=random_epsilon,
    )
    return expert_assist
