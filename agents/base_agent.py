from typing import List, Dict, Optional, Any

from transformers import AutoTokenizer

from agents.utils import AgenticTracker
from data_utils.template import Template

agentic_tracker = AgenticTracker()


class Agent:
    """
    The base Agent class which receives query, and generate response, parse response and generate meta log data.
    Attributes:
        tokenizer_id_hf: tokenizer id in huggingface, in order to apply tokenizing function
    """

    def __init__(
            self,
            llm = None,
            llm_parameters = None,
            tokenizer_id_hf = "",
            hf_token = "",
            agent_template: "Template" = None
    ):
        self.llm = llm
        self.llm_parameters = llm_parameters
        self.tokenizer_id_hf = tokenizer_id_hf
        if tokenizer_id_hf:
            self.tokenizer = AutoTokenizer.from_pretrained(
                tokenizer_id_hf, token=hf_token, trust_remote_code=True
            )
        else:
            self.tokenizer = None
        self.error_message = {"parsing": ""}

        self.agent_template = agent_template
        # Init tags
        self.thought_start = self.agent_template.thought_words[0]
        self.thought_end = self.agent_template.thought_words[1]

    def apply_chat_template(self, turns: List[Dict]) -> str:
        """
        convert chat history into str prompt
        :param turns: chat history of {"role": role, "content": content}
        :return: prompt str
        """
        prompt = self.tokenizer.apply_chat_template(
            turns,
            add_generation_prompt=True,
            tokenize=False,
        )
        return prompt

    @staticmethod
    def append_turn(role: str, content: str, chat: List[Dict]) -> List[Dict]:
        """
        Add current turn to chat history
        :param role: One of "system", "assistant", "user"
        :param content: content of the turn
        :param chat: chat history of {"content":content, "role":role}
        :return:
        """
        turn = {"role": role, "content": content}
        chat.append(turn)
        return chat

    def invoke_llm(self, prompt: str) -> str:
        """Generates a response based on the message."""
        raise NotImplementedError()

    def chat_openai_generatee_n(self, prompt: str) -> List[str]:
        """generate n response messages"""
        res = self.llm.generate(prompt)
        return res

    def parse_llm_response(self, response: str, prompt_type: str = "") -> Dict:
        """parse output of llm based on certain prompt"""
        raise NotImplementedError

    @agentic_tracker.track_agent
    def tracked_action(self, query: str, reward: Optional[float] = None) -> Dict:
        """agent action with logging"""
        raise NotImplementedError

    def take_action(self, state, include_thoughts: bool=True, reward: Optional[float] = None) -> Dict[str, Any]:
        """agent action"""
        raise NotImplementedError

    @agentic_tracker.track_agent
    def error_message_reflect(
            self,
            response: str,
            chats: List[Dict],
            reflect_limit: int = 3,
            reflect_role: str = ""
    ) -> Dict:
        """
        reflect based on last turn of conversation
        :param response: chat response
        :param chats: chat context
        :param reflect_limit: n iterations for reflection
        :return: result dict
        :param reflect_role: role of this reflection in logging
        """
        REFLECTION_PROMPT = """"""
        raise NotImplementedError


    def generate_meta(self) -> Dict:
        """generate meta data for logging"""
        raise NotImplementedError
