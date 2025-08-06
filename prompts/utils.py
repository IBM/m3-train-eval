import copy
import json
import re
import sys
from typing import List, Dict, Any

from loguru import logger


def get_parser_resolver_prompt(prompt: List[dict], response: str, error: str) -> List[dict]:
    updated_prompt = copy.deepcopy(prompt)
    updated_prompt.append(
        {
            "role": "assistant",
            "content": response,
        }
    )
    redo_prompt = """Your previous response failed to meet the required format for automated parsing.

Failure reason: {error}

You must now revise your response to strictly follow the format below:

Thought:
[Insert your reasoning here — this should explain your judgment.]

Conclusion:
[Insert either **Yes** or **No**, and only one of them, as a standalone word.]

Formatting rules (strictly enforced by parser):
- Thought must begin with **`Thought:`**
- Conclusion must begin with **`Conclusion:`**
- Conclusion must include only **Yes** or **No**, not both, and no additional text.

Now correct your output accordingly."""
    updated_prompt.append(
        {
            "role": "user",
            "content": redo_prompt.format(error=error),
        }
    )


def get_witness_prompt(
        query: str,
        expert_interaction_info: str,
        observations: str,
        use_sample=False
) -> List[dict]:

    from prompts.judge.observation_witness import SYSTEM_PROMPT, QUERY_PROMPT, SAMPLE_DEMO
    prompt = []
    if use_sample:
        system_prompt = SYSTEM_PROMPT + "\n\n" + SAMPLE_DEMO
    else:
        system_prompt = SYSTEM_PROMPT
    prompt.append(
        {
            "role": "system",
            "content": system_prompt,
        }
    )
    query_prompt = QUERY_PROMPT.format(query=query, expert_info=expert_interaction_info, observations=observations)
    prompt.append(
        {
            "role": "user",
            "content": query_prompt,
        }
    )

    return prompt


def parse_witness_response(response: str) -> Dict[str, str]:
    try:
        assert "thought" in response.lower()
    except AssertionError:
        logger.error(f"JudgeLLM's response `{response}` does not contain Thought")
        raise ValueError(f"No thought found. Must follow 'Thought:'")

    try:
        assert "conclusion" in response.lower()
    except AssertionError:
        logger.error(f"JudgeLLM's response `{response}` does not contain Conclusion")
        raise ValueError(f"No conclusion found. Must follow 'Conclusion:'")

    # Extract thought and conclusion
    conclusion = response.lower().split("conclusion")[-1].strip()
    if conclusion.startswith(":"):
        conclusion = conclusion[1:]

    thought = response.lower().split("conclusion")[0].split("thought")[-1].strip()
    if thought.startswith(":"):
        thought = thought[1:]

    # Compute score from conclusion
    yes_found = bool(re.search(r'\byes\b', conclusion, re.IGNORECASE))
    no_found = bool(re.search(r'\bno\b', conclusion, re.IGNORECASE))
    if yes_found and no_found:
        raise ValueError(
            f"Response Parsing failed. Both 'Yes' and 'No' detected as separate words in conclusion.\nResponse: {response}"
        )
    elif not yes_found and not no_found:
        raise ValueError(
            f"Response Parsing failed. Neither 'Yes' nor 'No' detected in conclusion.\nResponse: {response}"
        )
    elif yes_found:
        witnessed = True
    else:
        witnessed = False

    return {
        "witnessed": witnessed,
        "thought": thought.strip(),
        "conclusion": conclusion.strip(),
    }


def get_thought_rewriter_prompt(
        current_agent_trajectory: List[dict],
        expert_suggested_action: dict,
        original_expert_thought: str
) -> List[dict]:
    from prompts.editor.thought_rewriter import SYSTEM_PROMPT, QUERY_PROMPT
    prompt = []
    system_prompt = SYSTEM_PROMPT
    prompt.append(
        {
            "role": "system",
            "content": system_prompt,
        }
    )
    query_prompt = QUERY_PROMPT.format(
        current_agent_trajectory=json.dumps(current_agent_trajectory, indent=2),
        expert_suggested_action=json.dumps(expert_suggested_action, indent=2),
        original_expert_thought=original_expert_thought
    )
    prompt.append(
        {
            "role": "user",
            "content": query_prompt,
        }
    )

    return prompt


def parse_rewriter_response(response: str) -> str:
    try:
        assert "thought" in response.lower()
    except AssertionError:
        logger.error(f"JudgeLLM's response {response} does not contain Thought")
        sys.exit(-1)

    # Extract thought
    if 'Thought' in response:
        thought = response.split("Thought")[-1].strip()
    else:
        thought = response.lower().split("thought")[-1].strip()
    if thought.startswith(":"):
        thought = thought[1:]
    return thought.strip()


def get_scorer_prompt(user_query: str, golden_answer: str, predicted_final_answer: str, use_sample=False, partial_scoring=False) -> List[dict]:

    if partial_scoring:
        from prompts.judge.answer_matcher_partial import SYSTEM_PROMPT, QUERY_PROMPT, SAMPLE_DEMO
    else:
        from prompts.judge.answer_matcher_binary import SYSTEM_PROMPT, QUERY_PROMPT, SAMPLE_DEMO

    state = []

    if use_sample:
        system_prompt = SYSTEM_PROMPT + "\n\n" + SAMPLE_DEMO
    else:
        system_prompt = SYSTEM_PROMPT
    state.append(
        {
            "role": "system",
            "content": system_prompt,
        }
    )
    query_prompt = QUERY_PROMPT.format(user_query=user_query, golden_answer=golden_answer, predicted_final_answer=predicted_final_answer)
    state.append(
        {
            "role": "user",
            "content": query_prompt,
        }
    )

    return state


def parse_scorer_response(response: str, partial_scoring=False) -> Dict[str, Any]:
    try:
        assert "thought" in response.lower()
    except AssertionError:
        logger.error(f"ScoreJudge LLM's response {response} does not contain Thought")
        raise ValueError(f"No thought found. Must follow 'Thought:'")

    if partial_scoring:
        try:
            assert "score" in response.lower()
        except AssertionError:
            logger.error(f"PartialScoreJudge LLM's response {response} does not contain Score")
            sys.exit(-1)
    else:
        try:
            assert "conclusion" in response.lower()
        except AssertionError:
            logger.error(f"BinaryScoreJudge LLM's response {response} does not contain Conclusion")
            raise ValueError(f"No conclusion found. Must follow 'Conclusion:'")

    if partial_scoring:
        # Extract thought and score
        score = response.lower().split("score")[-1].strip()
        if score.startswith(":"):
            score = score[1:]
        thought = response.lower().split("score")[0].split("thought")[-1].strip()
        if thought.startswith(":"):
            thought = thought[1:]

        # Compute score
        from prompts.judge.answer_matcher_partial import SCORING_SCALE
        pred_score = None
        for s in SCORING_SCALE:
            if str(int(s)) in score or str(float(s)) in score:
                pred_score = float(s)
                break
        if pred_score is None:
            raise ValueError(
                f"Response Parsing failed. PartialScoreJudge LLM didn't follow the format to give the score.\nResponse: {response}")

        max_score = sorted(SCORING_SCALE, reverse=True)[0]
        if float(pred_score) == float(max_score):
            success = True
        else:
            success = False

        return {
            "thought": thought,
            "score": pred_score,
            "success": success,
        }
    else:
        # Extract thought and conclusion
        conclusion = response.lower().split("conclusion")[-1].strip()
        if conclusion.startswith(":"):
            conclusion = conclusion[1:]

        thought = response.lower().split("conclusion")[0].split("thought")[-1].strip()
        if thought.startswith(":"):
            thought = thought[1:]

        # Compute score from conclusion
        yes_found = bool(re.search(r'\byes\b', conclusion, re.IGNORECASE))
        no_found = bool(re.search(r'\bno\b', conclusion, re.IGNORECASE))
        if yes_found and no_found:
            raise ValueError(
                f"Response Parsing failed. Both 'Yes' and 'No' detected as separate words in conclusion.\nResponse: {response}"
            )
        elif not yes_found and not no_found:
            raise ValueError(
                f"Response Parsing failed. Neither 'Yes' nor 'No' detected in conclusion.\nResponse: {response}"
            )
        elif yes_found:
            pred_score = 1.0
            success = True
        else:
            pred_score = 0.0
            success = False

        return {
            "thought": thought,
            "conclusion": conclusion,
            "score": pred_score,
            "success": success,
        }


def get_overseer_prompt(
        query: str,
        agent_history: List[dict],
        ordered_sub_ques_composition: str
) -> List[dict]:

    from prompts.judge.overseer import SYSTEM_PROMPT, QUERY_PROMPT
    prompt = []
    system_prompt = SYSTEM_PROMPT
    prompt.append(
        {
            "role": "system",
            "content": system_prompt,
        }
    )
    query_prompt = QUERY_PROMPT.format(
        query=query,
        sub_ques_info=ordered_sub_ques_composition,
        agent_history=agent_history,
    )
    prompt.append(
        {
            "role": "user",
            "content": query_prompt,
        }
    )

    return prompt


def parse_overseer_response(response: str) -> Dict[str, str]:
    try:
        assert "thought" in response.lower()
    except AssertionError:
        logger.error(f"OverseerLLM's response `{response}` does not contain Thought")
        raise ValueError(f"No thought found. Must follow 'Thought:'")

    try:
        assert "conclusion" in response.lower()
    except AssertionError:
        logger.error(f"OverseerLLM's response `{response}` does not contain Conclusion")
        raise ValueError(f"No conclusion found. Must follow 'Conclusion:'")

    # Extract thought and conclusion
    conclusion = response.lower().split("conclusion")[-1].strip()
    if conclusion.startswith(":"):
        conclusion = conclusion[1:]

    thought = response.lower().split("conclusion")[0].split("thought")[-1].strip()
    if thought.startswith(":"):
        thought = thought[1:]

    # Compute score from conclusion
    yes_found = bool(re.search(r'\byes\b', conclusion, re.IGNORECASE))
    no_found = bool(re.search(r'\bno\b', conclusion, re.IGNORECASE))
    if yes_found and no_found:
        raise ValueError(
            f"Response Parsing failed. Both 'Yes' and 'No' detected as separate words in conclusion.\nResponse: {response}"
        )
    elif not yes_found and not no_found:
        raise ValueError(
            f"Response Parsing failed. Neither 'Yes' nor 'No' detected in conclusion.\nResponse: {response}"
        )
    elif yes_found:
        stuck = True
    else:
        stuck = False

    return {
        "thought": thought,
        "conclusion": conclusion,
        "stuck": stuck,
    }
