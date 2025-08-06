from typing import List

import torch
from transformers import StoppingCriteria


class KeywordsStopWordsCriteria(StoppingCriteria):
    def __init__(self, keywords_str: List[str], tokenizer):
        StoppingCriteria.__init__(self)
        self.current_context = []
        self.tokenizer = tokenizer
        self.keywords_str = keywords_str

    def __call__(
            self, input_ids: torch.LongTensor, scores: torch.FloatTensor, **kwargs
    ) -> bool:

        if len(self.current_context) == 0:
            self.current_context = [[] for _ in range(input_ids.shape[0])]

        # self.current_context = [[] for _ in range(input_ids.shape[0])]
        sequences_should_be_stopped = []
        for i in range(input_ids.shape[0]):
            _id = input_ids[i][-1].item()
            self.current_context[i].append(_id)
            current_context = self.tokenizer.decode(self.current_context[i])
            should_be_stopped = False
            for word in self.keywords_str:
                if word in current_context:  # This could lead to unintended stopping if the stop word is not a separate word but a subword of larger word
                    should_be_stopped = True
                    break
            sequences_should_be_stopped.append(should_be_stopped)
        return all(sequences_should_be_stopped)


class KeyWordsStopWordsIDsCriteria(StoppingCriteria):
    def __init__(self, stop_id_sequences):
        assert isinstance(stop_id_sequences[0], list), (
            "stop_id_sequences should be a list of list of ids"
        )
        self.stop_sequences = stop_id_sequences

    def __call__(
            self, input_ids: torch.LongTensor, scores: torch.FloatTensor, **kwargs
    ) -> bool:
        sequences_should_be_stopped = []
        for i in range(input_ids.shape[0]):
            sequence_should_be_stopped = False
            for stop_sequence in self.stop_sequences:
                if len(input_ids[i]) >= len(stop_sequence):
                    if input_ids[i][-len(stop_sequence):].tolist() == stop_sequence:
                        sequence_should_be_stopped = True
                        break
            sequences_should_be_stopped.append(sequence_should_be_stopped)
        return all(sequences_should_be_stopped)
