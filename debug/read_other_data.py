import json
import random

# Button-Instruct
def read_button_instruct(_path):
    """Link: https://github.com/PKU-Baichuan-MLSystemLab/BUTTON
    Contains 8K SFT Training Points.
    Similarities:
        > Two types of actions: tool-call and final answer
        > Multi-hop setting

    Differences:
        > No implementation of tools as executables (yet).
        > No Question-Decomposition provided.
        > Allows for parallel function calling."""
    samples = []
    with open(_path, "r") as f:
        for line in f:
            line = line.strip()
            if line != "":
                samples.append(json.loads(line))

    print("Number of samples: {}".format(len(samples)))
    some_sample = random.choice(samples)
    print(json.dumps(some_sample, indent=4))


if __name__ == "__main__":
    path = '../data/button_instruct.jsonl'
    read_button_instruct(path)