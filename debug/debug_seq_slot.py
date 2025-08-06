import os
from invocable_api_hub.tool_calling.sql_to_api.utils import execute_single_api
from invocable_api_hub.tool_calling.sql_to_api.sql_slot_filling_dataset_builder import SqlSlotFillingDatasetBuilder
from invocable_api_hub.tool_calling.sql_to_api.sql_sequencing_dataset_builder import SqlSequencingDatasetBuilder
from typing import Optional, Dict, Any
from collections import defaultdict


class DebugSeqSlot:
    def __init__(self, agent_mode: str, dataset_path: str, results_cache_path_unique: str, dataset_name: str):
        self.agent_mode = agent_mode
        self.dataset_name = dataset_name
        self.dataset_path = dataset_path
        self.results_cache_path_unique = results_cache_path_unique
        self.initialization_specs: Optional[Dict[str, Any]] = defaultdict(dict)

        if self.agent_mode == "SLOTFILLING":
            builder = SqlSlotFillingDatasetBuilder(
                self.dataset_name,
                self.dataset_path,
                cache_location=self.results_cache_path_unique,
                source_dataset_name="bird",
            )  # Specific
        elif self.agent_mode == "SEQUENCING":
            builder = SqlSequencingDatasetBuilder(
                self.dataset_name,
                self.dataset_path,
                cache_location=self.results_cache_path_unique,
                source_dataset_name="bird",
            )  # Specific
        else:
            raise ValueError("Unknown agent_mode '{}'".format(self.agent_mode))

        self.builder = builder
        self.builder.build()

        self.initialization_specs["arguments"]["database_path"] = os.path.join(
            results_cache_path_unique, f"{self.dataset_name}.sqlite"
        )


if __name__ == "__main__":
    dataset_name = "student_club"
    results_cache_path = '/Users/abhinavjain/Desktop/AgenticAI/data/bird-dev/cache'
    results_cache_path = os.path.join(results_cache_path, dataset_name)
    if not os.path.isdir(results_cache_path):
        os.mkdir(results_cache_path)

    dataset_path = '/Users/abhinavjain/Desktop/AgenticAI/data/bird-dev/dev_databases'
    debug_slot = DebugSeqSlot(
        agent_mode="SLOTFILLING",
        dataset_name=dataset_name,
        dataset_path=dataset_path,
        results_cache_path_unique=results_cache_path,
    )