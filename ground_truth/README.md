
---

## 🧾 Ground Truth Data Generation

To prepare data usable by the agentic pipeline, begin by transforming raw environment data into a structured, multi-turn format and optionally enrich it with LLM-generated thoughts and final answers.

All of this is handled via:

📄 [generate.py](generate.py)

---

### ⚙️ Mode 1: Parsing Raw Data into Multi-turn Format

This step converts raw scenario data into a pipeline-consumable multi-turn format.

**🔧 Required changes in `generate.py`:**

* ✅ **Uncomment** the call to `create_multi_turn_data(...)`
* ❌ **Comment out** the call to `create_and_inject_thoughts(...)`

**💡 Arguments:**

* `_raw_data_dir`: Path to the raw data directory
* `_save_parsed_data_at`: Path where the parsed multi-turn data should be saved

---

### 🤖 Mode 2: Injecting LLM-Generated Thoughts and Final Answers

After parsing, you can optionally inject reasoning traces and final answers using an LLM, guided by specific rules for tool response formatting and data quality.

**🔧 Required changes in `generate.py`:**

* ❌ **Comment out** the call to `create_multi_turn_data(...)`
* ✅ **Uncomment** the call to `create_and_inject_thoughts(...)`

This mode performs the following:

#### 🧠 Thought and Answer Generation

* Uses a **`thought_generator` prompt** to generate *all thoughts* for a turn in a single call
* Then uses a **`final_answer` prompt** to produce the final answer

#### 🧹 Tool Response Filtering Logic

When tool responses contain **long lists of objects** (e.g., APIs returning thousands of results), the following safeguards apply:

| Parameter                           | Description                                                                                                                                                         |
|-------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `range_tool_resp_cut_off`           | A tuple (e.g., `[3, 5]`) that defines a range from which `curr_resp_cutoff` is randomly sampled                                                                     |
| `curr_resp_cutoff`                  | The number of items the model is told to retain in its answer (set via `CONDENSE_TOOL_RESPONSE_INSTRUCTION` prompt defined in [constants.py](../envs/constants.py)) |
| `answer_generator_additional_instr` | Special instruction injected into the LLM prompt to restrict response size                                                                                          |
| `max_tool_resp_cut_off`             | If the tool response has more than this many objects, the **sample is discarded**                                                                                   |

The instruction `CONDENSE_TOOL_RESPONSE_INSTRUCTION` to control tool response passing into the final answer is along the lines of
```text
If the final tool response is a list of many objects, and all of them could be used to answer the query, retain randomly any {curr_resp_cutoff} objects.
        - Reflect this truncation clearly and intentionally in your thought.
        - Justify why the selected subset is sufficient
```



🗑️ **Discarded Samples:**

* Samples discarded due to:

  * Exceeding `max_tool_resp_cut_off`
  * Failed generation of thoughts or final answer
* Are saved **separately** in their original parsed format for review or later use

---

### 🧪 Example Usage

```bash
python generate.py
```

---

