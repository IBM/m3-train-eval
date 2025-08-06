
The code for generating the SLOT-BIRD and SEL-BIRD datasets in contained in this directory. 

In order to generate the NL to api sequence datasets from the source BIRD-SQL dataset. 

- Install the requirements in requirements.txt. 
- download the train or dev split (depending on which you want to use) of BIRD into the invocable-api-hub/db directory. 
- From the top level of the repository, run the following command

```
PYTHONPATH=. python invocable_api_hub/tool_calling/scripts/run_bird_translation.py --mode dev --size large
```

The `--mode` argument should be either 'dev' or 'train', and specifies which split of BIRD to choose source datasets from. The `--size` argument should be either 'small' or 'large'. Choosing large runs the process on all queries and all databases in the chosen split. Choosing small runs the process only on a single database and the queries associated with it (disney database for train, student_club for dev). 
