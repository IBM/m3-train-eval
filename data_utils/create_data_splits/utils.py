import re
import random
from collections import defaultdict

random.seed(100)


def strip_trailing_index(name: str) -> str:
    """
    Strip trajectory id from scenario names i.e. ONLY_RAG_IN_DOMAIN_0 or ONLY_RAG_IN_DOMAIN is converted to ONLY_RAG_IN_DOMAIN.
    """
    return re.sub(r'_\d+$', '', name)

def scenario_based_sample(data,sample_percent):
    """
    Create a data sample based on scenario class.
    """
    sample_data=[]
    data_sections=defaultdict(list)
    for item in data:
        scenario_name=strip_trailing_index(item["sample_id"].split("_sc_")[1]) # Remove trajectory id from scenario name
        data_sections[scenario_name].append(item)
    
    if sample_percent:
        for k,v in data_sections.items():
            n = int(sample_percent * len(v))
            if (n==0) and len(v)!=0:
                sample_data.extend(v)
            else:
                sample_data.extend(random.sample(v, n))
            # assert (n!=0) and (n<=len(v)); f"For scenario {k} sample percent {sample_percent} leading to {n} samples being picked."            
    return sample_data

def downsample_data(mixed_data,sample_disruptor=None,sample_nondisruptor=None):
    downsampled_data=[]
    data_sections=defaultdict(list)
    for item in mixed_data:
        data_sections[item["change_state"]].append(item)

    # Keep all base samples
    downsampled_data.extend(data_sections["base"])

    # Downsample Disruptors
    if sample_disruptor:
        percent_disruptor=(len(data_sections["base"])*sample_disruptor)/len(data_sections["disruptor"])
        # Bug Fix for the moment
        if percent_disruptor>=1.0:
             downsampled_data.extend(data_sections["disruptor"])
        else:
            disruptor_samples=scenario_based_sample(data_sections["disruptor"],percent_disruptor)
            downsampled_data.extend(disruptor_samples)
    else:
        downsampled_data.extend(data_sections["disruptor"]) # i.e. is sample_disruptor is None keep all samples

    # Downsample Non-Disruptors
    if sample_nondisruptor:
        percent_nondisruptor=((len(data_sections["base"])*sample_nondisruptor)/len(data_sections["nondisruptor"]))
        nondisruptor_samples=scenario_based_sample(data_sections["nondisruptor"], percent_nondisruptor)
        downsampled_data.extend(nondisruptor_samples)
    else:
        downsampled_data.extend(data_sections["nondisruptor"]) # i.e. is sample_nondisruptor is None keep all samples

    
    random.shuffle(downsampled_data)
    
    return downsampled_data