import os
import json
import random
import math

def create_dataset_split():
    random.seed(0)
    dataset_dir = "/Users/abhinavjain/Desktop/AgenticAI/Code/data/bird-dev"
    datasets = ["rag_before_api_gt.json", "api_before_rag_gt.json"]
    splits = ["train", "test"]
    percent_frac = 0.5

    for dataset in datasets:
        print("\nSplitting dataset {}...".format(dataset))
        path = os.path.join(dataset_dir, dataset)
        data = json.load(open(path, 'r'))

        total_samples = len(data)
        num_train = math.ceil(percent_frac * total_samples)

        # Shuffle the data
        data = random.sample(data, total_samples)

        # Create the training split manually (since some instances don't have G.T.)
        train_data = []
        test_data = []
        for curr_instance in data:
            if 'trajectory' not in curr_instance:
                test_data.append(curr_instance)
            else:
                if len(train_data) < num_train:
                    train_data.append(curr_instance)
                else:
                    test_data.append(curr_instance)
        assert len(train_data) == num_train

        print("Total instances: %d" % total_samples)
        print("Number of training instances: %d" % len(train_data))
        print("Number of test instances: %d" % len(test_data))

        for split in splits:
            split_path = os.path.join(dataset_dir, split)
            if not os.path.exists(split_path):
                os.makedirs(split_path)

            # Save the dataset
            save_dataset_name = dataset.replace("_gt", "")
            if split == "train":
                with open(os.path.join(split_path, save_dataset_name), 'w') as f:
                    json.dump(train_data, f)
            else:
                with open(os.path.join(split_path, save_dataset_name), 'w') as f:
                    json.dump(test_data, f)

