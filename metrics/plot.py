import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd


def plot_error_histogram(error_profile: dict, save_results_path: str):
    # Convert the dictionary to a sorted list of (error, frequency) tuples
    errors = list(error_profile.items())
    errors.sort(key=lambda x: x[1], reverse=True)  # Optional: sort by frequency

    # Separate the keys and values
    error_names, frequencies = zip(*errors)
    error_names = ["".join(n.split(" ")) for n in error_names]

    # Set the figure size dynamically based on number of errors
    plt.figure(figsize=(max(10, len(error_names) * 0.6), 6))

    # Create the bar plot
    sns.set_theme(style="whitegrid")
    sns.barplot(x=list(error_names), y=list(frequencies), palette="muted")

    # Improve layout
    plt.xticks(rotation=45, ha='right')
    plt.xlabel("Error Type")
    plt.ylabel("Frequency")
    plt.title("Error Frequency Histogram")
    plt.tight_layout()
    plt.savefig(save_results_path, dpi=300)

def plot_error_frequencies_comparison(error_profile_by_method: dict, save_results_path: str):
    """
    Plots comparative histograms of error types across different training methods.

    Args:
        error_profile_by_method: Dict[str, Dict[str, int]]
            A dictionary where each key is a training method name and the value is
            a dictionary of {error_type: count}.
        save_results_path: str
    """

    # Prepare DataFrame in long format for seaborn
    records = []
    for method, freq_dict in error_profile_by_method.items():
        for error_type, count in freq_dict.items():
            records.append({
                "Training Method": method,
                "Error Type": error_type,
                "Count": count
            })

    df = pd.DataFrame(records)

    # Fill in missing combinations with count 0
    df = df.pivot_table(index="Error Type", columns="Training Method", values="Count", fill_value=0)
    df = df.reset_index().melt(id_vars="Error Type", var_name="Training Method", value_name="Count")

    # Plot
    plt.figure(figsize=(10, 6))
    sns.set_theme(style="whitegrid", palette="muted")
    sns.barplot(
        data=df,
        x="Error Type",
        y="Count",
        hue="Training Method"
    )
    plt.xticks(rotation=45, ha='right')
    plt.xlabel("Error Type")
    plt.ylabel("Frequency")
    plt.title("Error Frequencies by Training Method")
    plt.tight_layout()
    plt.savefig(save_results_path, dpi=300)


def plot_freq_dist(freq_dist: dict, save_results_path: str):
    if not freq_dist:
        return
    records = []
    for _type, count in freq_dist.items():
        records.append({
            "Category": _type,
            "Count": count
        })

    df = pd.DataFrame(records)

    # Plot
    plt.figure(figsize=(10, 6))
    sns.set_theme(style="whitegrid", palette="muted")
    sns.barplot(
        data=df,
        x="Category",
        y="Count",
    )
    plt.xticks(rotation=45, ha='right')
    plt.xlabel("Category Type")
    plt.ylabel("Frequency")
    plt.title("Category Frequencies")
    plt.tight_layout()
    plt.savefig(save_results_path, dpi=300)