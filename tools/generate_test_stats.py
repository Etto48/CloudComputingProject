import matplotlib.pyplot as plt
import os
import argparse
import pandas as pd
import numpy as np

keys = [
    "Total Physical memory",
    "Total Virtual memory",
    "Peak Map Physical memory",
    "Peak Map Virtual memory",
    "Peak Reduce Physical memory",
    "Peak Reduce Virtual memory",
    "Splits",
    "Execution time"
]

test_results = {key: [] for key in keys}

expected_keyword = {
    "Total Physical memory": "Physical memory (bytes) snapshot=",
    "Total Virtual memory": "Virtual memory (bytes) snapshot=",
    "Peak Map Physical memory": "Peak Map Physical memory (bytes)=",
    "Peak Map Virtual memory": "Peak Map Virtual memory (bytes)=",
    "Peak Reduce Physical memory": "Peak Reduce Physical memory (bytes)=",
    "Peak Reduce Virtual memory": "Peak Reduce Virtual memory (bytes)=",
    "Splits": "number of splits:",
    "Execution time": "Execution time: "
}

def main(log_dir: str):
    files = os.listdir(log_dir)
    log_files = 0
    for file in files:
        if file.endswith(".log"):
            log_files += 1
    if log_files == 0:
        print("No log files found in the specified directory")
        return
    
    for i in range(log_files):
        try:
            log = open(f"{log_dir}/tests_{i}.log", "r")
        except FileNotFoundError:
            print(f"Could not find {log_dir}/tests_{i}.log")
            for key in test_results.keys():
                test_results[key].append([])
        else:
            with log:
                lines = log.readlines()
                for key in test_results.keys():
                    values_for_this_keyword = []
                    for line in lines:
                        if expected_keyword[key] in line:
                            starts_from = line.find(expected_keyword[key]) + len(expected_keyword[key])
                            value = line[starts_from:].strip()
                            if value.endswith("B") or value.endswith("s"):
                                value = value[:-1]
                            if value.isdigit():
                                value = int(value)
                            else:
                                value = float(value)
                            values_for_this_keyword.append(value)  
                    test_results[key].append(values_for_this_keyword)

    df = pd.DataFrame(test_results)

    # Transform the dataframe and unzip the lists
    df_transformed = pd.DataFrame()
    for col in df.columns:
        if col != "Execution time":
            df_transformed[f"{col}1"] = df[col].apply(lambda x: x[0] if len(x) > 0 else None)
            df_transformed[f"{col}2"] = df[col].apply(lambda x: x[1] if len(x) > 1 else None)
        else:
            df_transformed[col] = df[col].apply(lambda x: x[0] if len(x) > 0 else None)

    # Create the subplots
    fig, axes = plt.subplots(4, 2, sharex=True, figsize=(14,9))
    # Plot the data
    for i, col in enumerate(df.columns):
        row, col_idx = divmod(i, 2)
        ax: plt.Axes = axes[row, col_idx]
        if col == "Execution time":
            df_transformed[col].plot(kind='bar', ax=ax, logy=True)
            ax.set_ylabel('Seconds')
            ax.legend(["Total"], loc='best')
        elif col == "Splits":
            df_transformed[[f'{col}1', f'{col}2']].plot(kind='bar', ax=ax, legend=False)
            ax.set_ylabel('Splits')
        else:
            df_transformed[[f'{col}1', f'{col}2']].plot(kind='bar', ax=ax, logy=True, legend=False)
            ax.set_ylabel('Bytes')
        ax.set_title(col)
        ax.set_xlabel('Test ID')
        ax.set_xticks(range(len(df_transformed)))
        ax.set_xticklabels(df.index)
        ax.set_axisbelow(True)
        ax.grid(axis="y", linestyle="--", which="minor", linewidth=0.5)
        ax.grid(axis="y", linestyle="--", which="major")
    fig.legend(["Job 1", "Job 2"], loc="upper left")

    # Adjust the layout
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate statistics from test logs")
    parser.add_argument("log_dir", type=str, help="The directory containing the log files")
    args = parser.parse_args()
    main(args.log_dir)