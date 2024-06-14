import matplotlib.pyplot as plt
import os
import argparse
import pandas as pd

test_results = {
    "Total Physical memory": [],
    "Total Virtual memory": [],
    "Peak Map Physical memory": [],
    "Peak Map Virtual memory": [],
    "Peak Reduce Physical memory": [],
    "Peak Reduce Virtual memory": [],
    "Splits": [],
    "Execution time": []
}

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
                    
        size_for_each_key = {}
        for key in test_results.keys():
            for elem in test_results[key]:
                if key in size_for_each_key:
                    size_for_each_key[key] = max(size_for_each_key[key], len(elem))
                else:
                    size_for_each_key[key] = len(elem)
        
        for key in test_results.keys():
            for elem in test_results[key]:
                if len(elem) < size_for_each_key[key]:
                    elem.extend([0.0] * (size_for_each_key[key] - len(elem)))
        
        for key in test_results.keys():
            test_results[key] = zip(*test_results[key])

    df = pd.DataFrame(test_results)
    ax = df.plot(
        kind="bar", 
        subplots=True, 
        layout=(4,2), 
        figsize=(10,10), 
        legend=False, 
        logy=True,
    )
    plt.tight_layout()
    axes_index = 0
    exectution_time_index = df.columns.get_loc("Execution time")
    splits_index = df.columns.get_loc("Splits")
    
    for ax_line in ax:
        for a in ax_line:
            if axes_index == exectution_time_index:
                a.set_ylabel("Seconds")
            elif axes_index == splits_index:
                a.set_ylabel("Number of splits")
                a.set_yscale("linear")
            else:
                a.set_ylabel("Bytes")
            axes_index += 1
    plt.show()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate statistics from test logs")
    parser.add_argument("log_dir", type=str, help="The directory containing the log files")
    args = parser.parse_args()
    main(args.log_dir)