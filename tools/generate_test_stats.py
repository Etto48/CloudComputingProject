import matplotlib.pyplot as plt
import os
import argparse
import pandas as pd

test_results = {
    "Peak Map Physical memory": [],
    "Peak Map Virtual memory": [],
    "Peak Reduce Physical memory": [],
    "Peak Reduce Virtual memory": [],
    "Execution time": [],
    "Splits": []
}

expected_keyword = {
    "Peak Map Physical memory": "Peak Map Physical memory (bytes)=",
    "Peak Map Virtual memory": "Peak Map Virtual memory (bytes)=",
    "Peak Reduce Physical memory": "Peak Reduce Physical memory (bytes)=",
    "Peak Reduce Virtual memory": "Peak Reduce Virtual memory (bytes)=",
    "Execution time": "Execution time: ",
    "Splits": "number of splits:"
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
        with open(f"{log_dir}/tests_{i}.log", "r") as log:
            lines = log.readlines()
            for key in test_results.keys():
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
                        test_results[key].append(value)    
                        break
                else:
                    test_results[key].append(None)
                    print(f"Could not find {key} in {log_dir}/tests_{i}.log")
    df = pd.DataFrame(test_results)
    ax = df.plot(
        kind="bar", 
        subplots=True, 
        layout=(3,2), 
        figsize=(10,10), 
        legend=False, 
        logy=True,
    )
    plt.tight_layout()
    ax[0][0].set_ylabel("Bytes")
    ax[0][1].set_ylabel("Bytes")
    ax[1][0].set_ylabel("Bytes")
    ax[1][1].set_ylabel("Bytes")
    ax[2][0].set_ylabel("Seconds")
    plt.show()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate statistics from test logs")
    parser.add_argument("log_dir", type=str, help="The directory containing the log files")
    args = parser.parse_args()
    main(args.log_dir)