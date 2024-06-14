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
                    


    df = pd.DataFrame(test_results)
    df = df.drop(index=9)
    print(df)

    df = df.drop(columns="Execution time")

    # Trasforma il DataFrame in modo esplicito
    df_transformed = pd.DataFrame()
    for col in df.columns:
        df_transformed[f'{col}1'] = df[col].apply(lambda x: x[0])
        df_transformed[f'{col}2'] = df[col].apply(lambda x: x[1])

    # Creazione della figura e delle sottotrame
    fig, axes = plt.subplots(4, 2, figsize=(15, 15))

    # Creazione dei grafici per ciascuna coppia di colonne
    for i, col in enumerate(df.columns):
        row, col_idx = divmod(i, 2)
        df_transformed[[f'{col}1', f'{col}2']].plot(kind='bar', ax=axes[row, col_idx], width=0.8, logy=True)
        axes[row, col_idx].set_title(f'Grafico a barre per colonne {col}1 e {col}2')
        axes[row, col_idx].set_xlabel('Index')
        axes[row, col_idx].set_ylabel('Values')
        axes[row, col_idx].set_xticks(range(len(df_transformed)))
        axes[row, col_idx].set_xticklabels(df.index)

    # Miglioramento del layout
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate statistics from test logs")
    parser.add_argument("log_dir", type=str, help="The directory containing the log files")
    args = parser.parse_args()
    main(args.log_dir)