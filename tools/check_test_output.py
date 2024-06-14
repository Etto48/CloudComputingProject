import argparse
from typing import Optional
import pandas as pd
import os

def main(test_dir: str, max_tests: int, reference: Optional[str]):
    files = os.listdir(test_dir)
    csv_files = 0
    for file in files:
        if file.endswith(".csv"):
            csv_files += 1
    if csv_files == 0:
        print("No csv files found in the specified directory")
        return

    if csv_files > max_tests:
        print(f"Found {csv_files} csv files, limiting to {max_tests}")
    csv_files = min(csv_files, max_tests)

    dfs: list[pd.DataFrame] = []
    for i in range(csv_files):
        df = pd.read_csv(f"{test_dir}/tests_{i}.csv", sep="\t", header=None, names=["letter", "frequency"])
        dfs.append(df)
    
    reference_path = f"{test_dir}/tests_0.csv" if reference is None else reference
    
    reference = pd.read_csv(reference_path, sep="\t", header=None, names=["letter", "frequency"])
    
    # every dataframe should have the same probability distribution corresponding to the same letter
    # check if this is true
    
    for i in range(0, len(dfs)):
        if not dfs[i].equals(reference):
            print(f"{test_dir}/tests_{i}.csv is different from the reference ({reference_path})")
            return

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate statistics from test logs")
    parser.add_argument("-t","--test-dir", type=str, default="tests", help="The directory containing the test logs")
    parser.add_argument("-m","--max-tests", type=int, default=10, help="The maximum number of tests to consider")
    parser.add_argument("-r","--reference", type=str, default=None, help="The reference file to compare the tests to, if not set the first test is used as reference")
    args = parser.parse_args()
    
    main(args.test_dir, args.max_tests, args.reference)