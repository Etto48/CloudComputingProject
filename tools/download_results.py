import subprocess
import argparse
import os

def main(mode: str, from_index: int, to_index: int, connection_str: str):
    files = [f"{mode}_{i}.csv" for i in range(from_index, to_index)] +\
        [f"{mode}_{i}.log" for i in range(from_index, to_index)]
        
    for file in files:
        os.remove(f"./{mode}/{file}")
        
    subprocess.run(f"scp {connection_str}:~/{{{",".join(files)}}} ./{mode}/")
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Run the LetterFreq Hadoop job")
    parser.add_argument("-m","--mode", choices=["output", "tests"],
        default="tests",
        help="Whether to download the output files or run the tests")
    parser.add_argument("-f","--from-index", type=int, default=0, help="The index of the job to start from (inclusive)")
    parser.add_argument("-t","--to-index", type=int, default=None, help="The index of the job to end at (exclusive)")
    parser.add_argument("-c","--connection-str", type=str, 
        default="hadoop@10.1.1.38",
        help="The connection string to use for the download (e.g. user@host)")
    args = parser.parse_args()
    
    if args.to_index is None:
        if args.mode == "output":
            args.to_index = 3
        else:
            args.to_index = 21
    main(args.mode, args.from_index, args.to_index, args.connection_str)