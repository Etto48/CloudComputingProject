import argparse
import os
import subprocess

args_for_output = [
    "-i italian_1GB.txt -r 1",
    "-i english.txt -r 1",
    "-i spanish.txt -r 1",
]

args_for_tests = [
    "-i english.txt -r 1",
    "-i english.txt -r 2",
    "-i english.txt -r 4",
    "-i english.txt -r 8",
    "-i english.txt -r 1 --no-combiner",
    "-i english.txt -r 1 --no-in-mapper-combiner",
    "-i english.txt -r 1 --no-in-mapper-combiner --no-combiner",
    "-i english.txt -r 4 --no-combiner",
    "-i english.txt -r 4 --no-in-mapper-combiner",
    "-i english.txt -r 4 --no-in-mapper-combiner --no-combiner",
    "-i part_100MB.txt -r 1",
    "-i part_200MB.txt -r 1",
    "-i part_300MB.txt -r 1",
    "-i part_400MB.txt -r 1",
    "-i part_500MB.txt -r 1",
    "-i part_600MB.txt -r 1",
    "-i part_700MB.txt -r 1",
    "-i part_800MB.txt -r 1",
    "-i part_900MB.txt -r 1",
    "-i part_1000MB.txt -r 1",
    "-i part_1100MB.txt -r 1",
]

def run(args: str, index: int, mode: str):
    log_path = f"{mode}_{index}.log"
    output_path = f"{mode}_{index}.csv"
    
    cmd1 = f"hadoop jar letterfreq-0.1.0.jar it.unipi.LetterFreq {args}".split()
    cmd2 = f"hadoop fs -getmerge output {output_path}".split()
    
    try:
        log = subprocess.check_output(cmd1, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        with open(log_path, "wb") as f:
            f.write(e.output)
        print(f"Job {index} failed with exit code {e.returncode}. Check {log_path} for more information.")
        raise e
        
    _ = subprocess.check_output(cmd2, stderr=subprocess.STDOUT)
    
    with open(log_path, "wb") as f:
        f.write(log)
    

def main(mode: str, skip_to: int):
    arg_list = args_for_output if mode == "output" else args_for_tests
    for i,args in enumerate(arg_list):
        if i < skip_to:
            print(f"Skipping job {i}/{len(arg_list)} with args {args}...")
            continue
        
        print(f"Running job {i}/{len(arg_list)} with args {args}...")
        run(args, i, mode)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Run the LetterFreq Hadoop job")
    parser.add_argument("-m", "--mode", type=str, choices=["output", "tests"], default="output", help="Choose the mode to run the job")
    parser.add_argument("-s", "--skip-to", type=int, default=0, help="Skip to a specific job")
    args = parser.parse_args()
    main(args.mode, args.skip_to)