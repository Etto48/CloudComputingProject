import argparse
import os

args_for_output = [
    "-i italian_1GB.txt -r 2",
    "-i english.txt -r 2",
    "-i spanish.txt -r 2",
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
]

def run(args: str, index: int, mode: str):
    log_path = f"{mode}_{index}.log"
    output_path = f"{mode}_{index}.csv"
    
    os.system(f"hadoop jar letterfreq-0.1.0.jar it.unipi.LetterFreq {args} 2&>1 > {log_path}")
    os.system(f"hadoop fs -getmerge output {output_path} 2&>1 > /dev/null")

def main(mode: str):
    arg_list = args_for_output if mode == "output" else args_for_tests
    for i,args in enumerate(arg_list):
        print(f"Running job {i+1}/{len(args_for_output)} with args {args}...")
        run(args, i, mode)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Run the LetterFreq Hadoop job")
    parser.add_argument("-m", "--mode", type=str, choices=["output", "tests"], default="output", help="Choose the mode to run the job")
    args = parser.parse_args()
    main(args.mode)