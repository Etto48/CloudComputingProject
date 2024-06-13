import argparse
import subprocess
import psutil
import os
import time

commands: dict[tuple[str,str]] = {
        "py": ("baseline-py", "python baseline.py -i ../dataset/english.txt -o output.csv"),
        "rs": ("baseline-rs", "cargo run --release -- -i ../dataset/english.txt -o output.csv"),
    }

def main(mode: str):
    command: tuple[str,str] = commands[mode]
    
    os.chdir(command[0])
    process = subprocess.Popen(command[1].split(), shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    process_info = psutil.Process(process.pid)
    while len(process_info.children()) == 0:
        time.sleep(0.1)
    process_info = process_info.children()[0]
    if mode == "rs":
        while len(process_info.children()) == 0:
            time.sleep(0.1)
        process_info = process_info.children()[0]
        while len(process_info.children()) == 0:
            time.sleep(0.1)
        process_info = process_info.children()[0]
    
    start = time.time()
    print("Time,Virtual memory,Physical memory")
    while process.poll() is None:
        memory_info = process_info.memory_info()
        vm = memory_info.vms
        pm = memory_info.rss
        
        print(f"{(time.time() - start):0.2f},{vm},{pm}")
        time.sleep(0.1)
        
    assert process.wait() == 0
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Benchmark the baseline implementations")
    parser.add_argument("-m","--mode", choices=["py", "rs"], help="The mode to run the benchmark in", required=True)
    args = parser.parse_args()
    main(args.mode)
    
    
    