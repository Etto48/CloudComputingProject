import argparse
import subprocess
import psutil
import os
import time

commands: dict[tuple[str,str]] = {
        "py": ("baseline-py", "python baseline%s.py -i ../dataset/english.txt -o output.csv"),
        "rs": ("baseline-rs", "cargo run --release --bin baseline%s -- -i ../dataset/english.txt -o output.csv"),
    }

def find_child_with_name_like(process: psutil.Process, name: str) -> psutil.Process:
    for child in process.children():
        if name in child.name():
            return child
    else:
        for child in process.children():
            result = find_child_with_name_like(child, name)
            if result is not None:
                return result
    return None

def main(mode: str, no_streaming: bool):

    command: tuple[str,str] = commands[mode]
    command = (command[0], command[1] % ("-no-streaming" if no_streaming else ""))
    
    os.chdir(command[0])
    process = subprocess.Popen(command[1].split(), shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    process_info = psutil.Process(process.pid)
    time.sleep(1)
    process_info = find_child_with_name_like(process_info, "baseline" if mode == "rs" else "python")
    
    start = time.time()
    print("Time,Virtual memory,Physical memory")
    while process.poll() is None:
        try:    
            memory_info = process_info.memory_info()
            vm = memory_info.vms
            pm = memory_info.rss
            
            print(f"{(time.time() - start):0.2f},{vm},{pm}")
            time.sleep(0.1)
        except psutil.NoSuchProcess:
            break
        
    assert process.wait() == 0
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Benchmark the baseline implementations")
    parser.add_argument("-m","--mode", choices=["py", "rs"], help="The mode to run the benchmark in", required=True)
    parser.add_argument("-n","--no-streaming", action="store_true", help="Run the no-streaming version")
    args = parser.parse_args()
    main(args.mode, args.no_streaming)
    
    
    