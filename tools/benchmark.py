import argparse
import subprocess
import psutil
import os
import time

commands: dict[tuple[str,str]] = {
        "py": ("baseline-py", "python baseline%s.py -i ../dataset/english.txt -o output.csv"),
        "rs": ("baseline-rs", "cargo run --release --bin baseline%s -- -i ../dataset/english.txt -o output.csv"),
    }

def main(mode: str, no_streaming: bool):

    command: tuple[str,str] = commands[mode]
    command = (command[0], command[1] % ("-no-streaming" if no_streaming else ""))
    
    os.chdir(command[0])
    
    process = subprocess.Popen(command[1], shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    process_info = psutil.Process(process.pid)
    
    if os.name == "nt":
        # on windows there is no execve so we need to find the correct child process
        while len(process_info.children()) == 0:
            time.sleep(0.1)
        process_info = process_info.children()[0]
        # with python we have to take the first child because we go like:
        # cmd.exe -> python
        # with rust we have to take the third child because we go like:
        # cmd.exe -> cargo -> cargo -> baseline[-no-streaming]
        if mode == "rs":
            while len(process_info.children()) == 0:
                time.sleep(0.1)
            process_info = process_info.children()[0]
            while len(process_info.children()) == 0:
                time.sleep(0.1)
            process_info = process_info.children()[0]
    
        assert (mode == "py" and "python" in process_info.name()) or \
            (mode == "rs" and "baseline" in process_info.name())
    else:
        # it may happen that the process will be a child of sh
        while "sh" == process_info.name():
            if len(process_info.children()) != 0:
                process_info = process_info.children()[0]
        # wait for the process to become the correct one
        while "cargo" == process_info.name():
            pass
            
    # in linux the process may have still the wrong name here so no assert
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
    
    
    