import pandas as pd
import argparse
import time

def extract_txt_from_csv(csv_path, txt_path):
    print(f"Extracting text from {csv_path} to {txt_path}")
    start = time.time()
    lines: list[str] = []
    with open(csv_path, "rb") as f:
        finished = False
        while not finished:
            line = ""
            while True:
                try:
                    c = f.read(1).decode("utf-8")
                    if c == "\n":
                        lines.append(line)
                        break
                    if c == "":
                        finished = True
                        break
                    else:
                        line += c
                except UnicodeDecodeError:
                    continue
                
            
        new_lines = []    
        for line in lines:
            start = True
            new_line = ""
            for c in line:
                if start and (c.isnumeric() or c.isspace()):
                    continue
                else:
                    start = False
                    new_line += c
            new_lines.append(new_line)
        lines = new_lines
                
    with open(txt_path, "w") as f:
        for i,text in enumerate(lines):
            print(f"\r\033[JWriting {i}/{len(lines)}", end="")
            f.write(text + "\n")
    print(f"\nDone in {(time.time() - start)}s")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", type=str, required=True)
    parser.add_argument("--txt", type=str, required=True)
    args = parser.parse_args()
    extract_txt_from_csv(args.csv, args.txt)