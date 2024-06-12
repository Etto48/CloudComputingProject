import argparse
import os

def main(input_path: str, output_dir: str, size_increment_mb: int):
    with open(input_path, "r") as f:
        lines = f.readlines()

    target = size_increment_mb
    current_size = 0
    os.makedirs(output_dir, exist_ok=True)
    for i in range(0, len(lines)):
        if i % 100 == 0:
            print(f"\r\033[JProcessing line {i/len(lines)*100:0.1f}%", end="")
        size = len(lines[i])
        current_size += size
        if current_size >= target * 1024 * 1024:
            with open(f"{output_dir}/part_{current_size//(1024*1024)}MB.txt", "w") as f:
                f.writelines(lines[:i])
            target += size_increment_mb
    print("\nDone!")
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Split a dataset into parts")
    parser.add_argument("-i","--input", type=str, help="Path to the input txt file", required=True)
    parser.add_argument("-o","--output", type=str, help="Path to the output directory", required=True)
    parser.add_argument("-s","--size-increment", type=int, help="Size increment in MB", default=100)
    args = parser.parse_args()
    main(args.input, args.output, args.size_increment)