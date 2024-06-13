import matplotlib.pyplot as plt
import pandas as pd
import argparse

def main(input: str):
    py_df = pd.read_csv(f"{input}/baseline-py-memory.csv")
    rs_df = pd.read_csv(f"{input}/baseline-rs-memory.csv")
    
    py_df["Virtual memory"] = py_df["Virtual memory"] / 1024 / 1024 / 1024
    py_df["Physical memory"] = py_df["Physical memory"] / 1024 / 1024 / 1024
    
    rs_df["Virtual memory"] = rs_df["Virtual memory"] / 1024 / 1024 / 1024
    rs_df["Physical memory"] = rs_df["Physical memory"] / 1024 / 1024 / 1024
    
    ax = py_df.plot(kind="line", x="Time", grid=True)
    plt.tight_layout()
    ax.set_xlabel("Time (s)")
    ax.set_ylabel("Memory (GB)")
    plt.show()
    ax = rs_df.plot(kind="line", x="Time", grid=True)
    plt.tight_layout()
    ax.set_xlabel("Time (s)")
    ax.set_ylabel("Memory (GB)")
    plt.show()
    

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Generate baseline statistics from test logs')
    parser.add_argument("-i","--input", type=str, help="The input dir", required=True)
    args = parser.parse_args()
    main(args.input)
