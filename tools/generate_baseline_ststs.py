import matplotlib.pyplot as plt
import pandas as pd
import argparse

def main(input: str):
    py_df = pd.read_csv(f"{input}/baseline-py-memory.csv")
    py_ns_df = pd.read_csv(f"{input}/baseline-py-no-streaming-memory.csv")
    rs_df = pd.read_csv(f"{input}/baseline-rs-memory.csv")
    rs_ns_df = pd.read_csv(f"{input}/baseline-rs-no-streaming-memory.csv")
    
    py_df["Virtual memory"] = py_df["Virtual memory"] / 1024 / 1024 / 1024
    py_df["Physical memory"] = py_df["Physical memory"] / 1024 / 1024 / 1024
    
    py_ns_df["Virtual memory"] = py_ns_df["Virtual memory"] / 1024 / 1024 / 1024
    py_ns_df["Physical memory"] = py_ns_df["Physical memory"] / 1024 / 1024 / 1024
    
    rs_df["Virtual memory"] = rs_df["Virtual memory"] / 1024 / 1024 / 1024
    rs_df["Physical memory"] = rs_df["Physical memory"] / 1024 / 1024 / 1024
    
    rs_ns_df["Virtual memory"] = rs_ns_df["Virtual memory"] / 1024 / 1024 / 1024
    rs_ns_df["Physical memory"] = rs_ns_df["Physical memory"] / 1024 / 1024 / 1024
    
    fig, axes = plt.subplots(2,2)
    fig.tight_layout()
    ax = py_df.plot(kind="line", x="Time", grid=True, ax=axes[0,0], title="Python")
    ax.set_xlabel("Time (s)")
    ax.set_ylabel("Memory (GB)")
    ax = rs_df.plot(kind="line", x="Time", grid=True, ax=axes[0,1], title="Rust")
    ax.set_xlabel("Time (s)")
    ax.set_ylabel("Memory (GB)")
    ax = py_ns_df.plot(kind="line", x="Time", grid=True, ax=axes[1,0], title="Python without streaming")
    ax.set_xlabel("Time (s)")
    ax.set_ylabel("Memory (GB)")
    ax = rs_ns_df.plot(kind="line", x="Time", grid=True, ax=axes[1,1], title="Rust without streaming")
    ax.set_xlabel("Time (s)")
    ax.set_ylabel("Memory (GB)")
    plt.show()
    

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Generate baseline statistics from test logs')
    parser.add_argument("-i","--input", type=str, help="The input dir", required=True)
    args = parser.parse_args()
    main(args.input)
