import matplotlib.pyplot as plt
import pandas as pd
import argparse

def main(csv_file: str):
    df = pd.read_csv(csv_file, header=None, names=['letter', 'frequency', 'total'], delimiter='\t')
    df.sort_values('letter', inplace=True)
    df['letter'] = df['letter'].str.upper()
    ax = df.plot(x='letter', y='frequency', kind='bar', legend=False)
    
    plt.grid(axis='y', linestyle='--')
    plt.ylabel('Frequency')
    plt.xlabel('Letters')
    plt.xticks(rotation=0)
    
    plt.show()
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Visualize results')
    parser.add_argument('csv_file', type=str, help='Path to the CSV file')
    args = parser.parse_args()
    main(args.csv_file)
