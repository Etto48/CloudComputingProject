import matplotlib.pyplot as plt
import pandas as pd
import argparse

def main(csv_file: str):
    df = pd.read_csv(csv_file, header=None, names=['letter', 'frequency', 'total'], delimiter='\t')
    df.sort_values('letter', inplace=True)
    df.plot(x='letter', y='frequency', kind='bar', legend=False)
    plt.show()
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Visualize results')
    parser.add_argument('csv_file', type=str, help='Path to the CSV file')
    args = parser.parse_args()
    main(args.csv_file)
