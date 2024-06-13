import argparse
from unidecode import unidecode
import pandas as pd
import time

def main(input_file: str, output_file: str):
    start = time.time()
    with open(input_file, "r", buffering=1024, encoding="utf-8", errors='ignore') as f:
        file = f.read()
        
    total = 0
    letter_dict = {}
    
    file = unidecode(file).lower()
    for letter in file:
        if letter.islower() and letter.isalpha():
            if letter in letter_dict:
                letter_dict[letter] += 1
            else:
                letter_dict[letter] = 1
            total += 1
    
    for letter in letter_dict:
        letter_dict[letter] = (letter, letter_dict[letter] / total, letter_dict[letter])
    
    
    
    df = pd.DataFrame(letter_dict.values(), columns=['letter', 'frequency', 'count']).sort_values(by='letter')
    df.to_csv(output_file, sep='\t', index=False, header=False)
    print(f"Done in {(time.time() - start)}s")
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Extract letter frequency from text')
    parser.add_argument('-i','--input', type=str, help='Path to the input file')
    parser.add_argument('-o','--output', type=str, help='Path to the output file')
    args = parser.parse_args()
    main(args.input, args.output)