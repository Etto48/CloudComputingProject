use baseline_rs::{args::Args, csv_entry::CsvEntry};
use clap::Parser;
use unidecode::unidecode;


const LETTER_COUNT: usize = 26;
fn main() {
    let start = std::time::Instant::now();
    let mut values = [0;LETTER_COUNT + 1];
    let mut output = [
        CsvEntry::new('a'), CsvEntry::new('b'), CsvEntry::new('c'), CsvEntry::new('d'),
        CsvEntry::new('e'), CsvEntry::new('f'), CsvEntry::new('g'), CsvEntry::new('h'),
        CsvEntry::new('i'), CsvEntry::new('j'), CsvEntry::new('k'), CsvEntry::new('l'),
        CsvEntry::new('m'), CsvEntry::new('n'), CsvEntry::new('o'), CsvEntry::new('p'),
        CsvEntry::new('q'), CsvEntry::new('r'), CsvEntry::new('s'), CsvEntry::new('t'),
        CsvEntry::new('u'), CsvEntry::new('v'), CsvEntry::new('w'), CsvEntry::new('x'),
        CsvEntry::new('y'), CsvEntry::new('z')
    ];

    let args = Args::parse();
    
    let input = std::fs::read_to_string(&args.input).unwrap();
    let input = unidecode(&input);
    let input = input.to_lowercase();
    for c in input.chars() {
        if c.is_ascii_lowercase() {
            let index = c as usize - 'a' as usize;
            values[index] += 1;
            values[LETTER_COUNT] += 1;
        }
    }   

    for i in 0..LETTER_COUNT {
        let frequency = values[i] as f64 / values[LETTER_COUNT] as f64;
        output[i].frequency = frequency;
    }
    let output_file = std::fs::File::create(args.output).unwrap();
    let mut writer = csv::WriterBuilder::new()
        .delimiter(b'\t')
        .has_headers(false)
        .from_writer(output_file);
    for entry in output {
        writer.serialize(entry).unwrap();
    }
    writer.flush().unwrap();
    println!("Elapsed: {:?}", start.elapsed());
}
