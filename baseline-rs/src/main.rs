use baseline_rs::{args::Args, csv_entry::CsvEntry};
use clap::Parser;
use unidecode::unidecode;


const LETTER_COUNT: usize = 26;
fn main() {
    let start = std::time::Instant::now();
    let mut values = vec![0;LETTER_COUNT + 1];
    let args = Args::parse();
    
    let input = std::fs::read_to_string(args.input).unwrap();
    let input = input.to_lowercase();
    let input = unidecode(&input);
    for c in input.chars() {
        if c.is_ascii_alphabetic() {
            let index = c as usize - 'a' as usize;
            values[index] += 1;
            values[LETTER_COUNT] += 1;
        }
    }

    let mut output = Vec::new();
    for i in 0..LETTER_COUNT {
        let frequency = values[i] as f64 / values[LETTER_COUNT] as f64;
        output.push(CsvEntry {
            character: (i as u8 + 'a' as u8) as char,
            frequency,
            total: values[LETTER_COUNT],
        });
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
