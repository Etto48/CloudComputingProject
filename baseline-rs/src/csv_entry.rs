use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct CsvEntry {
    pub character: char,
    pub frequency: f64,
    pub count: u64,
}

impl CsvEntry {
    pub fn new(c: char) -> CsvEntry {
        CsvEntry {
            character: c,
            frequency: 0.0,
            count: 0,
        }
    }
}