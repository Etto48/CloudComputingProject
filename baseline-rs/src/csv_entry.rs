use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct CsvEntry {
    pub character: char,
    pub frequency: f64,
    pub count: u64,
}