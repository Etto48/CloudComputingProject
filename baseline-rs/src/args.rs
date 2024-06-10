#[derive(Debug, clap::Parser)]
#[clap(version, author, about = "A simple example of how to use clap.")]
pub struct Args {
    #[clap(short, long)]
    pub input: String,
    #[clap(short, long)]
    pub output: String,
}