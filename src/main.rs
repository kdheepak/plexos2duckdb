mod cli;

use clap::Parser;
use owo_colors::OwoColorize;

use crate::cli::Cli;

fn main() {
    let cli = Cli::parse();
    if let Err(err) = cli::run(cli) {
        eprintln!("{} {}", "Error:".red().bold(), err);
        std::process::exit(1);
    }
}
