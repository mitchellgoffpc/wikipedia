mod index;
mod analyse;
mod helpers;
mod dump;

use std::env;
use std::path::Path;

fn print_commands() {
    println!("Available commands:");
    println!("  index    - Run the indexing process");
    println!("  analyse  - Run the analysis process");
    println!("  dump     - Dump articles into individual files");
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        println!("Usage: {} <command> <data_path>", args[0]);
        print_commands();
        return;
    }

    let command = &args[1];
    let data_path = Path::new(&args[2]);
    match command.as_str() {
        "index" => index::index(data_path),
        "analyse" => analyse::analyse(data_path),
        "dump" => dump::dump(data_path),
        _ => {
            println!("Unknown command: {}", command);
            print_commands();
        }
    }
}
