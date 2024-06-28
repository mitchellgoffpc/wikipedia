mod index;
mod analyse;
use std::env;

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        println!("Usage: {} <command>", args[0]);
        println!("Available commands:");
        println!("  index    - Run the indexing process");
        println!("  analyse  - Run the analysis process");
        return;
    }

    match args[1].as_str() {
        "index" => {
            println!("Starting indexing process...");
            index::index();
            println!("Indexing process completed.");
        }
        "analyse" => {
            println!("Starting analysis process...");
            analyse::analyse();
            println!("Analysis process completed.");
        }
        _ => {
            println!("Unknown command: {}", args[1]);
            println!("Available commands:");
            println!("  index    - Run the indexing process");
            println!("  analyse  - Run the analysis process");
        }
    }
}
