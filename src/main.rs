mod index;
mod analyse;
use std::env;
use std::path::Path;

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() < 3 {
        println!("Usage: {} <command> <data_path>", args[0]);
        println!("Available commands:");
        println!("  index    - Run the indexing process");
        println!("  analyse  - Run the analysis process");
        return;
    }

    let command = &args[1];
    let data_path = Path::new(&args[2]);

    match command.as_str() {
        "index" => index::index(data_path),
        "analyse" => analyse::analyse(data_path),
        _ => {
            println!("Unknown command: {}", command);
            println!("Available commands:");
            println!("  index    - Run the indexing process");
            println!("  analyse  - Run the analysis process");
        }
    }
}
