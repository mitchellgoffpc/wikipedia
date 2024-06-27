use std::io::prelude::*;
use std::io::{BufRead, BufReader, Read};
use std::fs::File;
use std::time::Instant;
use std::collections::HashMap;
use bzip2::read::BzDecoder;
use xml::reader::{EventReader, XmlEvent};

const IGNORE: [&str; 7] = ["Category:", "Wikipedia:", "File:", "Template:", "Draft:", "Portal:", "Module:"];

fn load_index(file_path: &str) -> HashMap<u64, Vec<(u32, String)>> {
    let file = File::open(file_path).expect("Unable to open file");
    let reader = BufReader::new(file);

    let mut seek_position_map: HashMap<u64, Vec<(u32, String)>> = HashMap::new();
    for line in reader.lines() {
        if let Ok(line) = line {
            let parts: Vec<&str> = line.splitn(3, ':').collect();
            if parts.len() != 3 { continue; }

            let seek_position = parts[0].parse::<u64>().unwrap();
            let article_id = parts[1].parse::<u32>().unwrap();
            let article_title = parts[2].to_string();
            if IGNORE.iter().any(|prefix| article_title.starts_with(prefix)) { continue; }

            seek_position_map
                .entry(seek_position)
                .or_insert_with(Vec::new)
                .push((article_id, article_title));
        }
    }

    seek_position_map
}

fn parse_chunk(xml_text: &str) -> (HashMap<String, String>, usize) {
    let parser = EventReader::new(xml_text.as_bytes());
    let mut articles = HashMap::new();
    let mut in_page = false;
    let mut in_title = false;
    let mut in_text = false;
    let mut current_title = String::new();
    let mut current_text = String::new();
    let mut page_count = 0;

    for event in parser {
        match event {
            Ok(XmlEvent::StartElement { name, .. }) => {
                match name.local_name.as_str() {
                    "page" => in_page = true,
                    "title" => in_title = true,
                    "text" => in_text = true,
                    _ => {}
                }
            }
            Ok(XmlEvent::EndElement { name, .. }) => {
                match name.local_name.as_str() {
                    "page" => {
                        in_page = false;
                        page_count += 1;
                        articles.insert(current_title.clone(), current_text.clone());
                        current_title.clear();
                        current_text.clear();
                    }
                    "title" => in_title = false,
                    "text" => in_text = false,
                    _ => {}
                }
            }
            Ok(XmlEvent::Characters(text)) => {
                if in_page {
                    if in_title {
                        current_title.push_str(&text);
                    } else if in_text {
                        current_text.push_str(&text);
                    }
                }
            }
            _ => {}
        }
    }

    (articles, page_count)
}

fn main() {
    let index_path = "data/enwiki-20240420-pages-articles-multistream-index.txt";
    let main_articles_path = "data/enwiki-20240420-pages-articles-multistream.xml.bz2";

    let seek_position_map = load_index(index_path);

    let mut positions: Vec<&u64> = seek_position_map.keys().collect();
    positions.sort_unstable();
    let first_position = positions[0];
    let next_position = positions.get(1).copied().unwrap_or(first_position);

    let mut buffer = vec![0u8; (next_position - first_position) as usize];
    let mut file = File::open(main_articles_path).expect("Unable to open main articles file");
    file.seek(std::io::SeekFrom::Start(*first_position))
        .expect("Failed to seek to the first position");
    file.read(&mut buffer)
        .expect("Error reading from the file");

    let mut decoder = BzDecoder::new(&buffer[..]);
    let mut decompressed_data = Vec::new();
    decoder.read_to_end(&mut decompressed_data)
           .expect("Error during decompression");

    match String::from_utf8(decompressed_data) {
        Ok(xml_text) => {
            let num_iterations = 10;
            let mut durations = Vec::with_capacity(num_iterations);

            for _ in 0..num_iterations {
                let start = Instant::now();
                let (articles, page_count) = parse_chunk(&xml_text);
                let duration = start.elapsed();
                durations.push(duration);
                println!("Iteration {} - Pages extracted: {}, Duration: {:?}", durations.len(), page_count, duration);
            }

            let total_duration: std::time::Duration = durations.iter().sum();
            let avg_duration = total_duration / durations.len() as u32;
            println!("Average duration over {} iterations: {:?}", num_iterations, avg_duration);
        }
        Err(e) => println!("Failed to convert decompressed bytes to UTF-8: {}", e),
    }
}