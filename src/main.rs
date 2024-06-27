use std::io::prelude::*;
use std::io::{BufRead, BufReader, Read};
use std::fs::File;
use std::collections::HashMap;
use bzip2::read::BzDecoder;
use xml::reader::{EventReader, XmlEvent};
use std::sync::{Arc, Mutex};
use threadpool::ThreadPool;
use std::time::Instant;

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

fn parse_chunk(xml_text: &str) -> HashMap<u32, (String, Vec<String>)> {
    let parser = EventReader::new(xml_text.as_bytes());
    let mut articles = HashMap::new();
    let mut in_page = false;
    let mut in_title = false;
    let mut in_text = false;
    let mut in_id = false;
    let mut current_title = String::new();
    let mut current_text = String::new();
    let mut current_id = 0;

    for event in parser {
        match event {
            Ok(XmlEvent::StartElement { name, .. }) => {
                match name.local_name.as_str() {
                    "page" => in_page = true,
                    "title" => in_title = true,
                    "text" => in_text = true,
                    "id" if in_page && current_id == 0 => in_id = true,
                    _ => {}
                }
            }
            Ok(XmlEvent::EndElement { name, .. }) => {
                match name.local_name.as_str() {
                    "page" => {
                        in_page = false;
                        let links = extract_links(&current_text);
                        articles.insert(current_id, (current_title.clone(), links));
                        current_title.clear();
                        current_text.clear();
                        current_id = 0;
                    }
                    "title" => in_title = false,
                    "text" => in_text = false,
                    "id" => in_id = false,
                    _ => {}
                }
            }
            Ok(XmlEvent::Characters(text)) => {
                if in_page {
                    if in_title {
                        current_title.push_str(&text);
                    } else if in_text {
                        current_text.push_str(&text);
                    } else if in_id {
                        current_id = text.parse().unwrap_or(0);
                    }
                }
            }
            _ => {}
        }
    }

    articles
}

fn extract_links(text: &str) -> Vec<String> {
    let mut links = Vec::new();
    let mut start = 0;
    while let Some(open_bracket) = text[start..].find("[[") {
        if let Some(close_bracket) = text[start + open_bracket + 2..].find("]]") {
            let link_start = start + open_bracket + 2;
            let link_end = start + open_bracket + 2 + close_bracket;
            let link = text[link_start..link_end].to_string();
            if !link.contains('|') {
                links.push(link);
            } else {
                let parts: Vec<&str> = link.split('|').collect();
                links.push(parts[0].to_string());
            }
            start = link_end + 2;
        } else {
            break;
        }
    }
    links
}

fn main() {
    let index_path = "data/enwiki-20240420-pages-articles-multistream-index.txt";
    let main_articles_path = "data/enwiki-20240420-pages-articles-multistream.xml.bz2";

    let start_time = Instant::now();
    let seek_position_map = load_index(index_path);
    println!("Total number of chunks: {}", seek_position_map.len());
    println!("Index extraction time: {:.3?}", start_time.elapsed());

    let start_time = Instant::now();
    let mut positions: Vec<&u64> = seek_position_map.keys().collect();
    positions.sort_unstable();

    let num_threads = 4;
    let pool = ThreadPool::new(num_threads);
    let total_articles = Arc::new(Mutex::new(0));
    let total_links = Arc::new(Mutex::new(0));

    for chunk_index in 0..100.min(positions.len() - 1) {
        let start_position = *positions[chunk_index];
        let end_position = *positions[chunk_index + 1];
        let chunk_size = (end_position - start_position) as usize;

        let mut buffer = vec![0u8; chunk_size];
        let mut file = File::open(main_articles_path).expect("Unable to open main articles file");
        file.seek(std::io::SeekFrom::Start(start_position))
            .expect("Failed to seek to the position");
        file.read_exact(&mut buffer)
            .expect("Error reading from the file");

        let total_articles_clone = Arc::clone(&total_articles);
        let total_links_clone = Arc::clone(&total_links);

        pool.execute(move || {
            let mut decoder = BzDecoder::new(&buffer[..]);
            let mut decompressed_data = Vec::new();
            decoder.read_to_end(&mut decompressed_data)
                   .expect("Error during decompression");

            match String::from_utf8(decompressed_data) {
                Ok(xml_text) => {
                    let articles = parse_chunk(&xml_text);
                    let mut total_articles = total_articles_clone.lock().unwrap();
                    let mut total_links = total_links_clone.lock().unwrap();
                    *total_articles += articles.len();
                    for (_, (_, links)) in articles {
                        *total_links += links.len();
                    }
                }
                Err(e) => println!("Failed to convert decompressed bytes to UTF-8: {}", e),
            }
        });
    }

    pool.join();

    println!("Total articles extracted: {}", *total_articles.lock().unwrap());
    println!("Total links extracted: {}", *total_links.lock().unwrap());
    println!("Article extraction time: {:.3?}", start_time.elapsed());
}