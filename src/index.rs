use std::io::prelude::*;
use std::io::{Read, Write};
use std::path::Path;
use std::fs::File;
use std::collections::HashMap;
use bzip2::read::BzDecoder;
use std::sync::{Arc, Mutex};
use threadpool::ThreadPool;
use indicatif::ProgressIterator;
use html_escape::decode_html_entities;
use crate::helpers::{create_progress_bar, load_index, parse_chunk};

const IGNORE: [&str; 7] = ["Category:", "Wikipedia:", "File:", "Template:", "Draft:", "Portal:", "Module:"];

fn extract_links(text: &str) -> Vec<String> {
    let mut links = Vec::new();
    let mut start = 0;
    while let Some(open_bracket) = text[start..].find("[[") {
        if let Some(close_bracket) = text[start + open_bracket + 2..].find("]]") {
            let link_start = start + open_bracket + 2;
            let link_end = start + open_bracket + 2 + close_bracket;
            let mut link = text[link_start..link_end].to_string();
            if link.contains('|') {
                link = link.split('|').collect::<Vec<_>>()[0].to_string();
            }
            if link.contains('#') {
                link = link.split('#').collect::<Vec<_>>()[0].to_string();
            }
            let decoded_link = decode_html_entities(&link).to_string();
            if !IGNORE.iter().any(|prefix| decoded_link.starts_with(prefix)) {
                links.push(decoded_link.to_lowercase());
            }
            start = link_end + 2;
        } else {
            break;
        }
    }
    links
}

fn process_chunk(articles_path: &str, start_position: u64, end_position: u64, article_titles_to_ids: &HashMap<String, u32>) -> (HashMap<u32, Vec<u32>>, usize, usize, usize) {
    let chunk_size = (end_position - start_position) as usize;
    let mut buffer = vec![0u8; chunk_size];
    let mut file = File::open(articles_path).expect("Unable to open articles file");
    file.seek(std::io::SeekFrom::Start(start_position)).expect("Failed to seek to the position");
    file.read_exact(&mut buffer).expect("Error reading from the file");

    let mut decoder = BzDecoder::new(&buffer[..]);
    let mut decompressed_data = Vec::new();
    decoder.read_to_end(&mut decompressed_data).expect("Error during decompression");

    let xml_text = String::from_utf8(decompressed_data).expect("Failed to convert decompressed bytes to UTF-8");
    let articles = parse_chunk(&xml_text);
    let mut article_links = HashMap::new();
    let mut total_links = 0;
    let mut red_links = 0;

    for (article_id, (_, content)) in &articles {
        let links = extract_links(&content);
        let mut link_ids = Vec::new();
        for link in &links {
            match article_titles_to_ids.get(link) {
                Some(&link_id) => link_ids.push(link_id),
                None => red_links += 1,
            }
        }
        article_links.insert(*article_id, link_ids);
        total_links += links.len();
    }

    (article_links, articles.len(), total_links, red_links)
}

fn get_article_byte_string(article_id: u32, title: &str, link_ids: &[u32]) -> Vec<u8> {
    let mut output_buffer = Vec::new();
    output_buffer.extend_from_slice(&article_id.to_le_bytes());

    let title_bytes = title.as_bytes();
    output_buffer.extend_from_slice(&(title_bytes.len() as u32).to_le_bytes());
    output_buffer.extend_from_slice(title_bytes);

    output_buffer.extend_from_slice(&(link_ids.len() as u32).to_le_bytes());
    for &link_id in link_ids {
        output_buffer.extend_from_slice(&link_id.to_le_bytes());
    }

    output_buffer.extend_from_slice(&u32::MAX.to_le_bytes());
    output_buffer
}


pub fn index(data_path: &Path) {
    let index_path = data_path.join("enwiki-20240801-pages-articles-multistream-index.txt.bz2");
    let articles_path = data_path.join("enwiki-20240801-pages-articles-multistream.xml.bz2");
    if !index_path.exists() || !articles_path.exists() {
        eprintln!("Error: Unable to locate data files in {}", data_path.to_str().unwrap());
        std::process::exit(1);
    }

    let seek_position_map = load_index(index_path.to_str().unwrap());
    println!("Total number of chunks: {}", seek_position_map.len());

    let article_titles_to_ids: HashMap<String, u32> = seek_position_map
        .values()
        .progress_with(create_progress_bar(seek_position_map.len() as u64, "Creating title index..."))
        .flat_map(|articles| articles.iter().map(|(id, title)| (title.to_lowercase(), *id)))
        .collect();
    let article_ids_to_titles: HashMap<u32, String> = seek_position_map
        .values()
        .progress_with(create_progress_bar(seek_position_map.len() as u64, "Creating id index..."))
        .flat_map(|articles| articles.iter().map(|(id, title)| (*id, title.clone())))
        .collect();
    println!("Total articles: {}", article_titles_to_ids.len());

    let mut positions: Vec<&u64> = seek_position_map.keys().collect();
    let file = File::open(&articles_path).expect("Unable to open articles file");
    let file_size = file.metadata().expect("Failed to get file metadata").len();
    positions.push(&file_size);
    positions.sort_unstable();

    let num_threads = 8;
    let pool = ThreadPool::new(num_threads);
    let articles_path = Arc::new(articles_path.to_str().unwrap().to_string());
    let total_articles = Arc::new(Mutex::new(0));
    let total_links = Arc::new(Mutex::new(0));
    let red_links = Arc::new(Mutex::new(0));
    let article_titles_to_ids = Arc::new(article_titles_to_ids);
    let article_ids_to_titles = Arc::new(article_ids_to_titles);
    let progress_bar = Arc::new(create_progress_bar((positions.len()-1) as u64, "Extracting articles..."));
    let output_file = Arc::new(Mutex::new(File::create(data_path.join("links.bin")).expect("Failed to create output file")));

    // Process chunks in using the thread pool
    for chunk_index in 0..positions.len()-1 {
        let start_position = *positions[chunk_index];
        let end_position = *positions[chunk_index + 1];

        let total_articles = Arc::clone(&total_articles);
        let total_links = Arc::clone(&total_links);
        let red_links = Arc::clone(&red_links);
        let article_titles_to_ids = Arc::clone(&article_titles_to_ids);
        let article_ids_to_titles = Arc::clone(&article_ids_to_titles);
        let articles_path = Arc::clone(&articles_path);
        let progress_bar = Arc::clone(&progress_bar);
        let output_file = Arc::clone(&output_file);

        pool.execute(move || {
            let (chunk_article_links, chunk_article_count, chunk_total_links, chunk_red_links) =
                process_chunk(&articles_path, start_position, end_position, &article_titles_to_ids);

            *(total_articles.lock().unwrap()) += chunk_article_count;
            *(total_links.lock().unwrap()) += chunk_total_links;
            *(red_links.lock().unwrap()) += chunk_red_links;

            let mut output_file = output_file.lock().unwrap();
            for (&article_id, link_ids) in chunk_article_links.iter() {
                let title = article_ids_to_titles.get(&article_id).expect("Article ID not found");
                let output_buffer = get_article_byte_string(article_id, title, link_ids);
                output_file.write_all(&output_buffer).expect("Failed to write to output file");
            }

            progress_bar.inc(1);
        })
    }

    pool.join();
    progress_bar.finish_and_clear();

    println!("Total articles extracted: {}", *total_articles.lock().unwrap());
    println!("Total links extracted: {}", *total_links.lock().unwrap());
    println!("Total red links: {}", *red_links.lock().unwrap());
}