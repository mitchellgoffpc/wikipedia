use std::io::prelude::*;
use std::io::{BufRead, BufReader, Read, Write};
use std::fs::File;
use std::collections::HashMap;
use bzip2::read::BzDecoder;
use xml::reader::{EventReader, XmlEvent};
use std::sync::{Arc, Mutex};
use threadpool::ThreadPool;
use std::time::Instant;
use html_escape::decode_html_entities;

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
            let article_title = decode_html_entities(parts[2]).to_string();
            if IGNORE.iter().any(|prefix| article_title.starts_with(prefix)) { continue; }

            seek_position_map
                .entry(seek_position)
                .or_insert_with(Vec::new)
                .push((article_id, article_title));
        }
    }

    seek_position_map
}

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
                links.push(decoded_link);
            }
            start = link_end + 2;
        } else {
            break;
        }
    }
    links
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
                        if !IGNORE.iter().any(|prefix| current_title.starts_with(prefix)) {
                            articles.insert(current_id, (current_title.clone(), extract_links(&current_text)));
                        }
                        current_title.clear();
                        current_text.clear();
                        current_id = 0;
                        in_page = false;
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

fn process_chunk(articles_path: &str, start_position: u64, end_position: u64, article_titles_to_ids: &HashMap<String, u32>) -> (HashMap<u32, Vec<u32>>, usize, usize, usize) {
    let chunk_size = (end_position - start_position) as usize;
    let mut buffer = vec![0u8; chunk_size];
    let mut file = File::open(articles_path).expect("Unable to open articles file");
    file.seek(std::io::SeekFrom::Start(start_position))
        .expect("Failed to seek to the position");
    file.read_exact(&mut buffer)
        .expect("Error reading from the file");

    let mut decoder = BzDecoder::new(&buffer[..]);
    let mut decompressed_data = Vec::new();
    decoder.read_to_end(&mut decompressed_data)
           .expect("Error during decompression");

    let xml_text = String::from_utf8(decompressed_data)
        .expect("Failed to convert decompressed bytes to UTF-8");

    let articles = parse_chunk(&xml_text);
    let mut article_links = HashMap::new();
    let mut total_links = 0;
    let mut red_links = 0;

    for (article_id, (_, links)) in &articles {
        let mut link_ids = Vec::new();
        for link in links {
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


pub fn index() {
    let index_path = "data/enwiki-20240420-pages-articles-multistream-index.txt";
    let articles_path = "data/enwiki-20240420-pages-articles-multistream.xml.bz2";

    let start_time = Instant::now();
    let seek_position_map = load_index(index_path);
    println!("Total number of chunks: {}", seek_position_map.len());
    println!("Index extraction time: {:.3?}", start_time.elapsed());

    let start_time = Instant::now();
    let article_titles_to_ids: HashMap<String, u32> = seek_position_map
        .values()
        .flat_map(|articles| articles.iter().map(|(id, title)| (title.clone(), *id)))
        .collect();
    let article_ids_to_titles: HashMap<u32, String> = seek_position_map
        .values()
        .flat_map(|articles| articles.iter().map(|(id, title)| (*id, title.clone())))
        .collect();

    println!("Total articles: {}", article_titles_to_ids.len());
    println!("Title index creation time: {:.3?}", start_time.elapsed());

    let start_time = Instant::now();
    let mut positions: Vec<&u64> = seek_position_map.keys().collect();
    positions.sort_unstable();

    let num_threads = 8;
    let pool = ThreadPool::new(num_threads);
    let articles_path = Arc::new(articles_path.to_string());
    let total_articles = Arc::new(Mutex::new(0));
    let total_links = Arc::new(Mutex::new(0));
    let red_links = Arc::new(Mutex::new(0));
    let article_links = Arc::new(Mutex::new(HashMap::<u32, Vec<u32>>::new()));
    let article_titles_to_ids = Arc::new(article_titles_to_ids);

    for chunk_index in 0..100.min(positions.len() - 1) {
        let start_position = *positions[chunk_index];
        let end_position = *positions[chunk_index + 1];

        let total_articles = Arc::clone(&total_articles);
        let total_links = Arc::clone(&total_links);
        let red_links = Arc::clone(&red_links);
        let article_titles_to_ids = Arc::clone(&article_titles_to_ids);
        let article_links = Arc::clone(&article_links);
        let articles_path = Arc::clone(&articles_path);

        pool.execute(move || {
            let (chunk_article_links, chunk_article_count, chunk_total_links, chunk_red_links) =
                process_chunk(&articles_path, start_position, end_position, &article_titles_to_ids);

            *(total_articles.lock().unwrap()) += chunk_article_count;
            *(total_links.lock().unwrap()) += chunk_total_links;
            *(red_links.lock().unwrap()) += chunk_red_links;
            article_links.lock().unwrap().extend(chunk_article_links);
        })
    }

    pool.join();

    println!("Total articles extracted: {}", *total_articles.lock().unwrap());
    println!("Total links extracted: {}", *total_links.lock().unwrap());
    println!("Total red links: {}", *red_links.lock().unwrap());
    println!("Article extraction time: {:.3?}", start_time.elapsed());

    // Dump article links map
    let start_time = Instant::now();
    let article_links = article_links.lock().unwrap();
    let mut output_file = File::create("links.bin").expect("Failed to create output file");

    for (&article_id, link_ids) in article_links.iter() {
        output_file.write_all(&article_id.to_le_bytes()).expect("Failed to write article ID");

        let title = article_ids_to_titles.get(&article_id).expect("Article ID not found");
        let title_bytes = title.as_bytes();
        output_file.write_all(&(title_bytes.len() as u32).to_le_bytes()).expect("Failed to write title length");
        output_file.write_all(title_bytes).expect("Failed to write title");

        output_file.write_all(&(link_ids.len() as u32).to_le_bytes()).expect("Failed to write link count");
        for &link_id in link_ids {
            output_file.write_all(&link_id.to_le_bytes()).expect("Failed to write link ID");
        }

        output_file.write_all(&u32::MAX.to_le_bytes()).expect("Failed to write separator");
    }

    println!("Article links dump time: {:.3?}", start_time.elapsed());
}