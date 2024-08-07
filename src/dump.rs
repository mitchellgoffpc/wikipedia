use std::io::prelude::*;
use std::io::{Read, Write};
use std::path::Path;
use std::fs::{File, create_dir_all};
use bzip2::read::BzDecoder;
use std::sync::{Arc, Mutex};
use threadpool::ThreadPool;
use crate::helpers::{create_progress_bar, load_index, parse_chunk};

fn process_chunk(articles_path: &str, start_position: u64, end_position: u64, output_dir: &Path) -> usize {
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
    for (article_id, (title, content)) in &articles {
        let file_name = format!("{}.txt", article_id);
        let file_path = output_dir.join(file_name);
        let mut file = File::create(file_path).expect("Failed to create article file");
        writeln!(file, "{}", title).expect("Failed to write title");
        writeln!(file).expect("Failed to write newline");
        write!(file, "{}", content).expect("Failed to write content");
    }

    articles.len()
}

pub fn dump(data_path: &Path) {
    let index_path = data_path.join("enwiki-20240801-pages-articles-multistream-index.txt.bz2");
    let articles_path = data_path.join("enwiki-20240801-pages-articles-multistream.xml.bz2");
    if !index_path.exists() || !articles_path.exists() {
        eprintln!("Error: Unable to locate data files in {}", data_path.to_str().unwrap());
        std::process::exit(1);
    }

    let output_dir = data_path.join("articles");
    create_dir_all(&output_dir).expect("Failed to create output directory");

    let seek_position_map = load_index(index_path.to_str().unwrap());
    println!("Total number of chunks: {}", seek_position_map.len());

    let mut positions: Vec<&u64> = seek_position_map.keys().collect();
    let file = File::open(&articles_path).expect("Unable to open articles file");
    let file_size = file.metadata().expect("Failed to get file metadata").len();
    positions.push(&file_size);
    positions.sort_unstable();

    let num_threads = 8;
    let pool = ThreadPool::new(num_threads);
    let articles_path = Arc::new(articles_path.to_str().unwrap().to_string());
    let total_articles = Arc::new(Mutex::new(0));
    let progress_bar = Arc::new(create_progress_bar((positions.len()-1) as u64, "Dumping articles..."));
    let output_dir = Arc::new(output_dir);

    // Process chunks using the thread pool
    for chunk_index in 0..positions.len()-1 {
        let start_position = *positions[chunk_index];
        let end_position = *positions[chunk_index + 1];

        let total_articles = Arc::clone(&total_articles);
        let articles_path = Arc::clone(&articles_path);
        let progress_bar = Arc::clone(&progress_bar);
        let output_dir = Arc::clone(&output_dir);

        pool.execute(move || {
            let chunk_article_count = process_chunk(&articles_path, start_position, end_position, &output_dir);
            *(total_articles.lock().unwrap()) += chunk_article_count;
            progress_bar.inc(1);
        })
    }

    pool.join();
    progress_bar.finish_and_clear();

    println!("Total articles dumped: {}", *total_articles.lock().unwrap());
}