use std::io::prelude::*;
use std::path::Path;
use std::fs::{File, create_dir_all};
use std::sync::{Arc, Mutex};
use threadpool::ThreadPool;
use crate::helpers::{create_progress_bar, load_index, load_chunk};

fn process_chunk(articles_path: &str, start_position: u64, end_position: u64, output_dir: &Path, chunk_index: usize) -> usize {
    let articles = load_chunk(articles_path, start_position, end_position);
    let file_name = format!("{:0>6}.txt", chunk_index);
    let file_path = output_dir.join(file_name);
    let mut file = File::create(file_path).expect("Failed to create chunk file");

    for (_, (title, content)) in &articles {
        write!(file, "{}\n{}\n\n", title, content).expect("Failed to write article");
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

    let output_dir = data_path.join("chunks");
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
    let progress_bar = Arc::new(create_progress_bar((positions.len()-1) as u64, "Dumping chunks..."));
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
            let chunk_article_count = process_chunk(&articles_path, start_position, end_position, &output_dir, chunk_index);
            *(total_articles.lock().unwrap()) += chunk_article_count;
            progress_bar.inc(1);
        })
    }

    pool.join();
    progress_bar.finish_and_clear();

    println!("Total articles dumped: {}", *total_articles.lock().unwrap());
}