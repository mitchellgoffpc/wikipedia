use std::io::prelude::*;
use std::path::Path;
use std::fs::{File, create_dir_all};
use std::sync::{Arc, Mutex};
use regex::Regex;
use threadpool::ThreadPool;
use crate::helpers::{create_progress_bar, load_index, load_chunk};

fn find_next_bracket(text: &str, start: usize) -> Option<(usize, usize, char)> {
    let mut bracket_count = 0;
    let mut opening_pos = 0;
    let mut byte_offset = start;
    let mut prev_char = ' ';
    let mut prev_prev_char = ' ';
    let mut bracket_type = ' ';

    for c in text[start..].chars() {
        match c {
            '[' | '{' if bracket_type == ' ' && prev_char == c => {
                opening_pos = byte_offset - 1;
                bracket_count = 2;
                bracket_type = c;
            },
            '|' if bracket_type == ' ' && prev_char == '{' => {
                opening_pos = byte_offset - 1;
                bracket_count = 1;
                bracket_type = '|';
            },
            '[' | '{' if bracket_type != ' ' && prev_char == c => {
                bracket_count += if prev_prev_char != c { 2 } else { 1 };
            },
            ']' | '}' if bracket_type != ' ' && (prev_char == c || (bracket_type == '|' && prev_char == '|'))  => {
                bracket_count -= if prev_char != '|' && prev_prev_char != c { 2 } else { 1 };
                if bracket_count == 0 {
                    return Some((opening_pos, byte_offset + 1, bracket_type));
                }
            }
            _ => {}
        }
        byte_offset += c.len_utf8();
        prev_prev_char = prev_char;
        prev_char = c;
    }

    None
}

fn extract_link_text(text: &str) -> String {
    let content = text.trim().trim_start_matches("[[").trim_end_matches("]]");
    if content.starts_with("File:") || content.starts_with("Category:") || content.starts_with("Image:") {
        return "".to_string();
    }
    let inner_text = match content.find('|') {
        Some(index) => content[index + 1..].to_string(),
        None => content.to_string(),
    };
    expand_templates(&inner_text)
}

fn extract_template_text(text: &str) -> String {
    let content = text.trim().trim_start_matches("{{").trim_end_matches("}}");
    let parts: Vec<&str> = content.splitn(2, '|').collect();

    let inner_text = match parts[0].to_lowercase().trim() {
        _ if parts.len() == 1 => "".to_string(),
        "lang" => match parts[1].find('|') {
            Some(i) => parts[1][i + 1..].to_string(),
            None => "".to_string()
        },
        "blockquote" => parts[1..].join("|"),
        _ => "".to_string(),
    };
    expand_templates(&inner_text)
}

fn expand_templates(content: &str) -> String {
    let mut processed_content = String::with_capacity(content.len());
    let mut position = 0;

    while let Some((mut start, mut end, bracket_type)) = find_next_bracket(content, position) {
        let snippet = &content[start..end];
        let processed_text = match bracket_type {
            '[' => extract_link_text(snippet),
            '{' => extract_template_text(snippet),
            _ => "".to_string(),
        };

        // If expanding the template results in an empty bullet point, remove the entire line
        if processed_text.is_empty() && start >= 3 && end < content.len() && &content.as_bytes()[start-3..start] == "\n* ".as_bytes() && content.as_bytes()[end] == b'\n' {
            start -= 2;
        }

        // If expanding the template results in an empty line, remove all trailing newlines
        if processed_text.is_empty() && start > 0 && end < content.len() && content.as_bytes()[start - 1] == b'\n' && content.as_bytes()[end] == b'\n' {
            for &byte in content.as_bytes()[end..].iter() {
                match byte {
                    b'\n' => end += 1,
                    _ => break,
                }
            }
        }

        processed_content.push_str(&content[position..start]);
        processed_content.push_str(&processed_text);
        position = end;
    }

    processed_content.push_str(&content[position..]);
    processed_content
}

fn process_chunk(articles_path: &str, start_position: u64, end_position: u64, output_dir: &Path, chunk_index: usize) -> usize {
    let html_comment_regex = Regex::new(r"(?s)<!--.*?-->").unwrap();
    let articles = load_chunk(articles_path, start_position, end_position);
    let file_name = format!("{:0>6}.txt", chunk_index);
    let file_path = output_dir.join(file_name);
    let mut file = File::create(file_path).expect("Failed to create chunk file");

    for (_, title, content) in &articles {
        if content.starts_with("#REDIRECT") { continue; }
        let content = html_comment_regex.replace_all(content, "");
        let content = expand_templates(&content);
        writeln!(file, "= {} =", title).expect("Failed to write article title");
        writeln!(file, "{}", content).expect("Failed to write processed content");
        writeln!(file).expect("Failed to write newline");
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
    let progress_bar = Arc::new(create_progress_bar((positions.len()-1) as u64, "Dumping chunks"));
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