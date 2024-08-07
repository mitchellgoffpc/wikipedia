use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use bzip2::read::BzDecoder;
use indicatif::{ProgressBar, ProgressIterator, ProgressStyle};
use xml::reader::{EventReader, XmlEvent};
use html_escape::decode_html_entities;

const IGNORE: [&str; 7] = ["Category:", "Wikipedia:", "File:", "Template:", "Draft:", "Portal:", "Module:"];

pub fn create_progress_bar(total: u64, message: &str) -> ProgressBar {
    let progress_bar = ProgressBar::new(total);
    progress_bar.set_style(ProgressStyle::default_bar()
        .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} ({percent}%) {msg}")
        .unwrap()
        .progress_chars("##-"));
    progress_bar.set_message(message.to_owned());
    progress_bar
}

pub fn load_index(file_path: &str) -> HashMap<u64, Vec<(u32, String)>> {
    let file = File::open(file_path).expect("Unable to open file");
    let decoder = BzDecoder::new(file);
    let reader = BufReader::new(decoder);
    let total_lines = reader.lines().count();

    let file = File::open(file_path).expect("Unable to open file");
    let decoder = BzDecoder::new(file);
    let reader = BufReader::new(decoder);

    let mut seek_position_map: HashMap<u64, Vec<(u32, String)>> = HashMap::new();
    for line in reader.lines().progress_with(create_progress_bar(total_lines as u64, "Extracting index...")) {
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

pub fn parse_chunk(xml_text: &str) -> HashMap<u32, (String, String)> {  // id -> (title, content)
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
                            articles.insert(current_id, (current_title.clone(), current_text.clone()));
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