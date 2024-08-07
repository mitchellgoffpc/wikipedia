use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use bzip2::read::BzDecoder;
use indicatif::{ProgressBar, ProgressStyle};
use xml::reader::{EventReader, XmlEvent};
use html_escape::decode_html_entities;

pub const IGNORE: [&str; 7] = ["Category:", "Wikipedia:", "File:", "Template:", "Draft:", "Portal:", "Module:"];

struct ProgressReader<R: Read> { inner: R, progress_bar: ProgressBar }
impl<R: Read> ProgressReader<R> {
    fn new(inner: R, progress_bar: ProgressBar) -> Self { ProgressReader { inner, progress_bar }}
}
impl<R: Read> Read for ProgressReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let result = self.inner.read(buf);
        if let Ok(n) = result {
            self.progress_bar.inc(n as u64);
        }
        result
    }
}

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
    let file_size = file.metadata().expect("Unable to get file metadata").len();
    let progress_bar = create_progress_bar(file_size, "Extracting index...");
    let decoder = BzDecoder::new(ProgressReader::new(file, progress_bar));
    let reader = BufReader::new(decoder);

    let mut seek_position_map: HashMap<u64, Vec<(u32, String)>> = HashMap::new();
    for line in reader.lines().filter_map(Result::ok) {
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

    seek_position_map
}

pub fn load_chunk(file_path: &str, start_position: u64, end_position: u64) -> HashMap<u32, (String, String)> {  // id -> (title, content)
    let chunk_size = (end_position - start_position) as usize;
    let mut buffer = vec![0u8; chunk_size];
    let mut file = File::open(file_path).expect("Unable to open file");
    file.seek(SeekFrom::Start(start_position)).expect("Failed to seek to the position");
    file.read_exact(&mut buffer).expect("Error reading from the file");

    let mut decoder = BzDecoder::new(&buffer[..]);
    let mut decompressed_data = Vec::new();
    decoder.read_to_end(&mut decompressed_data).expect("Error during decompression");

    let xml_text = String::from_utf8(decompressed_data).expect("Failed to convert decompressed bytes to UTF-8");
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