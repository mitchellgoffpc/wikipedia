use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufReader, Read};
use indicatif::{ProgressBar, ProgressIterator, ProgressStyle};

fn create_progress_bar(total: u64, message: &str) -> ProgressBar {
    let progress_bar = ProgressBar::new(total);
    progress_bar.set_style(ProgressStyle::default_bar()
        .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} ({percent}%) {msg}")
        .unwrap()
        .progress_chars("##-"));
    progress_bar.set_message(message.to_owned());
    progress_bar
}

pub fn analyse() {
    let file = File::open("links.bin").expect("Unable to open links.bin");
    let mut reader = BufReader::new(file);
    let mut buffer = Vec::new();
    reader.read_to_end(&mut buffer).expect("Unable to read links.bin");

    // Parse the binary data
    let progress_bar = create_progress_bar(buffer.len() as u64, "Parsing links.bin");
    let mut links: HashMap<u32, Vec<u32>> = HashMap::new();
    let mut titles: HashMap<u32, String> = HashMap::new();
    let mut i = 0;
    while i < buffer.len() {
        let article_id = u32::from_le_bytes(buffer[i..i+4].try_into().unwrap());
        let title_length = u32::from_le_bytes(buffer[i+4..i+8].try_into().unwrap()) as usize;
        let title = String::from_utf8_lossy(&buffer[i+8..i+8+title_length]).to_string();
        let link_count = u32::from_le_bytes(buffer[i+8+title_length..i+8+title_length+4].try_into().unwrap()) as usize;
        let article_links: Vec<u32> = (0..link_count)
            .map(|j| { u32::from_le_bytes(buffer[i+8+title_length+4+4*j..i+8+title_length+4+4*j+4].try_into().unwrap()) })
            .collect();
        let separator = u32::from_le_bytes(buffer[i+8+title_length+4+4*link_count..i+8+title_length+4+4*link_count+4].try_into().unwrap());
        assert_eq!(separator, u32::MAX, "Expected separator u32::MAX not found");

        i += 8 + title_length + 4 + 4 * link_count + 4;
        titles.insert(article_id, title.to_lowercase());
        links.insert(article_id, article_links);

        progress_bar.set_position(i as u64);
    }
    progress_bar.finish_and_clear();
    println!("Found {} articles", links.len());

    // Analyze the link structure
    let total_articles = links.len();
    let total_links: usize = links.values().map(|v| v.len()).sum();
    let articles_with_links = links.values().filter(|v| !v.is_empty()).count();

    let progress_bar = create_progress_bar(links.len() as u64, "Analyzing links");
    let mut unique_links = HashSet::<u32>::new();
    for links in links.values().progress_with(progress_bar) {
        unique_links.extend(links);
    }

    // Find articles with the most outgoing and incoming links
    let mut outgoing_links = links.iter().map(|(k, v)| (*k, v.len())).collect::<Vec<_>>();
    outgoing_links.sort_by_key(|&(_, count)| std::cmp::Reverse(count));

    let progress_bar = create_progress_bar(links.len() as u64, "Calculating incoming links");
    let mut incoming_links = HashMap::new();
    for (_, links) in links.iter().progress_with(progress_bar) {
        for &link in links {
            *incoming_links.entry(link).or_insert(0) += 1;
        }
    }
    let mut incoming_links = incoming_links.into_iter().collect::<Vec<_>>();
    incoming_links.sort_by_key(|&(_, count)| std::cmp::Reverse(count));

    // Print analysis results
    println!("Total articles: {}", total_articles);
    println!("Total links: {}", total_links);
    println!("Articles with outgoing links: {}", articles_with_links);
    println!("Unique link targets: {}", unique_links.len());
    println!("Average links per article: {:.2}", total_links as f64 / total_articles as f64);

    println!("\nTop 10 articles with most outgoing links:");
    for (article_id, link_count) in outgoing_links.iter().take(10) {
        println!("Article: {}, Outgoing links: {}", titles.get(article_id).unwrap_or(&format!("Unknown (ID: {})", article_id)), link_count);
    }

    println!("\nTop 10 articles with most incoming links:");
    for (article_id, link_count) in incoming_links.iter().take(10) {
        println!("Article: {}, Incoming links: {}", titles.get(article_id).unwrap_or(&format!("Unknown (ID: {})", article_id)), link_count);
    }
}