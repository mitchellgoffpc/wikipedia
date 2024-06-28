use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufReader, Read};
use std::time::Instant;

pub fn analyse() {
    let start_time = Instant::now();

    // Read the links.bin file
    let file = File::open("links.bin").expect("Unable to open links.bin");
    let mut reader = BufReader::new(file);
    let mut buffer = Vec::new();
    reader.read_to_end(&mut buffer).expect("Unable to read links.bin");

    // Parse the binary data
    let mut links: HashMap<u32, Vec<u32>> = HashMap::new();
    let mut i = 0;
    while i < buffer.len() {
        let article_id = u32::from_le_bytes([buffer[i], buffer[i+1], buffer[i+2], buffer[i+3]]);
        i += 4;
        let mut article_links = Vec::new();
        loop {
            let link_id = u32::from_le_bytes([buffer[i], buffer[i+1], buffer[i+2], buffer[i+3]]);
            i += 4;
            if link_id == u32::MAX {
                break;
            }
            article_links.push(link_id);
        }
        links.insert(article_id, article_links);
    }

    println!("Parsing links.bin completed in {:.2?}", start_time.elapsed());

    // Analyze the link structure
    let total_articles = links.len();
    let total_links: usize = links.values().map(|v| v.len()).sum();
    let articles_with_links = links.values().filter(|v| !v.is_empty()).count();
    let mut unique_links = HashSet::<u32>::new();
    for links in links.values() {
        unique_links.extend(links);
    }

    // Find articles with the most outgoing and incoming links
    let mut outgoing_links = links.iter().map(|(k, v)| (*k, v.len())).collect::<Vec<_>>();
    outgoing_links.sort_by_key(|&(_, count)| std::cmp::Reverse(count));

    let mut incoming_links = HashMap::new();
    for (_, links) in &links {
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
        println!("Article ID: {}, Outgoing links: {}", article_id, link_count);
    }

    println!("\nTop 10 articles with most incoming links:");
    for (article_id, link_count) in incoming_links.iter().take(10) {
        println!("Article ID: {}, Incoming links: {}", article_id, link_count);
    }

    println!("\nAnalysis completed in {:.2?}", start_time.elapsed());
}