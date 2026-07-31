use anyhow::Result;
use csv::Writer;
use std::collections::HashMap;
use std::path::Path;

use crate::types::ScrapeResult;

pub fn export_to_csv(results: &[ScrapeResult], file_path: &str) -> Result<()> {
    let path = Path::new(file_path);
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }

    let mut wtr = Writer::from_path(path)?;

    let mut all_keys = Vec::new();
    for result in results {
        for record in &result.data {
            for key in record.keys() {
                if !all_keys.contains(key) {
                    all_keys.push(key.clone());
                }
            }
        }
    }
    all_keys.sort();

    if !all_keys.is_empty() {
        let header: Vec<&str> = all_keys.iter().map(|s| s.as_str()).collect();
        wtr.write_record(&header)?;
    }

    for result in results {
        for record in &result.data {
            let values: Vec<String> = all_keys
                .iter()
                .map(|k| record.get(k).cloned().unwrap_or_default())
                .collect();
            wtr.write_record(&values)?;
        }
    }

    wtr.flush()?;
    Ok(())
}

pub fn export_to_json(results: &[ScrapeResult], file_path: &str) -> Result<()> {
    let path = Path::new(file_path);
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }

    let all_data: Vec<&HashMap<String, String>> =
        results.iter().flat_map(|r| r.data.iter()).collect();

    let json = serde_json::to_string_pretty(&all_data)?;
    std::fs::write(path, json)?;
    Ok(())
}
