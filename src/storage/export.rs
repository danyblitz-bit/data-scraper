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
                .map(|k| sanitize_cell(record.get(k).cloned().unwrap_or_default()))
                .collect();
            wtr.write_record(&values)?;
        }
    }

    wtr.flush()?;
    Ok(())
}

// ponytail: cells from web pages are untrusted; a leading =, +, -, @ would be
// executed as a formula by Excel (CSV injection), so they get a ' prefix
fn sanitize_cell(value: String) -> String {
    match value.chars().next() {
        Some('=') | Some('+') | Some('-') | Some('@') => format!("'{}", value),
        _ => value,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ScrapeResult, ScrapeStatus};

    fn result_with(values: &[(&str, &str)]) -> ScrapeResult {
        let mut record = HashMap::new();
        for (k, v) in values {
            record.insert(k.to_string(), v.to_string());
        }
        ScrapeResult {
            id: 1,
            job_id: "j1".into(),
            url: "http://x".into(),
            timestamp: chrono::NaiveDateTime::default(),
            data: vec![record],
            status: ScrapeStatus::Success,
            error: None,
            duration_ms: 0,
            bytes_fetched: 0,
            record_count: 1,
        }
    }

    #[test]
    fn test_csv_cells_sanitized_against_formula_injection() {
        let tmp = std::env::temp_dir().join(format!("ds_csv_{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&tmp).unwrap();
        let path = tmp.join("out.csv").to_str().unwrap().to_string();
        let result = result_with(&[
            ("cmd", "=cmd|' /C calc'!A0"),
            ("plus", "+1+1"),
            ("minus", "-2+3"),
            ("at", "@SUM(1+1)"),
            ("safe", "hello"),
        ]);
        export_to_csv(&[result], &path).unwrap();
        let content = std::fs::read_to_string(&path).unwrap();
        assert!(content.contains("'=cmd"), "{}", content);
        assert!(content.contains("'+1+1"), "{}", content);
        assert!(content.contains("'-2+3"), "{}", content);
        assert!(content.contains("'@SUM(1+1)"), "{}", content);
        assert!(content.contains(",hello"), "{}", content);
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn test_export_to_json_roundtrip() {
        let tmp = std::env::temp_dir().join(format!("ds_json_{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&tmp).unwrap();
        let path = tmp.join("out.json").to_str().unwrap().to_string();
        let result = result_with(&[("name", "alice"), ("score", "42")]);
        export_to_json(&[result], &path).unwrap();
        let content = std::fs::read_to_string(&path).unwrap();
        let parsed: Vec<serde_json::Value> = serde_json::from_str(&content).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0]["name"], "alice");
        assert_eq!(parsed[0]["score"], "42");
        let _ = std::fs::remove_dir_all(&tmp);
    }
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
