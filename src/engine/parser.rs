use anyhow::Result;
use scraper::{Html, Selector};
use std::collections::HashMap;

use crate::types::{ExtractType, ScrapeResult, ScrapeStatus};

pub fn parse_html(
    html: &str,
    url: &str,
    job_id: &str,
    selectors: &[crate::types::Selector],
) -> ScrapeResult {
    let start = std::time::Instant::now();
    let document = Html::parse_document(html);

    let mut records = Vec::new();

    if let Some(first) = selectors.first() {
        if let Ok(css_sel) = Selector::parse(&first.css_selector) {
            for element in document.select(&css_sel) {
                let mut record = HashMap::new();
                for sel in selectors {
                    let value = extract_value(&element, sel);
                    record.insert(sel.name.clone(), value);
                }
                records.push(record);
            }
        }
    }

    let is_empty = records.is_empty();
    ScrapeResult {
        job_id: job_id.to_string(),
        url: url.to_string(),
        timestamp: chrono::Utc::now().naive_utc(),
        data: records,
        status: if is_empty {
            ScrapeStatus::Failed
        } else {
            ScrapeStatus::Success
        },
        error: if is_empty {
            Some("No elements matched the selectors".into())
        } else {
            None
        },
        duration_ms: start.elapsed().as_millis() as u64,
        bytes_fetched: html.len() as u64,
    }
}

fn extract_value(element: &scraper::ElementRef, sel: &crate::types::Selector) -> String {
    match &sel.extract {
        ExtractType::Text => element.text().collect::<Vec<_>>().join(" ").trim().to_string(),
        ExtractType::Html => element.inner_html(),
        ExtractType::Attribute(attr) => element
            .value()
            .attr(attr)
            .unwrap_or("")
            .to_string(),
        ExtractType::Link => {
            element
                .value()
                .attr("href")
                .unwrap_or("")
                .to_string()
        }
        ExtractType::Image => {
            element
                .value()
                .attr("src")
                .unwrap_or("")
                .to_string()
        }
    }
}

pub fn parse_json(json_str: &str) -> Result<serde_json::Value> {
    Ok(serde_json::from_str(json_str)?)
}

pub fn extract_json_value(
    value: &serde_json::Value,
    path: &str,
) -> Vec<HashMap<String, String>> {
    let mut results = Vec::new();

    let parts: Vec<&str> = path.split('/').collect();
    let mut current = value;

    for part in parts {
        match part {
            "*" => {
                if let Some(arr) = current.as_array() {
                    for item in arr {
                        let mut map = HashMap::new();
                        flatten_json(item, "", &mut map);
                        results.push(map);
                    }
                }
                return results;
            }
            key => {
                if let Some(obj) = current.as_object() {
                    current = obj.get(key).unwrap_or(&serde_json::Value::Null);
                }
            }
        }
    }

    let mut map = HashMap::new();
    flatten_json(current, "", &mut map);
    results.push(map);
    results
}

fn flatten_json(
    value: &serde_json::Value,
    prefix: &str,
    map: &mut HashMap<String, String>,
) {
    match value {
        serde_json::Value::Object(obj) => {
            for (k, v) in obj {
                let new_key = if prefix.is_empty() {
                    k.clone()
                } else {
                    format!("{}.{}", prefix, k)
                };
                flatten_json(v, &new_key, map);
            }
        }
        serde_json::Value::Array(arr) => {
            for (i, v) in arr.iter().enumerate() {
                let new_key = format!("{}[{}]", prefix, i);
                flatten_json(v, &new_key, map);
            }
        }
        serde_json::Value::String(s) => {
            map.insert(prefix.to_string(), s.clone());
        }
        serde_json::Value::Number(n) => {
            map.insert(prefix.to_string(), n.to_string());
        }
        serde_json::Value::Bool(b) => {
            map.insert(prefix.to_string(), b.to_string());
        }
        serde_json::Value::Null => {
            map.insert(prefix.to_string(), String::new());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Selector;

    #[test]
    fn test_parse_html() {
        let html = r#"<html><body><div class="item"><h2>Title 1</h2><p>Desc 1</p></div><div class="item"><h2>Title 2</h2><p>Desc 2</p></div></body></html>"#;
        let selectors = vec![
            Selector {
                name: "title".into(),
                css_selector: "h2".into(),
                attribute: None,
                extract: ExtractType::Text,
            },
        ];

        let result = parse_html(html, "http://test.com", "job1", &selectors);
        assert_eq!(result.status, ScrapeStatus::Success);
        assert_eq!(result.data.len(), 2);
        assert_eq!(result.data[0].get("title").unwrap(), "Title 1");
    }
}
