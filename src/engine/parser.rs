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
                for (i, sel) in selectors.iter().enumerate() {
                    if i == 0 {
                        record.insert(sel.name.clone(), extract_value(&element, sel, url));
                        continue;
                    }
                    // ponytail: field selector matches first element inside the row
                    let value = match Selector::parse(&sel.css_selector) {
                        Ok(field_sel) => element
                            .select(&field_sel)
                            .next()
                            .map(|f| extract_value(&f, sel, url))
                            .unwrap_or_default(),
                        Err(_) => String::new(),
                    };
                    record.insert(sel.name.clone(), value);
                }
                records.push(record);
            }
        }
    }

    let is_empty = records.is_empty();
    let record_count = records.len();
    ScrapeResult {
        id: 0,
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
        record_count,
    }
}

pub fn find_next_link(html: &str, base_url: &str, css_selector: &str) -> Option<String> {
    let document = Html::parse_document(html);
    let sel = Selector::parse(css_selector).ok()?;
    let href = document.select(&sel).next()?.value().attr("href")?;
    let base = url::Url::parse(base_url).ok()?;
    base.join(href).ok().map(|u| u.to_string())
}

fn extract_value(
    element: &scraper::ElementRef,
    sel: &crate::types::Selector,
    base_url: &str,
) -> String {
    match &sel.extract {
        ExtractType::Text => element
            .text()
            .collect::<Vec<_>>()
            .join(" ")
            .trim()
            .to_string(),
        ExtractType::Html => element.inner_html(),
        ExtractType::Attribute(attr) => element.value().attr(attr).unwrap_or("").to_string(),
        ExtractType::Link => resolve_url(base_url, element.value().attr("href").unwrap_or("")),
        ExtractType::Image => resolve_url(base_url, element.value().attr("src").unwrap_or("")),
    }
}

fn resolve_url(base: &str, value: &str) -> String {
    if value.is_empty() {
        return String::new();
    }
    url::Url::parse(base)
        .ok()
        .and_then(|u| u.join(value).ok())
        .map(|u| u.to_string())
        .unwrap_or_else(|| value.to_string())
}

pub fn parse_json(json_str: &str) -> Result<serde_json::Value> {
    Ok(serde_json::from_str(json_str)?)
}

pub fn decode_body(bytes: &[u8], content_type: &str) -> String {
    let (bom_enc, bom_len) =
        encoding_rs::Encoding::for_bom(bytes).unwrap_or((encoding_rs::UTF_8, 0));
    let enc = if bom_len > 0 {
        bom_enc
    } else {
        content_type_charset(content_type)
            .or_else(|| {
                if content_type.to_lowercase().contains("html") {
                    sniff_meta_charset(bytes)
                } else {
                    None
                }
            })
            .and_then(|l| encoding_rs::Encoding::for_label(l.as_bytes()))
            .unwrap_or(encoding_rs::UTF_8)
    };
    enc.decode_with_bom_removal(bytes).0.into_owned()
}

fn content_type_charset(ct: &str) -> Option<String> {
    for part in ct.split(';').skip(1) {
        if let Some(v) = part.trim().strip_prefix("charset=") {
            return Some(v.trim().trim_matches('"').to_string());
        }
    }
    None
}

// ponytail: first 4KB scan for <meta charset>, like browsers do
fn sniff_meta_charset(bytes: &[u8]) -> Option<String> {
    let head = String::from_utf8_lossy(&bytes[..bytes.len().min(4096)]);
    let lower = head.to_lowercase();
    let idx = lower.find("charset=")?;
    let value: String = head[idx + 8..]
        .chars()
        .skip_while(|c| *c == '"' || *c == '\'')
        .take_while(|c| c.is_alphanumeric() || *c == '-' || *c == '_')
        .collect();
    if value.is_empty() { None } else { Some(value) }
}

pub fn extract_json_string(value: &serde_json::Value, path: &str) -> Option<String> {
    let parts: Vec<&str> = path.split('/').filter(|p| !p.is_empty()).collect();
    let mut current = value;
    for part in parts {
        match part {
            "*" => {
                current = current.as_array()?.first()?;
            }
            key => {
                current = current.as_object()?.get(key)?;
            }
        }
    }
    match current {
        serde_json::Value::String(s) => Some(s.clone()),
        _ => None,
    }
}

pub fn extract_json_value(value: &serde_json::Value, path: &str) -> Vec<HashMap<String, String>> {
    let mut results = Vec::new();

    // ponytail: empty path means "the whole document": arrays become one
    // record per item, anything else is flattened into a single record
    let parts: Vec<&str> = if path.trim().is_empty() {
        vec!["*"]
    } else {
        path.split('/').collect()
    };
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
                    return results;
                }
            }
            key => match current.as_object() {
                Some(obj) => match obj.get(key) {
                    Some(v) => current = v,
                    None => return Vec::new(),
                },
                None => return Vec::new(),
            },
        }
    }

    let mut map = HashMap::new();
    flatten_json(current, "", &mut map);
    results.push(map);
    results
}

fn flatten_json(value: &serde_json::Value, prefix: &str, map: &mut HashMap<String, String>) {
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
                name: "item".into(),
                css_selector: ".item".into(),
                extract: ExtractType::Text,
            },
            Selector {
                name: "title".into(),
                css_selector: "h2".into(),
                extract: ExtractType::Text,
            },
            Selector {
                name: "desc".into(),
                css_selector: "p".into(),
                extract: ExtractType::Text,
            },
        ];

        let result = parse_html(html, "http://test.com", "job1", &selectors);
        assert_eq!(result.status, ScrapeStatus::Success);
        assert_eq!(result.data.len(), 2);
        assert_eq!(result.data[0].get("title").unwrap(), "Title 1");
        assert_eq!(
            result.data[0].get("desc").unwrap(),
            "Desc 1",
            "field selectors must match inside the row element"
        );
        assert_eq!(result.data[1].get("desc").unwrap(), "Desc 2");
    }

    #[test]
    fn test_find_next_link() {
        let html = r#"<html><body><a class="next" href="/page/2/">Next</a></body></html>"#;
        let link = find_next_link(html, "https://example.com/list", "a.next").unwrap();
        assert_eq!(link, "https://example.com/page/2/");

        assert!(find_next_link(html, "https://example.com/list", "a.missing").is_none());
    }

    #[test]
    fn test_link_and_image_urls_resolve_against_base() {
        let html = r#"<ul>
            <li class="item"><a href="/product/1">One</a><img src="images/pic.jpg"></li>
            <li class="item"><a href="https://abs.example/x">Two</a><img src="//cdn.example.com/pic2.jpg"></li>
        </ul>"#;
        let selectors = vec![
            Selector {
                name: "row".into(),
                css_selector: "li.item".into(),
                extract: ExtractType::Text,
            },
            Selector {
                name: "link".into(),
                css_selector: "a".into(),
                extract: ExtractType::Link,
            },
            Selector {
                name: "img".into(),
                css_selector: "img".into(),
                extract: ExtractType::Image,
            },
        ];

        let result = parse_html(html, "https://site.example/catalog/", "job1", &selectors);
        assert_eq!(result.status, ScrapeStatus::Success);
        assert_eq!(
            result.data[0].get("link").unwrap(),
            "https://site.example/product/1"
        );
        assert_eq!(
            result.data[0].get("img").unwrap(),
            "https://site.example/catalog/images/pic.jpg"
        );
        assert_eq!(result.data[1].get("link").unwrap(), "https://abs.example/x");
        assert_eq!(
            result.data[1].get("img").unwrap(),
            "https://cdn.example.com/pic2.jpg"
        );
    }

    #[test]
    fn test_decode_body_windows_1252() {
        let bytes = b"caf\xE9";
        let text = decode_body(bytes, "text/html; charset=windows-1252");
        assert_eq!(text, "café");
    }

    #[test]
    fn test_decode_body_utf16_with_bom() {
        let bytes = b"\xFF\xFEh\x00i\x00";
        let text = decode_body(bytes, "text/plain");
        assert_eq!(text, "hi");
    }

    #[test]
    fn test_decode_body_sniffs_html_meta_charset() {
        let bytes =
            b"<html><head><meta charset=\"windows-1252\"></head><body>caf\xE9</body></html>";
        let text = decode_body(bytes, "text/html");
        assert_eq!(
            text,
            "<html><head><meta charset=\"windows-1252\"></head><body>café</body></html>"
        );
    }

    #[test]
    fn test_decode_body_fallback_to_lossy_utf8() {
        let bytes = b"caf\xE9"; // invalid UTF-8, no charset declared
        let text = decode_body(bytes, "text/plain");
        assert_eq!(text, "caf\u{FFFD}");
    }

    #[test]
    fn test_json_empty_path_flattens_object_root() {
        let value = serde_json::json!({"a": 1, "b": {"c": 2}});
        let records = extract_json_value(&value, "");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].get("a").map(|s| s.as_str()), Some("1"));
        assert_eq!(records[0].get("b.c").map(|s| s.as_str()), Some("2"));
    }

    #[test]
    fn test_json_empty_path_on_array_returns_items() {
        let value = serde_json::json!([{"a": 1}, {"a": 2}]);
        let records = extract_json_value(&value, "");
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].get("a").map(|s| s.as_str()), Some("1"));
        assert_eq!(records[1].get("a").map(|s| s.as_str()), Some("2"));
    }

    #[test]
    fn test_json_star_on_object_root_flattens_one_record() {
        let value = serde_json::json!({"id": 7, "name": "x"});
        let records = extract_json_value(&value, "*");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].get("id").map(|s| s.as_str()), Some("7"));
    }

    #[test]
    fn test_json_missing_path_returns_no_records() {
        let value = serde_json::json!({"a": 1});
        assert!(extract_json_value(&value, "nope").is_empty());
        assert!(extract_json_value(&value, "a/b").is_empty());
    }

    #[test]
    fn test_json_extract_string_path() {
        let value = serde_json::json!({"links": {"next": "/items?page=2"}});
        assert_eq!(
            extract_json_string(&value, "links/next").as_deref(),
            Some("/items?page=2")
        );
        assert_eq!(extract_json_string(&value, "missing"), None);
        assert_eq!(extract_json_string(&value, "links/number"), None);
    }

    #[test]
    fn test_extract_value_html_mode() {
        use scraper::Html;
        let doc = Html::parse_document("<div><p>Hello <b>world</b></p></div>");
        let sel = scraper::Selector::parse("p").unwrap();
        let element = doc.select(&sel).next().unwrap();
        let s = crate::types::Selector {
            name: "test".into(),
            css_selector: "p".into(),
            extract: ExtractType::Html,
        };
        assert_eq!(
            extract_value(&element, &s, "https://example.com"),
            "Hello <b>world</b>"
        );
    }

    #[test]
    fn test_extract_value_attribute_mode() {
        use scraper::Html;
        let doc = Html::parse_document("<a href=\"/page/2\" data-id=\"42\">next</a>");
        let sel = scraper::Selector::parse("a").unwrap();
        let element = doc.select(&sel).next().unwrap();
        let s = crate::types::Selector {
            name: "test".into(),
            css_selector: "a".into(),
            extract: ExtractType::Attribute("data-id".into()),
        };
        assert_eq!(extract_value(&element, &s, "https://example.com"), "42");
    }

    #[test]
    fn test_extract_value_attribute_missing_returns_empty() {
        use scraper::Html;
        let doc = Html::parse_document("<a href=\"/page\">link</a>");
        let sel = scraper::Selector::parse("a").unwrap();
        let element = doc.select(&sel).next().unwrap();
        let s = crate::types::Selector {
            name: "test".into(),
            css_selector: "a".into(),
            extract: ExtractType::Attribute("data-id".into()),
        };
        assert_eq!(extract_value(&element, &s, "https://example.com"), "");
    }

    #[test]
    fn test_parse_json_error_path() {
        assert!(parse_json("not json at all").is_err());
        assert!(parse_json("").is_err());
        assert!(parse_json("{truncated").is_err());
    }

    #[test]
    fn test_parse_html_empty_selectors_fails() {
        let result = parse_html("<p>hi</p>", "https://example.com", "j1", &[]);
        assert_eq!(result.status, ScrapeStatus::Failed);
        assert!(result.error.unwrap().contains("No elements"));
    }

    #[test]
    fn test_parse_html_invalid_css_selector_fails() {
        let s = vec![crate::types::Selector {
            name: "x".into(),
            css_selector: "!!!invalid!!!".into(),
            extract: ExtractType::Text,
        }];
        let result = parse_html("<p>hi</p>", "https://example.com", "j1", &s);
        assert_eq!(result.status, ScrapeStatus::Failed);
    }

    #[test]
    fn test_find_next_link_invalid_css_returns_none() {
        assert_eq!(
            find_next_link(
                "<a href=\"/next\">go</a>",
                "https://example.com",
                "!!!bad!!!"
            ),
            None
        );
    }
}
