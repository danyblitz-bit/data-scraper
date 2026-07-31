use serde::{Deserialize, Serialize};
use std::collections::HashMap;

fn default_max_bytes() -> u64 {
    64 * 1024 * 1024
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScrapeJob {
    pub id: String,
    pub name: String,
    pub url: String,
    pub selectors: Vec<Selector>,
    pub headers: HashMap<String, String>,
    pub method: HttpMethod,
    pub body: Option<String>,
    pub interval_minutes: Option<u64>,
    pub max_pages: Option<u32>,
    pub next_link_selector: Option<String>,
    pub concurrency: u32,
    pub proxy: Option<String>,
    pub user_agent: Option<String>,
    pub auto_export: Option<ExportFormat>,
    pub enabled: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum ExportFormat {
    Csv,
    Json,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Selector {
    pub name: String,
    pub css_selector: String,
    pub extract: ExtractType,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ExtractType {
    Text,
    Html,
    Attribute(String),
    Link,
    Image,
}

impl Default for ExtractType {
    fn default() -> Self {
        Self::Text
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum HttpMethod {
    Get,
    Post,
    Put,
    Delete,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScrapeResult {
    pub id: i64,
    pub job_id: String,
    pub url: String,
    pub timestamp: chrono::NaiveDateTime,
    pub data: Vec<HashMap<String, String>>,
    pub status: ScrapeStatus,
    pub error: Option<String>,
    pub duration_ms: u64,
    pub bytes_fetched: u64,
    pub record_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ScrapeStatus {
    Success,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    pub max_concurrent_requests: u32,
    pub request_timeout_secs: u64,
    pub user_agent: String,
    pub database_path: String,
    pub export_path: String,
    pub theme: Theme,
    pub window_width: f32,
    pub window_height: f32,
    #[serde(default = "default_max_bytes")]
    pub max_response_bytes: u64,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            max_concurrent_requests: 10,
            request_timeout_secs: 30,
            user_agent: "DataScraper/1.0 (+https://github.com/datascraper)".into(),
            database_path: "data_scraper.db".into(),
            export_path: "exports".into(),
            theme: Theme::Dark,
            window_width: 1280.0,
            window_height: 800.0,
            max_response_bytes: default_max_bytes(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Theme {
    Dark,
    Light,
    System,
}

impl Default for Theme {
    fn default() -> Self {
        Self::Dark
    }
}
