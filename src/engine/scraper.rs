use anyhow::Result;
use chrono::Utc;
use futures::stream::StreamExt;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{Semaphore, RwLock};

use crate::storage::Storage;
use crate::types::*;
use crate::engine::http_client::fetch_url_with_retry;
use crate::engine::parser::{extract_json_value, parse_html, parse_json};

pub struct ScraperEngine {
    storage: Arc<RwLock<Storage>>,
    semaphore: Arc<Semaphore>,
    stats: Arc<RwLock<EngineStats>>,
}

#[derive(Debug, Clone, Default)]
pub struct EngineStats {
    pub total_requests: u64,
    pub successful_requests: u64,
    pub failed_requests: u64,
    pub total_bytes_fetched: u64,
    pub active_jobs: u32,
    pub running_job_ids: HashSet<String>,
    pub last_runs: HashMap<String, ScrapeResult>,
}

impl ScraperEngine {
    pub fn new(storage: Arc<RwLock<Storage>>, max_concurrent: u32) -> Self {
        Self {
            storage,
            semaphore: Arc::new(Semaphore::new(max_concurrent as usize)),
            stats: Arc::new(RwLock::new(EngineStats::default())),
        }
    }

    pub async fn run_job(&self, job: ScrapeJob) -> Result<ScrapeResult> {
        {
            let mut stats = self.stats.write().await;
            stats.active_jobs += 1;
            stats.running_job_ids.insert(job.id.clone());
        }

        let _permit = self.semaphore.acquire().await?;

        self.storage.write().await.save_job(&job).await?;

        let result = self.execute_job(&job).await;

        let mut failed: Option<ScrapeResult> = None;
        {
            let mut stats = self.stats.write().await;
            stats.active_jobs -= 1;
            stats.running_job_ids.remove(&job.id);
            let run = match &result {
                Ok(r) => {
                    stats.total_requests += 1;
                    stats.total_bytes_fetched += r.bytes_fetched;
                    match r.status {
                        ScrapeStatus::Success => stats.successful_requests += 1,
                        _ => stats.failed_requests += 1,
                    }
                    r.clone()
                }
                Err(e) => {
                    stats.failed_requests += 1;
                    let f = ScrapeResult {
                        id: 0,
                        job_id: job.id.clone(),
                        url: job.url.clone(),
                        timestamp: Utc::now().naive_utc(),
                        data: Vec::new(),
                        status: ScrapeStatus::Failed,
                        error: Some(e.to_string()),
                        duration_ms: 0,
                        bytes_fetched: 0,
                    };
                    failed = Some(f.clone());
                    f
                }
            };
            stats.last_runs.insert(job.id.clone(), run);
        }

        if let Some(f) = failed {
            self.storage.write().await.save_result(&f).await?;
        } else if let Ok(ref scrape_result) = result {
            self.storage.write().await.save_result(scrape_result).await?;
        }

        result
    }

    async fn execute_job(&self, job: &ScrapeJob) -> Result<ScrapeResult> {
        let start = std::time::Instant::now();
        let max_pages = job.max_pages.unwrap_or(1);
        let has_pagination = job.url.contains("{page}");
        let total_pages = if has_pagination { max_pages } else { 1 };

        let concurrency = job.concurrency.max(1) as usize;
        let pages: Vec<u32> = (1..=total_pages).collect();

        let page_results = futures::stream::iter(pages.into_iter().map(|page| {
            let job = job.clone();
            async move {
                let page_url = if has_pagination {
                    job.url.replace("{page}", &page.to_string())
                } else {
                    job.url.clone()
                };
                let resp = fetch_url_with_retry(
                    &page_url,
                    job.proxy.as_deref(),
                    job.user_agent.as_deref(),
                    job.timeout().as_secs(),
                    3,
                    &job.method,
                    job.body.as_deref(),
                    &job.headers,
                )
                .await?;

                let is_json = resp
                    .headers()
                    .get(reqwest::header::CONTENT_TYPE)
                    .and_then(|v| v.to_str().ok())
                    .map(|ct| ct.to_lowercase().contains("json"))
                    .unwrap_or(false);

                let bytes = resp.bytes().await?;
                let fetched = bytes.len() as u64;

                let mut records = Vec::new();
                if is_json {
                    let text = String::from_utf8_lossy(&bytes);
                    let json = parse_json(&text)?;
                    let path = job
                        .selectors
                        .first()
                        .map(|s| s.css_selector.clone())
                        .unwrap_or_default();
                    records.extend(extract_json_value(&json, &path));
                } else {
                    let html = String::from_utf8_lossy(&bytes);
                    let result = parse_html(&html, &page_url, &job.id, &job.selectors);
                    records.extend(result.data);
                }

                Ok::<(u64, Vec<HashMap<String, String>>), anyhow::Error>((fetched, records))
            }
        }))
        .buffered(concurrency)
        .collect::<Vec<_>>()
        .await;

        let mut all_data = Vec::new();
        let mut fetched_bytes = 0u64;
        for page in page_results {
            let (fetched, records) = page?;
            fetched_bytes += fetched;
            all_data.extend(records);
        }

        let elapsed = start.elapsed();
        let is_empty = all_data.is_empty();

        Ok(ScrapeResult {
            id: 0,
            job_id: job.id.clone(),
            url: job.url.clone(),
            timestamp: Utc::now().naive_utc(),
            data: all_data,
            status: if is_empty {
                ScrapeStatus::Failed
            } else {
                ScrapeStatus::Success
            },
            error: if is_empty {
                Some("No data extracted from the page".into())
            } else {
                None
            },
            duration_ms: elapsed.as_millis() as u64,
            bytes_fetched: fetched_bytes,
        })
    }

    pub async fn test_job(&self, job: &ScrapeJob) -> Result<ScrapeResult> {
        self.execute_job(job).await
    }

    pub async fn run_all_jobs(&self, jobs: &[ScrapeJob]) -> Vec<Result<ScrapeResult>> {
        let mut handles = Vec::new();
        for job in jobs.iter().filter(|j| j.enabled) {
            let job = job.clone();
            handles.push(self.run_job(job));
        }
        futures::future::join_all(handles).await
    }

    pub async fn get_stats(&self) -> EngineStats {
        self.stats.read().await.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Storage;
    use std::collections::HashMap;

    fn demo_job() -> ScrapeJob {
        ScrapeJob {
            id: "test-job-1".into(),
            name: "Quotes demo".into(),
            url: "https://quotes.toscrape.com".into(),
            selectors: vec![
                crate::types::Selector {
                    name: "quote".into(),
                    css_selector: ".quote".into(),
                    attribute: None,
                    extract: ExtractType::Text,
                },
                crate::types::Selector {
                    name: "text".into(),
                    css_selector: ".text".into(),
                    attribute: None,
                    extract: ExtractType::Text,
                },
                crate::types::Selector {
                    name: "author".into(),
                    css_selector: ".author".into(),
                    attribute: None,
                    extract: ExtractType::Text,
                },
            ],
            headers: HashMap::new(),
            method: HttpMethod::Get,
            body: None,
            interval_minutes: None,
            max_pages: Some(1),
            concurrency: 1,
            proxy: None,
            user_agent: None,
            output_format: OutputFormat::Csv,
            enabled: true,
        }
    }

    #[tokio::test]
    async fn end_to_end_real_scrape() {
        let tmp = std::env::temp_dir().join(format!("ds_test_{}", uuid::Uuid::new_v4()));
        let storage = Arc::new(RwLock::new(
            Storage::new(tmp.to_str().unwrap()).unwrap(),
        ));
        let engine = ScraperEngine::new(storage.clone(), 4);

        let job = demo_job();
        let result = engine.run_job(job).await.unwrap();

        assert_eq!(result.status, ScrapeStatus::Success);
        assert!(!result.data.is_empty(), "no records extracted");
        assert!(result.data[0].contains_key("author"));

        let saved = storage.read().await.get_all_results(10).await.unwrap();
        assert_eq!(saved.len(), 1);
        assert!(!saved[0].data.is_empty());

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn end_to_end_json_api() {
        let tmp = std::env::temp_dir().join(format!("ds_json_{}", uuid::Uuid::new_v4()));
        let storage = Arc::new(RwLock::new(
            Storage::new(tmp.to_str().unwrap()).unwrap(),
        ));
        let engine = ScraperEngine::new(storage.clone(), 4);

        let mut job = demo_job();
        job.id = "json-job-1".into();
        job.name = "Posts API".into();
        job.url = "https://jsonplaceholder.typicode.com/posts".into();
        job.selectors = vec![crate::types::Selector {
            name: "item".into(),
            css_selector: "*".into(),
            attribute: None,
            extract: ExtractType::Text,
        }];

        let result = engine.run_job(job).await.unwrap();

        assert_eq!(result.status, ScrapeStatus::Success);
        assert!(!result.data.is_empty(), "no JSON records extracted");
        assert!(result.data[0].contains_key("title"));
        assert!(result.data[0].contains_key("userId"));

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn post_with_json_body() {
        let tmp = std::env::temp_dir().join(format!("ds_post_{}", uuid::Uuid::new_v4()));
        let storage = Arc::new(RwLock::new(
            Storage::new(tmp.to_str().unwrap()).unwrap(),
        ));
        let engine = ScraperEngine::new(storage.clone(), 4);

        let mut job = demo_job();
        job.id = "post-job-1".into();
        job.name = "Echo POST".into();
        job.url = "https://postman-echo.com/post".into();
        job.method = HttpMethod::Post;
        job.body = Some("{\"hello\":\"world\"}".into());
        job.selectors = vec![crate::types::Selector {
            name: "echo".into(),
            css_selector: "json".into(),
            attribute: None,
            extract: ExtractType::Text,
        }];

        let result = engine.run_job(job).await.unwrap();

        assert_eq!(result.status, ScrapeStatus::Success);
        let echoed = result
            .data
            .first()
            .and_then(|r| r.get("hello"))
            .map(|v| v.as_str());
        assert_eq!(echoed, Some("world"));
        log::info!("echoed data: {:?}", result.data);

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn json_pagination() {
        let tmp = std::env::temp_dir().join(format!("ds_pages_{}", uuid::Uuid::new_v4()));
        let storage = Arc::new(RwLock::new(
            Storage::new(tmp.to_str().unwrap()).unwrap(),
        ));
        let engine = ScraperEngine::new(storage.clone(), 4);

        let mut job = demo_job();
        job.id = "pages-job-1".into();
        job.name = "Paginated API".into();
        job.url = "https://jsonplaceholder.typicode.com/posts?_page={page}".into();
        job.max_pages = Some(2);
        job.selectors = vec![crate::types::Selector {
            name: "item".into(),
            css_selector: "*".into(),
            attribute: None,
            extract: ExtractType::Text,
        }];

        let result = engine.run_job(job).await.unwrap();

        assert_eq!(result.status, ScrapeStatus::Success);
        assert_eq!(result.data.len(), 20, "two pages of 10 posts expected");
        let ids: Vec<String> = result
            .data
            .iter()
            .map(|r| r.get("id").cloned().unwrap_or_default())
            .collect();
        assert_eq!(ids[0], "1", "first record should be from page 1");
        assert_eq!(ids[10], "11", "tenth record should be from page 2");

        let _ = std::fs::remove_dir_all(&tmp);
    }
}
