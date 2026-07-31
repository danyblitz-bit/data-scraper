use anyhow::Result;
use chrono::Utc;
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
        }

        let _permit = self.semaphore.acquire().await?;

        self.storage.write().await.save_job(&job).await?;

        let result = self.execute_job(&job).await;

        {
            let mut stats = self.stats.write().await;
            stats.active_jobs -= 1;
            if let Ok(ref r) = result {
                stats.total_requests += 1;
                if r.status == ScrapeStatus::Success {
                    stats.successful_requests += 1;
                } else {
                    stats.failed_requests += 1;
                }
                stats.total_bytes_fetched += r.bytes_fetched;
            }
        }

        if let Ok(ref scrape_result) = result {
            let mut storage = self.storage.write().await;
            storage.save_result(scrape_result).await?;
        }

        result
    }

    async fn execute_job(&self, job: &ScrapeJob) -> Result<ScrapeResult> {
        let start = std::time::Instant::now();
        let max_pages = job.max_pages.unwrap_or(1);

        let mut all_data = Vec::new();
        let current_url = job.url.clone();
        let mut page = 0u32;

        while page < max_pages {
            let resp = fetch_url_with_retry(
                &current_url,
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

            if is_json {
                let text = String::from_utf8_lossy(&bytes);
                let json = parse_json(&text)?;
                let path = job
                    .selectors
                    .first()
                    .map(|s| s.css_selector.clone())
                    .unwrap_or_default();
                all_data.extend(extract_json_value(&json, &path));
            } else {
                let html = String::from_utf8_lossy(&bytes);
                let result = parse_html(&html, &current_url, &job.id, &job.selectors);
                all_data.extend(result.data);
            }

            page += 1;

            if page >= max_pages {
                break;
            }
        }

        let elapsed = start.elapsed();
        let is_empty = all_data.is_empty();

        Ok(ScrapeResult {
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
            bytes_fetched: 0,
        })
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

    pub async fn run_job_parallel(
        &self,
        job: ScrapeJob,
        urls: Vec<String>,
    ) -> Vec<Result<ScrapeResult>> {
        let mut handles = Vec::new();
        for url in urls {
            let mut page_job = job.clone();
            page_job.url = url;
            let engine = self; // Arc reference
            handles.push(engine.run_job(page_job));
        }
        futures::future::join_all(handles).await
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

        let saved = storage.read().await.get_results_for_job("test-job-1").await.unwrap();
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
}
