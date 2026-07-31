use anyhow::Result;
use chrono::Utc;
use futures::stream::StreamExt;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{RwLock, Semaphore};

use crate::storage::Storage;
use crate::types::*;
use crate::engine::http_client::fetch_url_with_retry;
use crate::engine::parser::{extract_json_value, find_next_link, parse_html, parse_json};

pub struct ScraperEngine {
    storage: Arc<RwLock<Storage>>,
    semaphore: Arc<RwLock<Arc<Semaphore>>>,
    timeout_secs: Arc<AtomicU64>,
    default_ua: Arc<std::sync::RwLock<String>>,
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
    pub fn new(storage: Arc<RwLock<Storage>>, max_concurrent: u32, timeout_secs: u64, user_agent: String) -> Self {
        Self {
            storage,
            semaphore: Arc::new(RwLock::new(Arc::new(Semaphore::new(
                max_concurrent.max(1) as usize,
            )))),
            timeout_secs: Arc::new(AtomicU64::new(timeout_secs.max(1))),
            default_ua: Arc::new(std::sync::RwLock::new(user_agent)),
            stats: Arc::new(RwLock::new(EngineStats::default())),
        }
    }

    pub async fn set_concurrency(&self, max_concurrent: u32) {
        let mut sem = self.semaphore.write().await;
        *sem = Arc::new(Semaphore::new(max_concurrent.max(1) as usize));
    }

    pub fn set_timeout(&self, secs: u64) {
        self.timeout_secs.store(secs.max(1), Ordering::Relaxed);
    }

    pub fn set_user_agent(&self, ua: String) {
        *self.default_ua.write().unwrap() = ua;
    }

    pub async fn run_job(&self, job: ScrapeJob) -> Result<ScrapeResult> {
        {
            let mut stats = self.stats.write().await;
            stats.active_jobs += 1;
            stats.running_job_ids.insert(job.id.clone());
        }

        let sem = self.semaphore.read().await.clone();
        let _permit = sem.acquire().await?;

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

        let timeout_secs = self.timeout_secs.load(Ordering::Relaxed);
        let effective_ua = {
            let global = self.default_ua.read().unwrap().clone();
            job.user_agent.clone().or_else(|| {
                let ua = global.trim().to_string();
                if ua.is_empty() { None } else { Some(ua) }
            })
        };

        let (all_data, fetched_bytes) = match &job.next_link_selector {
            Some(next_sel) => {
                let mut all_data = Vec::new();
                let mut fetched_bytes = 0u64;
                let mut url = job.url.clone();
                let mut visited = HashSet::new();
                for _ in 0..max_pages {
                    if !visited.insert(url.clone()) {
                        break;
                    }
                    let (fetched, records, next) = self
                        .fetch_and_parse_page(job, &url, Some(next_sel), timeout_secs, effective_ua.as_deref())
                        .await?;
                    fetched_bytes += fetched;
                    all_data.extend(records);
                    match next {
                        Some(n) if n != url => url = n,
                        _ => break,
                    }
                }
                (all_data, fetched_bytes)
            }
            None => {
                let total_pages = if has_pagination { max_pages } else { 1 };
                let concurrency = job.concurrency.max(1) as usize;
                let pages: Vec<u32> = (1..=total_pages).collect();
                let page_results = futures::stream::iter(pages.into_iter().map(|page| {
                    let job = job.clone();
                    let effective_ua = effective_ua.clone();
                    async move {
                        let page_url = if has_pagination {
                            job.url.replace("{page}", &page.to_string())
                        } else {
                            job.url.clone()
                        };
                        self.fetch_and_parse_page(&job, &page_url, None, timeout_secs, effective_ua.as_deref())
                            .await
                    }
                }))
                .buffered(concurrency)
                .collect::<Vec<_>>()
                .await;

                let mut all_data = Vec::new();
                let mut fetched_bytes = 0u64;
                for page in page_results {
                    let (fetched, records, _) = page?;
                    fetched_bytes += fetched;
                    all_data.extend(records);
                }
                (all_data, fetched_bytes)
            }
        };

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

    async fn fetch_and_parse_page(
        &self,
        job: &ScrapeJob,
        url: &str,
        next_sel: Option<&str>,
        timeout_secs: u64,
        effective_ua: Option<&str>,
    ) -> Result<(u64, Vec<HashMap<String, String>>, Option<String>)> {
        let resp = fetch_url_with_retry(
            url,
            job.proxy.as_deref(),
            effective_ua,
            timeout_secs,
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
        let mut next_link = None;
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
            let result = parse_html(&html, url, &job.id, &job.selectors);
            records.extend(result.data);
            if let Some(sel) = next_sel {
                next_link = find_next_link(&html, url, sel);
            }
        }

        Ok((fetched, records, next_link))
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
    use std::io::{BufRead, Write};

    fn demo_job() -> ScrapeJob {
        ScrapeJob {
            id: "test-job-1".into(),
            name: "Quotes demo".into(),
            url: "https://quotes.toscrape.com".into(),
            selectors: vec![
                crate::types::Selector {
                    name: "quote".into(),
                    css_selector: ".quote".into(),
                    extract: ExtractType::Text,
                },
                crate::types::Selector {
                    name: "text".into(),
                    css_selector: ".text".into(),
                    extract: ExtractType::Text,
                },
                crate::types::Selector {
                    name: "author".into(),
                    css_selector: ".author".into(),
                    extract: ExtractType::Text,
                },
            ],
            headers: HashMap::new(),
            method: HttpMethod::Get,
            body: None,
            interval_minutes: None,
            max_pages: Some(1),
            next_link_selector: None,
            concurrency: 1,
            proxy: None,
            user_agent: None,
            enabled: true,
        }
    }

    #[tokio::test]
    async fn end_to_end_real_scrape() {
        let tmp = std::env::temp_dir().join(format!("ds_test_{}", uuid::Uuid::new_v4()));
        let storage = Arc::new(RwLock::new(
            Storage::new(tmp.to_str().unwrap()).unwrap(),
        ));
        let engine = ScraperEngine::new(storage.clone(), 4, 30, "DataScraper/1.0".into());

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
        let engine = ScraperEngine::new(storage.clone(), 4, 30, "DataScraper/1.0".into());

        let mut job = demo_job();
        job.id = "json-job-1".into();
        job.name = "Posts API".into();
        job.url = "https://jsonplaceholder.typicode.com/posts".into();
        job.selectors = vec![crate::types::Selector {
            name: "item".into(),
            css_selector: "*".into(),
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
        let engine = ScraperEngine::new(storage.clone(), 4, 30, "DataScraper/1.0".into());

        let mut job = demo_job();
        job.id = "post-job-1".into();
        job.name = "Echo POST".into();
        job.url = "https://postman-echo.com/post".into();
        job.method = HttpMethod::Post;
        job.body = Some("{\"hello\":\"world\"}".into());
        job.selectors = vec![crate::types::Selector {
            name: "echo".into(),
            css_selector: "json".into(),
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
        let engine = ScraperEngine::new(storage.clone(), 4, 30, "DataScraper/1.0".into());

        let mut job = demo_job();
        job.id = "pages-job-1".into();
        job.name = "Paginated API".into();
        job.url = "https://jsonplaceholder.typicode.com/posts?_page={page}".into();
        job.max_pages = Some(2);
        job.selectors = vec![crate::types::Selector {
            name: "item".into(),
            css_selector: "*".into(),
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

    #[tokio::test]
    async fn html_next_link_pagination() {
        let tmp = std::env::temp_dir().join(format!("ds_next_{}", uuid::Uuid::new_v4()));
        let storage = Arc::new(RwLock::new(
            Storage::new(tmp.to_str().unwrap()).unwrap(),
        ));
        let engine = ScraperEngine::new(storage.clone(), 4, 30, "DataScraper/1.0".into());

        let mut job = demo_job();
        job.id = "next-job-1".into();
        job.name = "Quotes with next links".into();
        job.next_link_selector = Some("li.next a".into());
        job.max_pages = Some(2);

        let result = engine.run_job(job).await.unwrap();

        assert_eq!(result.status, ScrapeStatus::Success);
        assert_eq!(
            result.data.len(),
            20,
            "two pages of 10 quotes expected; single page would give 10"
        );

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn request_timeout_causes_failure() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                std::thread::sleep(std::time::Duration::from_secs(3));
                let _ = stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok");
            }
        });

        let tmp = std::env::temp_dir().join(format!("ds_timeout_{}", uuid::Uuid::new_v4()));
        let storage = Arc::new(RwLock::new(
            Storage::new(tmp.to_str().unwrap()).unwrap(),
        ));
        let engine = ScraperEngine::new(storage.clone(), 4, 1, "DataScraper/1.0".into());

        let mut job = demo_job();
        job.id = "timeout-job-1".into();
        job.name = "Slow server".into();
        job.url = format!("http://{addr}/");
        job.selectors = vec![crate::types::Selector {
            name: "item".into(),
            css_selector: "*".into(),
            extract: ExtractType::Text,
        }];

        let result = engine.run_job(job).await;
        assert!(result.is_err(), "expected the request to time out");

        let saved = storage.read().await.get_all_results(10).await.unwrap();
        assert_eq!(saved.len(), 1);
        assert_eq!(saved[0].status, ScrapeStatus::Failed);

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn user_agent_fallback_and_override() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            for _ in 0..2 {
                if let Ok((stream, _)) = listener.accept() {
                    let mut stream = stream;
                    let mut reader = std::io::BufReader::new(stream.try_clone().unwrap());
                    let mut ua = String::new();
                    let mut line = String::new();
                    loop {
                        line.clear();
                        if reader.read_line(&mut line).unwrap() == 0 || line == "\r\n" {
                            break;
                        }
                        if let Some((k, v)) = line.split_once(':') {
                            if k.trim().eq_ignore_ascii_case("user-agent") {
                                ua = v.trim().to_string();
                            }
                        }
                    }
                    let body = format!("{{\"ua\":{{\"value\":\"{}\"}}}}", ua);
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = stream.write_all(resp.as_bytes());
                }
            }
        });

        let tmp = std::env::temp_dir().join(format!("ds_ua_{}", uuid::Uuid::new_v4()));
        let storage = Arc::new(RwLock::new(
            Storage::new(tmp.to_str().unwrap()).unwrap(),
        ));
        let engine = ScraperEngine::new(storage.clone(), 4, 10, "TestAgent/1.0".into());

        let selector = crate::types::Selector {
            name: "ua".into(),
            css_selector: "ua".into(),
            extract: ExtractType::Text,
        };

        let mut job = demo_job();
        job.id = "ua-job-1".into();
        job.url = format!("http://{addr}/");
        job.selectors = vec![selector.clone()];

        let result = engine.run_job(job.clone()).await.unwrap();
        assert_eq!(result.status, ScrapeStatus::Success);
        assert_eq!(
            result.data[0].get("value").map(|s| s.as_str()),
            Some("TestAgent/1.0"),
            "job without UA should fall back to the global default; got {:?}",
            result.data[0]
        );

        job.id = "ua-job-2".into();
        job.user_agent = Some("JobAgent/2.0".into());
        let result = engine.run_job(job).await.unwrap();
        assert_eq!(result.status, ScrapeStatus::Success);
        assert_eq!(
            result.data[0].get("value").map(|s| s.as_str()),
            Some("JobAgent/2.0"),
            "per-job UA should override the global default"
        );

        let _ = std::fs::remove_dir_all(&tmp);
    }
}
