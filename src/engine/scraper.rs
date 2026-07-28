use anyhow::Result;
use chrono::Utc;
use std::sync::Arc;
use tokio::sync::{Semaphore, RwLock};

use crate::storage::Storage;
use crate::types::*;
use crate::engine::http_client::fetch_url_with_retry;
use crate::engine::parser::parse_html;

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
            )
            .await?;

            let bytes = resp.bytes().await?;
            let html = String::from_utf8_lossy(&bytes);

            let result = parse_html(&html, &current_url, &job.id, &job.selectors);
            all_data.extend(result.data);

            page += 1;

            if page >= max_pages {
                break;
            }
        }

        let elapsed = start.elapsed();

        Ok(ScrapeResult {
            job_id: job.id.clone(),
            url: job.url.clone(),
            timestamp: Utc::now().naive_utc(),
            data: all_data,
            status: ScrapeStatus::Success,
            error: None,
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
