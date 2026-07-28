use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::interval;

use crate::engine::ScraperEngine;
use crate::types::ScrapeJob;

pub struct Scheduler {
    engine: Arc<ScraperEngine>,
    jobs: Arc<RwLock<Vec<ScrapeJob>>>,
    task_map: Arc<RwLock<HashMap<String, bool>>>,
}

impl Scheduler {
    pub fn new(engine: Arc<ScraperEngine>, jobs: Arc<RwLock<Vec<ScrapeJob>>>) -> Self {
        Self {
            engine,
            jobs,
            task_map: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn start(&self) {
        let engine = self.engine.clone();
        let jobs = self.jobs.clone();
        let task_map = self.task_map.clone();

        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs(60));
            loop {
                ticker.tick().await;
                let current_jobs = jobs.read().await;
                for job in current_jobs.iter() {
                    if let Some(interval_min) = job.interval_minutes {
                        if interval_min > 0 && job.enabled {
                            let should_run = {
                                let mut map = task_map.write().await;
                                let last_run = map.get(&job.id).copied().unwrap_or(false);
                                if !last_run {
                                    map.insert(job.id.clone(), true);
                                    true
                                } else {
                                    false
                                }
                            };
                            if should_run {
                                let engine = engine.clone();
                                let job = job.clone();
                                tokio::spawn(async move {
                                    log::info!("Scheduled job '{}' starting", job.name);
                                    match engine.run_job(job).await {
                                        Ok(result) => {
                                            log::info!(
                                                "Scheduled job completed: {} items in {}ms",
                                                result.data.len(),
                                                result.duration_ms
                                            );
                                        }
                                        Err(e) => {
                                            log::error!("Scheduled job failed: {}", e);
                                        }
                                    }
                                });
                            }
                        }
                    }
                }
            }
        });
    }
}
