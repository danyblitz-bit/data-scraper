use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::interval;

use crate::engine::ScraperEngine;
use crate::storage::Storage;
use crate::types::ScrapeJob;

pub struct Scheduler {
    engine: Arc<ScraperEngine>,
    storage: Arc<RwLock<Storage>>,
    last_runs: Arc<RwLock<HashMap<String, chrono::NaiveDateTime>>>,
}

impl Scheduler {
    pub fn new(engine: Arc<ScraperEngine>, storage: Arc<RwLock<Storage>>) -> Self {
        Self {
            engine,
            storage,
            last_runs: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn start(&self) {
        let engine = self.engine.clone();
        let storage = self.storage.clone();
        let last_runs = self.last_runs.clone();

        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs(30));
            loop {
                // ponytail: interval fires immediately and last_runs starts empty, so every
                // interval job runs once at startup (fresh data on launch), then on schedule.
                ticker.tick().await;
                let jobs: Vec<ScrapeJob> = match storage.read().await.get_all_jobs().await {
                    Ok(jobs) => jobs,
                    Err(e) => {
                        log::error!("Scheduler: failed to load jobs: {}", e);
                        continue;
                    }
                };

                // ponytail: drop bookkeeping for deleted jobs, checked every tick
                last_runs
                    .write()
                    .await
                    .retain(|id, _| jobs.iter().any(|j| j.id == *id));

                let now = chrono::Utc::now().naive_utc();
                for job in jobs
                    .iter()
                    .filter(|j| j.enabled && j.interval_minutes.unwrap_or(0) > 0)
                {
                    let minutes = job.interval_minutes.unwrap_or(0) as i64;
                    let should_run = {
                        let mut map = last_runs.write().await;
                        let last = map.get(&job.id).copied().unwrap_or_default();
                        let due = is_due(last, now, minutes);
                        if due {
                            map.insert(job.id.clone(), now);
                        }
                        due
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
        });
    }
}

fn is_due(
    last_run: chrono::NaiveDateTime,
    now: chrono::NaiveDateTime,
    interval_minutes: i64,
) -> bool {
    last_run == chrono::NaiveDateTime::default()
        || (now - last_run).num_minutes() >= interval_minutes
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_due_never_run() {
        let now = chrono::Utc::now().naive_utc();
        assert!(is_due(chrono::NaiveDateTime::default(), now, 60));
    }

    #[test]
    fn test_is_due_enough_time_passed() {
        let now = chrono::Utc::now().naive_utc();
        let last = now - chrono::Duration::minutes(61);
        assert!(is_due(last, now, 60));
    }

    #[test]
    fn test_is_due_not_enough_time() {
        let now = chrono::Utc::now().naive_utc();
        let last = now - chrono::Duration::minutes(30);
        assert!(!is_due(last, now, 60));
    }

    #[test]
    fn test_is_due_exactly_on_boundary() {
        let now = chrono::Utc::now().naive_utc();
        let last = now - chrono::Duration::minutes(60);
        assert!(is_due(last, now, 60));
    }
}
