use anyhow::Result;
use chrono::NaiveDateTime;
use rusqlite::{params, Connection};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use parking_lot::Mutex;

use crate::types::{OutputFormat, ScrapeJob, ScrapeResult, ScrapeStatus};

pub struct Storage {
    conn: Arc<Mutex<Connection>>,
}

impl Storage {
    pub fn new(db_path: &str) -> Result<Self> {
        let path = Path::new(db_path);
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }

        let conn = Connection::open(db_path)?;
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA cache_size=-65536;")?;

        let storage = Self {
            conn: Arc::new(Mutex::new(conn)),
        };
        storage.initialize_schema()?;
        Ok(storage)
    }

    fn initialize_schema(&self) -> Result<()> {
        let conn = self.conn.lock();
        conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                url TEXT NOT NULL,
                config TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id TEXT NOT NULL,
                url TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                data TEXT NOT NULL,
                status TEXT NOT NULL,
                error TEXT,
                duration_ms INTEGER NOT NULL DEFAULT 0,
                bytes_fetched INTEGER NOT NULL DEFAULT 0,
                FOREIGN KEY (job_id) REFERENCES jobs(id)
            );

            CREATE TABLE IF NOT EXISTS raw_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                result_id INTEGER NOT NULL,
                field_name TEXT NOT NULL,
                field_value TEXT,
                FOREIGN KEY (result_id) REFERENCES results(id)
            );

            CREATE INDEX IF NOT EXISTS idx_results_job_id ON results(job_id);
            CREATE INDEX IF NOT EXISTS idx_results_timestamp ON results(timestamp);
            CREATE INDEX IF NOT EXISTS idx_raw_data_result_id ON raw_data(result_id);
            ",
        )?;
        Ok(())
    }

    pub async fn save_result(&mut self, result: &ScrapeResult) -> Result<()> {
        let conn = self.conn.lock();
        let tx = conn.unchecked_transaction()?;

        tx.execute(
            "INSERT INTO results (job_id, url, timestamp, data, status, error, duration_ms, bytes_fetched)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                result.job_id,
                result.url,
                result.timestamp.to_string(),
                serde_json::to_string(&result.data)?,
                format!("{:?}", result.status),
                result.error,
                result.duration_ms as i64,
                result.bytes_fetched as i64,
            ],
        )?;

        let result_id = tx.last_insert_rowid();

        for record in &result.data {
            for (key, value) in record {
                tx.execute(
                    "INSERT INTO raw_data (result_id, field_name, field_value) VALUES (?1, ?2, ?3)",
                    params![result_id, key, value],
                )?;
            }
        }

        tx.commit()?;
        Ok(())
    }

    pub async fn get_all_results(&self, limit: i64) -> Result<Vec<ScrapeResult>> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare(
            "SELECT id, job_id, url, timestamp, data, status, error, duration_ms, bytes_fetched
             FROM results ORDER BY timestamp DESC LIMIT ?1",
        )?;

        let results = stmt
            .query_map(params![limit], map_result_row)?
            .filter_map(|r| r.ok())
            .collect();

        Ok(results)
    }

    pub async fn save_job(&self, job: &ScrapeJob) -> Result<()> {
        let conn = self.conn.lock();
        conn.execute(
            "INSERT OR REPLACE INTO jobs (id, name, url, config, updated_at)
             VALUES (?1, ?2, ?3, ?4, datetime('now'))",
            params![
                job.id,
                job.name,
                job.url,
                serde_json::to_string(job)?,
            ],
        )?;
        Ok(())
    }

    pub async fn delete_job(&self, job_id: &str) -> Result<()> {
        let conn = self.conn.lock();
        conn.execute("DELETE FROM results WHERE job_id = ?1", params![job_id])?;
        conn.execute("DELETE FROM jobs WHERE id = ?1", params![job_id])?;
        Ok(())
    }

    pub async fn delete_result(&self, result_id: i64) -> Result<()> {
        let conn = self.conn.lock();
        conn.execute("DELETE FROM raw_data WHERE result_id = ?1", params![result_id])?;
        conn.execute("DELETE FROM results WHERE id = ?1", params![result_id])?;
        Ok(())
    }

    pub async fn get_all_jobs(&self) -> Result<Vec<ScrapeJob>> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare("SELECT config FROM jobs ORDER BY name")?;
        let jobs = stmt
            .query_map([], |row| {
                let config_str: String = row.get(0)?;
                Ok(serde_json::from_str(&config_str).unwrap_or_else(|_| {
                    ScrapeJob {
                        id: String::new(),
                        name: "invalid".into(),
                        url: String::new(),
                        selectors: Vec::new(),
                        headers: HashMap::new(),
                        method: crate::types::HttpMethod::Get,
                        body: None,
                        interval_minutes: None,
                        max_pages: None,
                        concurrency: 1,
                        proxy: None,
                        user_agent: None,
                        output_format: OutputFormat::Csv,
                        enabled: false,
                    }
                }))
            })?
            .filter_map(|r| r.ok())
            .collect();
        Ok(jobs)
    }

    pub async fn get_stats_summary(&self) -> Result<(u64, u64, u64, f64)> {
        let conn = self.conn.lock();
        let total_jobs: u64 = conn
            .query_row("SELECT COUNT(*) FROM jobs", [], |row| row.get::<_, i64>(0))
            .unwrap_or(0) as u64;
        let total_results: u64 = conn
            .query_row("SELECT COUNT(*) FROM results", [], |row| row.get::<_, i64>(0))
            .unwrap_or(0) as u64;
        let total_bytes: u64 = conn
            .query_row(
                "SELECT COALESCE(SUM(bytes_fetched), 0) FROM results",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap_or(0) as u64;
        let avg_duration: f64 = conn
            .query_row(
                "SELECT COALESCE(AVG(duration_ms), 0) FROM results",
                [],
                |row| row.get(0),
            )
            .unwrap_or(0.0);

        Ok((total_jobs, total_results, total_bytes, avg_duration))
    }
}

fn map_result_row(row: &rusqlite::Row) -> rusqlite::Result<ScrapeResult> {
    let data_str: String = row.get(4)?;
    let data: Vec<HashMap<String, String>> = serde_json::from_str(&data_str).unwrap_or_default();
    let status_str: String = row.get(5)?;
    let status = match status_str.as_str() {
        "Success" => ScrapeStatus::Success,
        "Partial" => ScrapeStatus::Partial,
        "Failed" => ScrapeStatus::Failed,
        "Running" => ScrapeStatus::Running,
        _ => ScrapeStatus::Pending,
    };

    Ok(ScrapeResult {
        id: row.get(0)?,
        job_id: row.get(1)?,
        url: row.get(2)?,
        timestamp: NaiveDateTime::parse_from_str(&row.get::<_, String>(3)?, "%Y-%m-%d %H:%M:%S")
            .unwrap_or_default(),
        data,
        status,
        error: row.get(6)?,
        duration_ms: row.get::<_, i64>(7)? as u64,
        bytes_fetched: row.get::<_, i64>(8)? as u64,
    })
}
