use anyhow::Result;
use chrono::NaiveDateTime;
use rusqlite::{params, Connection};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use parking_lot::Mutex;

use crate::types::{ScrapeJob, ScrapeResult, ScrapeStatus};

const MAX_RESULTS: i64 = 2000;

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
                data_hash TEXT NOT NULL DEFAULT '',
                FOREIGN KEY (job_id) REFERENCES jobs(id)
            );

            CREATE INDEX IF NOT EXISTS idx_results_job_id ON results(job_id);
            CREATE INDEX IF NOT EXISTS idx_results_timestamp ON results(timestamp);
            ",
        )?;

        // migration v0 -> v1: data_hash for run dedup; fresh DBs already have it
        let has_hash: bool = conn
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('results') WHERE name = 'data_hash'",
                [],
                |r| r.get(0),
            )
            .unwrap_or(0)
            > 0;
        if !has_hash {
            conn.execute(
                "ALTER TABLE results ADD COLUMN data_hash TEXT NOT NULL DEFAULT ''",
                [],
            )?;
        }
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_results_dedup ON results(job_id, data_hash)",
            [],
        )?;
        conn.execute("PRAGMA user_version = 1", [])?;

        Ok(())
    }

    pub async fn save_result(&mut self, result: &ScrapeResult) -> Result<()> {
        let conn = self.conn.lock();
        let data_json = serde_json::to_string(&result.data)?;
        // ponytail: row-level dedup: an identical run (same canonical data) of
        // the same job is skipped; empty runs (failures) are never deduped.
        // Per-record overlap dedup needs a normalized table, add if it matters.
        let data_hash = if result.data.is_empty() {
            String::new()
        } else {
            canonical_hash(&result.data)
        };
        if !data_hash.is_empty() {
            let exists: bool = conn
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM results WHERE job_id = ?1 AND data_hash = ?2)",
                    params![result.job_id, &data_hash],
                    |r| r.get(0),
                )
                .unwrap_or(false);
            if exists {
                return Ok(());
            }
        }

        conn.execute(
            "INSERT INTO results (job_id, url, timestamp, data, status, error, duration_ms, bytes_fetched, data_hash)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                result.job_id,
                result.url,
                result.timestamp.to_string(),
                data_json,
                format!("{:?}", result.status),
                result.error,
                result.duration_ms as i64,
                result.bytes_fetched as i64,
                data_hash,
            ],
        )?;
        // ponytail: keep the table bounded on write; fixed cap, per-job caps if needed
        conn.execute(
            "DELETE FROM results WHERE id NOT IN \
             (SELECT id FROM results ORDER BY timestamp DESC LIMIT ?1)",
            params![MAX_RESULTS],
        )?;

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

    /// List rows without their data payload: cheap enough to poll every frame
    /// tick; the full row is loaded on demand via get_result_by_id.
    pub async fn get_results_meta(&self, limit: i64) -> Result<Vec<ScrapeResult>> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare(
            "SELECT id, job_id, url, timestamp, status, error, duration_ms, bytes_fetched,
                    COALESCE(json_array_length(data), 0)
             FROM results ORDER BY timestamp DESC LIMIT ?1",
        )?;

        let results = stmt
            .query_map(params![limit], map_meta_row)?
            .filter_map(|r| r.ok())
            .collect();

        Ok(results)
    }

    pub async fn get_result_by_id(&self, result_id: i64) -> Result<Option<ScrapeResult>> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare(
            "SELECT id, job_id, url, timestamp, data, status, error, duration_ms, bytes_fetched
             FROM results WHERE id = ?1",
        )?;
        let mut rows = stmt.query_map(params![result_id], map_result_row)?;
        Ok(rows.next().transpose()?)
    }

    pub async fn save_job(&self, job: &ScrapeJob) -> Result<()> {
        let conn = self.conn.lock();
        // UPSERT: preserves created_at across edits (INSERT OR REPLACE reset it)
        conn.execute(
            "INSERT INTO jobs (id, name, url, config, updated_at)
             VALUES (?1, ?2, ?3, ?4, datetime('now'))
             ON CONFLICT(id) DO UPDATE SET
                name = excluded.name,
                url = excluded.url,
                config = excluded.config,
                updated_at = excluded.updated_at",
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
        conn.execute("DELETE FROM results WHERE id = ?1", params![result_id])?;
        Ok(())
    }

    pub async fn prune_results(&self, keep: i64) -> Result<()> {
        let conn = self.conn.lock();
        conn.execute(
            "DELETE FROM results WHERE id NOT IN \
             (SELECT id FROM results ORDER BY timestamp DESC LIMIT ?1)",
            params![keep],
        )?;
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
                        next_link_selector: None,
                        concurrency: 1,
                        proxy: None,
                        user_agent: None,
                        auto_export: None,
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
    let record_count = data.len();
    let status_str: String = row.get(5)?;
    let status = match status_str.as_str() {
        "Success" => ScrapeStatus::Success,
        _ => ScrapeStatus::Failed,
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
        record_count,
    })
}

fn map_meta_row(row: &rusqlite::Row) -> rusqlite::Result<ScrapeResult> {
    let status_str: String = row.get(4)?;
    let status = match status_str.as_str() {
        "Success" => ScrapeStatus::Success,
        _ => ScrapeStatus::Failed,
    };
    Ok(ScrapeResult {
        id: row.get(0)?,
        job_id: row.get(1)?,
        url: row.get(2)?,
        timestamp: NaiveDateTime::parse_from_str(&row.get::<_, String>(3)?, "%Y-%m-%d %H:%M:%S")
            .unwrap_or_default(),
        data: Vec::new(),
        status,
        error: row.get(5)?,
        duration_ms: row.get::<_, i64>(6)? as u64,
        bytes_fetched: row.get::<_, i64>(7)? as u64,
        record_count: row.get::<_, i64>(8)? as usize,
    })
}

// FNV-1a 64: stable across app versions and processes, no crypto needed for
// dedup (collision chance is negligible at these row counts)
fn fnv1a64(input: &str) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for b in input.bytes() {
        hash ^= b as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn canonical_hash(records: &[HashMap<String, String>]) -> String {
    let mut map = serde_json::Map::new();
    for record in records {
        let mut keys: Vec<&String> = record.keys().collect();
        keys.sort();
        for k in keys {
            map.insert(
                k.clone(),
                serde_json::Value::String(record[k].clone()),
            );
        }
    }
    fnv1a64(&serde_json::to_string(&map).unwrap_or_default()).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ScrapeResult;

    fn make_result(job_id: &str, records: Vec<HashMap<String, String>>) -> ScrapeResult {
        let record_count = records.len();
        ScrapeResult {
            id: 0,
            job_id: job_id.into(),
            url: "http://x".into(),
            timestamp: chrono::NaiveDateTime::default(),
            data: records,
            status: ScrapeStatus::Success,
            error: None,
            duration_ms: 0,
            bytes_fetched: 0,
            record_count,
        }
    }

    fn record(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    // FK is enforced in bundled sqlite; the real flow always saves the job first
    async fn save_job_row(s: &Storage, id: &str) {
        let job = ScrapeJob {
            id: id.into(),
            name: id.into(),
            url: "http://x".into(),
            selectors: Vec::new(),
            headers: HashMap::new(),
            method: crate::types::HttpMethod::Get,
            body: None,
            interval_minutes: None,
            max_pages: Some(1),
            next_link_selector: None,
            concurrency: 1,
            proxy: None,
            user_agent: None,
            auto_export: None,
            enabled: true,
        };
        s.save_job(&job).await.unwrap();
    }

    #[tokio::test]
    async fn test_identical_run_is_skipped() {
        let tmp = std::env::temp_dir().join(format!("ds_dedup_{}", uuid::Uuid::new_v4()));
        let mut s = Storage::new(tmp.to_str().unwrap()).unwrap();
        save_job_row(&s, "j1").await;
        let r = make_result("j1", vec![record(&[("a", "1"), ("b", "2")])]);
        s.save_result(&r).await.unwrap();
        s.save_result(&r).await.unwrap();
        let all = s.get_all_results(10).await.unwrap();
        assert_eq!(all.len(), 1, "identical run must be skipped");
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn test_canonical_hash_ignores_map_order() {
        let tmp = std::env::temp_dir().join(format!("ds_dedup2_{}", uuid::Uuid::new_v4()));
        let mut s = Storage::new(tmp.to_str().unwrap()).unwrap();
        save_job_row(&s, "j2").await;
        let r1 = record(&[("a", "1"), ("b", "2")]);
        let r2 = record(&[("b", "2"), ("a", "1")]);
        // maps with different insertion orders are the same record
        assert_eq!(canonical_hash(&[r1.clone()]), canonical_hash(&[r2.clone()]));
        s.save_result(&make_result("j2", vec![r1.clone()]))
            .await
            .unwrap();
        s.save_result(&make_result("j2", vec![r2.clone()]))
            .await
            .unwrap();
        let all = s.get_all_results(10).await.unwrap();
        assert_eq!(all.len(), 1, "same data with different map order must dedup");
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn test_changed_data_is_stored() {
        let tmp = std::env::temp_dir().join(format!("ds_dedup3_{}", uuid::Uuid::new_v4()));
        let mut s = Storage::new(tmp.to_str().unwrap()).unwrap();
        save_job_row(&s, "j3").await;
        s.save_result(&make_result("j3", vec![record(&[("a", "1")])]))
            .await
            .unwrap();
        s.save_result(&make_result(
            "j3",
            vec![record(&[("a", "1")]), record(&[("a", "2")])],
        ))
        .await
        .unwrap();
        let all = s.get_all_results(10).await.unwrap();
        assert_eq!(all.len(), 2, "changed data is a new run");
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn test_failed_empty_runs_never_deduped() {
        let tmp = std::env::temp_dir().join(format!("ds_dedup4_{}", uuid::Uuid::new_v4()));
        let mut s = Storage::new(tmp.to_str().unwrap()).unwrap();
        save_job_row(&s, "j4").await;
        let mut f = make_result("j4", Vec::new());
        f.status = ScrapeStatus::Failed;
        f.error = Some("boom".into());
        s.save_result(&f).await.unwrap();
        s.save_result(&f).await.unwrap();
        let all = s.get_all_results(10).await.unwrap();
        assert_eq!(all.len(), 2, "failure runs are always recorded");
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn test_save_job_preserves_created_at() {
        let tmp = std::env::temp_dir().join(format!("ds_dedup5_{}", uuid::Uuid::new_v4()));
        let s = Storage::new(tmp.to_str().unwrap()).unwrap();
        let mut job = ScrapeJob {
            id: "j5".into(),
            name: "one".into(),
            url: "http://x".into(),
            selectors: Vec::new(),
            headers: HashMap::new(),
            method: crate::types::HttpMethod::Get,
            body: None,
            interval_minutes: None,
            max_pages: Some(1),
            next_link_selector: None,
            concurrency: 1,
            proxy: None,
            user_agent: None,
            auto_export: None,
            enabled: true,
        };
        s.save_job(&job).await.unwrap();
        let conn = s.conn.lock();
        let created: String = conn
            .query_row("SELECT created_at FROM jobs WHERE id = 'j5'", [], |r| r.get(0))
            .unwrap();
        drop(conn);
        job.name = "two".into();
        tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
        s.save_job(&job).await.unwrap();
        let conn = s.conn.lock();
        let created2: String = conn
            .query_row("SELECT created_at FROM jobs WHERE id = 'j5'", [], |r| r.get(0))
            .unwrap();
        assert_eq!(created, created2, "created_at must survive edits");
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn test_migration_adds_data_hash_to_old_db() {
        let tmp = std::env::temp_dir().join(format!("ds_migr_{}", uuid::Uuid::new_v4()));
        let old = rusqlite::Connection::open(&tmp).unwrap();
        old.execute_batch(
            "CREATE TABLE jobs (
                id TEXT PRIMARY KEY, name TEXT NOT NULL, url TEXT NOT NULL,
                config TEXT NOT NULL, created_at TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at TEXT NOT NULL DEFAULT (datetime('now')));
             CREATE TABLE results (
                id INTEGER PRIMARY KEY AUTOINCREMENT, job_id TEXT NOT NULL,
                url TEXT NOT NULL, timestamp TEXT NOT NULL, data TEXT NOT NULL,
                status TEXT NOT NULL, error TEXT, duration_ms INTEGER NOT NULL DEFAULT 0,
                bytes_fetched INTEGER NOT NULL DEFAULT 0);
             INSERT INTO jobs (id, name, url, config) VALUES ('old1', 'Old', 'http://x', '{}');
             INSERT INTO results (job_id, url, timestamp, data, status)
                VALUES ('old1', 'http://x', '2026-01-01 00:00:00', '[{\"a\":\"1\"}]', 'Success');",
        )
        .unwrap();
        drop(old);

        let s = Storage::new(tmp.to_str().unwrap()).unwrap();
        let all = s.get_all_results(10).await.unwrap();
        assert_eq!(all.len(), 1, "old rows survive the migration");
        assert_eq!(all[0].data[0].get("a").map(|s| s.as_str()), Some("1"));
        let jobs = s.get_all_jobs().await.unwrap();
        assert_eq!(jobs.len(), 1, "old jobs survive the migration");
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn test_meta_list_and_detail_fetch() {
        let tmp = std::env::temp_dir().join(format!("ds_meta_{}", uuid::Uuid::new_v4()));
        let mut s = Storage::new(tmp.to_str().unwrap()).unwrap();
        save_job_row(&s, "j6").await;
        s.save_result(&make_result("j6", vec![record(&[("a", "1")]), record(&[("b", "2")])]))
            .await
            .unwrap();

        let meta = s.get_results_meta(10).await.unwrap();
        assert_eq!(meta.len(), 1);
        assert!(meta[0].data.is_empty(), "meta rows carry no data payload");
        assert_eq!(meta[0].record_count, 2, "count comes from the query");

        let full = s.get_result_by_id(meta[0].id).await.unwrap().unwrap();
        assert_eq!(full.data.len(), 2, "detail fetch returns the payload");
        assert_eq!(full.record_count, 2);

        assert!(
            s.get_result_by_id(9999).await.unwrap().is_none(),
            "missing id returns None"
        );
        let _ = std::fs::remove_dir_all(&tmp);
    }
}
