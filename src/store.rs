//! Persistent metric storage backed by SQLite (bundled, so the binary stays
//! self-contained).
//!
//! Two layers of data, mirroring the design in `Entwurf.md`:
//!
//! * **Raw samples** — every collected value, kept only until "point A"
//!   ([`Store::prune_raw`] drops anything older).
//! * **Trends** — min/max/median rollups per time bucket, computed by the
//!   collector and persisted here. Trends are cheap and kept long-term.
//!
//! The connection lives behind `Arc<Mutex<_>>` so the store is `Clone` and can
//! be shared between the collector (writer) and web handlers (readers). All
//! methods are synchronous; async callers should wrap them in
//! `tokio::task::spawn_blocking`.

use std::path::Path;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use rusqlite::Connection;
use serde::Serialize;

use crate::model::{ContainerMetrics, HostMetrics};

/// Which metric a trend row describes. Stored as a short string so new metrics
/// (network, block I/O) can be added without a schema change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Metric {
    Cpu,
    Mem,
}

impl Metric {
    pub fn as_str(self) -> &'static str {
        match self {
            Metric::Cpu => "cpu",
            Metric::Mem => "mem",
        }
    }
}

/// A single min/max/median rollup over one time bucket for one container.
///
/// We store `name` and `stack` alongside the container `id` so history stays
/// meaningful when a container is recreated (e.g. `docker compose up` after a
/// `down` gives the same name a new id): the UI can present history by logical
/// service while the id still distinguishes individual instances.
#[derive(Debug, Clone, Serialize)]
pub struct ContainerTrend {
    pub bucket_start_ms: u64,
    pub bucket_secs: u64,
    pub id: String,
    pub name: String,
    pub stack: Option<String>,
    pub metric: &'static str,
    pub min: f64,
    pub max: f64,
    pub median: f64,
    pub samples: u32,
}

/// One raw host sample, as needed to seed the live charts on first page load.
#[derive(Debug, Clone, Serialize)]
pub struct HostPoint {
    pub ts_ms: u64,
    pub cpu_percent: f64,
    pub mem_used: Option<u64>,
}

/// A min/max/median rollup over one time bucket for the host.
#[derive(Debug, Clone, Serialize)]
pub struct HostTrend {
    pub bucket_start_ms: u64,
    pub bucket_secs: u64,
    pub metric: &'static str,
    pub min: f64,
    pub max: f64,
    pub median: f64,
    pub samples: u32,
}

#[derive(Clone)]
pub struct Store {
    conn: Arc<Mutex<Connection>>,
}

impl Store {
    /// Open (creating if needed) the database at `path` and run migrations.
    pub fn open(path: &Path) -> Result<Self> {
        let conn = Connection::open(path)
            .with_context(|| format!("opening database at {}", path.display()))?;
        Self::from_connection(conn)
    }

    /// Open an in-memory database — used by tests.
    #[cfg(test)]
    pub fn open_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory().context("opening in-memory database")?;
        Self::from_connection(conn)
    }

    fn from_connection(conn: Connection) -> Result<Self> {
        conn.pragma_update(None, "journal_mode", "WAL")
            .context("enabling WAL")?;
        conn.execute_batch(MIGRATIONS)
            .context("running migrations")?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Connection> {
        // The mutex only guards the connection; nothing held across the lock
        // can panic, so a poisoned lock is not expected. Recover regardless.
        self.conn
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Persist one collection cycle: host sample plus all container samples,
    /// in a single transaction.
    pub fn insert_samples(
        &self,
        ts_ms: u64,
        host: &HostMetrics,
        containers: &[ContainerMetrics],
    ) -> Result<()> {
        let mut conn = self.lock();
        let tx = conn.transaction().context("begin sample transaction")?;
        tx.execute(
            "INSERT INTO host_sample (ts_ms, cpu_percent, load1, load5, load15, mem_used, mem_total)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            rusqlite::params![
                ts_ms,
                f64::from(host.cpu_percent),
                host.load_avg.one,
                host.load_avg.five,
                host.load_avg.fifteen,
                host.mem_used,
                host.mem_total,
            ],
        )
        .context("insert host sample")?;

        {
            let mut stmt = tx
                .prepare_cached(
                    "INSERT INTO container_sample (ts_ms, id, name, cpu_percent, mem_used, mem_limit)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                )
                .context("prepare container insert")?;
            for c in containers {
                stmt.execute(rusqlite::params![
                    ts_ms,
                    c.id,
                    c.name,
                    c.cpu_percent,
                    c.mem_used,
                    c.mem_limit,
                ])
                .context("insert container sample")?;
            }
        }
        tx.commit().context("commit sample transaction")?;
        Ok(())
    }

    /// Persist a batch of container trend rollups.
    pub fn insert_container_trends(&self, trends: &[ContainerTrend]) -> Result<()> {
        if trends.is_empty() {
            return Ok(());
        }
        let mut conn = self.lock();
        let tx = conn.transaction().context("begin trend transaction")?;
        {
            let mut stmt = tx
                .prepare_cached(
                    "INSERT INTO container_trend
                       (bucket_start_ms, bucket_secs, id, name, stack, metric, min, max, median, samples)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                )
                .context("prepare container trend insert")?;
            for t in trends {
                stmt.execute(rusqlite::params![
                    t.bucket_start_ms,
                    t.bucket_secs,
                    t.id,
                    t.name,
                    t.stack,
                    t.metric,
                    t.min,
                    t.max,
                    t.median,
                    t.samples,
                ])
                .context("insert container trend")?;
            }
        }
        tx.commit().context("commit trend transaction")?;
        Ok(())
    }

    /// Persist a batch of host trend rollups.
    pub fn insert_host_trends(&self, trends: &[HostTrend]) -> Result<()> {
        if trends.is_empty() {
            return Ok(());
        }
        let mut conn = self.lock();
        let tx = conn.transaction().context("begin host trend transaction")?;
        {
            let mut stmt = tx
                .prepare_cached(
                    "INSERT INTO host_trend
                       (bucket_start_ms, bucket_secs, metric, min, max, median, samples)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                )
                .context("prepare host trend insert")?;
            for t in trends {
                stmt.execute(rusqlite::params![
                    t.bucket_start_ms,
                    t.bucket_secs,
                    t.metric,
                    t.min,
                    t.max,
                    t.median,
                    t.samples,
                ])
                .context("insert host trend")?;
            }
        }
        tx.commit().context("commit host trend transaction")?;
        Ok(())
    }

    /// Delete raw samples (host and container) older than `before_ms`. This is
    /// "point A": trends survive, raw data is dropped. Returns the number of
    /// rows removed.
    pub fn prune_raw(&self, before_ms: u64) -> Result<usize> {
        let conn = self.lock();
        let host = conn
            .execute("DELETE FROM host_sample WHERE ts_ms < ?1", [before_ms])
            .context("prune host samples")?;
        let containers = conn
            .execute("DELETE FROM container_sample WHERE ts_ms < ?1", [before_ms])
            .context("prune container samples")?;
        Ok(host + containers)
    }

    /// Delete trend rollups (host and container) whose bucket starts before
    /// `before_ms`. This is the separate, longer trend retention: trends from
    /// long-gone containers age out here, independent of "point A" for raw
    /// data. Returns the number of rows removed.
    pub fn prune_trends(&self, before_ms: u64) -> Result<usize> {
        let conn = self.lock();
        let host = conn
            .execute(
                "DELETE FROM host_trend WHERE bucket_start_ms < ?1",
                [before_ms],
            )
            .context("prune host trends")?;
        let containers = conn
            .execute(
                "DELETE FROM container_trend WHERE bucket_start_ms < ?1",
                [before_ms],
            )
            .context("prune container trends")?;
        Ok(host + containers)
    }

    /// Raw host samples collected at or after `since_ms`, oldest first. Used to
    /// seed the live charts when a page first loads.
    pub fn recent_host_samples(&self, since_ms: u64) -> Result<Vec<HostPoint>> {
        let conn = self.lock();
        let mut stmt = conn
            .prepare_cached(
                "SELECT ts_ms, cpu_percent, mem_used
                 FROM host_sample WHERE ts_ms >= ?1 ORDER BY ts_ms ASC",
            )
            .context("prepare recent host query")?;
        let rows = stmt
            .query_map([since_ms], |r| {
                Ok(HostPoint {
                    ts_ms: r.get(0)?,
                    cpu_percent: r.get(1)?,
                    mem_used: r.get(2)?,
                })
            })
            .context("query recent host samples")?
            .collect::<rusqlite::Result<Vec<_>>>()
            .context("collect recent host samples")?;
        Ok(rows)
    }

    /// Count rows in a table — test/diagnostic helper.
    #[cfg(test)]
    pub fn count(&self, table: &str) -> Result<u64> {
        let conn = self.lock();
        let n: u64 = conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r.get(0))?;
        Ok(n)
    }
}

const MIGRATIONS: &str = "
CREATE TABLE IF NOT EXISTS host_sample (
    ts_ms       INTEGER NOT NULL,
    cpu_percent REAL    NOT NULL,
    load1       REAL,
    load5       REAL,
    load15      REAL,
    mem_used    INTEGER,
    mem_total   INTEGER
);
CREATE INDEX IF NOT EXISTS host_sample_ts ON host_sample(ts_ms);

CREATE TABLE IF NOT EXISTS container_sample (
    ts_ms       INTEGER NOT NULL,
    id          TEXT    NOT NULL,
    name        TEXT    NOT NULL,
    cpu_percent REAL,
    mem_used    INTEGER,
    mem_limit   INTEGER
);
CREATE INDEX IF NOT EXISTS container_sample_id_ts ON container_sample(id, ts_ms);

CREATE TABLE IF NOT EXISTS host_trend (
    bucket_start_ms INTEGER NOT NULL,
    bucket_secs     INTEGER NOT NULL,
    metric          TEXT    NOT NULL,
    min             REAL    NOT NULL,
    max             REAL    NOT NULL,
    median          REAL    NOT NULL,
    samples         INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS host_trend_ts ON host_trend(metric, bucket_start_ms);

CREATE TABLE IF NOT EXISTS container_trend (
    bucket_start_ms INTEGER NOT NULL,
    bucket_secs     INTEGER NOT NULL,
    id              TEXT    NOT NULL,
    name            TEXT    NOT NULL,
    stack           TEXT,
    metric          TEXT    NOT NULL,
    min             REAL    NOT NULL,
    max             REAL    NOT NULL,
    median          REAL    NOT NULL,
    samples         INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS container_trend_ts ON container_trend(id, metric, bucket_start_ms);
CREATE INDEX IF NOT EXISTS container_trend_name ON container_trend(name, metric, bucket_start_ms);
";

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ContainerState, HealthState, LoadAverage};

    fn host_sample(cpu: f32) -> HostMetrics {
        HostMetrics {
            cpu_percent: cpu,
            load_avg: LoadAverage {
                one: 1.0,
                five: 0.5,
                fifteen: 0.25,
            },
            mem_total: 1000,
            mem_used: 400,
            cpu_count: 8,
        }
    }

    fn container_sample(id: &str, cpu: Option<f64>) -> ContainerMetrics {
        ContainerMetrics {
            id: id.to_string(),
            name: format!("c-{id}"),
            image: "img:latest".to_string(),
            state: ContainerState::Running,
            status: "Up".to_string(),
            health: HealthState::None,
            stack: None,
            cpu_percent: cpu,
            mem_used: Some(123),
            mem_limit: Some(456),
        }
    }

    #[test]
    fn insert_and_prune_roundtrip() {
        let store = Store::open_in_memory().unwrap();
        let containers = vec![
            container_sample("a", Some(1.0)),
            container_sample("b", None),
        ];

        store
            .insert_samples(1_000, &host_sample(10.0), &containers)
            .unwrap();
        store
            .insert_samples(5_000, &host_sample(20.0), &containers)
            .unwrap();

        assert_eq!(store.count("host_sample").unwrap(), 2);
        assert_eq!(store.count("container_sample").unwrap(), 4);

        // Prune everything strictly before ts 5000 → first cycle dropped.
        let removed = store.prune_raw(5_000).unwrap();
        assert_eq!(removed, 1 + 2);
        assert_eq!(store.count("host_sample").unwrap(), 1);
        assert_eq!(store.count("container_sample").unwrap(), 2);
    }

    #[test]
    fn insert_trends_roundtrip() {
        let store = Store::open_in_memory().unwrap();
        store
            .insert_container_trends(&[ContainerTrend {
                bucket_start_ms: 0,
                bucket_secs: 60,
                id: "a".to_string(),
                name: "c-a".to_string(),
                stack: Some("web".to_string()),
                metric: Metric::Cpu.as_str(),
                min: 1.0,
                max: 9.0,
                median: 4.0,
                samples: 20,
            }])
            .unwrap();
        store
            .insert_host_trends(&[HostTrend {
                bucket_start_ms: 0,
                bucket_secs: 60,
                metric: Metric::Mem.as_str(),
                min: 100.0,
                max: 900.0,
                median: 400.0,
                samples: 20,
            }])
            .unwrap();
        assert_eq!(store.count("container_trend").unwrap(), 1);
        assert_eq!(store.count("host_trend").unwrap(), 1);
    }

    #[test]
    fn recent_host_samples_filters_and_orders() {
        let store = Store::open_in_memory().unwrap();
        store
            .insert_samples(5_000, &host_sample(20.0), &[])
            .unwrap();
        store
            .insert_samples(1_000, &host_sample(10.0), &[])
            .unwrap();
        store
            .insert_samples(9_000, &host_sample(30.0), &[])
            .unwrap();

        let points = store.recent_host_samples(5_000).unwrap();
        assert_eq!(points.len(), 2);
        // Oldest first, only ts >= 5000.
        assert_eq!(points[0].ts_ms, 5_000);
        assert_eq!(points[1].ts_ms, 9_000);
    }

    #[test]
    fn prune_trends_drops_old_buckets() {
        let store = Store::open_in_memory().unwrap();
        let trend = |start: u64| ContainerTrend {
            bucket_start_ms: start,
            bucket_secs: 60,
            id: "a".to_string(),
            name: "c-a".to_string(),
            stack: None,
            metric: Metric::Cpu.as_str(),
            min: 1.0,
            max: 2.0,
            median: 1.5,
            samples: 10,
        };
        store
            .insert_container_trends(&[trend(1_000), trend(50_000)])
            .unwrap();
        let removed = store.prune_trends(50_000).unwrap();
        assert_eq!(removed, 1);
        assert_eq!(store.count("container_trend").unwrap(), 1);
    }
}
