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
/// can be added without a trend-schema change (the `metric` column is text).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Metric {
    Cpu,
    Mem,
    /// Network receive rate, bytes/second.
    NetRx,
    /// Network transmit rate, bytes/second.
    NetTx,
    /// Block-device read rate, bytes/second.
    DiskRead,
    /// Block-device write rate, bytes/second.
    DiskWrite,
}

impl Metric {
    pub fn as_str(self) -> &'static str {
        match self {
            Metric::Cpu => "cpu",
            Metric::Mem => "mem",
            Metric::NetRx => "net_rx",
            Metric::NetTx => "net_tx",
            Metric::DiskRead => "disk_read",
            Metric::DiskWrite => "disk_write",
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

/// One raw metric point at a timestamp, used to seed the live charts on first
/// page load and streamed live over SSE. Shared by host and per-container
/// series; the I/O rates are `None` for the host (which has no per-container
/// I/O) and bytes/second elsewhere.
#[derive(Debug, Clone, Serialize)]
pub struct MetricPoint {
    pub ts_ms: u64,
    pub cpu_percent: f64,
    pub mem_used: Option<u64>,
    pub net_rx: Option<f64>,
    pub net_tx: Option<f64>,
    pub disk_read: Option<f64>,
    pub disk_write: Option<f64>,
}

/// One bucket of the history view: median line plus min–max envelope per
/// metric. A field is `None` when the bucket has no rows for that metric (e.g.
/// the host, which has no per-container I/O, or a container the runtime reports
/// no block-I/O for). The network and disk metrics are dual-line (rx/tx,
/// read/write), so each direction has its own envelope.
#[derive(Debug, Clone, Serialize)]
pub struct HistoryPoint {
    pub ts_ms: u64,
    pub cpu_min: Option<f64>,
    pub cpu_med: Option<f64>,
    pub cpu_max: Option<f64>,
    pub mem_min: Option<f64>,
    pub mem_med: Option<f64>,
    pub mem_max: Option<f64>,
    pub net_rx_min: Option<f64>,
    pub net_rx_med: Option<f64>,
    pub net_rx_max: Option<f64>,
    pub net_tx_min: Option<f64>,
    pub net_tx_med: Option<f64>,
    pub net_tx_max: Option<f64>,
    pub disk_read_min: Option<f64>,
    pub disk_read_med: Option<f64>,
    pub disk_read_max: Option<f64>,
    pub disk_write_min: Option<f64>,
    pub disk_write_med: Option<f64>,
    pub disk_write_max: Option<f64>,
}

impl HistoryPoint {
    /// Lift a raw sample into the history shape: a single value is its own
    /// minimum, median and maximum. Used for ranges inside the raw retention,
    /// where we have full-resolution data instead of trend buckets.
    pub fn from_raw(p: &MetricPoint) -> Self {
        #[allow(clippy::cast_precision_loss)] // chart data; precision is moot
        let mem = p.mem_used.map(|m| m as f64);
        // A raw point's single value is its own min/med/max, so each envelope
        // collapses to the point's value (`None` stays an empty envelope).
        Self {
            ts_ms: p.ts_ms,
            cpu_min: Some(p.cpu_percent),
            cpu_med: Some(p.cpu_percent),
            cpu_max: Some(p.cpu_percent),
            mem_min: mem,
            mem_med: mem,
            mem_max: mem,
            net_rx_min: p.net_rx,
            net_rx_med: p.net_rx,
            net_rx_max: p.net_rx,
            net_tx_min: p.net_tx,
            net_tx_med: p.net_tx,
            net_tx_max: p.net_tx,
            disk_read_min: p.disk_read,
            disk_read_med: p.disk_read,
            disk_read_max: p.disk_read,
            disk_write_min: p.disk_write,
            disk_write_med: p.disk_write,
            disk_write_max: p.disk_write,
        }
    }
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
        // We use a single connection behind a Mutex, so all access is already
        // serialised — WAL's concurrent-reader benefit doesn't apply, and its
        // side files (`-wal`/`-shm`) only grow without paying for themselves.
        // The default rollback journal keeps no persistent extra files. Setting
        // it explicitly also migrates any database left in WAL mode by an
        // earlier build, cleaning up its stale `-wal`/`-shm`.
        conn.pragma_update(None, "journal_mode", "DELETE")
            .context("setting rollback journal mode")?;
        conn.execute_batch(MIGRATIONS)
            .context("running migrations")?;
        // Columns added after the first release: present in the CREATE above for
        // fresh databases, added here for ones created by an earlier build.
        for (col, decl) in [
            ("net_rx", "REAL"),
            ("net_tx", "REAL"),
            ("disk_read", "REAL"),
            ("disk_write", "REAL"),
        ] {
            add_column_if_missing(&conn, "container_sample", col, decl)?;
        }
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
                to_db(ts_ms),
                f64::from(host.cpu_percent),
                host.load_avg.one,
                host.load_avg.five,
                host.load_avg.fifteen,
                to_db(host.mem_used),
                to_db(host.mem_total),
            ],
        )
        .context("insert host sample")?;

        {
            let mut stmt = tx
                .prepare_cached(
                    "INSERT INTO container_sample
                       (ts_ms, id, name, cpu_percent, mem_used, mem_limit,
                        net_rx, net_tx, disk_read, disk_write)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                )
                .context("prepare container insert")?;
            for c in containers {
                stmt.execute(rusqlite::params![
                    to_db(ts_ms),
                    c.id,
                    c.name,
                    c.cpu_percent,
                    c.mem_used.map(to_db),
                    c.mem_limit.map(to_db),
                    c.net_rx_bps,
                    c.net_tx_bps,
                    c.disk_read_bps,
                    c.disk_write_bps,
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
                    to_db(t.bucket_start_ms),
                    to_db(t.bucket_secs),
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
                    to_db(t.bucket_start_ms),
                    to_db(t.bucket_secs),
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
            .execute(
                "DELETE FROM host_sample WHERE ts_ms < ?1",
                [to_db(before_ms)],
            )
            .context("prune host samples")?;
        let containers = conn
            .execute(
                "DELETE FROM container_sample WHERE ts_ms < ?1",
                [to_db(before_ms)],
            )
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
                [to_db(before_ms)],
            )
            .context("prune host trends")?;
        let containers = conn
            .execute(
                "DELETE FROM container_trend WHERE bucket_start_ms < ?1",
                [to_db(before_ms)],
            )
            .context("prune container trends")?;
        Ok(host + containers)
    }

    /// Raw host samples collected at or after `since_ms`, oldest first. Used to
    /// seed the host charts when a page first loads.
    pub fn recent_host_samples(&self, since_ms: u64) -> Result<Vec<MetricPoint>> {
        let conn = self.lock();
        let mut stmt = conn
            .prepare_cached(
                "SELECT ts_ms, cpu_percent, mem_used, NULL, NULL, NULL, NULL
                 FROM host_sample WHERE ts_ms >= ?1 ORDER BY ts_ms ASC",
            )
            .context("prepare recent host query")?;
        let rows = stmt
            .query_map([to_db(since_ms)], row_to_point)
            .context("query recent host samples")?
            .collect::<rusqlite::Result<Vec<_>>>()
            .context("collect recent host samples")?;
        Ok(rows)
    }

    /// Raw samples for one container at or after `since_ms`, oldest first. Used
    /// to seed a container detail page's charts.
    pub fn recent_container_samples(&self, id: &str, since_ms: u64) -> Result<Vec<MetricPoint>> {
        let conn = self.lock();
        let mut stmt = conn
            .prepare_cached(
                "SELECT ts_ms, cpu_percent, mem_used, net_rx, net_tx, disk_read, disk_write
                 FROM container_sample WHERE id = ?1 AND ts_ms >= ?2 ORDER BY ts_ms ASC",
            )
            .context("prepare recent container query")?;
        let rows = stmt
            .query_map(rusqlite::params![id, to_db(since_ms)], row_to_point)
            .context("query recent container samples")?
            .collect::<rusqlite::Result<Vec<_>>>()
            .context("collect recent container samples")?;
        Ok(rows)
    }

    /// Aggregate trend history for a whole stack at or after `since_ms`, oldest
    /// first, as chart seed points. Stacks have no raw per-stack series, so the
    /// detail page seeds from trends: per bucket we sum the member medians
    /// (mirroring the live aggregate, which sums current member values). Trends
    /// are stored long (one row per metric), so we pivot cpu/mem into one point.
    pub fn recent_stack_trends(&self, stack: &str, since_ms: u64) -> Result<Vec<MetricPoint>> {
        let conn = self.lock();
        let mut stmt = conn
            .prepare_cached(
                "SELECT bucket_start_ms,
                        SUM(CASE WHEN metric = 'cpu' THEN median ELSE 0 END) AS cpu,
                        SUM(CASE WHEN metric = 'mem' THEN median ELSE 0 END) AS mem,
                        SUM(CASE WHEN metric = 'net_rx' THEN median END) AS net_rx,
                        SUM(CASE WHEN metric = 'net_tx' THEN median END) AS net_tx,
                        SUM(CASE WHEN metric = 'disk_read' THEN median END) AS disk_read,
                        SUM(CASE WHEN metric = 'disk_write' THEN median END) AS disk_write
                 FROM container_trend
                 WHERE stack = ?1 AND bucket_start_ms >= ?2
                 GROUP BY bucket_start_ms
                 ORDER BY bucket_start_ms ASC",
            )
            .context("prepare recent stack trend query")?;
        let rows = stmt
            .query_map(
                rusqlite::params![stack, to_db(since_ms)],
                trend_row_to_point,
            )
            .context("query recent stack trends")?
            .collect::<rusqlite::Result<Vec<_>>>()
            .context("collect recent stack trends")?;
        Ok(rows)
    }

    /// Host trend history between `since_ms` and `until_ms` (inclusive),
    /// downsampled into `group_ms` windows, oldest first. Within a window the
    /// envelope keeps the extremes (MIN of minima, MAX of maxima) while the
    /// line averages the bucket medians — an approximation of the true median
    /// that is fine for display.
    pub fn history_host(
        &self,
        since_ms: u64,
        until_ms: u64,
        group_ms: u64,
    ) -> Result<Vec<HistoryPoint>> {
        let conn = self.lock();
        let mut stmt = conn
            .prepare_cached(&format!(
                "SELECT (bucket_start_ms / ?3) * ?3 AS bucket, {HISTORY_ENVELOPE}
                 FROM host_trend
                 WHERE bucket_start_ms >= ?1 AND bucket_start_ms <= ?2
                 GROUP BY bucket ORDER BY bucket ASC"
            ))
            .context("prepare host history query")?;
        let rows = stmt
            .query_map(
                [to_db(since_ms), to_db(until_ms), to_db(group_ms.max(1))],
                history_row_to_point,
            )
            .context("query host history")?
            .collect::<rusqlite::Result<Vec<_>>>()
            .context("collect host history")?;
        Ok(rows)
    }

    /// Trend history for one container, downsampled like [`Self::history_host`].
    pub fn history_container(
        &self,
        id: &str,
        since_ms: u64,
        until_ms: u64,
        group_ms: u64,
    ) -> Result<Vec<HistoryPoint>> {
        let conn = self.lock();
        let mut stmt = conn
            .prepare_cached(&format!(
                "SELECT (bucket_start_ms / ?4) * ?4 AS bucket, {HISTORY_ENVELOPE}
                 FROM container_trend
                 WHERE id = ?1 AND bucket_start_ms >= ?2 AND bucket_start_ms <= ?3
                 GROUP BY bucket ORDER BY bucket ASC"
            ))
            .context("prepare container history query")?;
        let rows = stmt
            .query_map(
                rusqlite::params![id, to_db(since_ms), to_db(until_ms), to_db(group_ms.max(1))],
                history_row_to_point,
            )
            .context("query container history")?
            .collect::<rusqlite::Result<Vec<_>>>()
            .context("collect container history")?;
        Ok(rows)
    }

    /// Trend history for a whole stack: per trend bucket the member values are
    /// summed (mirroring the live aggregate and [`Self::recent_stack_trends`]),
    /// then the summed buckets are downsampled like [`Self::history_host`].
    pub fn history_stack(
        &self,
        stack: &str,
        since_ms: u64,
        until_ms: u64,
        group_ms: u64,
    ) -> Result<Vec<HistoryPoint>> {
        let conn = self.lock();
        let mut stmt = conn
            .prepare_cached(&format!(
                "WITH per_bucket AS (
                     SELECT bucket_start_ms AS b, {STACK_SUM_PER_BUCKET}
                     FROM container_trend
                     WHERE stack = ?1 AND bucket_start_ms >= ?2 AND bucket_start_ms <= ?3
                     GROUP BY b
                 )
                 SELECT (b / ?4) * ?4 AS bucket, {STACK_OUTER_ENVELOPE}
                 FROM per_bucket
                 GROUP BY bucket ORDER BY bucket ASC"
            ))
            .context("prepare stack history query")?;
        let rows = stmt
            .query_map(
                rusqlite::params![
                    stack,
                    to_db(since_ms),
                    to_db(until_ms),
                    to_db(group_ms.max(1))
                ],
                history_row_to_point,
            )
            .context("query stack history")?
            .collect::<rusqlite::Result<Vec<_>>>()
            .context("collect stack history")?;
        Ok(rows)
    }

    /// Count rows in a table — test/diagnostic helper.
    #[cfg(test)]
    pub fn count(&self, table: &str) -> Result<u64> {
        let conn = self.lock();
        let n: i64 = conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r.get(0))?;
        Ok(from_db(n))
    }
}

/// SQLite stores signed 64-bit integers, and rusqlite 0.40 dropped `u64`
/// binding to avoid silent overflow. Our counters (Unix-ms timestamps, byte
/// counts, bucket widths) are always well within `i64` range, so we convert at
/// the storage boundary. `saturating` rather than panicking keeps a freak value
/// from taking down the collector.
/// Add `column` to `table` if it isn't there yet, so databases from earlier
/// builds gain columns introduced later. `ALTER TABLE ADD COLUMN` errors if the
/// column already exists, so we check `table_info` first.
fn add_column_if_missing(conn: &Connection, table: &str, column: &str, decl: &str) -> Result<()> {
    let mut stmt = conn
        .prepare(&format!("PRAGMA table_info({table})"))
        .with_context(|| format!("reading columns of {table}"))?;
    let exists = stmt
        .query_map([], |r| r.get::<_, String>(1))?
        .filter_map(std::result::Result::ok)
        .any(|name| name == column);
    if !exists {
        conn.execute(
            &format!("ALTER TABLE {table} ADD COLUMN {column} {decl}"),
            [],
        )
        .with_context(|| format!("adding column {column} to {table}"))?;
    }
    Ok(())
}

fn to_db(v: u64) -> i64 {
    i64::try_from(v).unwrap_or(i64::MAX)
}

fn from_db(v: i64) -> u64 {
    u64::try_from(v).unwrap_or(0)
}

/// Map a `(ts_ms, cpu_percent, mem_used, net_rx, net_tx, disk_read, disk_write)`
/// row to a [`MetricPoint`]. The I/O columns are `NULL` for host samples.
fn row_to_point(r: &rusqlite::Row<'_>) -> rusqlite::Result<MetricPoint> {
    Ok(MetricPoint {
        ts_ms: from_db(r.get::<_, i64>(0)?),
        cpu_percent: r.get(1)?,
        mem_used: r.get::<_, Option<i64>>(2)?.map(from_db),
        net_rx: r.get(3)?,
        net_tx: r.get(4)?,
        disk_read: r.get(5)?,
        disk_write: r.get(6)?,
    })
}

/// Map a pivoted trend row `(bucket_start_ms, cpu, mem)` to a [`MetricPoint`].
/// Trend medians are floating-point (and summed across members for stacks), so
/// cpu/mem come back as `REAL`; memory is rounded back to whole bytes.
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn trend_row_to_point(r: &rusqlite::Row<'_>) -> rusqlite::Result<MetricPoint> {
    let mem: Option<f64> = r.get(2)?;
    Ok(MetricPoint {
        ts_ms: from_db(r.get::<_, i64>(0)?),
        cpu_percent: r.get::<_, Option<f64>>(1)?.unwrap_or(0.0),
        mem_used: mem.map(|m| m.round().max(0.0) as u64),
        net_rx: r.get(3)?,
        net_tx: r.get(4)?,
        disk_read: r.get(5)?,
        disk_write: r.get(6)?,
    })
}

/// Map a downsampled history row to a [`HistoryPoint`]. All value columns are
/// nullable: a CASE without ELSE yields NULL for the other metric's rows, and
/// MIN/AVG/MAX ignore NULLs but return NULL when nothing matched.
fn history_row_to_point(r: &rusqlite::Row<'_>) -> rusqlite::Result<HistoryPoint> {
    Ok(HistoryPoint {
        ts_ms: from_db(r.get::<_, i64>(0)?),
        cpu_min: r.get(1)?,
        cpu_med: r.get(2)?,
        cpu_max: r.get(3)?,
        mem_min: r.get(4)?,
        mem_med: r.get(5)?,
        mem_max: r.get(6)?,
        net_rx_min: r.get(7)?,
        net_rx_med: r.get(8)?,
        net_rx_max: r.get(9)?,
        net_tx_min: r.get(10)?,
        net_tx_med: r.get(11)?,
        net_tx_max: r.get(12)?,
        disk_read_min: r.get(13)?,
        disk_read_med: r.get(14)?,
        disk_read_max: r.get(15)?,
        disk_write_min: r.get(16)?,
        disk_write_med: r.get(17)?,
        disk_write_max: r.get(18)?,
    })
}

/// The per-metric envelope columns shared by the host and container history
/// queries: for each metric, `MIN(min) AVG(median) MAX(max)` over the
/// downsampling group, in the column order [`history_row_to_point`] expects.
/// Metrics a table never stores (net/disk for the host) come back as NULL.
const HISTORY_ENVELOPE: &str = "\
    MIN(CASE WHEN metric='cpu' THEN min END), AVG(CASE WHEN metric='cpu' THEN median END), MAX(CASE WHEN metric='cpu' THEN max END),
    MIN(CASE WHEN metric='mem' THEN min END), AVG(CASE WHEN metric='mem' THEN median END), MAX(CASE WHEN metric='mem' THEN max END),
    MIN(CASE WHEN metric='net_rx' THEN min END), AVG(CASE WHEN metric='net_rx' THEN median END), MAX(CASE WHEN metric='net_rx' THEN max END),
    MIN(CASE WHEN metric='net_tx' THEN min END), AVG(CASE WHEN metric='net_tx' THEN median END), MAX(CASE WHEN metric='net_tx' THEN max END),
    MIN(CASE WHEN metric='disk_read' THEN min END), AVG(CASE WHEN metric='disk_read' THEN median END), MAX(CASE WHEN metric='disk_read' THEN max END),
    MIN(CASE WHEN metric='disk_write' THEN min END), AVG(CASE WHEN metric='disk_write' THEN median END), MAX(CASE WHEN metric='disk_write' THEN max END)";

/// Stack history sums members per bucket before downsampling. These are the
/// per-bucket sums (the CTE body): each metric's min/median/max summed across
/// the stack's members, aliased for the outer envelope below.
const STACK_SUM_PER_BUCKET: &str = "\
    SUM(CASE WHEN metric='cpu' THEN min END) AS cpu_min, SUM(CASE WHEN metric='cpu' THEN median END) AS cpu_med, SUM(CASE WHEN metric='cpu' THEN max END) AS cpu_max,
    SUM(CASE WHEN metric='mem' THEN min END) AS mem_min, SUM(CASE WHEN metric='mem' THEN median END) AS mem_med, SUM(CASE WHEN metric='mem' THEN max END) AS mem_max,
    SUM(CASE WHEN metric='net_rx' THEN min END) AS net_rx_min, SUM(CASE WHEN metric='net_rx' THEN median END) AS net_rx_med, SUM(CASE WHEN metric='net_rx' THEN max END) AS net_rx_max,
    SUM(CASE WHEN metric='net_tx' THEN min END) AS net_tx_min, SUM(CASE WHEN metric='net_tx' THEN median END) AS net_tx_med, SUM(CASE WHEN metric='net_tx' THEN max END) AS net_tx_max,
    SUM(CASE WHEN metric='disk_read' THEN min END) AS disk_read_min, SUM(CASE WHEN metric='disk_read' THEN median END) AS disk_read_med, SUM(CASE WHEN metric='disk_read' THEN max END) AS disk_read_max,
    SUM(CASE WHEN metric='disk_write' THEN min END) AS disk_write_min, SUM(CASE WHEN metric='disk_write' THEN median END) AS disk_write_med, SUM(CASE WHEN metric='disk_write' THEN max END) AS disk_write_max";

/// The outer envelope over the summed per-bucket columns above, in the column
/// order [`history_row_to_point`] expects.
const STACK_OUTER_ENVELOPE: &str = "\
    MIN(cpu_min), AVG(cpu_med), MAX(cpu_max),
    MIN(mem_min), AVG(mem_med), MAX(mem_max),
    MIN(net_rx_min), AVG(net_rx_med), MAX(net_rx_max),
    MIN(net_tx_min), AVG(net_tx_med), MAX(net_tx_max),
    MIN(disk_read_min), AVG(disk_read_med), MAX(disk_read_max),
    MIN(disk_write_min), AVG(disk_write_med), MAX(disk_write_max)";

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
    mem_limit   INTEGER,
    net_rx      REAL,
    net_tx      REAL,
    disk_read   REAL,
    disk_write  REAL
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
            net_rx_bps: Some(10.0),
            net_tx_bps: Some(20.0),
            disk_read_bps: Some(30.0),
            disk_write_bps: Some(40.0),
            ports: Vec::new(),
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
    fn container_samples_roundtrip_io_rates() {
        let store = Store::open_in_memory().unwrap();
        store
            .insert_samples(
                1_000,
                &host_sample(10.0),
                &[container_sample("a", Some(1.0))],
            )
            .unwrap();
        let points = store.recent_container_samples("a", 0).unwrap();
        assert_eq!(points.len(), 1);
        let p = &points[0];
        assert_eq!(p.net_rx, Some(10.0));
        assert_eq!(p.net_tx, Some(20.0));
        assert_eq!(p.disk_read, Some(30.0));
        assert_eq!(p.disk_write, Some(40.0));
        // Host samples carry no per-container I/O.
        let host = store.recent_host_samples(0).unwrap();
        assert_eq!(host[0].net_rx, None);
        assert_eq!(host[0].disk_write, None);
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
    fn recent_stack_trends_sums_members_and_pivots() {
        let store = Store::open_in_memory().unwrap();
        let ct = |bucket: u64, id: &str, stack: &str, metric, median: f64| ContainerTrend {
            bucket_start_ms: bucket,
            bucket_secs: 60,
            id: id.to_string(),
            name: format!("c-{id}"),
            stack: Some(stack.to_string()),
            metric,
            min: median,
            max: median,
            median,
            samples: 20,
        };
        store
            .insert_container_trends(&[
                // bucket 0: web → cpu 4+6=10, mem 100+200=300
                ct(0, "a", "web", Metric::Cpu.as_str(), 4.0),
                ct(0, "a", "web", Metric::Mem.as_str(), 100.0),
                ct(0, "b", "web", Metric::Cpu.as_str(), 6.0),
                ct(0, "b", "web", Metric::Mem.as_str(), 200.0),
                // bucket 60_000: web → cpu 2+3=5, mem 150+250=400
                ct(60_000, "a", "web", Metric::Cpu.as_str(), 2.0),
                ct(60_000, "a", "web", Metric::Mem.as_str(), 150.0),
                ct(60_000, "b", "web", Metric::Cpu.as_str(), 3.0),
                ct(60_000, "b", "web", Metric::Mem.as_str(), 250.0),
                // network rates pivot and sum like cpu/mem: rx 5+15=20
                ct(0, "a", "web", Metric::NetRx.as_str(), 5.0),
                ct(0, "b", "web", Metric::NetRx.as_str(), 15.0),
                // a different stack must not leak into the sum
                ct(0, "c", "db", Metric::Cpu.as_str(), 99.0),
            ])
            .unwrap();

        let points = store.recent_stack_trends("web", 0).unwrap();
        assert_eq!(points.len(), 2);
        assert_eq!(points[0].ts_ms, 0);
        assert!((points[0].cpu_percent - 10.0).abs() < 1e-9);
        assert_eq!(points[0].mem_used, Some(300));
        assert_eq!(points[0].net_rx, Some(20.0));
        // No disk trends inserted → the pivot yields NULL → None.
        assert_eq!(points[0].disk_read, None);
        // Second bucket has no network rows at all.
        assert_eq!(points[1].net_rx, None);
        assert_eq!(points[1].ts_ms, 60_000);
        assert!((points[1].cpu_percent - 5.0).abs() < 1e-9);
        assert_eq!(points[1].mem_used, Some(400));

        // since_ms filters out older buckets.
        let recent = store.recent_stack_trends("web", 60_000).unwrap();
        assert_eq!(recent.len(), 1);
        assert_eq!(recent[0].ts_ms, 60_000);
    }

    #[test]
    fn history_host_downsamples_into_groups() {
        let store = Store::open_in_memory().unwrap();
        let ht = |bucket: u64, metric, min: f64, median: f64, max: f64| HostTrend {
            bucket_start_ms: bucket,
            bucket_secs: 60,
            metric,
            min,
            max,
            median,
            samples: 20,
        };
        store
            .insert_host_trends(&[
                ht(0, Metric::Cpu.as_str(), 1.0, 4.0, 9.0),
                ht(60_000, Metric::Cpu.as_str(), 2.0, 6.0, 20.0),
                ht(0, Metric::Mem.as_str(), 100.0, 400.0, 900.0),
                ht(120_000, Metric::Cpu.as_str(), 3.0, 5.0, 7.0),
            ])
            .unwrap();

        // Group = bucket size → buckets pass through unchanged.
        let fine = store.history_host(0, u64::MAX, 60_000).unwrap();
        assert_eq!(fine.len(), 3);
        assert_eq!(fine[0].ts_ms, 0);
        assert_eq!(fine[0].cpu_min, Some(1.0));
        assert_eq!(fine[0].cpu_med, Some(4.0));
        assert_eq!(fine[0].cpu_max, Some(9.0));
        assert_eq!(fine[0].mem_med, Some(400.0));
        // Bucket without mem rows yields None for the mem envelope.
        assert_eq!(fine[1].mem_med, None);

        // Coarser group: first two buckets merge — envelope keeps the
        // extremes, the line averages the medians.
        let coarse = store.history_host(0, u64::MAX, 120_000).unwrap();
        assert_eq!(coarse.len(), 2);
        assert_eq!(coarse[0].cpu_min, Some(1.0));
        assert_eq!(coarse[0].cpu_med, Some(5.0)); // avg(4, 6)
        assert_eq!(coarse[0].cpu_max, Some(20.0));
        assert_eq!(coarse[1].ts_ms, 120_000);

        // since_ms and until_ms bound the window at both ends.
        let late = store.history_host(120_000, u64::MAX, 60_000).unwrap();
        assert_eq!(late.len(), 1);
        let middle = store.history_host(60_000, 60_000, 60_000).unwrap();
        assert_eq!(middle.len(), 1);
        assert_eq!(middle[0].ts_ms, 60_000);
    }

    #[test]
    fn history_container_carries_io_envelope() {
        let store = Store::open_in_memory().unwrap();
        store
            .insert_container_trends(&[
                ContainerTrend {
                    bucket_start_ms: 0,
                    bucket_secs: 60,
                    id: "a".to_string(),
                    name: "c-a".to_string(),
                    stack: None,
                    metric: Metric::NetRx.as_str(),
                    min: 100.0,
                    max: 900.0,
                    median: 400.0,
                    samples: 20,
                },
                ContainerTrend {
                    bucket_start_ms: 0,
                    bucket_secs: 60,
                    id: "a".to_string(),
                    name: "c-a".to_string(),
                    stack: None,
                    metric: Metric::DiskWrite.as_str(),
                    min: 1.0,
                    max: 3.0,
                    median: 2.0,
                    samples: 20,
                },
            ])
            .unwrap();

        let h = store.history_container("a", 0, u64::MAX, 60_000).unwrap();
        assert_eq!(h.len(), 1);
        assert_eq!(h[0].net_rx_min, Some(100.0));
        assert_eq!(h[0].net_rx_med, Some(400.0));
        assert_eq!(h[0].net_rx_max, Some(900.0));
        assert_eq!(h[0].disk_write_med, Some(2.0));
        // Metrics with no rows in the bucket stay None.
        assert_eq!(h[0].net_tx_med, None);
        assert_eq!(h[0].cpu_med, None);
    }

    #[test]
    fn history_stack_sums_members_then_downsamples() {
        let store = Store::open_in_memory().unwrap();
        let ct = |bucket: u64, id: &str, min: f64, median: f64, max: f64| ContainerTrend {
            bucket_start_ms: bucket,
            bucket_secs: 60,
            id: id.to_string(),
            name: format!("c-{id}"),
            stack: Some("web".to_string()),
            metric: Metric::Cpu.as_str(),
            min,
            max,
            median,
            samples: 20,
        };
        store
            .insert_container_trends(&[
                // bucket 0: summed envelope = min 3, med 10, max 19
                ct(0, "a", 1.0, 4.0, 9.0),
                ct(0, "b", 2.0, 6.0, 10.0),
                // bucket 60_000: summed envelope = min 2, med 5, max 8
                ct(60_000, "a", 1.0, 2.0, 3.0),
                ct(60_000, "b", 1.0, 3.0, 5.0),
            ])
            .unwrap();

        let fine = store.history_stack("web", 0, u64::MAX, 60_000).unwrap();
        assert_eq!(fine.len(), 2);
        assert_eq!(fine[0].cpu_min, Some(3.0));
        assert_eq!(fine[0].cpu_med, Some(10.0));
        assert_eq!(fine[0].cpu_max, Some(19.0));

        // Coarse group merges the summed buckets.
        let coarse = store.history_stack("web", 0, u64::MAX, 120_000).unwrap();
        assert_eq!(coarse.len(), 1);
        assert_eq!(coarse[0].cpu_min, Some(2.0));
        assert_eq!(coarse[0].cpu_med, Some(7.5)); // avg(10, 5)
        assert_eq!(coarse[0].cpu_max, Some(19.0));

        // Unknown stack and container queries return empty, not errors.
        assert!(
            store
                .history_stack("nope", 0, u64::MAX, 60_000)
                .unwrap()
                .is_empty()
        );
        assert!(
            store
                .history_container("nope", 0, u64::MAX, 60_000)
                .unwrap()
                .is_empty()
        );
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
