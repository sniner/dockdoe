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
//! Trend rows are stored *wide* — one row per container and bucket carrying
//! every metric's envelope — in a `WITHOUT ROWID` table whose primary key
//! leads with `(host, name, bucket_start_ms)`. That clusters a container's
//! history physically: a 30-day chart reads a few thousand adjacent pages
//! instead of touching the whole table. The long format the bucketer emits
//! (one [`ContainerTrend`] per metric) is pivoted on insert; a database from
//! a build that stored the long format is migrated in place on open.
//!
//! The connection lives behind `Arc<Mutex<_>>` so the store is `Clone` and can
//! be shared between the collector (writer) and web handlers (readers). All
//! methods are synchronous; async callers should wrap them in
//! `tokio::task::spawn_blocking`.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Instant;

use anyhow::{Context, Result};
use rusqlite::types::Value;
use rusqlite::{Connection, params_from_iter};
use serde::{Deserialize, Serialize};

use crate::model::ContainerMetrics;

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
    /// Every metric, in the column order of the wide trend row and of
    /// [`HistoryPoint`]. The SQL builders below iterate this, so the column
    /// lists and the row mappers cannot drift apart.
    pub const ALL: [Metric; 6] = [
        Metric::Cpu,
        Metric::Mem,
        Metric::NetRx,
        Metric::NetTx,
        Metric::DiskRead,
        Metric::DiskWrite,
    ];

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

/// A single min/max/median rollup over one time bucket for one container and
/// one metric — the long format the bucketer emits. The store pivots a batch of
/// these into one wide row per container and bucket (see the module docs).
///
/// We store `name` and `stack` alongside the container `id` so history stays
/// meaningful when a container is recreated (e.g. `docker compose up` after a
/// `down` gives the same name a new id): the UI can present history by logical
/// service while the id still distinguishes individual instances.
#[derive(Debug, Clone, Serialize)]
pub struct ContainerTrend {
    pub bucket_start_ms: u64,
    pub bucket_secs: u64,
    /// Which Docker host this rollup belongs to (the host's config `name`).
    pub host: String,
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
/// page load and streamed live over SSE. The I/O rates are `None` until a
/// second sample gives a delta, or when the runtime reports no such stats.
///
/// Also read back by a federation hub from a node's metrics endpoints, so it
/// is part of the additive-only payload contract (see `crate::model`).
#[derive(Debug, Clone, Serialize, Deserialize)]
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

    fn from_connection(mut conn: Connection) -> Result<Self> {
        // We use a single connection behind a Mutex, so all access is already
        // serialised — WAL's concurrent-reader benefit doesn't apply, and its
        // side files (`-wal`/`-shm`) only grow without paying for themselves.
        // The default rollback journal keeps no persistent extra files. Setting
        // it explicitly also migrates any database left in WAL mode by an
        // earlier build, cleaning up its stale `-wal`/`-shm`.
        conn.pragma_update(None, "journal_mode", "DELETE")
            .context("setting rollback journal mode")?;
        // Migrate in ordered steps so a database from an earlier build can be
        // brought up to the current schema in place. Tables come first, then
        // columns added in later releases are backfilled, then a long-format
        // trend table is pivoted into the wide layout, and only then the
        // indexes — several of which lead with `host`, a column the backfill
        // step adds. Creating those indexes before the column exists is exactly
        // what broke the in-place upgrade: SQLite rejects them with "no such
        // column: host" and the whole migration (and startup) fails. The trend
        // pivot likewise needs `host` in place, since it is part of the new
        // primary key.
        conn.execute_batch(CREATE_TABLES)
            .context("creating tables")?;
        conn.execute_batch(CREATE_TREND_TABLE)
            .context("creating trend table")?;
        // Columns added after an earlier release: present in the CREATE above
        // for fresh databases, added here for ones created by an earlier build.
        // `host` is NOT NULL, so existing rows are backfilled with the default
        // single-host name ("local") — the same name the config layer
        // synthesises for a pre-multi-host single-host setup, so old data stays
        // attributed to the host that produced it.
        for (table, col, decl) in [
            ("container_sample", "host", "TEXT NOT NULL DEFAULT 'local'"),
            ("container_trend", "host", "TEXT NOT NULL DEFAULT 'local'"),
            ("container_sample", "net_rx", "REAL"),
            ("container_sample", "net_tx", "REAL"),
            ("container_sample", "disk_read", "REAL"),
            ("container_sample", "disk_write", "REAL"),
        ] {
            add_column_if_missing(&conn, table, col, decl)?;
        }
        migrate_trend_layout(&mut conn)?;
        conn.execute_batch(CREATE_INDEXES)
            .context("creating indexes")?;
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

    /// The persistent session-signing secret, generated on first use and stored
    /// in the `meta` table. Stable across restarts so login sessions survive
    /// them; deleting the row rotates the secret (logs everyone out).
    pub fn session_secret(&self) -> Result<Vec<u8>> {
        use rusqlite::OptionalExtension;
        let conn = self.lock();
        let existing: Option<Vec<u8>> = conn
            .query_row(
                "SELECT value FROM meta WHERE key = 'session_secret'",
                [],
                |r| r.get(0),
            )
            .optional()
            .context("reading session secret")?;
        if let Some(secret) = existing {
            return Ok(secret);
        }
        let secret = crate::auth::generate_secret();
        conn.execute(
            "INSERT INTO meta (key, value) VALUES ('session_secret', ?1)",
            rusqlite::params![secret],
        )
        .context("storing session secret")?;
        Ok(secret)
    }

    /// Persist one collection cycle for one host: all container samples, in a
    /// single transaction.
    pub fn insert_samples(
        &self,
        host: &str,
        ts_ms: u64,
        containers: &[ContainerMetrics],
    ) -> Result<()> {
        let mut conn = self.lock();
        let tx = conn.transaction().context("begin sample transaction")?;
        {
            let mut stmt = tx
                .prepare_cached(
                    "INSERT INTO container_sample
                       (ts_ms, host, id, name, cpu_percent, mem_used, mem_limit,
                        net_rx, net_tx, disk_read, disk_write)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                )
                .context("prepare container insert")?;
            for c in containers {
                stmt.execute(rusqlite::params![
                    to_db(ts_ms),
                    host,
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

    /// Persist a batch of container trend rollups, pivoted into one wide row
    /// per container and bucket. A row that already exists — the same name in
    /// the same bucket, e.g. a container recreated within one bucket whose two
    /// incarnations were flushed separately — is merged rather than rejected:
    /// the envelope keeps the extremes, the median is weighted by sample count.
    pub fn insert_container_trends(&self, trends: &[ContainerTrend]) -> Result<()> {
        if trends.is_empty() {
            return Ok(());
        }
        let rows = pivot_trends(trends);
        let mut conn = self.lock();
        let tx = conn.transaction().context("begin trend transaction")?;
        {
            let mut stmt = tx
                .prepare_cached(&INSERT_TREND_SQL)
                .context("prepare container trend insert")?;
            for ((host, name, bucket_start_ms), row) in rows {
                stmt.execute(params_from_iter(row.into_values(
                    host,
                    name,
                    bucket_start_ms,
                )))
                .context("insert container trend")?;
            }
        }
        tx.commit().context("commit trend transaction")?;
        Ok(())
    }

    /// Delete raw container samples older than `before_ms`. This is "point A":
    /// trends survive, raw data is dropped. Returns the number of rows removed.
    pub fn prune_raw(&self, before_ms: u64) -> Result<usize> {
        let conn = self.lock();
        let containers = conn
            .execute(
                "DELETE FROM container_sample WHERE ts_ms < ?1",
                [to_db(before_ms)],
            )
            .context("prune container samples")?;
        Ok(containers)
    }

    /// Delete container trend rollups whose bucket starts before `before_ms`.
    /// This is the separate, longer trend retention: trends from long-gone
    /// containers age out here, independent of "point A" for raw data. Returns
    /// the number of rows removed.
    pub fn prune_trends(&self, before_ms: u64) -> Result<usize> {
        let conn = self.lock();
        let containers = conn
            .execute(
                "DELETE FROM container_trend WHERE bucket_start_ms < ?1",
                [to_db(before_ms)],
            )
            .context("prune container trends")?;
        Ok(containers)
    }

    /// Raw samples for one container at or after `since_ms`, oldest first. Used
    /// to seed a container detail page's charts.
    pub fn recent_container_samples(
        &self,
        host: &str,
        id: &str,
        since_ms: u64,
    ) -> Result<Vec<MetricPoint>> {
        let conn = self.lock();
        let mut stmt = conn
            .prepare_cached(
                "SELECT ts_ms, cpu_percent, mem_used, net_rx, net_tx, disk_read, disk_write
                 FROM container_sample
                 WHERE host = ?1 AND id = ?2 AND ts_ms >= ?3 ORDER BY ts_ms ASC",
            )
            .context("prepare recent container query")?;
        let rows = stmt
            .query_map(rusqlite::params![host, id, to_db(since_ms)], row_to_point)
            .context("query recent container samples")?
            .collect::<rusqlite::Result<Vec<_>>>()
            .context("collect recent container samples")?;
        Ok(rows)
    }

    /// History between `since_ms` and `until_ms` (inclusive) served from *raw*
    /// samples, downsampled into `group_ms` windows, oldest first. Used for
    /// windows inside the raw retention, where full-resolution data exists. A
    /// group smaller than the sample interval keeps every sample as its own
    /// point (envelope collapsed to the value), so short windows stay exact;
    /// larger groups cap the point count — without the cap, a raised
    /// `raw_retention_secs` could turn one request into hundreds of thousands
    /// of JSON points. Raw samples are id-keyed (unlike trend history): the raw
    /// retention is too short for recreation continuity to matter.
    pub fn history_container_raw(
        &self,
        host: &str,
        id: &str,
        since_ms: u64,
        until_ms: u64,
        group_ms: u64,
    ) -> Result<Vec<HistoryPoint>> {
        let conn = self.lock();
        let mut stmt = conn
            .prepare_cached(RAW_HISTORY_SQL)
            .context("prepare raw history query")?;
        let rows = stmt
            .query_map(
                rusqlite::params![
                    host,
                    id,
                    to_db(since_ms),
                    to_db(until_ms),
                    to_db(group_ms.max(1))
                ],
                history_row_to_point,
            )
            .context("query raw history")?
            .collect::<rusqlite::Result<Vec<_>>>()
            .context("collect raw history")?;
        Ok(rows)
    }

    /// Aggregate trend history for a whole stack at or after `since_ms`, oldest
    /// first, as chart seed points. Stacks have no raw per-stack series, so the
    /// detail page seeds from trends: per bucket we sum the member medians
    /// (mirroring the live aggregate, which sums current member values).
    pub fn recent_stack_trends(
        &self,
        host: &str,
        stack: &str,
        since_ms: u64,
    ) -> Result<Vec<MetricPoint>> {
        let conn = self.lock();
        let mut stmt = conn
            .prepare_cached(RECENT_STACK_TRENDS_SQL)
            .context("prepare recent stack trend query")?;
        let rows = stmt
            .query_map(
                rusqlite::params![host, stack, to_db(since_ms)],
                trend_row_to_point,
            )
            .context("query recent stack trends")?
            .collect::<rusqlite::Result<Vec<_>>>()
            .context("collect recent stack trends")?;
        Ok(rows)
    }

    /// Trend history for one container between `since_ms` and `until_ms`
    /// (inclusive), downsampled into `group_ms` windows, oldest first. Within a
    /// window the envelope keeps the extremes (MIN of minima, MAX of maxima)
    /// while the line averages the bucket medians — an approximation of the true
    /// median that is fine for display.
    ///
    /// Keyed by container *name*, not id: a recreated container (`docker
    /// compose up` after `down`, image update) gets a new id but keeps its
    /// name, and history should span recreations — that is why trends store
    /// the name at all. Names are unique per host among running containers
    /// (Docker enforces it), so merging by name is the logical-service view.
    pub fn history_container(
        &self,
        host: &str,
        name: &str,
        since_ms: u64,
        until_ms: u64,
        group_ms: u64,
    ) -> Result<Vec<HistoryPoint>> {
        let conn = self.lock();
        let mut stmt = conn
            .prepare_cached(&HISTORY_CONTAINER_SQL)
            .context("prepare container history query")?;
        let rows = stmt
            .query_map(
                rusqlite::params![
                    host,
                    name,
                    to_db(since_ms),
                    to_db(until_ms),
                    to_db(group_ms.max(1))
                ],
                history_row_to_point,
            )
            .context("query container history")?
            .collect::<rusqlite::Result<Vec<_>>>()
            .context("collect container history")?;
        Ok(rows)
    }

    /// Trend history for a whole stack: per trend bucket the member values are
    /// summed (mirroring the live aggregate and [`Self::recent_stack_trends`]),
    /// then the summed buckets are downsampled like [`Self::history_container`].
    pub fn history_stack(
        &self,
        host: &str,
        stack: &str,
        since_ms: u64,
        until_ms: u64,
        group_ms: u64,
    ) -> Result<Vec<HistoryPoint>> {
        let conn = self.lock();
        let mut stmt = conn
            .prepare_cached(&HISTORY_STACK_SQL)
            .context("prepare stack history query")?;
        let rows = stmt
            .query_map(
                rusqlite::params![
                    host,
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

    /// The container name recorded in the trend table for `id`, if any. Used by
    /// the history endpoint to resolve its URL's container id into the name the
    /// trends are queried by when the container is no longer in the live
    /// snapshot. Nothing is keyed by `(host, id)`, so this scans the host's
    /// rows — acceptable for a rare fallback with `LIMIT 1` over a table that
    /// holds one row per container and bucket.
    pub fn container_name(&self, host: &str, id: &str) -> Result<Option<String>> {
        use rusqlite::OptionalExtension;
        let conn = self.lock();
        let name = conn
            .query_row(
                "SELECT name FROM container_trend WHERE host = ?1 AND id = ?2 LIMIT 1",
                rusqlite::params![host, id],
                |r| r.get(0),
            )
            .optional()
            .context("resolving container name from trends")?;
        Ok(name)
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
/// row to a [`MetricPoint`]. The I/O columns are `NULL` when the runtime
/// reported no network/block-I/O stats for the container.
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
/// nullable: a metric the container has no data for is NULL in the wide row,
/// and MIN/AVG/MAX ignore NULLs but return NULL when nothing matched.
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

/// A metric's envelope inside a wide trend row.
#[derive(Debug, Clone, Copy)]
struct Envelope {
    min: f64,
    median: f64,
    max: f64,
    samples: u32,
}

impl Envelope {
    /// Fold another rollup of the same metric and bucket in: extremes are
    /// kept, the median is weighted by sample count so a long-lived incarnation
    /// outweighs a brief one.
    fn merge(self, other: Envelope) -> Envelope {
        let samples = self.samples + other.samples;
        let weight = |e: Envelope| e.median * f64::from(e.samples);
        Envelope {
            min: self.min.min(other.min),
            median: if samples == 0 {
                self.median
            } else {
                (weight(self) + weight(other)) / f64::from(samples)
            },
            max: self.max.max(other.max),
            samples,
        }
    }
}

/// One wide trend row minus its key `(host, name, bucket_start_ms)`: every
/// metric's envelope for one container in one bucket, `None` for metrics the
/// bucket has no data for.
#[derive(Debug, Default)]
struct WideTrend {
    bucket_secs: u64,
    id: String,
    stack: Option<String>,
    /// Samples behind the best-covered metric.
    samples: u32,
    /// Indexed like [`Metric::ALL`].
    metrics: [Option<Envelope>; 6],
}

impl WideTrend {
    /// The bind values for [`INSERT_TREND_SQL`], key first, then the metric
    /// envelopes in [`Metric::ALL`] order.
    fn into_values(self, host: String, name: String, bucket_start_ms: u64) -> Vec<Value> {
        let mut values = vec![
            Value::Text(host),
            Value::Text(name),
            Value::Integer(to_db(bucket_start_ms)),
            Value::Integer(to_db(self.bucket_secs)),
            Value::Text(self.id),
            self.stack.map_or(Value::Null, Value::Text),
            Value::Integer(i64::from(self.samples)),
        ];
        for envelope in self.metrics {
            for value in [
                envelope.map(|e| e.min),
                envelope.map(|e| e.median),
                envelope.map(|e| e.max),
            ] {
                values.push(value.map_or(Value::Null, Value::Real));
            }
        }
        values
    }
}

/// Pivot the bucketer's long rollups into wide rows, keyed and ordered by
/// `(host, name, bucket_start_ms)` — the table's primary key, so the inserts
/// walk the B-tree in order. Two incarnations of one name in the same bucket
/// (recreated within a minute) merge; the row's `id` is the last one seen.
fn pivot_trends(trends: &[ContainerTrend]) -> BTreeMap<(String, String, u64), WideTrend> {
    let mut rows: BTreeMap<(String, String, u64), WideTrend> = BTreeMap::new();
    for t in trends {
        let Some(slot) = Metric::ALL.iter().position(|m| m.as_str() == t.metric) else {
            tracing::warn!(metric = t.metric, "dropping trend row for unknown metric");
            continue;
        };
        let row = rows
            .entry((t.host.clone(), t.name.clone(), t.bucket_start_ms))
            .or_default();
        row.bucket_secs = t.bucket_secs;
        row.id.clone_from(&t.id);
        if row.stack.is_none() {
            row.stack.clone_from(&t.stack);
        }
        let envelope = Envelope {
            min: t.min,
            median: t.median,
            max: t.max,
            samples: t.samples,
        };
        let merged = row.metrics[slot].map_or(envelope, |existing| existing.merge(envelope));
        row.samples = row.samples.max(merged.samples);
        row.metrics[slot] = Some(merged);
    }
    rows
}

/// Build a comma-joined column expression per metric, in [`Metric::ALL`]
/// order — the order [`history_row_to_point`] and [`WideTrend`] expect. Every
/// trend SQL below is assembled from this, so the column lists cannot drift.
fn per_metric(f: impl Fn(&str) -> String) -> String {
    Metric::ALL
        .iter()
        .map(|m| f(m.as_str()))
        .collect::<Vec<_>>()
        .join(",\n           ")
}

/// The wide row's metric value columns, `cpu_min, cpu_med, cpu_max, mem_min, …`.
fn value_columns() -> String {
    per_metric(|m| format!("{m}_min, {m}_med, {m}_max"))
}

/// The downsampling envelope over wide rows: extremes of the extremes, mean of
/// the medians — an approximation of the true median that is fine for display.
fn envelope_columns() -> String {
    per_metric(|m| format!("MIN({m}_min), AVG({m}_med), MAX({m}_max)"))
}

/// SQL of [`Store::insert_container_trends`]: insert a wide row, or merge it
/// into the row already there for that name and bucket. SQLite's scalar
/// `min`/`max` return NULL if any argument is, hence the `coalesce` pairs; the
/// median merge weights both sides by their sample counts. Every right-hand
/// side sees the *stored* row, so the order of assignments does not matter.
static INSERT_TREND_SQL: LazyLock<String> = LazyLock::new(|| {
    let placeholders = (8..=25)
        .map(|i| format!("?{i}"))
        .collect::<Vec<_>>()
        .join(", ");
    let merge = per_metric(|m| {
        format!(
            "{m}_min = min(coalesce({m}_min, excluded.{m}_min), coalesce(excluded.{m}_min, {m}_min)),
           {m}_med = CASE WHEN {m}_med IS NULL THEN excluded.{m}_med
                          WHEN excluded.{m}_med IS NULL THEN {m}_med
                          ELSE ({m}_med * samples + excluded.{m}_med * excluded.samples)
                               / (samples + excluded.samples) END,
           {m}_max = max(coalesce({m}_max, excluded.{m}_max), coalesce(excluded.{m}_max, {m}_max))"
        )
    });
    format!(
        "INSERT INTO container_trend
           (host, name, bucket_start_ms, bucket_secs, id, stack, samples,
           {})
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, {placeholders})
         ON CONFLICT (host, name, bucket_start_ms) DO UPDATE SET
           bucket_secs = excluded.bucket_secs,
           id = excluded.id,
           stack = coalesce(excluded.stack, stack),
           {merge},
           samples = samples + excluded.samples",
        value_columns()
    )
});

/// SQL of [`Store::recent_stack_trends`] — a named item (rather than inline in
/// the method) so the query-plan guard test checks the exact query that runs.
/// Column order follows [`trend_row_to_point`]. CPU and memory fall back to 0
/// for a bucket without them, the I/O rates stay NULL (unknown, not zero).
const RECENT_STACK_TRENDS_SQL: &str = "\
    SELECT bucket_start_ms,
           COALESCE(SUM(cpu_med), 0), COALESCE(SUM(mem_med), 0),
           SUM(net_rx_med), SUM(net_tx_med), SUM(disk_read_med), SUM(disk_write_med)
    FROM container_trend
    WHERE host = ?1 AND stack = ?2 AND bucket_start_ms >= ?3
    GROUP BY bucket_start_ms
    ORDER BY bucket_start_ms ASC";

/// SQL of [`Store::history_container_raw`] — see [`RECENT_STACK_TRENDS_SQL`].
/// The envelope is plain MIN/AVG/MAX per column, in the order
/// [`history_row_to_point`] expects. The aggregates ignore NULLs and return
/// NULL for a group with no values, matching the trend queries.
const RAW_HISTORY_SQL: &str = "\
    SELECT (ts_ms / ?5) * ?5 AS bucket,
           MIN(cpu_percent), AVG(cpu_percent), MAX(cpu_percent),
           MIN(mem_used), AVG(mem_used), MAX(mem_used),
           MIN(net_rx), AVG(net_rx), MAX(net_rx),
           MIN(net_tx), AVG(net_tx), MAX(net_tx),
           MIN(disk_read), AVG(disk_read), MAX(disk_read),
           MIN(disk_write), AVG(disk_write), MAX(disk_write)
    FROM container_sample
    WHERE host = ?1 AND id = ?2 AND ts_ms >= ?3 AND ts_ms <= ?4
    GROUP BY bucket
    ORDER BY bucket ASC";

/// SQL of [`Store::history_container`] — see [`RECENT_STACK_TRENDS_SQL`]. The
/// primary key `(host, name, bucket_start_ms)` seeks straight to the window;
/// with the table clustered by that key the rows come off adjacent pages.
static HISTORY_CONTAINER_SQL: LazyLock<String> = LazyLock::new(|| {
    format!(
        "SELECT (bucket_start_ms / ?5) * ?5 AS bucket,
           {}
         FROM container_trend
         WHERE host = ?1 AND name = ?2 AND bucket_start_ms >= ?3 AND bucket_start_ms <= ?4
         GROUP BY bucket ORDER BY bucket ASC",
        envelope_columns()
    )
});

/// SQL of [`Store::history_stack`] — see [`RECENT_STACK_TRENDS_SQL`]. Members
/// are summed per bucket first (mirroring the live aggregate), then the summed
/// buckets are downsampled like the container history.
static HISTORY_STACK_SQL: LazyLock<String> = LazyLock::new(|| {
    let sums = per_metric(|m| {
        format!("SUM({m}_min) AS {m}_min, SUM({m}_med) AS {m}_med, SUM({m}_max) AS {m}_max")
    });
    format!(
        "WITH per_bucket AS (
             SELECT bucket_start_ms AS b,
           {sums}
             FROM container_trend
             WHERE host = ?1 AND stack = ?2 AND bucket_start_ms >= ?3 AND bucket_start_ms <= ?4
             GROUP BY b
         )
         SELECT (b / ?5) * ?5 AS bucket,
           {}
         FROM per_bucket
         GROUP BY bucket ORDER BY bucket ASC",
        envelope_columns()
    )
});

/// Pivot one chunk of a long-format trend table into the wide one, for
/// [`migrate_trend_layout`]. Two incarnations of a name in one bucket (the
/// legacy table has no uniqueness) merge like [`Envelope::merge`]: extremes
/// kept, median weighted by samples; `id` and `stack` are whichever sorts
/// last, which for a recreation is arbitrary but harmless.
static LEGACY_PIVOT_SQL: LazyLock<String> = LazyLock::new(|| {
    let pivot = per_metric(|m| {
        format!(
            "MIN(CASE WHEN metric = '{m}' THEN min END),
           SUM(CASE WHEN metric = '{m}' THEN median * samples END)
               / SUM(CASE WHEN metric = '{m}' THEN samples END),
           MAX(CASE WHEN metric = '{m}' THEN max END)"
        )
    });
    format!(
        "INSERT INTO container_trend
           (host, name, bucket_start_ms, bucket_secs, id, stack, samples,
           {})
         SELECT host, name, bucket_start_ms, MAX(bucket_secs), MAX(id), MAX(stack), MAX(samples),
           {pivot}
         FROM container_trend_legacy
         WHERE bucket_start_ms >= ?1 AND bucket_start_ms < ?2
         GROUP BY host, name, bucket_start_ms",
        value_columns()
    )
});

/// Width of one migration chunk: an hour of buckets at a time keeps the sort
/// behind `GROUP BY` small enough to stay in memory on any host.
const MIGRATION_CHUNK_MS: i64 = 3_600_000;

/// Rewrite a long-format trend table (one row per container, bucket *and
/// metric*, as stored by builds before the wide layout) into the wide,
/// clustered table — in place, on open.
///
/// The long table was written time-major, so a container's rows were spread
/// over the whole file and a 30-day chart read most of it. The pivot walks the
/// legacy rows in time order too, an hour at a time via the retention index,
/// so it reads the old table roughly sequentially instead of repeating that
/// scatter once per container; the two host-keyed legacy indexes are dropped
/// first so the planner has no reason to prefer them. Everything happens in
/// one transaction — a crash mid-way leaves the legacy table intact and the
/// next start migrates again. A `VACUUM` afterwards returns the freed space
/// (the wide table is several times smaller); failing that is only a warning.
fn migrate_trend_layout(conn: &mut Connection) -> Result<()> {
    let legacy: bool = conn
        .query_row(
            "SELECT EXISTS (SELECT 1 FROM pragma_table_info('container_trend')
                            WHERE name = 'metric')",
            [],
            |r| r.get(0),
        )
        .context("checking trend table layout")?;
    if !legacy {
        return Ok(());
    }
    let legacy_rows: i64 = conn
        .query_row("SELECT COUNT(*) FROM container_trend", [], |r| r.get(0))
        .context("counting legacy trend rows")?;
    tracing::info!(
        rows = legacy_rows,
        "migrating trend rows to the clustered layout (one-time; may take a few minutes on a large database)"
    );
    let started = Instant::now();
    let tx = conn.transaction().context("begin trend migration")?;
    tx.execute_batch(
        "ALTER TABLE container_trend RENAME TO container_trend_legacy;
         DROP INDEX IF EXISTS container_trend_host_name_ts;
         DROP INDEX IF EXISTS container_trend_host_stack_ts;
         DROP INDEX IF EXISTS container_trend_host_id;
         DROP INDEX IF EXISTS container_trend_host_name;
         DROP INDEX IF EXISTS container_trend_ts;",
    )
    .context("renaming legacy trend table")?;
    tx.execute_batch(CREATE_TREND_TABLE)
        .context("creating wide trend table")?;
    let span: (Option<i64>, Option<i64>) = tx
        .query_row(
            "SELECT MIN(bucket_start_ms), MAX(bucket_start_ms) FROM container_trend_legacy",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .context("reading legacy trend span")?;
    if let (Some(first), Some(last)) = span {
        let mut pivot = tx
            .prepare(&LEGACY_PIVOT_SQL)
            .context("prepare legacy trend pivot")?;
        let mut from = first;
        while from <= last {
            let to = from.saturating_add(MIGRATION_CHUNK_MS);
            pivot
                .execute([from, to])
                .context("pivot legacy trend chunk")?;
            from = to;
        }
    }
    tx.execute_batch("DROP TABLE container_trend_legacy")
        .context("dropping legacy trend table")?;
    tx.commit().context("commit trend migration")?;
    let wide_rows: i64 = conn
        .query_row("SELECT COUNT(*) FROM container_trend", [], |r| r.get(0))
        .context("counting migrated trend rows")?;
    tracing::info!(
        rows = wide_rows,
        elapsed = ?started.elapsed(),
        "trend layout migrated; compacting the database"
    );
    if let Err(err) = conn.execute_batch("VACUUM") {
        tracing::warn!(
            %err,
            "compacting the database failed; it keeps its size and reuses the freed pages"
        );
    }
    Ok(())
}

/// Table definitions, created first so the column backfill and index steps in
/// [`Store::from_connection`] have something to operate on. Splitting tables
/// from indexes is what lets an in-place upgrade add the `host` column before
/// the host-leading indexes below reference it. The trend table has its own
/// statement, [`CREATE_TREND_TABLE`], because the layout migration creates it
/// too.
const CREATE_TABLES: &str = "
CREATE TABLE IF NOT EXISTS container_sample (
    ts_ms       INTEGER NOT NULL,
    host        TEXT    NOT NULL,
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

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value BLOB NOT NULL
);
";

/// The wide trend table: one row per container and bucket, every metric's
/// envelope as columns (NULL where the bucket has no data for it). `WITHOUT
/// ROWID` makes the primary key the table's physical order, so a container's
/// history is contiguous on disk — the property the chart queries live on.
/// `id` is the incarnation that wrote the row (history is keyed by name, see
/// [`Store::history_container`]); `samples` is the count behind the
/// best-covered metric.
const CREATE_TREND_TABLE: &str = "
CREATE TABLE IF NOT EXISTS container_trend (
    host            TEXT    NOT NULL,
    name            TEXT    NOT NULL,
    bucket_start_ms INTEGER NOT NULL,
    bucket_secs     INTEGER NOT NULL,
    id              TEXT    NOT NULL,
    stack           TEXT,
    samples         INTEGER NOT NULL,
    cpu_min         REAL, cpu_med        REAL, cpu_max        REAL,
    mem_min         REAL, mem_med        REAL, mem_max        REAL,
    net_rx_min      REAL, net_rx_med     REAL, net_rx_max     REAL,
    net_tx_min      REAL, net_tx_med     REAL, net_tx_max     REAL,
    disk_read_min   REAL, disk_read_med  REAL, disk_read_max  REAL,
    disk_write_min  REAL, disk_write_med REAL, disk_write_max REAL,
    PRIMARY KEY (host, name, bucket_start_ms)
) WITHOUT ROWID;
";

/// Indexes, created after the column backfill so the host-leading ones resolve.
///
/// Container history needs no index: the trend table's primary key *is*
/// `(host, name, bucket_start_ms)`. Stack history filters on `host` + `stack`
/// plus the same time range, so its index must put the time column directly
/// after the equality columns — with anything in between, SQLite cannot seek
/// the range. The two `*_retention` indexes lead with the column the retention
/// prunes filter on (`ts_ms` / `bucket_start_ms`) so those DELETEs can seek to
/// the cutoff instead of scanning the whole table.
const CREATE_INDEXES: &str = "
CREATE INDEX IF NOT EXISTS container_sample_host_id_ts ON container_sample(host, id, ts_ms);
CREATE INDEX IF NOT EXISTS container_trend_stack_ts ON container_trend(host, stack, bucket_start_ms) WHERE stack IS NOT NULL;
CREATE INDEX IF NOT EXISTS container_sample_retention ON container_sample(ts_ms);
CREATE INDEX IF NOT EXISTS container_trend_retention ON container_trend(bucket_start_ms);
";

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ContainerState, HealthState};

    const HOST: &str = "local";

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

        store.insert_samples(HOST, 1_000, &containers).unwrap();
        store.insert_samples(HOST, 5_000, &containers).unwrap();

        assert_eq!(store.count("container_sample").unwrap(), 4);

        // Prune everything strictly before ts 5000 → first cycle dropped.
        let removed = store.prune_raw(5_000).unwrap();
        assert_eq!(removed, 2);
        assert_eq!(store.count("container_sample").unwrap(), 2);
    }

    #[test]
    fn migrates_pre_multi_host_database_in_place() {
        // A database from before the `host` column existed: the old host-less
        // tables and indexes, with a row already present so the NOT NULL `host`
        // backfill is exercised. Opening it used to fail at index creation
        // ("no such column: host"); it must now upgrade in place.
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE container_sample (
                 ts_ms INTEGER NOT NULL, id TEXT NOT NULL, name TEXT NOT NULL,
                 cpu_percent REAL, mem_used INTEGER, mem_limit INTEGER,
                 net_rx REAL, net_tx REAL, disk_read REAL, disk_write REAL);
             CREATE INDEX container_sample_id_ts ON container_sample(id, ts_ms);
             CREATE TABLE container_trend (
                 bucket_start_ms INTEGER NOT NULL, bucket_secs INTEGER NOT NULL,
                 id TEXT NOT NULL, name TEXT NOT NULL, stack TEXT, metric TEXT NOT NULL,
                 min REAL NOT NULL, max REAL NOT NULL, median REAL NOT NULL, samples INTEGER NOT NULL);
             CREATE INDEX container_trend_ts ON container_trend(id, metric, bucket_start_ms);
             CREATE TABLE meta (key TEXT PRIMARY KEY, value BLOB NOT NULL);
             INSERT INTO container_sample (ts_ms, id, name) VALUES (1000, 'abc', 'c1');
             INSERT INTO container_trend
                 (bucket_start_ms, bucket_secs, id, name, stack, metric, min, max, median, samples)
             VALUES
                 -- one bucket of `web`, two incarnations (recreated mid-minute)
                 (0, 60, 'old', 'web', 'shop', 'cpu', 1.0, 3.0, 2.0, 10),
                 (0, 60, 'new', 'web', 'shop', 'cpu', 0.5, 5.0, 4.0, 30),
                 (0, 60, 'new', 'web', 'shop', 'mem', 100.0, 100.0, 100.0, 30),
                 -- a later bucket, so the chunked pivot spans a range
                 (7200000, 60, 'new', 'web', 'shop', 'cpu', 2.0, 2.0, 2.0, 20);",
        )
        .unwrap();

        // Opening must succeed — this is the upgrade that used to abort startup.
        let store = Store::from_connection(conn).unwrap();

        // The long-format trend rows were pivoted into wide ones: one row per
        // name and bucket, the two incarnations merged (extremes kept, median
        // weighted by samples: (2·10 + 4·30) / 40 = 3.5), the memory metric
        // sitting in the same row, and the stack index usable.
        assert_eq!(store.count("container_trend").unwrap(), 2);
        let h = store
            .history_container("local", "web", 0, u64::MAX, 60_000)
            .unwrap();
        assert_eq!(h.len(), 2);
        assert_eq!(h[0].cpu_min, Some(0.5));
        assert_eq!(h[0].cpu_med, Some(3.5));
        assert_eq!(h[0].cpu_max, Some(5.0));
        assert_eq!(h[0].mem_med, Some(100.0));
        assert_eq!(h[1].ts_ms, 7_200_000);
        assert_eq!(h[1].mem_med, None);
        let s = store
            .history_stack("local", "shop", 0, u64::MAX, 60_000)
            .unwrap();
        assert_eq!(s.len(), 2);
        assert_eq!(s[0].cpu_max, Some(5.0));
        // Opening the migrated database again is a no-op.
        assert_eq!(store.count("container_trend").unwrap(), 2);

        // The existing row is backfilled with the synthesised single-host name,
        // so pre-multi-host data stays attributed to the host that produced it.
        let backfilled = {
            let conn = store.lock();
            conn.query_row(
                "SELECT host FROM container_sample WHERE id = 'abc'",
                [],
                |r| r.get::<_, String>(0),
            )
            .unwrap()
        };
        assert_eq!(backfilled, "local");

        // New host-aware inserts (the path that logged "insert container sample"
        // failures against an un-migrated database) now work.
        store
            .insert_samples("nas", 2_000, &[container_sample("z", Some(2.0))])
            .unwrap();
        assert_eq!(store.count("container_sample").unwrap(), 2);
    }

    #[test]
    fn retention_prunes_seek_by_index_instead_of_scanning() {
        // The prunes filter only on the time column, with no host/id, so they
        // need a leading index on that column — otherwise they full-scan the
        // whole table on every call. Guard the plan so the index can't be
        // dropped or shadowed unnoticed.
        let store = Store::open_in_memory().unwrap();
        let conn = store.lock();
        let plan = |sql: &str| -> String {
            let mut stmt = conn
                .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
                .expect("prepare explain");
            stmt.query_map([], |r| r.get::<_, String>(3))
                .expect("query plan")
                .map(Result::unwrap)
                .collect::<Vec<_>>()
                .join("; ")
        };

        let sample = plan("DELETE FROM container_sample WHERE ts_ms < 1");
        assert!(
            sample.contains("container_sample_retention") && !sample.contains("SCAN"),
            "sample prune should seek by index, got: {sample}"
        );
        let trend = plan("DELETE FROM container_trend WHERE bucket_start_ms < 1");
        assert!(
            trend.contains("container_trend_retention") && !trend.contains("SCAN"),
            "trend prune should seek by index, got: {trend}"
        );
    }

    #[test]
    fn trend_history_reads_seek_time_range_by_index() {
        // The chart queries filter on host + name/stack + a bucket_start_ms
        // range. Container history must seek the primary key (that is what
        // clusters the rows), stack history its own index with the time column
        // right after the equality columns — with anything in between SQLite
        // cannot seek the range and scans the container's — for stacks, the
        // host's — full trend retention on every chart request; that was the
        // first "history is slow" bug. Guard the plans of the exact production
        // SQL so the regression can't sneak back.
        let store = Store::open_in_memory().unwrap();
        let conn = store.lock();
        let plan = |sql: &str| -> String {
            let mut stmt = conn
                .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
                .expect("prepare explain");
            // The plan doesn't depend on the bound values, but rusqlite insists
            // every placeholder is bound — dummies suffice.
            let dummies = vec![rusqlite::types::Value::Integer(1); stmt.parameter_count()];
            stmt.query_map(rusqlite::params_from_iter(dummies), |r| {
                r.get::<_, String>(3)
            })
            .expect("query plan")
            .map(Result::unwrap)
            .collect::<Vec<_>>()
            .join("; ")
        };

        for (what, sql, index, time_col, table) in [
            (
                "container history",
                HISTORY_CONTAINER_SQL.clone(),
                "PRIMARY KEY",
                "bucket_start_ms>",
                "container_trend",
            ),
            (
                "stack history",
                HISTORY_STACK_SQL.clone(),
                "container_trend_stack_ts",
                "bucket_start_ms>",
                "container_trend",
            ),
            (
                "stack seed",
                RECENT_STACK_TRENDS_SQL.to_string(),
                "container_trend_stack_ts",
                "bucket_start_ms>",
                "container_trend",
            ),
            (
                "raw history",
                RAW_HISTORY_SQL.to_string(),
                "container_sample_host_id_ts",
                "ts_ms>",
                "container_sample",
            ),
        ] {
            let p = plan(&sql);
            assert!(
                p.contains(index) && p.contains(time_col),
                "{what} should range-seek {time_col} via {index}, got: {p}"
            );
            assert!(
                !p.contains(&format!("SCAN {table}")),
                "{what} should not scan {table}, got: {p}"
            );
        }
    }

    #[test]
    fn history_survives_container_recreation() {
        // `docker compose up` after `down` (or an image update) recreates a
        // container: new id, same name. History is keyed by name exactly so the
        // logical service keeps its past across such recreations — before this,
        // every recreation silently cut the visible history short.
        let store = Store::open_in_memory().unwrap();
        let ct = |bucket: u64, id: &str| ContainerTrend {
            bucket_start_ms: bucket,
            bucket_secs: 60,
            host: HOST.into(),
            id: id.to_string(),
            name: "web-1".to_string(),
            stack: None,
            metric: Metric::Cpu.as_str(),
            min: 1.0,
            max: 3.0,
            median: 2.0,
            samples: 20,
        };
        store
            .insert_container_trends(&[ct(0, "old-id"), ct(60_000, "new-id")])
            .unwrap();

        let h = store
            .history_container(HOST, "web-1", 0, u64::MAX, 60_000)
            .unwrap();
        assert_eq!(h.len(), 2, "history must span both incarnations");

        // The web handler resolves a no-longer-running container's id to its
        // name through the trend table.
        assert_eq!(
            store.container_name(HOST, "old-id").unwrap().as_deref(),
            Some("web-1")
        );
        assert_eq!(store.container_name(HOST, "gone").unwrap(), None);
    }

    #[test]
    fn container_samples_roundtrip_io_rates() {
        let store = Store::open_in_memory().unwrap();
        store
            .insert_samples(HOST, 1_000, &[container_sample("a", Some(1.0))])
            .unwrap();
        let points = store.recent_container_samples(HOST, "a", 0).unwrap();
        assert_eq!(points.len(), 1);
        let p = &points[0];
        assert_eq!(p.net_rx, Some(10.0));
        assert_eq!(p.net_tx, Some(20.0));
        assert_eq!(p.disk_read, Some(30.0));
        assert_eq!(p.disk_write, Some(40.0));
    }

    #[test]
    fn session_secret_is_generated_once_and_stable() {
        let store = Store::open_in_memory().unwrap();
        let first = store.session_secret().unwrap();
        assert_eq!(first.len(), 32);
        // A second call returns the same persisted secret, not a fresh one.
        assert_eq!(store.session_secret().unwrap(), first);
    }

    #[test]
    fn insert_trends_roundtrip() {
        let store = Store::open_in_memory().unwrap();
        store
            .insert_container_trends(&[ContainerTrend {
                bucket_start_ms: 0,
                bucket_secs: 60,
                host: HOST.into(),
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
        assert_eq!(store.count("container_trend").unwrap(), 1);
    }

    #[test]
    fn insert_pivots_metrics_into_one_row_and_merges_incarnations() {
        // One container's metrics for one bucket land in a single wide row.
        // A second incarnation of the same name in the same bucket — whether
        // in the same batch or a later one — merges into that row instead of
        // failing the primary key: extremes kept, median weighted by samples.
        let store = Store::open_in_memory().unwrap();
        let ct = |id: &str, metric: Metric, min: f64, median: f64, max: f64, samples: u32| {
            ContainerTrend {
                bucket_start_ms: 60_000,
                bucket_secs: 60,
                host: HOST.into(),
                id: id.to_string(),
                name: "web".to_string(),
                stack: Some("shop".to_string()),
                metric: metric.as_str(),
                min,
                max,
                median,
                samples,
            }
        };
        store
            .insert_container_trends(&[
                ct("a", Metric::Cpu, 1.0, 2.0, 3.0, 10),
                ct("a", Metric::Mem, 100.0, 100.0, 100.0, 10),
                // same batch, second incarnation
                ct("b", Metric::Cpu, 0.5, 4.0, 5.0, 30),
            ])
            .unwrap();
        assert_eq!(store.count("container_trend").unwrap(), 1);
        let h = store
            .history_container(HOST, "web", 0, u64::MAX, 60_000)
            .unwrap();
        assert_eq!(h.len(), 1);
        assert_eq!(h[0].cpu_min, Some(0.5));
        assert_eq!(h[0].cpu_med, Some(3.5)); // (2·10 + 4·30) / 40
        assert_eq!(h[0].cpu_max, Some(5.0));
        assert_eq!(h[0].mem_med, Some(100.0));
        assert_eq!(h[0].net_rx_med, None);

        // A later batch for the same name and bucket merges via the upsert.
        store
            .insert_container_trends(&[
                ct("c", Metric::Cpu, 6.0, 6.0, 6.0, 40),
                ct("c", Metric::NetRx, 1.0, 1.0, 1.0, 40),
            ])
            .unwrap();
        assert_eq!(store.count("container_trend").unwrap(), 1);
        let h = store
            .history_container(HOST, "web", 0, u64::MAX, 60_000)
            .unwrap();
        assert_eq!(h[0].cpu_min, Some(0.5));
        assert_eq!(h[0].cpu_med, Some(4.75)); // (3.5·40 + 6·40) / 80
        assert_eq!(h[0].cpu_max, Some(6.0));
        assert_eq!(h[0].mem_med, Some(100.0)); // untouched by a batch without mem
        assert_eq!(h[0].net_rx_med, Some(1.0)); // filled in by the later batch
        assert_eq!(
            store.container_name(HOST, "c").unwrap().as_deref(),
            Some("web")
        );
    }

    #[test]
    fn recent_stack_trends_sums_members_and_pivots() {
        let store = Store::open_in_memory().unwrap();
        let ct = |bucket: u64, id: &str, stack: &str, metric, median: f64| ContainerTrend {
            bucket_start_ms: bucket,
            bucket_secs: 60,
            host: HOST.into(),
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

        let points = store.recent_stack_trends(HOST, "web", 0).unwrap();
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
        let recent = store.recent_stack_trends(HOST, "web", 60_000).unwrap();
        assert_eq!(recent.len(), 1);
        assert_eq!(recent[0].ts_ms, 60_000);
    }

    #[test]
    fn history_container_carries_io_envelope() {
        let store = Store::open_in_memory().unwrap();
        store
            .insert_container_trends(&[
                ContainerTrend {
                    bucket_start_ms: 0,
                    bucket_secs: 60,
                    host: HOST.into(),
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
                    host: HOST.into(),
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

        let h = store
            .history_container(HOST, "c-a", 0, u64::MAX, 60_000)
            .unwrap();
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
            host: HOST.into(),
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

        let fine = store
            .history_stack(HOST, "web", 0, u64::MAX, 60_000)
            .unwrap();
        assert_eq!(fine.len(), 2);
        assert_eq!(fine[0].cpu_min, Some(3.0));
        assert_eq!(fine[0].cpu_med, Some(10.0));
        assert_eq!(fine[0].cpu_max, Some(19.0));

        // Coarse group merges the summed buckets.
        let coarse = store
            .history_stack(HOST, "web", 0, u64::MAX, 120_000)
            .unwrap();
        assert_eq!(coarse.len(), 1);
        assert_eq!(coarse[0].cpu_min, Some(2.0));
        assert_eq!(coarse[0].cpu_med, Some(7.5)); // avg(10, 5)
        assert_eq!(coarse[0].cpu_max, Some(19.0));

        // Unknown stack and container queries return empty, not errors.
        assert!(
            store
                .history_stack(HOST, "nope", 0, u64::MAX, 60_000)
                .unwrap()
                .is_empty()
        );
        assert!(
            store
                .history_container(HOST, "nope", 0, u64::MAX, 60_000)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn raw_history_downsamples_and_respects_window() {
        let store = Store::open_in_memory().unwrap();
        let c = |cpu| [container_sample("a", Some(cpu))];
        for (ts, cpu) in [(0, 10.0), (1_000, 30.0), (2_000, 20.0), (5_000, 40.0)] {
            store.insert_samples(HOST, ts, &c(cpu)).unwrap();
        }

        // Group of 3s: samples 0/1000/2000 merge into one envelope point.
        let h = store
            .history_container_raw(HOST, "a", 0, u64::MAX, 3_000)
            .unwrap();
        assert_eq!(h.len(), 2, "4 samples in 3s groups must yield 2 points");
        assert_eq!(h[0].cpu_min, Some(10.0));
        assert_eq!(h[0].cpu_med, Some(20.0)); // avg(10, 30, 20)
        assert_eq!(h[0].cpu_max, Some(30.0));

        // A group below the sample spacing keeps full resolution: each sample
        // its own point, envelope collapsed to the value.
        let full = store
            .history_container_raw(HOST, "a", 0, u64::MAX, 500)
            .unwrap();
        assert_eq!(full.len(), 4);
        assert_eq!(full[3].cpu_min, full[3].cpu_max);

        // `until` bounds the query itself (not post-filtering in Rust).
        let bounded = store
            .history_container_raw(HOST, "a", 1_000, 2_000, 500)
            .unwrap();
        assert_eq!(bounded.len(), 2);
    }

    #[test]
    fn recent_container_samples_filters_and_orders() {
        let store = Store::open_in_memory().unwrap();
        let c = |cpu| [container_sample("a", Some(cpu))];
        store.insert_samples(HOST, 5_000, &c(20.0)).unwrap();
        store.insert_samples(HOST, 1_000, &c(10.0)).unwrap();
        store.insert_samples(HOST, 9_000, &c(30.0)).unwrap();

        let points = store.recent_container_samples(HOST, "a", 5_000).unwrap();
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
            host: HOST.into(),
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
