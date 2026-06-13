//! Background collection task.
//!
//! Periodically samples host and container metrics, publishes the latest
//! snapshot into shared state for the web layer, and persists raw samples plus
//! trend rollups to the store. Keeping collection off the request path means
//! the UI never blocks on the Docker socket or the database.

use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tracing::{debug, error, info, warn};

use crate::docker::DockerClient;
use crate::host::HostSampler;
use crate::model::Dashboard;
use crate::notify::Notifier;
use crate::store::Store;
use crate::trend::Bucketer;

/// The latest dashboard snapshot — the single source of truth shared between
/// the collector (writer) and the web handlers (readers, including the live SSE
/// streams, which poll it). `None` until the first successful collection.
pub type SharedDashboard = Arc<RwLock<Option<Dashboard>>>;

/// Tunables for the collection loop.
pub struct Config {
    /// Time between samples.
    pub interval: Duration,
    /// How long raw samples are kept ("point A").
    pub raw_retention: Duration,
    /// Width of a trend bucket, in seconds.
    pub trend_bucket_secs: u64,
    /// How long trend rollups are kept (separate, longer than raw).
    pub trend_retention: Duration,
}

/// Run the collection loop forever.
pub async fn run(
    mut docker: DockerClient,
    mut host: HostSampler,
    store: Store,
    config: Config,
    shared: SharedDashboard,
    mut notifier: Option<Notifier>,
) {
    info!(
        interval = ?config.interval,
        raw_retention = ?config.raw_retention,
        trend_bucket_secs = config.trend_bucket_secs,
        "starting metrics collector"
    );
    let mut ticker = tokio::time::interval(config.interval);
    let mut bucketer = Bucketer::new(config.trend_bucket_secs);
    let raw_retention_ms = duration_ms(config.raw_retention);
    let trend_retention_ms = duration_ms(config.trend_retention);
    let mut primed = false;

    loop {
        ticker.tick().await;

        let host_metrics = host.sample();
        let containers = match docker.collect().await {
            Ok(containers) => containers,
            Err(err) => {
                error!(%err, "collecting container metrics failed; keeping last snapshot");
                continue;
            }
        };

        // The first successful cycle only primes the delta-based readings:
        // sysinfo's first CPU refresh reads ~0%, and container CPU% is None
        // without a previous sample. Publishing or persisting it would put a
        // bogus 0% dip at the start of every chart after a restart — discard
        // it and let the next cycle deliver real values.
        if !primed {
            primed = true;
            debug!("priming cycle done; publishing starts next cycle");
            continue;
        }

        let ts_ms = now_unix_ms();

        // Check for container state changes worth notifying about. Runs only on
        // real cycles (the priming cycle above already `continue`d), so the
        // notifier's first sighting is a genuine baseline, not a startup flood.
        if let Some(n) = notifier.as_mut() {
            n.observe(ts_ms, &containers);
        }

        let flushed = bucketer.push(ts_ms, &host_metrics, &containers);
        if !flushed.is_empty() {
            debug!(
                host_trends = flushed.host.len(),
                container_trends = flushed.containers.len(),
                "trend bucket closed"
            );
        }

        let dashboard = Dashboard {
            generated_at_unix_ms: ts_ms,
            host: host_metrics,
            containers,
        };
        // Publish the latest snapshot; the web layer (page render and live SSE)
        // reads it from here.
        publish(&shared, dashboard.clone());

        persist(
            &store,
            ts_ms,
            Arc::new(dashboard),
            flushed,
            ts_ms.saturating_sub(raw_retention_ms),
            ts_ms.saturating_sub(trend_retention_ms),
        )
        .await;
    }
}

fn publish(shared: &SharedDashboard, dashboard: Dashboard) {
    match shared.write() {
        Ok(mut guard) => *guard = Some(dashboard),
        // A reader panicked while holding the lock; recover and carry on.
        Err(poisoned) => *poisoned.into_inner() = Some(dashboard),
    }
}

/// Persist one cycle on the blocking pool: raw samples, any closed trend
/// buckets, and retention pruning. Failures are logged, not fatal.
async fn persist(
    store: &Store,
    ts_ms: u64,
    dashboard: Arc<Dashboard>,
    flushed: crate::trend::Flushed,
    raw_cutoff_ms: u64,
    trend_cutoff_ms: u64,
) {
    let store = store.clone();
    let result = tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
        store.insert_samples(ts_ms, &dashboard.host, &dashboard.containers)?;
        store.insert_host_trends(&flushed.host)?;
        store.insert_container_trends(&flushed.containers)?;
        store.prune_raw(raw_cutoff_ms)?;
        store.prune_trends(trend_cutoff_ms)?;
        Ok(())
    })
    .await;

    match result {
        Ok(Ok(())) => {}
        Ok(Err(err)) => warn!(%err, "persisting metrics failed"),
        Err(join_err) => warn!(%join_err, "persistence task panicked"),
    }
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

fn duration_ms(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}
