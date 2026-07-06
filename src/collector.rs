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
    /// How often to run retention pruning. Pruning every sample cycle is pure
    /// overhead — retention is hours/days, so nothing ages out between two
    /// 3-second samples. We batch it onto this slower cadence instead.
    pub prune_interval: Duration,
}

/// Run the collection loop forever for one Docker host. `host` is the host's
/// config `name`, stamped onto every sample and trend row.
pub async fn run(
    host: String,
    mut docker: DockerClient,
    cpu_count: usize,
    store: Store,
    config: Config,
    shared: SharedDashboard,
    mut notifier: Option<Notifier>,
) {
    info!(
        host = %host,
        interval = ?config.interval,
        raw_retention = ?config.raw_retention,
        trend_bucket_secs = config.trend_bucket_secs,
        prune_interval = ?config.prune_interval,
        "starting metrics collector"
    );
    let mut ticker = tokio::time::interval(config.interval);
    let mut bucketer = Bucketer::new(config.trend_bucket_secs, host.clone());
    let raw_retention_ms = duration_ms(config.raw_retention);
    let trend_retention_ms = duration_ms(config.trend_retention);
    // Prune every Nth cycle rather than every cycle.
    let prune_every_cycles = prune_cadence(config.prune_interval, config.interval);
    let mut cycle: u64 = 0;
    let mut primed = false;

    loop {
        ticker.tick().await;

        let containers = match docker.collect().await {
            Ok(containers) => containers,
            Err(err) => {
                error!(%err, "collecting container metrics failed; keeping last snapshot");
                continue;
            }
        };

        // The first successful cycle only primes the delta-based readings:
        // container CPU% is None without a previous sample to delta against.
        // Publishing or persisting it would put a bogus 0% dip at the start of
        // every chart after a restart — discard it and let the next cycle
        // deliver real values.
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

        let flushed = bucketer.push(ts_ms, &containers);
        if !flushed.is_empty() {
            debug!(
                container_trends = flushed.containers.len(),
                "trend bucket closed"
            );
        }

        let dashboard = Dashboard {
            generated_at_unix_ms: ts_ms,
            cpu_count,
            containers,
        };
        // Publish the latest snapshot; the web layer (page render and live SSE)
        // reads it from here.
        publish(&shared, dashboard.clone());

        // Prune on the first real cycle (clearing anything that aged out while
        // we were down) and every `prune_every_cycles` after; otherwise just
        // persist the samples. `Some(cutoffs)` requests the prune, `None` skips.
        let prune_cutoffs = cycle.is_multiple_of(prune_every_cycles).then(|| {
            (
                ts_ms.saturating_sub(raw_retention_ms),
                ts_ms.saturating_sub(trend_retention_ms),
            )
        });
        cycle += 1;

        persist(
            &store,
            host.clone(),
            ts_ms,
            Arc::new(dashboard),
            flushed,
            prune_cutoffs,
        )
        .await;
    }
}

pub(crate) fn publish(shared: &SharedDashboard, dashboard: Dashboard) {
    match shared.write() {
        Ok(mut guard) => *guard = Some(dashboard),
        // A reader panicked while holding the lock; recover and carry on.
        Err(poisoned) => *poisoned.into_inner() = Some(dashboard),
    }
}

/// Persist one cycle on the blocking pool: raw samples, any closed trend
/// buckets, and — when `prune_cutoffs` is `Some((raw, trend))` — retention
/// pruning to those cutoffs. Failures are logged, not fatal.
async fn persist(
    store: &Store,
    host: String,
    ts_ms: u64,
    dashboard: Arc<Dashboard>,
    flushed: crate::trend::Flushed,
    prune_cutoffs: Option<(u64, u64)>,
) {
    let store = store.clone();
    let result = tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
        store.insert_samples(&host, ts_ms, &dashboard.containers)?;
        store.insert_container_trends(&flushed.containers)?;
        if let Some((raw_cutoff_ms, trend_cutoff_ms)) = prune_cutoffs {
            let removed_raw = store.prune_raw(raw_cutoff_ms)?;
            let removed_trends = store.prune_trends(trend_cutoff_ms)?;
            // Row counts make retention behaviour diagnosable from the log —
            // "is pruning running, and how much does it chew per pass?"
            debug!(%host, removed_raw, removed_trends, "pruned aged-out data");
        }
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

/// How many sample cycles between retention prunes: prune roughly every
/// `prune_interval`, given one sample every `interval`. Floors to at least 1 so
/// a `prune_interval` at or below one sample cycle prunes every cycle rather
/// than dividing to zero.
fn prune_cadence(prune_interval: Duration, interval: Duration) -> u64 {
    (prune_interval.as_secs() / interval.as_secs().max(1)).max(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prune_cadence_batches_by_interval() {
        // Default: prune hourly, sample every 3s → every 1200 cycles.
        let cad = |p, i| prune_cadence(Duration::from_secs(p), Duration::from_secs(i));
        assert_eq!(cad(3600, 3), 1200);
        // Sample every second → 3600 cycles.
        assert_eq!(cad(3600, 1), 3600);
        // Prune interval equal to the sample interval → every cycle.
        assert_eq!(cad(5, 5), 1);
        // Prune interval below one sample cycle → still prune every cycle,
        // never zero (which would panic the `cycle % n` modulo).
        assert_eq!(cad(1, 3), 1);
    }
}
