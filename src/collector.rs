//! Background collection task.
//!
//! Periodically samples host and container metrics and publishes the latest
//! snapshot into shared state for the web layer to read. Keeping the collector
//! separate from the request path means the UI never blocks on the Docker
//! socket, and it's the natural seam where the SQLite store will plug in
//! (milestone 2).

use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tracing::{error, info};

use crate::docker::DockerClient;
use crate::host::HostSampler;
use crate::model::Dashboard;

/// The latest dashboard snapshot, shared between collector and web handlers.
/// `None` until the first successful collection.
pub type SharedDashboard = Arc<RwLock<Option<Dashboard>>>;

/// Run the collection loop forever, sampling every `interval`.
pub async fn run(
    mut docker: DockerClient,
    mut host: HostSampler,
    interval: Duration,
    shared: SharedDashboard,
) {
    info!(?interval, "starting metrics collector");
    let mut ticker = tokio::time::interval(interval);

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

        let dashboard = Dashboard {
            generated_at_unix_ms: now_unix_ms(),
            host: host_metrics,
            containers,
        };

        match shared.write() {
            Ok(mut guard) => *guard = Some(dashboard),
            Err(poisoned) => {
                // A reader panicked while holding the lock. Recover the guard
                // and carry on rather than poisoning the whole collector.
                *poisoned.into_inner() = Some(dashboard);
            }
        }
    }
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}
