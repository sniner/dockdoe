//! DockDoe — a single-binary Docker host monitor with an embedded web UI.

// Pedantic lints we deliberately accept project-wide:
// - cast_precision_loss: metric math converts byte/nanosecond counters to f64
//   for percentages and human-readable sizes; f64 precision is far more than
//   enough at these magnitudes and the values are display-only.
// - doc_markdown: "DockDoe" is the product name in prose, not a code item.
#![allow(clippy::cast_precision_loss, clippy::doc_markdown)]

mod collector;
mod docker;
mod host;
mod model;
mod store;
mod trend;
mod web;

use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::{Context, Result};
use tracing::info;
use tracing_subscriber::EnvFilter;

use crate::collector::Config;
use crate::docker::DockerClient;
use crate::host::HostSampler;
use crate::store::Store;

/// Default address the web UI binds to.
const DEFAULT_BIND: &str = "127.0.0.1:8080";
/// Default seconds between metric samples.
const DEFAULT_INTERVAL_SECS: u64 = 3;
/// Default path of the SQLite database.
const DEFAULT_DB_PATH: &str = "dockdoe.sqlite";
/// Default raw-sample retention ("point A"): one hour.
const DEFAULT_RAW_RETENTION_SECS: u64 = 3600;
/// Default trend bucket width: one minute.
const DEFAULT_TREND_BUCKET_SECS: u64 = 60;
/// Default trend retention: 30 days.
const DEFAULT_TREND_RETENTION_SECS: u64 = 30 * 24 * 3600;

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    let bind = std::env::var("DOCKDOE_BIND").unwrap_or_else(|_| DEFAULT_BIND.to_string());
    let db_path = PathBuf::from(
        std::env::var("DOCKDOE_DB_PATH").unwrap_or_else(|_| DEFAULT_DB_PATH.to_string()),
    );
    let config = Config {
        interval: env_secs("DOCKDOE_INTERVAL_SECS", DEFAULT_INTERVAL_SECS),
        raw_retention: env_secs("DOCKDOE_RAW_RETENTION_SECS", DEFAULT_RAW_RETENTION_SECS),
        trend_bucket_secs: env_u64("DOCKDOE_TREND_BUCKET_SECS", DEFAULT_TREND_BUCKET_SECS),
        trend_retention: env_secs("DOCKDOE_TREND_RETENTION_SECS", DEFAULT_TREND_RETENTION_SECS),
    };

    let store =
        Store::open(&db_path).with_context(|| format!("opening store at {}", db_path.display()))?;
    info!(db = %db_path.display(), "store ready");

    let docker = DockerClient::connect()?;
    let host = HostSampler::new();
    let shared = Arc::new(RwLock::new(None));
    let (snapshots, _) = tokio::sync::broadcast::channel(16);

    // The chart seed window matches raw retention — that's how much history we
    // can show before the live stream takes over.
    let seed_window = config.raw_retention;

    tokio::spawn(collector::run(
        docker,
        host,
        store.clone(),
        config,
        Arc::clone(&shared),
        snapshots.clone(),
    ));

    let app = web::router(web::AppState {
        shared,
        snapshots,
        store,
        seed_window,
    });
    let listener = tokio::net::TcpListener::bind(&bind)
        .await
        .with_context(|| format!("binding to {bind}"))?;
    info!(%bind, "DockDoe listening");

    axum::serve(listener, app)
        .await
        .context("running the web server")?;
    Ok(())
}

fn init_tracing() {
    let filter = EnvFilter::try_from_env("DOCKDOE_LOG")
        .or_else(|_| EnvFilter::try_new("info"))
        .unwrap_or_default();
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

/// Read a `u64` from an env var, falling back to `default` if unset or
/// unparseable.
fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

/// Read a duration in seconds from an env var.
fn env_secs(key: &str, default_secs: u64) -> Duration {
    Duration::from_secs(env_u64(key, default_secs))
}
