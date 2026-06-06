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
use clap::Parser;
use tracing::info;
use tracing_subscriber::EnvFilter;

use crate::collector::Config;
use crate::docker::{DockerClient, DockerHandle};
use crate::host::HostSampler;
use crate::store::Store;

/// DockDoe — a single-binary Docker host monitor with an embedded web UI.
///
/// Every option can also be set via its `DOCKDOE_*` environment variable; the
/// command-line flag wins when both are given.
#[derive(Parser)]
#[command(name = "dockdoe", version, about, long_about = None)]
struct Cli {
    /// Address the web UI binds to. Use 0.0.0.0:8080 to expose it on the network.
    #[arg(long, env = "DOCKDOE_BIND", default_value = "127.0.0.1:8080")]
    bind: String,

    /// Path to the SQLite database file.
    #[arg(long, env = "DOCKDOE_DB_PATH", default_value = "dockdoe.sqlite")]
    db_path: PathBuf,

    /// Seconds between metric samples.
    #[arg(long, env = "DOCKDOE_INTERVAL_SECS", default_value_t = 3)]
    interval_secs: u64,

    /// How long raw samples are kept, in seconds ("point A").
    #[arg(long, env = "DOCKDOE_RAW_RETENTION_SECS", default_value_t = 3600)]
    raw_retention_secs: u64,

    /// Trend rollup window (min/max/median per bucket), in seconds.
    #[arg(long, env = "DOCKDOE_TREND_BUCKET_SECS", default_value_t = 60)]
    trend_bucket_secs: u64,

    /// How long trend rollups are kept, in seconds (default 30 days).
    #[arg(long, env = "DOCKDOE_TREND_RETENTION_SECS", default_value_t = 30 * 24 * 3600)]
    trend_retention_secs: u64,

    /// Tracing filter, e.g. "info" or "dockdoe=debug".
    #[arg(long, env = "DOCKDOE_LOG", default_value = "info")]
    log: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    init_tracing(&cli.log);

    let config = Config {
        interval: Duration::from_secs(cli.interval_secs),
        raw_retention: Duration::from_secs(cli.raw_retention_secs),
        trend_bucket_secs: cli.trend_bucket_secs,
        trend_retention: Duration::from_secs(cli.trend_retention_secs),
    };

    let store = Store::open(&cli.db_path)
        .with_context(|| format!("opening store at {}", cli.db_path.display()))?;
    info!(db = %cli.db_path.display(), "store ready");

    let docker = DockerClient::connect()?;
    let docker_handle = DockerHandle::connect()?;
    let host = HostSampler::new();
    let shared = Arc::new(RwLock::new(None));

    // The chart seed window matches raw retention — that's how much history we
    // can show before the live stream takes over.
    let seed_window = config.raw_retention;

    tokio::spawn(collector::run(
        docker,
        host,
        store.clone(),
        config,
        Arc::clone(&shared),
    ));

    let app = web::router(web::AppState {
        shared,
        store,
        docker: docker_handle,
        seed_window,
    });
    let listener = tokio::net::TcpListener::bind(&cli.bind)
        .await
        .with_context(|| format!("binding to {}", cli.bind))?;
    info!(bind = %cli.bind, "DockDoe listening");

    axum::serve(listener, app)
        .await
        .context("running the web server")?;
    Ok(())
}

fn init_tracing(filter: &str) {
    let filter = EnvFilter::try_new(filter)
        .or_else(|_| EnvFilter::try_new("info"))
        .unwrap_or_default();
    tracing_subscriber::fmt().with_env_filter(filter).init();
}
