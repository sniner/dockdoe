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
mod web;

use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::{Context, Result};
use tracing::info;
use tracing_subscriber::EnvFilter;

use crate::docker::DockerClient;
use crate::host::HostSampler;

/// Default address the web UI binds to.
const DEFAULT_BIND: &str = "127.0.0.1:8080";
/// Default seconds between metric samples.
const DEFAULT_INTERVAL_SECS: u64 = 3;

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    let bind = std::env::var("DOCKDOE_BIND").unwrap_or_else(|_| DEFAULT_BIND.to_string());
    let interval = Duration::from_secs(
        std::env::var("DOCKDOE_INTERVAL_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_INTERVAL_SECS),
    );

    let docker = DockerClient::connect()?;
    let host = HostSampler::new();
    let shared = Arc::new(RwLock::new(None));

    tokio::spawn(collector::run(docker, host, interval, Arc::clone(&shared)));

    let app = web::router(shared);
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
