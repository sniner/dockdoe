//! DockDoe — a single-binary Docker host monitor with an embedded web UI.

// Pedantic lints we deliberately accept project-wide:
// - cast_precision_loss: metric math converts byte/nanosecond counters to f64
//   for percentages and human-readable sizes; f64 precision is far more than
//   enough at these magnitudes and the values are display-only.
// - doc_markdown: "DockDoe" is the product name in prose, not a code item.
#![allow(clippy::cast_precision_loss, clippy::doc_markdown)]

mod auth;
mod collector;
mod config;
mod docker;
mod model;
mod notify;
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
use crate::notify::Notifier;
use crate::store::Store;

/// DockDoe — a single-binary Docker host monitor with an embedded web UI.
///
/// Every option can also be set via its `DOCKDOE_*` environment variable; the
/// command-line flag wins when both are given.
#[derive(Parser)]
#[command(name = "dockdoe", version, about, long_about = None)]
struct Cli {
    /// Path to a `config.toml` listing the Docker hosts to monitor (and,
    /// optionally, the global options below). Without it, DockDoe monitors a
    /// single local host configured from these flags/environment variables.
    #[arg(long, env = "DOCKDOE_CONFIG")]
    config: Option<PathBuf>,

    /// Address the web UI binds to. Use 0.0.0.0:8080 to expose it on the network.
    #[arg(long, env = "DOCKDOE_BIND", default_value = "127.0.0.1:8080")]
    bind: String,

    /// Path to the SQLite database file.
    #[arg(long, env = "DOCKDOE_DB_PATH", default_value = "dockdoe.sqlite")]
    db_path: PathBuf,

    /// Seconds between metric samples.
    // Zero would panic in `tokio::time::interval` — inside the spawned
    // collector task, killing it silently while the web UI keeps running.
    #[arg(long, env = "DOCKDOE_INTERVAL_SECS", default_value_t = 3,
          value_parser = clap::value_parser!(u64).range(1..))]
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

    /// Hostnames the web UI may be addressed as, comma-separated (e.g.
    /// "dockhost.lan"). When set, requests whose Host header matches neither
    /// this list nor a localhost form are rejected — a guard against DNS
    /// rebinding. Unset disables the check.
    #[arg(long, env = "DOCKDOE_ALLOWED_HOSTS", value_delimiter = ',')]
    allowed_hosts: Vec<String>,

    /// Apprise endpoint to POST container state-change notifications to, e.g.
    /// "https://apprise.example/notify/<key>". Unset disables notifications
    /// entirely. The target services are configured in Apprise, not here.
    #[arg(long, env = "DOCKDOE_APPRISE_URL")]
    apprise_url: Option<String>,

    /// How long a container's new state must persist before it is notified
    /// about, in seconds. Swallows flapping (restart loops, brief blips).
    #[arg(long, env = "DOCKDOE_NOTIFY_DELAY", default_value_t = 30)]
    notify_delay_secs: u64,

    /// Username for the web UI login. Set together with `--auth-password` to
    /// require authentication; leave both unset to keep the UI open.
    #[arg(long, env = "DOCKDOE_AUTH_USER")]
    auth_user: Option<String>,

    /// Password for the web UI login (see `--auth-user`).
    #[arg(long, env = "DOCKDOE_AUTH_PASSWORD")]
    auth_password: Option<String>,

    /// Mark the session cookie `Secure` (sent only over HTTPS). Leave off for
    /// plain-http access on a LAN; turn on when serving behind a TLS proxy.
    #[arg(long, env = "DOCKDOE_COOKIE_SECURE", default_value_t = false)]
    cookie_secure: bool,

    /// Host the published container ports are reachable at, used for the port
    /// pills' links. Unset: link to the host the browser uses (right when
    /// browsing DockDoe directly on the Docker host). Set to an IP/hostname when
    /// a reverse proxy serves the UI but the ports live elsewhere. Set to "off"
    /// to render ports as plain pills with no links (proxy-only setups).
    #[arg(long, env = "DOCKDOE_PORT_HOST")]
    port_host: Option<String>,

    /// Tracing filter, e.g. "info" or "dockdoe=debug".
    #[arg(long, env = "DOCKDOE_LOG", default_value = "info")]
    log: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let cfg = config::load(cli.config.as_deref(), &cli)?;
    init_tracing(&cfg.log);

    let collector_config = Config {
        interval: Duration::from_secs(cfg.interval_secs),
        raw_retention: Duration::from_secs(cfg.raw_retention_secs),
        trend_bucket_secs: cfg.trend_bucket_secs,
        trend_retention: Duration::from_secs(cfg.trend_retention_secs),
    };

    let store = Store::open(&cfg.db_path)
        .with_context(|| format!("opening store at {}", cfg.db_path.display()))?;
    info!(db = %cfg.db_path.display(), "store ready");

    // M3: a single host (the first configured one). M4 loops over all of them.
    let host_cfg = &cfg.hosts[0];
    let docker = DockerClient::connect_from(host_cfg)
        .with_context(|| format!("connecting to host {:?}", host_cfg.name))?;
    let docker_handle = DockerHandle::connect_from(host_cfg)
        .with_context(|| format!("connecting to host {:?}", host_cfg.name))?;
    let shared = Arc::new(RwLock::new(None));

    // CPU count scales the container CPU bars (a full bar is the whole host);
    // sourced from the daemon's `info` (NCPU), falling back to local parallelism.
    let cpu_count = docker.cpu_count().await;

    // The chart seed window matches raw retention — that's how much history we
    // can show before the live stream takes over.
    let seed_window = collector_config.raw_retention;

    let notifier = cfg.apprise_url.clone().map(|url| {
        info!(
            delay_secs = cfg.notify_delay_secs,
            "Apprise notifications enabled"
        );
        Notifier::new(url, Duration::from_secs(cfg.notify_delay_secs))
    });

    // M2: a single collector still drives the one local host. The host key is
    // hardcoded "local" to match the web layer's reads; M4 wires the real
    // per-host names from the config and spawns one collector per host.
    tokio::spawn(collector::run(
        "local".to_string(),
        docker,
        cpu_count,
        store.clone(),
        collector_config,
        Arc::clone(&shared),
        notifier,
    ));

    let allowed_hosts = web::normalize_allowed_hosts(&cfg.allowed_hosts);
    if !allowed_hosts.is_empty() {
        info!(hosts = ?allowed_hosts, "Host allowlist enabled (plus localhost forms)");
    }

    // Authentication is opt-in: both credentials must be set. Exactly one set is
    // almost certainly a misconfiguration, so fail loudly rather than silently
    // leaving the UI open.
    let auth = match (cfg.auth_user.clone(), cfg.auth_password.clone()) {
        (Some(user), Some(password)) => {
            let secret = store.session_secret().context("loading session secret")?;
            info!(
                secure_cookie = cfg.cookie_secure,
                "web UI authentication enabled"
            );
            Some(auth::Auth::new(user, password, secret, cfg.cookie_secure))
        }
        (None, None) => None,
        _ => anyhow::bail!(
            "set both DOCKDOE_AUTH_USER and DOCKDOE_AUTH_PASSWORD to enable authentication, or neither"
        ),
    };

    let app = web::router(web::AppState {
        shared,
        store,
        docker: docker_handle,
        seed_window,
        allowed_hosts,
        auth,
        port_host: cfg.hosts.first().and_then(|h| h.public_host.clone()),
    });
    let listener = tokio::net::TcpListener::bind(&cfg.bind)
        .await
        .with_context(|| format!("binding to {}", cfg.bind))?;
    info!(bind = %cfg.bind, "DockDoe listening");

    // Stop on SIGTERM (what `docker stop` sends) or SIGINT (Ctrl-C). Without a
    // handler this matters most in a container: as PID 1 the process gets no
    // default action for these signals, so SIGTERM is ignored and Docker falls
    // back to SIGKILL after its grace period (~10 s). We exit on the signal
    // instead of draining connections — the long-lived SSE streams would never
    // close on their own, and a monitoring UI has nothing to flush (the next
    // collector cycle is abandoned; SQLite's rollback journal keeps the file
    // consistent). Browsers simply reconnect on the next start.
    tokio::select! {
        result = axum::serve(listener, app) => {
            result.context("running the web server")?;
        }
        () = shutdown_signal() => {
            info!("shutdown signal received; exiting");
        }
    }
    Ok(())
}

/// Resolves when the process is asked to stop: SIGTERM or SIGINT.
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("install Ctrl-C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {},
        () = terminate => {},
    }
}

fn init_tracing(filter: &str) {
    let filter = EnvFilter::try_new(filter)
        .or_else(|_| EnvFilter::try_new("info"))
        .unwrap_or_default();
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_interval_is_rejected_at_parse_time() {
        assert!(Cli::try_parse_from(["dockdoe", "--interval-secs", "0"]).is_err());
        assert!(Cli::try_parse_from(["dockdoe", "--interval-secs", "1"]).is_ok());
    }
}
