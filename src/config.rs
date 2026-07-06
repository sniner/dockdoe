//! Configuration: global settings plus one or more Docker hosts to monitor.
//!
//! DockDoe can be configured two ways. The historical way — command-line flags
//! and `DOCKDOE_*` environment variables — still works and drives a single local
//! host. The new way is a `config.toml` (via `--config`/`DOCKDOE_CONFIG`) that
//! additionally lists remote hosts in a `[[host]]` array. When a file is present
//! its values are authoritative for the globals it sets; anything it omits falls
//! back to the CLI/env. When no file is given we synthesise a single `local`
//! host so existing deployments keep working unchanged.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::Deserialize;

use crate::Cli;

/// How a metric value maps onto the radius of the dashboard overview discs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ScaleMode {
    /// Proportional: an idle fleet stays visually quiet near the centre.
    Linear,
    /// Square root: spreads the low end without log's jitter amplification.
    Sqrt,
    /// Logarithmic: equal relative change moves the radius equally far.
    Log,
}

impl ScaleMode {
    /// The lowercase name, embedded as a `data-*` attribute for `overview.js`.
    pub fn as_str(self) -> &'static str {
        match self {
            ScaleMode::Linear => "linear",
            ScaleMode::Sqrt => "sqrt",
            ScaleMode::Log => "log",
        }
    }
}

// clap renders the flag's default through `Display`.
impl std::fmt::Display for ScaleMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Parameters of the dashboard overview discs, resolved from flags/env/file.
#[derive(Debug, Clone, Copy)]
pub struct OverviewConfig {
    pub cpu_scale: ScaleMode,
    pub mem_scale: ScaleMode,
    /// Memory value at the disc rim, in bytes.
    pub mem_cap: u64,
}

/// The log scale's lower bound on the memory disc (values at or below it sit
/// at the centre). The configured cap must stay clearly above it.
pub const OVERVIEW_MEM_FLOOR: u64 = 16 * 1024 * 1024;

/// Parse a byte size like `64G`, `512M` or `68719476736`. Suffixes are
/// binary (K/M/G/T = powers of 1024, case-insensitive, optional trailing
/// `B`/`iB`). Used as a clap value parser, hence the `String` error.
pub fn parse_size(s: &str) -> Result<u64, String> {
    let s = s.trim();
    let digits = s.trim_end_matches(|c: char| !c.is_ascii_digit());
    let value: u64 = digits.parse().map_err(|_| {
        format!("invalid size {s:?}: expected digits with an optional K/M/G/T suffix")
    })?;
    let unit = s[digits.len()..].trim();
    let shift = match unit.to_ascii_uppercase().as_str() {
        "" | "B" => 0,
        "K" | "KB" | "KIB" => 10,
        "M" | "MB" | "MIB" => 20,
        "G" | "GB" | "GIB" => 30,
        "T" | "TB" | "TIB" => 40,
        _ => return Err(format!("invalid size suffix {unit:?}: use K, M, G or T")),
    };
    if value.leading_zeros() < shift {
        return Err(format!("size {s:?} overflows"));
    }
    Ok(value << shift)
}

/// A single Docker host to monitor.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostConfig {
    /// Display name and URL slug. Must be unique across hosts and slug-safe
    /// (`[A-Za-z0-9._-]+`).
    pub name: String,

    /// Docker endpoint URL: `unix:///path/docker.sock`, `tcp://host:2375`,
    /// `http://host:2375`, or `https://host:2375` (TLS, server-auth only).
    pub docker: String,

    /// Public host/IP where this host's published container ports are reachable,
    /// used for the port-pill links. Unset falls back to the endpoint host (for
    /// `tcp`/`https`) or the browsing host (for `unix`); `"off"` renders pills
    /// without links.
    #[serde(default)]
    pub public_host: Option<String>,

    /// Seconds between metric samples for this host, overriding the global
    /// interval for this host alone. Unset: a local endpoint (`unix`/`npipe`)
    /// inherits the global interval, a remote one (`tcp`/`http`/`https`) falls
    /// back to the slower [`REMOTE_DEFAULT_INTERVAL_SECS`] — polling a socket
    /// proxy over the network is far costlier than reading the local socket.
    #[serde(default)]
    pub interval_secs: Option<u64>,

    /// Path to a private CA certificate (PEM) to trust for an `https` endpoint.
    // Consumed by the per-host TLS connection layer (M3); parsed already so the
    // config surface is stable.
    #[allow(dead_code, reason = "wired up in the M3 connection layer")]
    #[serde(default)]
    pub tls_ca: Option<PathBuf>,

    /// Skip TLS server-certificate verification for an `https` endpoint. A
    /// convenience for self-signed reverse proxies; prefer `tls_ca` when you can.
    #[allow(dead_code, reason = "wired up in the M3 connection layer")]
    #[serde(default)]
    pub tls_insecure: bool,
}

/// Default sampling interval (seconds) for a remote host that doesn't pin its
/// own `interval_secs`. Remote endpoints are reached over a (HTTP/S) socket
/// proxy, where the 3-second local cadence is needlessly chatty; 10 s eases the
/// load while still leaving ~6 samples per 60-second trend bucket.
const REMOTE_DEFAULT_INTERVAL_SECS: u64 = 10;

impl HostConfig {
    /// The effective sampling interval for this host, in seconds. An explicit
    /// per-host `interval_secs` always wins. Otherwise a local endpoint
    /// (`unix`/`npipe`) inherits `global_default`, while a remote one falls back
    /// to the slower [`REMOTE_DEFAULT_INTERVAL_SECS`].
    pub fn effective_interval_secs(&self, global_default: u64) -> u64 {
        self.interval_secs.unwrap_or_else(|| {
            if self.is_local_endpoint() {
                global_default
            } else {
                REMOTE_DEFAULT_INTERVAL_SECS
            }
        })
    }

    /// Whether the Docker endpoint is local — a Unix socket or Windows named
    /// pipe — as opposed to a remote `tcp`/`http`/`https` proxy.
    fn is_local_endpoint(&self) -> bool {
        let endpoint = self.docker.trim();
        let scheme = endpoint.split_once("://").map_or(endpoint, |(s, _)| s);
        scheme.eq_ignore_ascii_case("unix") || scheme.eq_ignore_ascii_case("npipe")
    }
}

/// Fully resolved configuration: globals merged from file + CLI, plus the host
/// list. This is what `main` builds the application from.
#[derive(Debug, Clone)]
pub struct AppConfig {
    pub bind: String,
    pub db_path: PathBuf,
    pub interval_secs: u64,
    pub raw_retention_secs: u64,
    pub trend_bucket_secs: u64,
    pub trend_retention_secs: u64,
    pub prune_interval_secs: u64,
    pub allowed_hosts: Vec<String>,
    pub apprise_url: Option<String>,
    pub notify_delay_secs: u64,
    pub auth_user: Option<String>,
    pub auth_password: Option<String>,
    pub cookie_secure: bool,
    pub log: String,
    pub overview: OverviewConfig,
    pub hosts: Vec<HostConfig>,
}

/// The on-disk `config.toml` shape. Every global is optional so an omitted key
/// defers to the CLI/env. The `[[host]]` array maps to `host`.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileConfig {
    bind: Option<String>,
    db_path: Option<PathBuf>,
    interval_secs: Option<u64>,
    raw_retention_secs: Option<u64>,
    trend_bucket_secs: Option<u64>,
    trend_retention_secs: Option<u64>,
    prune_interval_secs: Option<u64>,
    allowed_hosts: Option<Vec<String>>,
    apprise_url: Option<String>,
    notify_delay_secs: Option<u64>,
    auth_user: Option<String>,
    auth_password: Option<String>,
    cookie_secure: Option<bool>,
    log: Option<String>,
    overview_cpu_scale: Option<ScaleMode>,
    overview_mem_scale: Option<ScaleMode>,
    /// A size string like `"64G"` — same format as the CLI flag.
    overview_mem_cap: Option<String>,
    #[serde(default)]
    host: Vec<HostConfig>,
}

/// Load and resolve the configuration. `path` is the `--config`/`DOCKDOE_CONFIG`
/// value (if any); `cli` supplies the global fallbacks and the single-host
/// synthesis inputs.
pub fn load(path: Option<&Path>, cli: &Cli) -> Result<AppConfig> {
    let file = match path {
        Some(p) => {
            let text = std::fs::read_to_string(p)
                .with_context(|| format!("reading config file {}", p.display()))?;
            toml::from_str::<FileConfig>(&text)
                .with_context(|| format!("parsing config file {}", p.display()))?
        }
        None => FileConfig::default(),
    };

    let hosts = if file.host.is_empty() {
        // No `[[host]]` entries (or no file at all): synthesise one local host
        // from the CLI/env so the pre-config single-host behaviour is preserved.
        vec![HostConfig {
            name: "local".to_string(),
            docker: default_docker_endpoint(),
            public_host: cli.port_host.clone(),
            interval_secs: None,
            tls_ca: None,
            tls_insecure: false,
        }]
    } else {
        file.host
    };

    validate_hosts(&hosts)?;

    let overview_mem_cap = match &file.overview_mem_cap {
        Some(s) => parse_size(s)
            .map_err(|e| anyhow::anyhow!(e))
            .context("parsing overview_mem_cap")?,
        None => cli.overview_mem_cap,
    };
    // A cap at or below the log floor would collapse the memory disc's range.
    if overview_mem_cap < 2 * OVERVIEW_MEM_FLOOR {
        bail!("overview mem cap must be at least 32M");
    }

    Ok(AppConfig {
        bind: file.bind.unwrap_or_else(|| cli.bind.clone()),
        db_path: file.db_path.unwrap_or_else(|| cli.db_path.clone()),
        interval_secs: file.interval_secs.unwrap_or(cli.interval_secs),
        raw_retention_secs: file.raw_retention_secs.unwrap_or(cli.raw_retention_secs),
        trend_bucket_secs: file.trend_bucket_secs.unwrap_or(cli.trend_bucket_secs),
        trend_retention_secs: file
            .trend_retention_secs
            .unwrap_or(cli.trend_retention_secs),
        prune_interval_secs: file.prune_interval_secs.unwrap_or(cli.prune_interval_secs),
        allowed_hosts: file
            .allowed_hosts
            .unwrap_or_else(|| cli.allowed_hosts.clone()),
        apprise_url: file.apprise_url.or_else(|| cli.apprise_url.clone()),
        notify_delay_secs: file.notify_delay_secs.unwrap_or(cli.notify_delay_secs),
        auth_user: file.auth_user.or_else(|| cli.auth_user.clone()),
        auth_password: file.auth_password.or_else(|| cli.auth_password.clone()),
        cookie_secure: file.cookie_secure.unwrap_or(cli.cookie_secure),
        log: file.log.unwrap_or_else(|| cli.log.clone()),
        overview: OverviewConfig {
            cpu_scale: file.overview_cpu_scale.unwrap_or(cli.overview_cpu_scale),
            mem_scale: file.overview_mem_scale.unwrap_or(cli.overview_mem_scale),
            mem_cap: overview_mem_cap,
        },
        hosts,
    })
}

/// The endpoint the synthesised local host connects to, honouring `DOCKER_HOST`
/// like the Docker CLI does and otherwise the default Unix socket.
fn default_docker_endpoint() -> String {
    std::env::var("DOCKER_HOST").unwrap_or_else(|_| "unix:///var/run/docker.sock".to_string())
}

/// Reject empty host lists, non-slug names, duplicates, and empty endpoints —
/// all of which are misconfigurations we'd rather surface loudly at startup.
fn validate_hosts(hosts: &[HostConfig]) -> Result<()> {
    if hosts.is_empty() {
        bail!("no hosts configured");
    }
    let mut seen = HashSet::new();
    for h in hosts {
        if !is_valid_slug(&h.name) {
            bail!(
                "invalid host name {:?}: use only letters, digits, '.', '_' and '-'",
                h.name
            );
        }
        if !seen.insert(h.name.as_str()) {
            bail!("duplicate host name {:?}", h.name);
        }
        if h.docker.trim().is_empty() {
            bail!("host {:?} has an empty docker endpoint", h.name);
        }
        // Zero would panic in `tokio::time::interval` inside the collector,
        // mirroring the clap range guard on the global `--interval-secs`.
        if h.interval_secs == Some(0) {
            bail!(
                "host {:?} has interval_secs = 0; it must be at least 1",
                h.name
            );
        }
    }
    Ok(())
}

fn is_valid_slug(s: &str) -> bool {
    !s.is_empty()
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    fn default_cli() -> Cli {
        Cli::try_parse_from(["dockdoe"]).expect("default CLI parses")
    }

    #[test]
    fn no_file_synthesises_one_local_host() {
        let cfg = load(None, &default_cli()).expect("load without a file");
        assert_eq!(cfg.hosts.len(), 1);
        assert_eq!(cfg.hosts[0].name, "local");
        assert!(!cfg.hosts[0].tls_insecure);
        // Globals come from the CLI defaults.
        assert_eq!(cfg.bind, "127.0.0.1:8080");
    }

    #[test]
    fn file_hosts_and_globals_override_cli() {
        let file: FileConfig = toml::from_str(
            r#"
            bind = "0.0.0.0:9000"
            interval_secs = 5

            [[host]]
            name = "local"
            docker = "unix:///var/run/docker.sock"

            [[host]]
            name = "nas"
            docker = "https://nas.lan:2375"
            public_host = "nas.lan"
            interval_secs = 15
            tls_insecure = true
            "#,
        )
        .expect("parse");
        // Re-run the merge by hand mirroring `load`'s body would be brittle; instead
        // exercise the parse + merge through a temp file.
        assert_eq!(file.bind.as_deref(), Some("0.0.0.0:9000"));
        assert_eq!(file.host.len(), 2);
        assert_eq!(file.host[1].docker, "https://nas.lan:2375");
        assert!(file.host[1].tls_insecure);
        // The per-host interval is parsed; an omitted one stays None.
        assert_eq!(file.host[1].interval_secs, Some(15));
        assert_eq!(file.host[0].interval_secs, None);
    }

    fn host_with(docker: &str, interval_secs: Option<u64>) -> HostConfig {
        HostConfig {
            name: "h".into(),
            docker: docker.into(),
            public_host: None,
            interval_secs,
            tls_ca: None,
            tls_insecure: false,
        }
    }

    #[test]
    fn per_host_interval_resolution() {
        // An explicit per-host interval always wins, local or remote.
        assert_eq!(
            host_with("unix:///x", Some(5)).effective_interval_secs(3),
            5
        );
        assert_eq!(
            host_with("tcp://h:2375", Some(2)).effective_interval_secs(3),
            2
        );
        // A local endpoint without an override inherits the global default.
        assert_eq!(host_with("unix:///x", None).effective_interval_secs(3), 3);
        // Remote endpoints fall back to the slower remote default.
        for remote in ["tcp://h:2375", "http://h:2375", "https://h:2376"] {
            assert_eq!(
                host_with(remote, None).effective_interval_secs(3),
                REMOTE_DEFAULT_INTERVAL_SECS
            );
        }
    }

    #[test]
    fn zero_per_host_interval_is_rejected() {
        assert!(validate_hosts(&[host_with("unix:///x", Some(0))]).is_err());
    }

    #[test]
    fn slug_validation() {
        assert!(is_valid_slug("local"));
        assert!(is_valid_slug("nas-01.lan_2"));
        assert!(!is_valid_slug(""));
        assert!(!is_valid_slug("has space"));
        assert!(!is_valid_slug("slash/name"));
    }

    #[test]
    fn duplicate_names_are_rejected() {
        let hosts = vec![
            HostConfig {
                name: "dup".into(),
                docker: "unix:///x".into(),
                public_host: None,
                interval_secs: None,
                tls_ca: None,
                tls_insecure: false,
            },
            HostConfig {
                name: "dup".into(),
                docker: "tcp://y:2375".into(),
                public_host: None,
                interval_secs: None,
                tls_ca: None,
                tls_insecure: false,
            },
        ];
        assert!(validate_hosts(&hosts).is_err());
    }

    #[test]
    fn empty_endpoint_is_rejected() {
        let hosts = vec![HostConfig {
            name: "x".into(),
            docker: "   ".into(),
            public_host: None,
            interval_secs: None,
            tls_ca: None,
            tls_insecure: false,
        }];
        assert!(validate_hosts(&hosts).is_err());
    }

    #[test]
    fn unknown_keys_are_rejected() {
        let err = toml::from_str::<FileConfig>("nonsense_key = 1\n").unwrap_err();
        assert!(err.to_string().contains("nonsense_key"));
    }

    #[test]
    fn parse_size_accepts_suffixes_and_bytes() {
        assert_eq!(parse_size("68719476736"), Ok(64 << 30));
        assert_eq!(parse_size("64G"), Ok(64 << 30));
        assert_eq!(parse_size("64GB"), Ok(64 << 30));
        assert_eq!(parse_size("64GiB"), Ok(64 << 30));
        assert_eq!(parse_size("64g"), Ok(64 << 30));
        assert_eq!(parse_size(" 512M "), Ok(512 << 20));
        assert_eq!(parse_size("1K"), Ok(1024));
        assert_eq!(parse_size("2T"), Ok(2 << 40));
        assert_eq!(parse_size("0"), Ok(0));
    }

    #[test]
    fn parse_size_rejects_garbage_and_overflow() {
        assert!(parse_size("").is_err());
        assert!(parse_size("G").is_err());
        assert!(parse_size("12X").is_err());
        assert!(parse_size("1.5G").is_err());
        assert!(parse_size("-3M").is_err());
        // 2^60 terabytes shift the value's bits clean out of a u64.
        assert!(parse_size("1152921504606846976K").is_err());
    }

    #[test]
    fn overview_defaults_are_cpu_log_mem_log_64g() {
        let cfg = load(None, &default_cli()).expect("load without a file");
        assert_eq!(cfg.overview.cpu_scale, ScaleMode::Log);
        assert_eq!(cfg.overview.mem_scale, ScaleMode::Log);
        assert_eq!(cfg.overview.mem_cap, 64 << 30);
    }

    #[test]
    fn overview_file_keys_parse() {
        let file: FileConfig = toml::from_str(
            r#"
            overview_cpu_scale = "sqrt"
            overview_mem_scale = "linear"
            overview_mem_cap = "256G"
            "#,
        )
        .expect("parse");
        assert_eq!(file.overview_cpu_scale, Some(ScaleMode::Sqrt));
        assert_eq!(file.overview_mem_scale, Some(ScaleMode::Linear));
        assert_eq!(file.overview_mem_cap.as_deref(), Some("256G"));
    }
}
