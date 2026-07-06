//! Federation client: how a hub talks to a remote DockDoe node.
//!
//! A "node" is just another DockDoe instance running on the Docker host,
//! reached over its JSON API (`/api/hosts`, `/host/{host}/api/snapshot`)
//! with a bearer token. The hub never talks to the remote Docker daemon —
//! the node's socket stays local to it, and the node keeps its own history.
//!
//! The payload types live in [`crate::model`] and are an additive-only
//! contract between DockDoe versions; see the note there. This build's
//! version is [`VERSION`], sent nowhere but compared against what nodes
//! report so skew shows up in the logs instead of as silent weirdness.

use std::time::Duration;

use anyhow::{Context, Result, bail};
use tracing::warn;

use crate::config::HostConfig;
use crate::model::{HostEntry, Snapshot};

/// This build's version, for skew warnings against a node's reported version.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Overall deadline per node request. Snapshot payloads are small (tens of
/// KB); a node that can't answer within this is effectively down for the
/// poll cycle and will be retried on the next one.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// TCP/TLS connect deadline, kept shorter than [`REQUEST_TIMEOUT`] so an
/// unreachable node fails fast.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// HTTP client for one federated node.
#[derive(Clone)]
pub struct NodeClient {
    http: reqwest::Client,
    /// Node base URL without a trailing slash, e.g. `https://node:8080`.
    base: String,
}

impl NodeClient {
    /// Build the client for a `dockdoe = …` host entry: bearer token attached
    /// to every request, TLS trusting the compiled-in Mozilla roots plus an
    /// optional `tls_ca`, or unverified with `tls_insecure`.
    pub fn from_config(cfg: &HostConfig) -> Result<Self> {
        let base = cfg
            .dockdoe
            .as_deref()
            .context("host has no dockdoe endpoint")?
            .trim()
            .trim_end_matches('/')
            .to_string();

        let mut headers = reqwest::header::HeaderMap::new();
        if let Some(token) = cfg.token.as_deref() {
            let value = format!("Bearer {token}")
                .parse()
                .context("token contains characters not allowed in a header")?;
            headers.insert(reqwest::header::AUTHORIZATION, value);
        }

        let mut builder = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .connect_timeout(CONNECT_TIMEOUT)
            .default_headers(headers);
        builder = if cfg.tls_insecure {
            warn!(node = %base, "tls_insecure is set: TLS server-certificate verification is disabled");
            builder.danger_accept_invalid_certs(true)
        } else {
            // Same trust model as the Apprise client and the https Docker
            // transport: the compiled-in Mozilla roots (the scratch image has
            // no system store), plus the host's private CA when given.
            let mut roots: Vec<reqwest::Certificate> = webpki_root_certs::TLS_SERVER_ROOT_CERTS
                .iter()
                .filter_map(|der| reqwest::Certificate::from_der(der.as_ref()).ok())
                .collect();
            if let Some(ca) = cfg.tls_ca.as_deref() {
                let pem = std::fs::read(ca)
                    .with_context(|| format!("reading tls_ca {}", ca.display()))?;
                let certs = reqwest::Certificate::from_pem_bundle(&pem)
                    .with_context(|| format!("parsing tls_ca {}", ca.display()))?;
                if certs.is_empty() {
                    bail!("tls_ca {} contained no certificates", ca.display());
                }
                roots.extend(certs);
            }
            builder.tls_certs_only(roots)
        };

        Ok(Self {
            http: builder
                .build()
                .context("building the HTTP client for the federated node")?,
            base,
        })
    }

    /// The hosts the node monitors — `GET /api/hosts`.
    pub async fn hosts(&self) -> Result<Vec<HostEntry>> {
        self.get_json("/api/hosts").await
    }

    /// The current snapshot of one of the node's hosts. `Ok(None)` while the
    /// node is still warming up (503 — no collector cycle has published yet).
    pub async fn snapshot(&self, node_host: &str) -> Result<Option<Snapshot>> {
        let resp = self
            .http
            .get(format!("{}/host/{node_host}/api/snapshot", self.base))
            .send()
            .await
            .with_context(|| format!("requesting snapshot from node {}", self.base))?;
        if resp.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE {
            return Ok(None);
        }
        let resp = check_status(resp)?;
        Ok(Some(resp.json().await.with_context(|| {
            format!("decoding snapshot from node {}", self.base)
        })?))
    }

    /// Resolve which of the node's hosts this entry mirrors: an explicit
    /// `node_host` must exist on the node; without one the node must have
    /// exactly one host. Warns when the node runs a different DockDoe version.
    pub async fn resolve_node_host(&self, configured: Option<&str>) -> Result<HostEntry> {
        let hosts = self
            .hosts()
            .await
            .with_context(|| format!("listing hosts of node {}", self.base))?;
        let picked = pick_host(&hosts, configured)?.clone();
        if picked.version != VERSION {
            warn!(
                node = %self.base,
                node_version = %picked.version,
                hub_version = %VERSION,
                "node runs a different DockDoe version; the API is additive, but keep them close"
            );
        }
        Ok(picked)
    }

    async fn get_json<T: serde::de::DeserializeOwned>(&self, path: &str) -> Result<T> {
        let resp = self
            .http
            .get(format!("{}{path}", self.base))
            .send()
            .await
            .with_context(|| format!("requesting {path} from node {}", self.base))?;
        let resp = check_status(resp)?;
        resp.json()
            .await
            .with_context(|| format!("decoding {path} from node {}", self.base))
    }
}

/// Turn a non-success status into a diagnosable error. 401 is called out
/// explicitly — a missing or wrong `token` is the most likely misconfiguration.
fn check_status(resp: reqwest::Response) -> Result<reqwest::Response> {
    match resp.status() {
        s if s.is_success() => Ok(resp),
        reqwest::StatusCode::UNAUTHORIZED => {
            bail!(
                "node rejected the API token (401): check this host's `token` against the node's `api_token`"
            )
        }
        // The node's login redirect: it doesn't know bearer tokens (pre-0.10)
        // or none was configured here, so the request fell through to the
        // browser flow.
        reqwest::StatusCode::SEE_OTHER => {
            bail!(
                "node redirected to its login page: set `token` here, and make sure the node is DockDoe ≥ 0.10 with `api_token` configured"
            )
        }
        s => bail!("node answered {s}"),
    }
}

/// The host `configured` names, or the node's only host when unset.
fn pick_host<'a>(hosts: &'a [HostEntry], configured: Option<&str>) -> Result<&'a HostEntry> {
    let names = || {
        hosts
            .iter()
            .map(|h| h.name.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    };
    match configured {
        Some(want) => hosts
            .iter()
            .find(|h| h.name == want)
            .with_context(|| format!("node has no host {want:?} (it has: {})", names())),
        None => match hosts {
            [only] => Ok(only),
            [] => bail!("node reports no hosts"),
            _ => bail!(
                "node monitors several hosts ({}); set node_host to pick one",
                names()
            ),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(name: &str) -> HostEntry {
        HostEntry {
            name: name.into(),
            version: "0.10.0".into(),
        }
    }

    #[test]
    fn pick_host_resolves_explicit_and_sole_hosts() {
        let hosts = vec![entry("local"), entry("nas")];
        assert_eq!(pick_host(&hosts, Some("nas")).unwrap().name, "nas");
        // Unknown name errors and lists what the node has.
        let err = pick_host(&hosts, Some("prod")).unwrap_err().to_string();
        assert!(err.contains("local, nas"), "got: {err}");
        // Several hosts without node_host is ambiguous.
        assert!(pick_host(&hosts, None).is_err());

        let sole = vec![entry("local")];
        assert_eq!(pick_host(&sole, None).unwrap().name, "local");
        assert!(pick_host(&[], None).is_err());
    }
}
