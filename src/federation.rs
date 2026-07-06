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

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use tracing::{debug, info, warn};

use futures_util::StreamExt;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

use crate::collector::{SharedDashboard, publish};
use crate::config::HostConfig;
use crate::model::{HostEntry, Snapshot};
use crate::store::MetricPoint;

/// The byte stream under the node-side WebSocket: plain TCP or TLS.
pub trait AsyncStream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Unpin {}
impl<T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Unpin> AsyncStream for T {}

/// The hub's client end of a node's exec WebSocket.
pub type NodeWebSocket = tokio_tungstenite::WebSocketStream<Box<dyn AsyncStream>>;

/// Whether an [`NodeClient::exec_socket`] error was the node *denying* the
/// upgrade (401/403 — bad token, or the node latched read-only) rather than a
/// transport problem.
pub fn ws_denied(err: &anyhow::Error) -> bool {
    matches!(
        err.downcast_ref::<tokio_tungstenite::tungstenite::Error>(),
        Some(tokio_tungstenite::tungstenite::Error::Http(resp))
            if matches!(resp.status().as_u16(), 401 | 403)
    )
}

/// This build's version, for skew warnings against a node's reported version.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Overall deadline per node request. Snapshot payloads are small (tens of
/// KB); a node that can't answer within this is effectively down for the
/// poll cycle and will be retried on the next one.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// TCP/TLS connect deadline, kept shorter than [`REQUEST_TIMEOUT`] so an
/// unreachable node fails fast.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Per-request timeout override for the snapshot event stream — long-lived by
/// design, so the client-wide [`REQUEST_TIMEOUT`] must not apply. One day, not
/// infinity: a periodic clean reconnect costs nothing (snapshots are deduped
/// and the last one is kept) and clears any half-dead connection state.
#[allow(clippy::duration_suboptimal_units)]
const STREAM_SESSION_MAX: Duration = Duration::from_secs(24 * 3600);

/// How long the event stream may stay silent before the hub declares it dead
/// and reconnects. The node keep-alives every ~15 s, so a minute of silence
/// means the TCP connection is gone without a FIN (node powered off, NAT
/// dropped the mapping).
// Same stable-toolchain trade-off as the other Duration constants.
#[allow(clippy::duration_suboptimal_units)]
const STREAM_STALL: Duration = Duration::from_secs(60);

/// HTTP client for one federated node.
#[derive(Clone)]
pub struct NodeClient {
    http: reqwest::Client,
    /// Node base URL without a trailing slash, e.g. `https://node:8080`.
    base: String,
    /// The node-side host name this entry mirrors, set by the poll loop once
    /// resolved. Shared across clones so the web layer sees it too.
    node_host: Arc<OnceLock<String>>,
    /// `Authorization: Bearer …`, for the connections we dial ourselves (the
    /// terminal WebSocket); the reqwest client carries its own copy.
    auth_header: Option<reqwest::header::HeaderValue>,
    /// TLS config for self-dialed connections on an `https` node, sharing the
    /// trust model (and `tls_ca`/`tls_insecure`) with the rest of the app.
    /// `None` for a plain-`http` node.
    tls: Option<Arc<rustls::ClientConfig>>,
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

        let auth_header = cfg
            .token
            .as_deref()
            .map(|token| {
                format!("Bearer {token}")
                    .parse::<reqwest::header::HeaderValue>()
                    .context("token contains characters not allowed in a header")
            })
            .transpose()?;
        let mut headers = reqwest::header::HeaderMap::new();
        if let Some(value) = &auth_header {
            headers.insert(reqwest::header::AUTHORIZATION, value.clone());
        }

        // For the connections we dial ourselves (the terminal WebSocket).
        let tls = base
            .starts_with("https://")
            .then(|| {
                crate::docker::tls_client_config(cfg.tls_ca.as_deref(), cfg.tls_insecure)
                    .map(Arc::new)
            })
            .transpose()?;

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
            node_host: Arc::new(OnceLock::new()),
            auth_header,
            tls,
        })
    }

    /// The node-side host name this entry mirrors — `None` until the poll
    /// loop's first successful resolution.
    pub fn node_host(&self) -> Option<&str> {
        self.node_host.get().map(String::as_str)
    }

    /// Open the node's exec WebSocket for a container — the upstream side of
    /// the hub's terminal bridge. Dials the node ourselves (TCP, plus rustls
    /// for an `https` node) because a browser can't attach the bearer token to
    /// a WebSocket, but we can.
    pub async fn exec_socket(&self, id: &str, cmd: &str) -> Result<NodeWebSocket> {
        let nh = self
            .node_host()
            .context("node not resolved yet (unreachable since startup)")?;
        let base: reqwest::Url = self
            .base
            .parse()
            .with_context(|| format!("invalid node URL {}", self.base))?;
        let host = base.host_str().context("node URL has no host")?;
        let port = base
            .port_or_known_default()
            .context("node URL has no port")?;

        let tcp = tokio::net::TcpStream::connect((host, port))
            .await
            .with_context(|| format!("connecting to node {host}:{port}"))?;
        let stream: Box<dyn AsyncStream> = match &self.tls {
            Some(tls) => {
                let server = rustls::pki_types::ServerName::try_from(host.to_string())
                    .with_context(|| format!("invalid TLS server name {host:?}"))?;
                Box::new(
                    tokio_rustls::TlsConnector::from(Arc::clone(tls))
                        .connect(server, tcp)
                        .await
                        .with_context(|| format!("TLS handshake with node {host}:{port}"))?,
                )
            }
            None => Box::new(tcp),
        };

        let mut url = base;
        // http→ws / https→wss; both are valid schemes, so set_scheme can't fail.
        let scheme = if self.tls.is_some() { "wss" } else { "ws" };
        url.set_scheme(scheme)
            .map_err(|()| anyhow::anyhow!("cannot make a WebSocket URL from {}", self.base))?;
        url.set_path(&format!("/host/{nh}/ws/container/{id}/exec"));
        if !cmd.is_empty() {
            url.query_pairs_mut().append_pair("cmd", cmd);
        }

        let mut request = url
            .as_str()
            .into_client_request()
            .context("building the WebSocket handshake request")?;
        if let Some(auth) = &self.auth_header {
            request
                .headers_mut()
                .insert(reqwest::header::AUTHORIZATION, auth.clone());
        }
        let (ws, _response) = tokio_tungstenite::client_async(request, stream)
            .await
            .with_context(|| format!("WebSocket handshake with node {}", self.base))?;
        Ok(ws)
    }

    /// Forward a GET to the node's same-shaped, host-scoped endpoint:
    /// `api_path` is the part after `/host/{node_host}` (e.g.
    /// `/api/container/{id}/logs`), `query` the raw query string to relay.
    pub async fn forward_get(
        &self,
        api_path: &str,
        query: Option<&str>,
    ) -> Result<reqwest::Response> {
        let mut url = self.host_scoped(api_path)?;
        if let Some(q) = query.filter(|q| !q.is_empty()) {
            url.push('?');
            url.push_str(q);
        }
        self.http
            .get(&url)
            .send()
            .await
            .with_context(|| format!("requesting {api_path} from node {}", self.base))
    }

    /// Forward a lifecycle action POST to the node. Returns the node's status
    /// and body text — the caller maps them onto the hub's own UI responses
    /// (fresh buttons, read-only latch, error toast).
    pub async fn forward_action(&self, api_path: &str) -> Result<(reqwest::StatusCode, String)> {
        let url = self.host_scoped(api_path)?;
        let resp = self
            .http
            .post(&url)
            .send()
            .await
            .with_context(|| format!("posting {api_path} to node {}", self.base))?;
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        Ok((status, body))
    }

    /// Open the node's snapshot event stream (`GET /host/{nh}/api/events`).
    /// The response body is a long-lived SSE stream; the per-request timeout
    /// override keeps the client-wide deadline from killing it.
    async fn snapshot_events(&self, node_host: &str) -> Result<reqwest::Response> {
        self.http
            .get(format!("{}/host/{node_host}/api/events", self.base))
            .header(reqwest::header::ACCEPT, "text/event-stream")
            .timeout(STREAM_SESSION_MAX)
            .send()
            .await
            .with_context(|| format!("opening the event stream of node {}", self.base))
    }

    /// Chart points from the node (seed of the detail pages): its metrics
    /// endpoints serve the same `Vec<MetricPoint>` JSON the hub would read
    /// from its own store.
    pub async fn metric_points(&self, api_path: &str, since_ms: u64) -> Result<Vec<MetricPoint>> {
        let resp = self
            .forward_get(api_path, Some(&format!("since_ms={since_ms}")))
            .await?;
        let resp = check_status(resp)?;
        resp.json()
            .await
            .with_context(|| format!("decoding {api_path} from node {}", self.base))
    }

    /// `{base}/host/{node_host}{api_path}`, or an error while the node host is
    /// still unresolved (node not reachable since hub start).
    fn host_scoped(&self, api_path: &str) -> Result<String> {
        let nh = self
            .node_host()
            .context("node not resolved yet (unreachable since startup)")?;
        Ok(format!("{}/host/{nh}{api_path}", self.base))
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

/// Run the federation loop forever for one hub host entry, publishing the
/// node's snapshots as this host's shared dashboard — the same slot a local
/// collector would fill, so the dashboard, SSE streams and overview discs
/// work unchanged.
///
/// Live data preferably arrives over the node's snapshot **event stream**
/// (one long-lived connection, every sample as the node collects it); a node
/// without the stream endpoint (pre-0.12) is **polled** every `interval`
/// instead. The node host to mirror is (re)resolved inside the loop, so a
/// node that is down at hub startup is simply retried; `interval` also paces
/// reconnects and retries. Errors keep the last snapshot, like a local
/// collector losing its daemon: the header age shows the staleness, and the
/// next successful fetch recovers. `read_only` mirrors what the node reports.
pub async fn run(
    host: String,
    client: NodeClient,
    configured_node_host: Option<String>,
    interval: Duration,
    shared: SharedDashboard,
    read_only: Arc<AtomicBool>,
) {
    info!(host = %host, node = %client.base, interval = ?interval, "starting federation");
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Assume a modern node until it answers 404 for the stream endpoint.
    let mut streaming = true;
    loop {
        ticker.tick().await;
        if client.node_host().is_none() {
            match client
                .resolve_node_host(configured_node_host.as_deref())
                .await
            {
                Ok(entry) => {
                    info!(
                        host = %host,
                        node = %client.base,
                        node_host = %entry.name,
                        node_version = %entry.version,
                        "federated node resolved"
                    );
                    // Publishes the name to every clone (the web layer's
                    // passthrough waits on it, too).
                    let _ = client.node_host.set(entry.name);
                }
                Err(e) => {
                    warn!(host = %host, error = %format!("{e:#}"), "resolving federated node failed; retrying");
                    continue;
                }
            }
        }
        let target = client.node_host().expect("resolved above").to_string();

        if streaming {
            match consume_stream(&client, &target, &host, &shared, &read_only).await {
                StreamEnd::Unsupported => {
                    info!(host = %host, "node has no snapshot stream (pre-0.12); polling instead");
                    streaming = false; // fall through to the poll below
                }
                // The ticker paces the reconnect (immediately after a
                // long-lived session, a full interval after a quick failure).
                StreamEnd::Disconnected => continue,
            }
        }

        match client.snapshot(&target).await {
            Ok(Some(snap)) => {
                read_only.store(snap.read_only, Ordering::Relaxed);
                publish(&shared, snap.dashboard);
            }
            Ok(None) => debug!(host = %host, "node is warming up; no snapshot yet"),
            Err(e) => {
                warn!(host = %host, error = %format!("{e:#}"), "polling node failed; keeping last snapshot");
            }
        }
    }
}

/// Why a streaming session ended.
enum StreamEnd {
    /// The node doesn't have the endpoint (404) — it predates it; poll instead.
    Unsupported,
    /// Connection failed, went silent, or closed; worth reconnecting.
    Disconnected,
}

/// Consume the node's snapshot event stream until it ends: publish every
/// `snapshot` event into the shared slot and mirror `read_only`. Returns why
/// the session ended; transient errors are logged here.
async fn consume_stream(
    client: &NodeClient,
    target: &str,
    host: &str,
    shared: &SharedDashboard,
    read_only: &AtomicBool,
) -> StreamEnd {
    let resp = match client.snapshot_events(target).await {
        Ok(resp) => resp,
        Err(e) => {
            warn!(host = %host, error = %format!("{e:#}"), "connecting to node stream failed");
            return StreamEnd::Disconnected;
        }
    };
    if resp.status() == reqwest::StatusCode::NOT_FOUND {
        return StreamEnd::Unsupported;
    }
    if !resp.status().is_success() {
        warn!(host = %host, status = %resp.status(), "node rejected the event stream");
        return StreamEnd::Disconnected;
    }

    let mut stream = resp.bytes_stream();
    let mut parser = SseParser::default();
    loop {
        match tokio::time::timeout(STREAM_STALL, stream.next()).await {
            Err(_) => {
                warn!(host = %host, "node stream went silent; reconnecting");
                return StreamEnd::Disconnected;
            }
            Ok(None) => {
                debug!(host = %host, "node stream closed; reconnecting");
                return StreamEnd::Disconnected;
            }
            Ok(Some(Err(e))) => {
                warn!(host = %host, error = %format!("{e:#}"), "node stream failed; reconnecting");
                return StreamEnd::Disconnected;
            }
            Ok(Some(Ok(chunk))) => {
                for (event, data) in parser.push(&chunk) {
                    if event != "snapshot" {
                        continue; // future event types: additive, ignorable
                    }
                    match serde_json::from_str::<Snapshot>(&data) {
                        Ok(snap) => {
                            read_only.store(snap.read_only, Ordering::Relaxed);
                            publish(shared, snap.dashboard);
                        }
                        Err(e) => warn!(host = %host, %e, "undecodable snapshot event"),
                    }
                }
            }
        }
    }
}

/// Minimal incremental parser for the SSE wire format: feed it chunks, get
/// completed `(event, data)` pairs back. Handles events split across chunks,
/// multi-line `data:` fields (joined with `\n` per spec), `\r\n` line ends,
/// and ignores comment lines (the keep-alives) and unknown fields.
#[derive(Default)]
struct SseParser {
    buf: String,
}

impl SseParser {
    fn push(&mut self, chunk: &[u8]) -> Vec<(String, String)> {
        // The payload is JSON (which escapes control characters, so a raw CR
        // can only be a line ending) and SSE field names are ASCII — safe to
        // normalise CRLF away and decode lossily.
        self.buf
            .push_str(&String::from_utf8_lossy(chunk).replace('\r', ""));
        let mut out = Vec::new();
        // An event ends at a blank line.
        while let Some(pos) = self.buf.find("\n\n") {
            let block: String = self.buf.drain(..pos + 2).collect();
            let mut event = String::from("message");
            let mut data: Vec<&str> = Vec::new();
            for line in block.lines() {
                if let Some(v) = line.strip_prefix("event:") {
                    event = v.trim_start().to_string();
                } else if let Some(v) = line.strip_prefix("data:") {
                    data.push(v.strip_prefix(' ').unwrap_or(v));
                }
                // Anything else (comments, id:, retry:) is irrelevant here.
            }
            if !data.is_empty() {
                out.push((event, data.join("\n")));
            }
        }
        out
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
    fn sse_parser_handles_chunking_comments_and_multiline_data() {
        let mut p = SseParser::default();
        // A keep-alive comment alone yields nothing.
        assert!(p.push(b": keep-alive\n\n").is_empty());
        // An event split across arbitrary chunk boundaries comes out whole.
        assert!(p.push(b"event: snap").is_empty());
        assert!(p.push(b"shot\ndata: {\"a\":").is_empty());
        let events = p.push(b"1}\n\n");
        assert_eq!(events, vec![("snapshot".into(), "{\"a\":1}".into())]);
        // CRLF line endings, multi-line data (joined with \n), default event
        // name, and two events in one chunk.
        let events = p.push(b"data: x\r\ndata: y\r\n\r\nevent: e2\ndata: z\n\n");
        assert_eq!(
            events,
            vec![("message".into(), "x\ny".into()), ("e2".into(), "z".into())]
        );
        // Unknown fields are ignored, remainder stays buffered.
        assert!(p.push(b"id: 7\nretry: 100\ndata: tail").is_empty());
        assert_eq!(p.push(b"\n\n"), vec![("message".into(), "tail".into())]);
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
