//! Web layer: axum router, embedded assets, SSE streaming, and HTML rendering.
//!
//! Pages are rendered server-side with maud and kept live without a full reload
//! over a *single* SSE connection per page (`live.js`). That one stream polls
//! the shared snapshot — the same source a plain page render reads, so the live
//! view can't drift from a reload — and emits named events the client swaps in:
//! `header` (host header), `containers` (dashboard / stack member table),
//! `detail` (a container page's state + facts), and `metrics` (a JSON point for
//! the uPlot charts, seeded from the store on first load). One connection per
//! page keeps us under the browser's per-host HTTP/1.1 connection limit.

use std::collections::{BTreeMap, HashMap};
use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Query, Request, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Redirect, Response};
use axum::routing::get;
use axum::{Form, Json, Router};
use futures_util::{SinkExt, Stream, StreamExt};
use maud::{DOCTYPE, Markup, html};
use rust_embed::RustEmbed;
use tokio::io::AsyncWriteExt;

use crate::auth::{self, Auth};
use crate::collector::SharedDashboard;
use crate::docker::{Action, DockerHandle, ExecSession, is_forbidden};
use crate::model::{ContainerMetrics, ContainerState, Dashboard, HealthState, Port};
use crate::store::{MetricPoint, Store};

/// How often the live SSE streams poll the latest snapshot.
const SNAPSHOT_POLL: Duration = Duration::from_secs(1);

/// Shared state for the web layer.
#[derive(Clone)]
pub struct AppState {
    /// The monitored Docker hosts, keyed by their config `name` (the URL slug).
    pub hosts: Arc<HashMap<String, HostRuntime>>,
    /// Host names in config order — drives the index page and host switcher.
    pub host_order: Arc<[String]>,
    /// Store, for seeding charts with recent history.
    pub store: Store,
    /// How far back to seed charts on first load.
    pub seed_window: Duration,
    /// Hostnames the UI may be addressed as (normalized: lowercase, no port).
    /// Empty disables the Host check; localhost forms are always allowed.
    pub allowed_hosts: Arc<[String]>,
    /// Login credentials and session signing. `None` leaves the UI open.
    pub auth: Option<Auth>,
}

/// Everything the web layer needs for one monitored host.
#[derive(Clone)]
pub struct HostRuntime {
    /// The latest snapshot for this host — single source of truth for both the
    /// initial render and the live SSE streams.
    pub shared: SharedDashboard,
    /// Docker handle for this host's lifecycle actions and logs.
    pub docker: DockerHandle,
    /// How this host's published-port pills link out.
    pub links: PortLinks,
    /// Set once a lifecycle/exec call returns `403 Forbidden` — the socket proxy
    /// is read-only. The UI then disables this host's action buttons and
    /// terminal. Latches on; cleared only by a restart.
    pub read_only: Arc<AtomicBool>,
}

impl HostRuntime {
    fn is_read_only(&self) -> bool {
        self.read_only.load(Ordering::Relaxed)
    }
}

/// How published-port pills link out, resolved per host from `public_host` and
/// the endpoint URL.
#[derive(Debug, Clone)]
pub enum PortLinks {
    /// The pill carries `data-port` and a `localhost` fallback href, which
    /// live.js rewrites to whatever host the browser is pointed at. The right
    /// default for the local host browsed directly on the Docker machine.
    Browser,
    /// A fixed host (IP or name) the published ports are reachable at — a remote
    /// host's address, or an override when the browsing host isn't where the
    /// ports live.
    Host(String),
    /// Render ports as plain, unlinked pills — for setups reachable only through
    /// a proxy, where no direct `host:port` link works from the browser.
    Off,
}

impl PortLinks {
    /// Resolve the port-link mode for a host: an explicit `public_host` wins
    /// (`off`/`none`/`-` → no links, otherwise a fixed host); failing that, the
    /// host parsed from a `tcp`/`http`/`https` endpoint URL; failing that (a unix
    /// socket — the local host), the browsing host.
    #[must_use]
    pub fn from_config(public_host: Option<&str>, docker_url: &str) -> Self {
        if let Some(s) = public_host.map(str::trim).filter(|s| !s.is_empty()) {
            return if s.eq_ignore_ascii_case("off") || s.eq_ignore_ascii_case("none") || s == "-" {
                PortLinks::Off
            } else {
                PortLinks::Host(s.to_string())
            };
        }
        endpoint_host(docker_url).map_or(PortLinks::Browser, PortLinks::Host)
    }
}

/// The host part of a `tcp`/`http`/`https` Docker endpoint URL (for deriving the
/// default port-link host). `None` for a unix socket or a bare path.
fn endpoint_host(url: &str) -> Option<String> {
    let rest = ["tcp://", "http://", "https://"]
        .iter()
        .find_map(|scheme| url.strip_prefix(scheme))?;
    // IPv6 literal: keep everything inside the brackets.
    if let Some(after) = rest.strip_prefix('[') {
        return after.split(']').next().map(str::to_string);
    }
    let host = rest.split(['/', ':']).next().unwrap_or("");
    (!host.is_empty()).then(|| host.to_string())
}

/// Per-request render context: which host's pages we're building, and how its
/// published-port pills link out. Threaded through the `maud` helpers so every
/// in-page link is host-scoped (`/host/{host}/…`).
struct Render<'a> {
    host: &'a str,
    links: &'a PortLinks,
    /// Whether this host is read-only (action buttons + terminal disabled).
    read_only: bool,
}

#[derive(RustEmbed)]
#[folder = "assets/"]
struct Assets;

/// Build the application router.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/", get(host_index))
        .route("/host/{host}", get(dashboard))
        .route("/host/{host}/container/{id}", get(container_detail))
        .route("/host/{host}/stack/{name}", get(stack_detail))
        .route("/host/{host}/events", get(events_dashboard))
        .route("/host/{host}/events/container/{id}", get(events_container))
        .route("/host/{host}/events/stack/{name}", get(events_stack))
        .route("/host/{host}/api/container/{id}/logs", get(container_logs))
        .route("/host/{host}/api/stack/{name}/compose", get(stack_compose))
        .route(
            "/host/{host}/api/metrics/container/{id}",
            get(metrics_container),
        )
        .route("/host/{host}/api/metrics/stack/{name}", get(metrics_stack))
        .route(
            "/host/{host}/api/history/container/{id}",
            get(history_container),
        )
        .route("/host/{host}/api/history/stack/{name}", get(history_stack))
        .route(
            "/host/{host}/api/container/{id}/{action}",
            axum::routing::post(container_action).layer(middleware::from_fn(require_htmx)),
        )
        .route(
            "/host/{host}/api/stack/{name}/{action}",
            axum::routing::post(stack_action).layer(middleware::from_fn(require_htmx)),
        )
        .route("/login", get(login_page).post(login_submit))
        .route("/logout", axum::routing::post(logout))
        .route("/host/{host}/ws/container/{id}/exec", get(ws_exec))
        .route("/assets/{*path}", get(asset))
        // Auth wraps everything (it allowlists /login and /assets internally);
        // the host check sits outside it so rejected hosts never reach auth.
        .layer(middleware::from_fn_with_state(state.clone(), require_auth))
        .layer(middleware::from_fn_with_state(state.clone(), check_host))
        .with_state(state)
}

/// Gate every request on a valid session when authentication is configured.
/// The login page and static assets stay public (the login page needs its CSS
/// and icon); everything else needs a valid session cookie. Unauthenticated
/// requests get an `HX-Redirect` (for htmx) or a 303 to `/login`.
async fn require_auth(State(state): State<AppState>, req: Request, next: Next) -> Response {
    let Some(auth) = state.auth.as_ref() else {
        return next.run(req).await; // authentication disabled
    };
    // /logout deliberately requires a session: combined with SameSite=Lax
    // (which withholds the cookie on cross-site POSTs), a page elsewhere
    // can't log the user out — its POST arrives unauthenticated and bounces
    // to /login without touching the victim's cookie.
    let path = req.uri().path();
    if path == "/login" || path.starts_with("/assets/") {
        return next.run(req).await;
    }
    if request_is_authenticated(auth, req.headers()) {
        return next.run(req).await;
    }
    // htmx follows HX-Redirect with a client-side navigation; a plain browser
    // request gets a normal redirect to the login page.
    if is_htmx_request(req.headers()) {
        return (StatusCode::UNAUTHORIZED, [("HX-Redirect", "/login")]).into_response();
    }
    Redirect::to("/login").into_response()
}

/// Whether the request carries a valid session cookie for `auth`.
fn request_is_authenticated(auth: &Auth, headers: &HeaderMap) -> bool {
    headers
        .get(header::COOKIE)
        .and_then(|v| v.to_str().ok())
        .and_then(|cookies| auth::cookie_value(cookies, auth::COOKIE_NAME))
        .is_some_and(|token| auth.token_valid(token, now_unix_secs()))
}

/// CSRF guard for the state-changing POST endpoints: require the
/// `HX-Request: true` header htmx adds to every request it issues. A cross-site
/// HTML form can't set custom headers, and setting one from a script makes the
/// request preflight-checked — which fails, since we never answer CORS
/// preflights. Same-origin htmx buttons are unaffected.
async fn require_htmx(req: Request, next: Next) -> Response {
    if is_htmx_request(req.headers()) {
        next.run(req).await
    } else {
        (
            StatusCode::FORBIDDEN,
            "rejected: not an htmx request (missing HX-Request header)",
        )
            .into_response()
    }
}

fn is_htmx_request(headers: &HeaderMap) -> bool {
    headers
        .get("hx-request")
        .is_some_and(|v| v.as_bytes() == b"true")
}

/// Host-header allowlist, a guard against DNS rebinding (where the attacker's
/// page *is* same-origin, so the htmx-header check above can't help). Active
/// only when hosts are configured; localhost forms always pass so local access
/// keeps working alongside a configured LAN hostname.
async fn check_host(State(state): State<AppState>, req: Request, next: Next) -> Response {
    if host_allowed(req.headers(), &state.allowed_hosts) {
        next.run(req).await
    } else {
        (StatusCode::FORBIDDEN, "rejected: Host not allowed").into_response()
    }
}

fn host_allowed(headers: &HeaderMap, allowed: &[String]) -> bool {
    if allowed.is_empty() {
        return true;
    }
    let Some(host) = headers.get(header::HOST).and_then(|v| v.to_str().ok()) else {
        return false;
    };
    let host = host_without_port(host.trim()).to_ascii_lowercase();
    matches!(host.as_str(), "localhost" | "127.0.0.1" | "::1") || allowed.contains(&host)
}

/// The hostname part of a `Host` header value: strips a `:port` suffix and the
/// brackets of an IPv6 literal (`[::1]:8080` → `::1`).
fn host_without_port(host: &str) -> &str {
    if let Some(rest) = host.strip_prefix('[') {
        rest.split(']').next().unwrap_or(rest)
    } else {
        host.rsplit_once(':').map_or(host, |(h, _)| h)
    }
}

/// Normalize the configured Host allowlist for [`AppState::allowed_hosts`]:
/// trim, drop empties, lowercase, strip ports/brackets.
pub fn normalize_allowed_hosts(hosts: &[String]) -> Arc<[String]> {
    hosts
        .iter()
        .map(|h| host_without_port(h.trim()).to_ascii_lowercase())
        .filter(|h| !h.is_empty())
        .collect()
}

/// Apply a start/stop/restart action to a container, then return the freshly
/// rendered action-button group so HTMX can swap it in place.
async fn container_action(
    State(state): State<AppState>,
    axum::extract::Path((host, id, action)): axum::extract::Path<(String, String, String)>,
) -> Response {
    let rt = match host_rt(&state, &host) {
        Ok(rt) => rt,
        Err(resp) => return resp,
    };
    let Some(action) = Action::parse(&action) else {
        return (StatusCode::BAD_REQUEST, "unknown action").into_response();
    };
    match rt.docker.apply(&id, action).await {
        Ok(()) => {
            tracing::info!(%host, %id, ?action, "applied container action");
            // The next collector cycle refreshes state; echo the buttons back.
            action_buttons(&host, &id, rt.is_read_only()).into_response()
        }
        Err(err) if is_forbidden(&err) => {
            mark_read_only(rt, &host);
            // Swap in disabled buttons so the UI reflects the read-only host.
            action_buttons(&host, &id, true).into_response()
        }
        Err(err) => {
            tracing::warn!(%host, %id, ?action, %err, "container action failed");
            (StatusCode::BAD_GATEWAY, format!("action failed: {err}")).into_response()
        }
    }
}

/// Latch a host into read-only after it returned `403 Forbidden` (a socket
/// proxy denying POST/exec). Logged once per flip.
fn mark_read_only(rt: &HostRuntime, host: &str) {
    if !rt.read_only.swap(true, Ordering::Relaxed) {
        tracing::warn!(%host, "host is read-only (proxy returned 403); disabling actions");
    }
}

/// Apply an action to every container in a stack, then echo the stack's action
/// buttons back for HTMX to swap.
async fn stack_action(
    State(state): State<AppState>,
    axum::extract::Path((host, name, action)): axum::extract::Path<(String, String, String)>,
) -> Response {
    let rt = match host_rt(&state, &host) {
        Ok(rt) => rt,
        Err(resp) => return resp,
    };
    let Some(action) = Action::parse(&action) else {
        return (StatusCode::BAD_REQUEST, "unknown action").into_response();
    };

    // Orchestrate dependency-aware: the docker layer reads the compose
    // depends_on labels and starts/stops members in the right order.
    match rt.docker.stack_action(&name, action).await {
        Ok(outcome) => {
            tracing::info!(
                %host, stack = %name, ?action,
                total = outcome.total, failed = outcome.failed,
                "applied stack action"
            );
        }
        Err(err) if is_forbidden(&err) => {
            mark_read_only(rt, &host);
            return stack_action_buttons(&host, &name, true).into_response();
        }
        Err(err) => {
            tracing::warn!(%host, stack = %name, ?action, %err, "stack action failed");
            return (
                StatusCode::BAD_GATEWAY,
                format!("stack action failed: {err}"),
            )
                .into_response();
        }
    }
    stack_action_buttons(&host, &name, rt.is_read_only()).into_response()
}

/// Look up a host's runtime by its slug, or a 404 response if there's no such
/// configured host.
// The `Err` is a ready-to-return `Response`, which is a large type — that's the
// point here (callers `return` it directly), not a value to shrink.
#[allow(clippy::result_large_err, reason = "Err is a ready Response to return")]
fn host_rt<'a>(state: &'a AppState, host: &str) -> Result<&'a HostRuntime, Response> {
    state
        .hosts
        .get(host)
        .ok_or_else(|| (StatusCode::NOT_FOUND, "unknown host").into_response())
}

/// The landing page at `/`. With a single host it redirects straight to that
/// host; with several it shows a chooser.
async fn host_index(State(state): State<AppState>) -> Response {
    if let [only] = state.host_order.as_ref() {
        return Redirect::to(&format!("/host/{only}")).into_response();
    }
    host_index_page(&state.host_order, state.auth.is_some()).into_response()
}

/// The current snapshot for one host, cloned out of its shared lock.
fn current_snapshot(rt: &HostRuntime) -> Option<Dashboard> {
    rt.shared.read().ok().and_then(|guard| guard.clone())
}

/// Number of log lines to tail for the logs panel.
const LOG_TAIL_LINES: u32 = 200;

/// Return the tail of a container's logs as an HTML fragment for the logs panel.
async fn container_logs(
    State(state): State<AppState>,
    axum::extract::Path((host, id)): axum::extract::Path<(String, String)>,
) -> Markup {
    let Some(rt) = state.hosts.get(&host) else {
        return html! { span.muted { "Unknown host." } };
    };
    match rt.docker.logs_tail(&id, LOG_TAIL_LINES).await {
        Ok(text) if text.trim().is_empty() => html! { span.muted { "(no logs)" } },
        Ok(text) => render_log_lines(&strip_ansi(&text)),
        Err(err) => {
            tracing::warn!(%host, %id, %err, "fetching logs failed");
            html! { span.muted { "Could not read logs: " (err) } }
        }
    }
}

/// Render log lines with their daemon timestamp prefix as a styled span.
/// `data-ts` carries the timestamp truncated to seconds (plain RFC 3339, no
/// fractional part) so the browser can re-render it in local time; the span
/// text is a UTC fallback for the brief moment before that happens.
fn render_log_lines(text: &str) -> Markup {
    html! {
        @for line in text.lines() {
            @if let Some((ts, msg)) = split_log_timestamp(line) {
                span.log-ts data-ts=(format!("{}Z", &ts[..19])) {
                    (ts[..19].replacen('T', " ", 1))
                } " " (msg) "\n"
            } @else {
                (line) "\n"
            }
        }
    }
}

/// Split a daemon-timestamped log line (`2026-06-12T10:15:30.123456789Z msg`)
/// into timestamp and message. Returns `None` when the line doesn't carry the
/// expected prefix (e.g. logs from a driver without timestamp support).
fn split_log_timestamp(line: &str) -> Option<(&str, &str)> {
    let (ts, msg) = line.split_once(' ')?;
    let b = ts.as_bytes();
    if b.len() < 20 || !ts.ends_with('Z') {
        return None;
    }
    // "YYYY-MM-DDTHH:MM:SS" — digits with fixed separators. Checking every
    // byte also guarantees the `ts[..19]` slice above lands on a char boundary.
    let pattern_ok = b[..19].iter().enumerate().all(|(i, c)| match i {
        4 | 7 => *c == b'-',
        10 => *c == b'T',
        13 | 16 => *c == b':',
        _ => c.is_ascii_digit(),
    });
    pattern_ok.then_some((ts, msg))
}

/// Return a stack's compose file(s) as an HTML fragment for the compose panel.
async fn stack_compose(
    State(state): State<AppState>,
    axum::extract::Path((host, name)): axum::extract::Path<(String, String)>,
) -> Markup {
    let Some(rt) = state.hosts.get(&host) else {
        return html! { span.muted { "Unknown host." } };
    };
    let paths = match rt.docker.compose_config_files(&name).await {
        Ok(paths) => paths,
        Err(err) => {
            tracing::warn!(%host, stack = %name, %err, "resolving compose files failed");
            return html! { span.muted { "Could not determine compose file: " (err) } };
        }
    };
    if paths.is_empty() {
        return html! { span.muted { "No compose file recorded for this stack." } };
    }

    let files = tokio::task::spawn_blocking(move || read_compose_files(&paths))
        .await
        .unwrap_or_default();
    html! {
        @for (path, body) in &files {
            div.compose-file {
                div.compose-path { (path) }
                pre.logs { (body) }
            }
        }
    }
}

/// Read each compose file from the host filesystem. Guarded to YAML paths so a
/// crafted container label can't make us read arbitrary files. Returns
/// `(path, contents-or-message)` pairs.
fn read_compose_files(paths: &[String]) -> Vec<(String, String)> {
    paths
        .iter()
        .map(|p| {
            let is_yaml = std::path::Path::new(p)
                .extension()
                .is_some_and(|e| e.eq_ignore_ascii_case("yml") || e.eq_ignore_ascii_case("yaml"));
            let body = if is_yaml {
                std::fs::read_to_string(p).unwrap_or_else(|e| format!("(could not read: {e})"))
            } else {
                "(refusing to read a non-YAML path)".to_string()
            };
            (p.clone(), body)
        })
        .collect()
}

/// Strip ANSI/VT escape sequences (CSI `ESC [ … final-byte`) so logs render
/// cleanly in a `<pre>` instead of showing raw escape codes.
fn strip_ansi(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut chars = input.chars();
    while let Some(c) = chars.next() {
        if c == '\u{1b}' {
            // Skip the introducer and everything up to the final byte (@-~).
            if chars.next() == Some('[') {
                for seq in chars.by_ref() {
                    if ('\u{40}'..='\u{7e}').contains(&seq) {
                        break;
                    }
                }
            }
        } else {
            out.push(c);
        }
    }
    out
}

/// Serve an embedded static asset.
async fn asset(axum::extract::Path(path): axum::extract::Path<String>) -> Response {
    match Assets::get(&path) {
        Some(file) => {
            let mime = mime_for(&path);
            ([(header::CONTENT_TYPE, mime)], file.data.into_owned()).into_response()
        }
        None => (StatusCode::NOT_FOUND, "not found").into_response(),
    }
}

fn mime_for(path: &str) -> &'static str {
    match path.rsplit('.').next() {
        Some("css") => "text/css; charset=utf-8",
        Some("js") => "text/javascript; charset=utf-8",
        Some("svg") => "image/svg+xml",
        _ => "application/octet-stream",
    }
}

/// Submitted login form.
#[derive(serde::Deserialize)]
struct LoginForm {
    username: String,
    password: String,
}

/// The login page. With auth off there is nothing to log in to, so send callers
/// home; with a valid session already present, likewise.
async fn login_page(State(state): State<AppState>, req: Request) -> Response {
    match state.auth.as_ref() {
        None => Redirect::to("/").into_response(),
        Some(auth) if request_is_authenticated(auth, req.headers()) => {
            Redirect::to("/").into_response()
        }
        Some(_) => login_shell(false).into_response(),
    }
}

/// Verify submitted credentials; on success set the session cookie and send the
/// user to the dashboard, otherwise re-render the form with an error.
async fn login_submit(State(state): State<AppState>, Form(form): Form<LoginForm>) -> Response {
    let Some(auth) = state.auth.as_ref() else {
        return Redirect::to("/").into_response();
    };
    if auth.credentials_valid(&form.username, &form.password) {
        let cookie = auth.issue_cookie(now_unix_secs());
        let mut resp = Redirect::to("/").into_response();
        if let Ok(value) = cookie.parse() {
            resp.headers_mut().insert(header::SET_COOKIE, value);
        }
        resp
    } else {
        // Fixed delay on failure: the comparison itself is constant-time, but
        // attempts were unlimited and instant. Half a second caps brute force
        // at ~2 guesses/s per connection without any rate-limit state to keep;
        // being async, it doesn't tie up a worker thread.
        tokio::time::sleep(Duration::from_millis(500)).await;
        (StatusCode::UNAUTHORIZED, login_shell(true)).into_response()
    }
}

/// Query for the exec WebSocket: the command to run (defaults to `/bin/sh`).
#[derive(serde::Deserialize)]
struct ExecQuery {
    cmd: Option<String>,
}

/// A terminal resize message from the client.
#[derive(serde::Deserialize)]
struct ResizeMsg {
    cols: u16,
    rows: u16,
}

/// WebSocket endpoint backing the container terminal. Authentication (when on)
/// is already enforced by the middleware on the upgrade request, which carries
/// the session cookie. Binary frames are stdin, text frames are resize JSON.
async fn ws_exec(
    State(state): State<AppState>,
    axum::extract::Path((host, id)): axum::extract::Path<(String, String)>,
    Query(q): Query<ExecQuery>,
    ws: WebSocketUpgrade,
) -> Response {
    let Some(rt) = state.hosts.get(&host) else {
        return (StatusCode::NOT_FOUND, "unknown host").into_response();
    };
    let cmd = q.cmd.unwrap_or_default();
    let docker = rt.docker.clone();
    let read_only = Arc::clone(&rt.read_only);
    ws.on_upgrade(move |socket| exec_bridge(socket, docker, host, id, cmd, read_only))
}

/// Bridge a WebSocket to a container exec session: engine output → client
/// (binary), client input → engine stdin (binary), client resize JSON →
/// `resize_exec` (text). Ends — closing the exec — as soon as either side does.
async fn exec_bridge(
    mut socket: WebSocket,
    docker: DockerHandle,
    host: String,
    id: String,
    cmd: String,
    read_only: Arc<AtomicBool>,
) {
    let session = match docker.exec_start(&id, &cmd).await {
        Ok(session) => session,
        Err(err) => {
            // A proxy denying exec (403) latches the host read-only, so the
            // detail page disables the terminal on its next render.
            if is_forbidden(&err) && !read_only.swap(true, Ordering::Relaxed) {
                tracing::warn!(%host, "host is read-only (proxy denied exec); disabling terminal");
            }
            // Surface the failure in the terminal itself, then close.
            let msg = format!("\r\n\x1b[31mfailed to start terminal: {err}\x1b[0m\r\n");
            let _ = socket.send(Message::Text(msg.into())).await;
            let _ = socket.close().await;
            return;
        }
    };
    let ExecSession {
        exec_id,
        mut output,
        mut input,
    } = session;
    let (mut ws_tx, mut ws_rx) = socket.split();

    // Engine → client.
    let to_client = async {
        while let Some(chunk) = output.next().await {
            let Ok(out) = chunk else { break };
            if ws_tx.send(Message::Binary(out.into_bytes())).await.is_err() {
                break;
            }
        }
    };

    // Client → engine (stdin) and resize control.
    let from_client = async {
        while let Some(Ok(msg)) = ws_rx.next().await {
            match msg {
                Message::Binary(bytes) => {
                    if input.write_all(&bytes).await.is_err() {
                        break;
                    }
                    let _ = input.flush().await;
                }
                Message::Text(text) => {
                    if let Ok(r) = serde_json::from_str::<ResizeMsg>(text.as_str()) {
                        let _ = docker.exec_resize(&exec_id, r.cols, r.rows).await;
                    }
                }
                Message::Close(_) => break,
                _ => {}
            }
        }
    };

    // Whichever side ends first tears down the other (dropping `input` ends the
    // exec, closing the socket ends the browser session).
    tokio::select! {
        () = to_client => {},
        () = from_client => {},
    }
}

/// Clear the session cookie and return to the login page.
async fn logout(State(state): State<AppState>) -> Response {
    let mut resp = Redirect::to("/login").into_response();
    if let Some(auth) = state.auth.as_ref()
        && let Ok(value) = auth.clear_cookie().parse()
    {
        resp.headers_mut().insert(header::SET_COOKIE, value);
    }
    resp
}

/// Query for the chart backfill endpoints: return points at or after this
/// Unix-ms timestamp (clamped to the seed window server-side).
#[derive(serde::Deserialize)]
struct SinceQuery {
    #[serde(default)]
    since_ms: u64,
}

/// Clamp a client-supplied `since_ms` to the seed window, so a stale or
/// malicious value can't request unbounded history in one response.
fn clamp_since(since_ms: u64, window: Duration) -> u64 {
    since_ms.max(now_unix_ms().saturating_sub(duration_ms(window)))
}

/// Run a blocking store query for chart points, degrading to an empty series.
/// The charts just show what they get — but a failing store is worth a log
/// line, not silence.
async fn fetch_points<T, F>(f: F) -> Vec<T>
where
    T: Send + 'static,
    F: FnOnce() -> anyhow::Result<Vec<T>> + Send + 'static,
{
    match tokio::task::spawn_blocking(f).await {
        Ok(Ok(points)) => points,
        Ok(Err(err)) => {
            tracing::warn!(%err, "loading chart points failed");
            Vec::new()
        }
        Err(join_err) => {
            tracing::warn!(%join_err, "chart point task panicked");
            Vec::new()
        }
    }
}

/// JSON backfill for a container detail page's charts. `live.js` calls this
/// before (re)opening the SSE stream to fill the gap that built up while the
/// page was hidden, in the bfcache, or suspended.
async fn metrics_container(
    State(state): State<AppState>,
    axum::extract::Path((host, id)): axum::extract::Path<(String, String)>,
    Query(q): Query<SinceQuery>,
) -> Json<Vec<MetricPoint>> {
    let since = clamp_since(q.since_ms, state.seed_window);
    let store = state.store.clone();
    Json(fetch_points(move || store.recent_container_samples(&host, &id, since)).await)
}

/// JSON backfill for a stack detail page's charts (trend-based, like the seed).
async fn metrics_stack(
    State(state): State<AppState>,
    axum::extract::Path((host, name)): axum::extract::Path<(String, String)>,
    Query(q): Query<SinceQuery>,
) -> Json<Vec<MetricPoint>> {
    let since = clamp_since(q.since_ms, state.seed_window);
    let store = state.store.clone();
    Json(fetch_points(move || store.recent_stack_trends(&host, &name, since)).await)
}

/// Selectable named history ranges: query value and lookback window.
// No `Duration::from_hours`/`from_days` on stable; same trade-off as elsewhere.
#[allow(clippy::duration_suboptimal_units)]
const HISTORY_RANGES: &[(&str, Duration)] = &[
    ("1h", Duration::from_secs(3600)),
    ("6h", Duration::from_secs(6 * 3600)),
    ("24h", Duration::from_secs(24 * 3600)),
    ("7d", Duration::from_secs(7 * 24 * 3600)),
    ("30d", Duration::from_secs(30 * 24 * 3600)),
];

/// Query for the history endpoints: either a named range from
/// [`HISTORY_RANGES`] or a free `since_ms`/`until_ms` window (used by the
/// drag-to-drill-down selection in the UI).
#[derive(serde::Deserialize)]
struct HistoryQuery {
    range: Option<String>,
    since_ms: Option<u64>,
    until_ms: Option<u64>,
}

/// A resolved history request window.
struct HistoryWindow {
    since: u64,
    until: u64,
    group_ms: u64,
    /// Serve from raw samples (window lies inside the raw retention) instead
    /// of trend buckets — full resolution, exact values.
    raw: bool,
}

/// Resolve a history query into a concrete window, or `None` if it is neither
/// a known named range nor a plausible custom window.
fn resolve_history(q: &HistoryQuery, seed_window: Duration) -> Option<HistoryWindow> {
    let now = now_unix_ms();
    let (since, until) = match (&q.range, q.since_ms, q.until_ms) {
        (Some(range), None, None) => {
            let &(_, window) = HISTORY_RANGES.iter().find(|r| r.0 == *range)?;
            (now.saturating_sub(duration_ms(window)), now)
        }
        (None, Some(since), Some(until)) if since < until => (since, until),
        _ => return None,
    };
    Some(HistoryWindow {
        since,
        until,
        group_ms: group_for_window(until - since),
        raw: since >= now.saturating_sub(duration_ms(seed_window)),
    })
}

/// Downsample group for a window: aim for ≤ ~1440 points, in whole trend
/// buckets (60 s). 24 h stays at one point per bucket; 30 d collapses into
/// 30 min windows.
fn group_for_window(window_ms: u64) -> u64 {
    const TARGET_POINTS: u64 = 1440;
    (window_ms / TARGET_POINTS).div_ceil(60_000).max(1) * 60_000
}

/// Downsample group for the *raw* path: same point target, but not quantized
/// to trend buckets. For windows short enough that the group stays below the
/// sample interval this is a no-op (every sample its own group) — the raw
/// path keeps its full resolution. For long windows it caps the response:
/// `raw_retention_secs` is a documented knob, and without the cap raising it
/// to days would let one request return hundreds of thousands of points,
/// stalling the browser for minutes.
fn raw_group_for_window(window_ms: u64) -> u64 {
    const TARGET_POINTS: u64 = 1440;
    (window_ms / TARGET_POINTS).max(1)
}

const INVALID_RANGE: (StatusCode, &str) = (
    StatusCode::BAD_REQUEST,
    "invalid range: pass ?range=1h|6h|24h|7d|30d or ?since_ms=&until_ms=",
);

/// History for one container's charts: median plus min–max envelope over the
/// window.
async fn history_container(
    State(state): State<AppState>,
    axum::extract::Path((host, id)): axum::extract::Path<(String, String)>,
    Query(q): Query<HistoryQuery>,
) -> Response {
    let Some(w) = resolve_history(&q, state.seed_window) else {
        return INVALID_RANGE.into_response();
    };
    // Trend history is keyed by container *name* so it spans recreations (see
    // `Store::history_container`), but the URL carries the id. Resolve it via
    // the live snapshot; for a container that is no longer running, fall back
    // to the name its trends were recorded under.
    let snapshot_name = state.hosts.get(&host).and_then(|rt| {
        current_snapshot(rt).and_then(|d| {
            d.containers
                .iter()
                .find(|c| c.id == id)
                .map(|c| c.name.clone())
        })
    });
    let store = state.store.clone();
    let points = if w.raw {
        let group = raw_group_for_window(w.until - w.since);
        fetch_points(move || store.history_container_raw(&host, &id, w.since, w.until, group)).await
    } else {
        fetch_points(move || {
            let Some(name) =
                snapshot_name.map_or_else(|| store.container_name(&host, &id), |n| Ok(Some(n)))?
            else {
                return Ok(Vec::new());
            };
            store.history_container(&host, &name, w.since, w.until, w.group_ms)
        })
        .await
    };
    Json(points).into_response()
}

/// History for a stack's aggregate charts. Stacks have no raw series, so all
/// windows come from the trend buckets.
async fn history_stack(
    State(state): State<AppState>,
    axum::extract::Path((host, name)): axum::extract::Path<(String, String)>,
    Query(q): Query<HistoryQuery>,
) -> Response {
    let Some(w) = resolve_history(&q, state.seed_window) else {
        return INVALID_RANGE.into_response();
    };
    let store = state.store.clone();
    Json(
        fetch_points(move || store.history_stack(&host, &name, w.since, w.until, w.group_ms)).await,
    )
    .into_response()
}

/// Render the dashboard from the latest snapshot. The dashboard has no charts
/// of its own (the host-metrics header was removed); it only lists containers
/// and updates them live over SSE.
async fn dashboard(
    State(state): State<AppState>,
    axum::extract::Path(host): axum::extract::Path<String>,
) -> Response {
    let rt = match host_rt(&state, &host) {
        Ok(rt) => rt,
        Err(resp) => return resp,
    };
    let snapshot = current_snapshot(rt);
    dashboard_page(
        &state,
        &host,
        snapshot.as_ref(),
        &rt.links,
        rt.is_read_only(),
    )
    .into_response()
}

/// Detail page for a single container.
async fn container_detail(
    State(state): State<AppState>,
    axum::extract::Path((host, id)): axum::extract::Path<(String, String)>,
) -> Response {
    let rt = match host_rt(&state, &host) {
        Ok(rt) => rt,
        Err(resp) => return resp,
    };
    let snapshot = current_snapshot(rt);
    let r = Render {
        host: &host,
        links: &rt.links,
        read_only: rt.is_read_only(),
    };
    let live_url = format!("/host/{host}/events");
    let Some(container) = snapshot
        .as_ref()
        .and_then(|d| d.containers.iter().find(|c| c.id == id).cloned())
    else {
        let body = shell(
            &state,
            &host,
            snapshot.as_ref(),
            html! { p.empty { "Container not found." } },
            &[],
            &live_url,
            "",
            false,
        );
        return (StatusCode::NOT_FOUND, body).into_response();
    };

    let since = now_unix_ms().saturating_sub(duration_ms(state.seed_window));
    let store = state.store.clone();
    let (seed_host, seed_id) = (host.clone(), id.clone());
    let seed =
        fetch_points(move || store.recent_container_samples(&seed_host, &seed_id, since)).await;

    let live_url = format!("/host/{host}/events/container/{id}");
    let backfill_url = format!("/host/{host}/api/metrics/container/{id}");
    shell(
        &state,
        &host,
        snapshot.as_ref(),
        container_detail_main(&container, &r),
        &seed,
        &live_url,
        &backfill_url,
        true,
    )
    .into_response()
}

/// Detail page for a compose stack.
async fn stack_detail(
    State(state): State<AppState>,
    axum::extract::Path((host, name)): axum::extract::Path<(String, String)>,
) -> Response {
    let rt = match host_rt(&state, &host) {
        Ok(rt) => rt,
        Err(resp) => return resp,
    };
    let snapshot = current_snapshot(rt);
    let r = Render {
        host: &host,
        links: &rt.links,
        read_only: rt.is_read_only(),
    };
    let members: Vec<ContainerMetrics> = snapshot
        .as_ref()
        .map(|d| {
            d.containers
                .iter()
                .filter(|c| c.stack.as_deref() == Some(name.as_str()))
                .cloned()
                .collect()
        })
        .unwrap_or_default();

    if members.is_empty() {
        let body = shell(
            &state,
            &host,
            snapshot.as_ref(),
            html! { p.empty { "Stack not found." } },
            &[],
            &format!("/host/{host}/events"),
            "",
            false,
        );
        return (StatusCode::NOT_FOUND, body).into_response();
    }

    // Stack charts stream live aggregates. Raw samples aren't keyed by stack,
    // so we seed from trends: the sum of member medians per bucket, which lines
    // up with the live aggregate (sum of current member values).
    let since = now_unix_ms().saturating_sub(duration_ms(state.seed_window));
    let store = state.store.clone();
    let (seed_host, seed_name) = (host.clone(), name.clone());
    let seed = fetch_points(move || store.recent_stack_trends(&seed_host, &seed_name, since)).await;

    // Members are non-empty here, so a snapshot exists; 1 is just a fallback.
    let cpu_count = snapshot.as_ref().map_or(1, |d| d.cpu_count);
    let live_url = format!("/host/{host}/events/stack/{name}");
    let backfill_url = format!("/host/{host}/api/metrics/stack/{name}");
    shell(
        &state,
        &host,
        snapshot.as_ref(),
        stack_detail_main(&name, &members, cpu_count, &r),
        &seed,
        &live_url,
        &backfill_url,
        false,
    )
    .into_response()
}

/// Single live stream for the dashboard: `header` + `containers` HTML fragments
/// per snapshot. One SSE connection per page keeps us well under the browser's
/// per-host connection limit. The dashboard has no charts, so no `metrics`.
async fn events_dashboard(
    State(state): State<AppState>,
    axum::extract::Path(host): axum::extract::Path<String>,
) -> Response {
    let Some(rt) = state.hosts.get(&host) else {
        return (StatusCode::NOT_FOUND, "unknown host").into_response();
    };
    let auth = state.auth.is_some();
    let order = Arc::clone(&state.host_order);
    let links = rt.links.clone();
    let read_only = Arc::clone(&rt.read_only);
    let stream = dashboard_stream(rt.shared.clone()).flat_map(move |dash| {
        let r = Render {
            host: &host,
            links: &links,
            read_only: read_only.load(Ordering::Relaxed),
        };
        futures_util::stream::iter([
            Ok::<_, Infallible>(header_event(&dash, &host, &order, auth)),
            Ok(containers_event(&dash, &r)),
        ])
    });
    Sse::new(stream)
        .keep_alive(KeepAlive::default())
        .into_response()
}

/// Single live stream for a container detail page: `header` plus the
/// container's `metrics` point.
async fn events_container(
    State(state): State<AppState>,
    axum::extract::Path((host, id)): axum::extract::Path<(String, String)>,
) -> Response {
    let Some(rt) = state.hosts.get(&host) else {
        return (StatusCode::NOT_FOUND, "unknown host").into_response();
    };
    let auth = state.auth.is_some();
    let order = Arc::clone(&state.host_order);
    let links = rt.links.clone();
    let stream = dashboard_stream(rt.shared.clone()).flat_map(move |dash| {
        let mut events: Vec<Result<Event, Infallible>> =
            vec![Ok(header_event(&dash, &host, &order, auth))];
        if let Some(c) = dash.containers.iter().find(|c| c.id == id) {
            events.push(Ok(detail_event(c, &links)));
            events.push(Ok(metrics_event(&MetricPoint {
                ts_ms: dash.generated_at_unix_ms,
                cpu_percent: c.cpu_percent.unwrap_or(0.0),
                mem_used: c.mem_used,
                net_rx: c.net_rx_bps,
                net_tx: c.net_tx_bps,
                disk_read: c.disk_read_bps,
                disk_write: c.disk_write_bps,
            })));
        }
        futures_util::stream::iter(events)
    });
    Sse::new(stream)
        .keep_alive(KeepAlive::default())
        .into_response()
}

/// Single live stream for a stack detail page: `header` plus an aggregate
/// `metrics` point (CPU and memory summed across the stack's containers).
async fn events_stack(
    State(state): State<AppState>,
    axum::extract::Path((host, name)): axum::extract::Path<(String, String)>,
) -> Response {
    let Some(rt) = state.hosts.get(&host) else {
        return (StatusCode::NOT_FOUND, "unknown host").into_response();
    };
    let auth = state.auth.is_some();
    let order = Arc::clone(&state.host_order);
    let links = rt.links.clone();
    let read_only = Arc::clone(&rt.read_only);
    let stream = dashboard_stream(rt.shared.clone()).flat_map(move |dash| {
        let r = Render {
            host: &host,
            links: &links,
            read_only: read_only.load(Ordering::Relaxed),
        };
        let members: Vec<&ContainerMetrics> = dash
            .containers
            .iter()
            .filter(|c| c.stack.as_deref() == Some(name.as_str()))
            .collect();
        let mut cpu = 0.0;
        let mut mem = 0u64;
        let mut io = StackIo::default();
        for c in &members {
            cpu += c.cpu_percent.unwrap_or(0.0);
            mem += c.mem_used.unwrap_or(0);
            io.add(c);
        }
        let members_event = Event::default()
            .event("containers")
            .data(stack_members_table(&members, dash.cpu_count, &r).into_string());
        futures_util::stream::iter([
            Ok::<_, Infallible>(header_event(&dash, &host, &order, auth)),
            Ok(members_event),
            Ok(metrics_event(&MetricPoint {
                ts_ms: dash.generated_at_unix_ms,
                cpu_percent: cpu,
                mem_used: Some(mem),
                net_rx: io.net_rx,
                net_tx: io.net_tx,
                disk_read: io.disk_read,
                disk_write: io.disk_write,
            })),
        ])
    });
    Sse::new(stream)
        .keep_alive(KeepAlive::default())
        .into_response()
}

/// The `detail` event: a container detail page's live region (state + facts).
fn detail_event(c: &ContainerMetrics, links: &PortLinks) -> Event {
    Event::default()
        .event("detail")
        .data(container_detail_live(c, links).into_string())
}

/// The `header` event: the host header's inner HTML.
fn header_event(dash: &Dashboard, host: &str, hosts: &[String], auth_enabled: bool) -> Event {
    Event::default()
        .event("header")
        .data(host_header_inner(Some(dash), host, hosts, auth_enabled).into_string())
}

/// The `containers` event: the container table's HTML.
fn containers_event(dash: &Dashboard, r: &Render) -> Event {
    Event::default()
        .event("containers")
        .data(container_section(&dash.containers, dash.cpu_count, r).into_string())
}

/// Accumulates a stack's per-second I/O rates across its members. A category
/// stays `None` until at least one member reports it, then sums it (members
/// without that category contribute nothing) — mirroring the trend-based seed,
/// where `SUM` over all-NULL is NULL but any value makes it that sum.
#[derive(Default)]
struct StackIo {
    net_rx: Option<f64>,
    net_tx: Option<f64>,
    disk_read: Option<f64>,
    disk_write: Option<f64>,
}

impl StackIo {
    fn add(&mut self, c: &ContainerMetrics) {
        add_opt(&mut self.net_rx, c.net_rx_bps);
        add_opt(&mut self.net_tx, c.net_tx_bps);
        add_opt(&mut self.disk_read, c.disk_read_bps);
        add_opt(&mut self.disk_write, c.disk_write_bps);
    }
}

fn add_opt(acc: &mut Option<f64>, v: Option<f64>) {
    if let Some(v) = v {
        *acc = Some(acc.unwrap_or(0.0) + v);
    }
}

/// The `metrics` event: a metric point as JSON.
fn metrics_event(point: &MetricPoint) -> Event {
    let data = serde_json::to_string(point).unwrap_or_else(|_| "{}".to_string());
    Event::default().event("metrics").data(data)
}

/// A stream of the latest snapshot: yields the current snapshot immediately,
/// then the newest one whenever it changes (deduped by timestamp). It polls the
/// shared snapshot — the same source the page render reads — so the live view
/// can never drift from a plain reload.
fn dashboard_stream(shared: SharedDashboard) -> impl Stream<Item = Arc<Dashboard>> + use<> {
    let ticker = tokio::time::interval(SNAPSHOT_POLL);
    futures_util::stream::unfold(
        (shared, 0u64, ticker),
        |(shared, last, mut ticker)| async move {
            loop {
                ticker.tick().await;
                let snapshot = shared.read().ok().and_then(|guard| guard.clone());
                if let Some(dash) = snapshot
                    && dash.generated_at_unix_ms != last
                {
                    let ts = dash.generated_at_unix_ms;
                    return Some((Arc::new(dash), (shared, ts, ticker)));
                }
            }
        },
    )
}

/// Full HTML shell shared by every page: the live host header, the page's main
/// content, and the chart machinery. `live_url` is the SSE endpoint the charts
/// subscribe to; `seed` pre-fills them with history; `backfill_url` is the JSON
/// endpoint `live.js` uses to close chart gaps before reconnecting.
// `main_content` is moved in by builder convention — callers hand off a
// freshly-built fragment they no longer need.
#[allow(
    clippy::needless_pass_by_value,
    clippy::too_many_arguments,
    reason = "page assembly genuinely needs all of these inputs"
)]
fn shell(
    state: &AppState,
    host: &str,
    snapshot: Option<&Dashboard>,
    main_content: Markup,
    seed: &[MetricPoint],
    live_url: &str,
    backfill_url: &str,
    terminal: bool,
) -> Markup {
    let auth_enabled = state.auth.is_some();
    let seed_json = serde_json::to_string(seed).unwrap_or_else(|_| "[]".to_string());
    // The history endpoints mirror the backfill endpoints by design — same
    // entity, same path shape — so the URL is derived instead of threaded
    // through every page handler.
    let history_url = backfill_url.replacen("/api/metrics/", "/api/history/", 1);
    html! {
        (DOCTYPE)
        html lang="en" {
            head {
                meta charset="utf-8";
                meta name="viewport" content="width=device-width, initial-scale=1";
                title { "DockDoe" }
                // Two favicon variants picked by the browser's tab-bar theme:
                // the dark mark for light chrome, the white one for dark.
                link rel="icon" type="image/svg+xml" href="/assets/dockdoe.svg"
                    media="(prefers-color-scheme: light)";
                link rel="icon" type="image/svg+xml" href="/assets/dockdoe-white.svg"
                    media="(prefers-color-scheme: dark)";
                link rel="stylesheet" href="/assets/vendor/uPlot.min.css";
                @if terminal {
                    link rel="stylesheet" href="/assets/vendor/xterm.css";
                }
                link rel="stylesheet" href="/assets/dockdoe.css";
            }
            body {
                header.host id="host-header" {
                    (host_header_inner(snapshot, host, &state.host_order, auth_enabled))
                }
                main { (main_content) }
                // Error toast: filled by live.js on htmx request failures.
                // Lives outside the live regions so the 1s SSE swaps of
                // #containers / #detail-live can't wipe the message.
                div id="toast" role="alert" {}
                // History overlay, opened by the expand button on any chart
                // card and driven by history.js. The inner .history-body
                // covers the whole dialog so a click landing on the <dialog>
                // element itself can only mean the backdrop.
                dialog id="history-dialog" {
                    div.history-body {
                        div.history-head {
                            span.history-title id="history-title" {}
                            // Rendered from HISTORY_RANGES so the buttons,
                            // the server's accepted values and the deep-link
                            // validation (history.js checks these buttons)
                            // stay one list.
                            div.history-ranges id="history-ranges" {
                                @for (name, _) in HISTORY_RANGES {
                                    button type="button" data-range=(name) { (name) }
                                }
                            }
                            // Shown by history.js while a drag-selected
                            // window is displayed.
                            span.history-hint id="history-hint" {
                                "double-click to zoom out"
                            }
                            span.chart-readout id="history-readout" {}
                            button.history-close id="history-close" type="button"
                                aria-label="Close" { "✕" }
                        }
                        div id="history-chart" {}
                    }
                }
                script id="seed-data" type="application/json"
                    data-live-url=(live_url) data-backfill-url=(backfill_url)
                    data-history-url=(history_url) {
                    (maud::PreEscaped(seed_json))
                }
                script src="/assets/vendor/htmx.min.js" {}
                script src="/assets/vendor/uPlot.iife.min.js" {}
                script src="/assets/live.js" {}
                script src="/assets/history.js" {}
                @if terminal {
                    script src="/assets/vendor/xterm.js" {}
                    script src="/assets/vendor/addon-fit.js" {}
                    script src="/assets/terminal.js" {}
                }
            }
        }
    }
}

/// The dashboard body: the live container table. No charts — the host-metrics
/// header that owned them was removed.
fn dashboard_page(
    state: &AppState,
    host: &str,
    snapshot: Option<&Dashboard>,
    links: &PortLinks,
    read_only: bool,
) -> Markup {
    let r = Render {
        host,
        links,
        read_only,
    };
    let main = html! {
        div id="containers" {
            @match snapshot {
                Some(d) => (container_section(&d.containers, d.cpu_count, &r)),
                None => p.empty { "Collecting first metrics sample…" },
            }
        }
    };
    // No charts here, so no seed or backfill endpoint; the SSE stream still
    // drives the header and the container table.
    let live_url = format!("/host/{host}/events");
    shell(state, host, snapshot, main, &[], &live_url, "", false)
}

/// Markup for a pair of live charts (CPU + memory). Data comes from `live.js`,
/// which reads the seed and live URL off the page.
fn charts_section(cpu_title: &str, mem_title: &str) -> Markup {
    html! {
        section.charts {
            div.chart-card {
                div.chart-head {
                    span.chart-title { (cpu_title) }
                    // Filled by live.js while hovering: "21:43:05 · 3.2%".
                    span.chart-readout id="readout-cpu" {}
                    button.chart-zoom type="button" data-metric="cpu"
                        title="Show history" aria-label="Show history" { "⤢" }
                }
                div id="chart-cpu" {}
            }
            div.chart-card {
                div.chart-head {
                    span.chart-title { (mem_title) }
                    span.chart-readout id="readout-mem" {}
                    button.chart-zoom type="button" data-metric="mem"
                        title="Show history" aria-label="Show history" { "⤢" }
                }
                div id="chart-mem" {}
            }
        }
    }
}

/// Network and disk-I/O charts: two dual-line cards (rx/read in blue,
/// tx/write in green), fed by `live.js` from the same SSE point stream as the
/// CPU/memory charts. The expand button opens the same history overlay, which
/// renders both directions with their own min–max bands.
fn io_charts_section() -> Markup {
    html! {
        section.charts {
            (io_chart_card("Network", "net", "rx", "tx"))
            (io_chart_card("Disk I/O", "disk", "read", "write"))
        }
    }
}

/// One dual-line I/O chart card: a title, a two-key colour legend, a hover
/// readout, the history-expand button, and the uPlot mount point
/// (`chart-net` / `chart-disk`).
fn io_chart_card(title: &str, key: &str, in_label: &str, out_label: &str) -> Markup {
    html! {
        div.chart-card {
            div.chart-head {
                span.chart-title { (title) }
                span.chart-legend {
                    span.k.in { (in_label) }
                    span.k.out { (out_label) }
                }
                span.chart-readout id=(format!("readout-{key}")) {}
                button.chart-zoom type="button" data-metric=(key)
                    title="Show history" aria-label="Show history" { "⤢" }
            }
            div id=(format!("chart-{key}")) {}
        }
    }
}

/// Body of a single-container detail page.
fn container_detail_main(c: &ContainerMetrics, r: &Render) -> Markup {
    let host = r.host;
    html! {
        section.detail-head {
            div.detail-title {
                a.back href=(format!("/host/{host}")) { "← Dashboard" }
                h1 { (c.name) }
                @if let Some(stack) = &c.stack {
                    a.stack-pill href=(format!("/host/{host}/stack/{stack}")) { (stack) }
                }
            }
            (action_buttons(host, &c.id, r.read_only))
        }
        // Live region: swapped wholesale by the `detail` SSE event so state and
        // facts track reality. The charts below are left untouched.
        div id="detail-live" { (container_detail_live(c, r.links)) }
        (charts_section(&format!("{} · CPU", c.name), &format!("{} · Memory", c.name)))
        (io_charts_section())
        section.panel {
            div.panel-head {
                h3 { "Logs " span.count { "(last " (LOG_TAIL_LINES) " lines)" } }
                button.refresh type="button"
                    hx-get=(format!("/host/{host}/api/container/{}/logs", c.id))
                    hx-target="#logs" hx-swap="innerHTML" { "↻ Refresh" }
            }
            pre.logs id="logs"
                hx-get=(format!("/host/{host}/api/container/{}/logs", c.id))
                hx-trigger="load" hx-swap="innerHTML" { "Loading logs…" }
        }
        (terminal_panel(c, host, r.read_only))
    }
}

/// The interactive terminal panel. Connects lazily (a shell is a real process,
/// so we don't spawn one on every page view) and only for running containers;
/// `terminal.js` opens the WebSocket on "Connect". The `⛶` button toggles a
/// fullscreen class on the panel. Stopped containers get a disabled note.
fn terminal_panel(c: &ContainerMetrics, host: &str, read_only: bool) -> Markup {
    let interactive = c.state == ContainerState::Running && !read_only;
    html! {
        section.panel.terminal id="terminal" data-ws=(format!("/host/{host}/ws/container/{}/exec", c.id)) {
            div.panel-head {
                h3 { "Terminal" }
                @if interactive {
                    div.term-controls {
                        input.term-cmd type="text" value="/bin/sh"
                            aria-label="Command" spellcheck="false" autocapitalize="off";
                        button.term-connect type="button" { "Connect" }
                        button.term-fs type="button" title="Toggle fullscreen"
                            aria-label="Toggle fullscreen" { "⛶" }
                    }
                }
            }
            @if interactive {
                div.term-view id="term-view" {}
            } @else if read_only {
                p.empty { "This host is read-only — the terminal is disabled." }
            } @else {
                p.empty { "Container is not running." }
            }
        }
    }
}

/// The shared table header of every container table. The column classes carry
/// fixed widths (see dockdoe.css), so tables in different stack sections line
/// up with each other — keep this the single definition.
fn container_table_head() -> Markup {
    html! {
        thead {
            tr {
                th { "Container" }
                th.col-image { "Image" }
                th.col-state { "State" }
                th.num.col-cpu { "CPU" }
                th.num.col-mem { "Memory" }
                th.actions-col { "Actions" }
            }
        }
    }
}

/// A stack's member containers as a table (live region on the stack page).
fn stack_members_table(members: &[&ContainerMetrics], cpu_count: usize, r: &Render) -> Markup {
    html! {
        section.stack {
            table {
                (container_table_head())
                tbody {
                    @for c in members { (container_row(c, cpu_count, r)) }
                }
            }
        }
    }
}

/// The live-updating part of a container detail page: state badge, health, and
/// the facts grid. Re-rendered and pushed via the `detail` SSE event.
fn container_detail_live(c: &ContainerMetrics, links: &PortLinks) -> Markup {
    html! {
        div.status-line {
            span.badge.(state_name(c.state)) { (state_name(c.state)) }
            (health_marker(c.health))
            (port_pills(&c.ports, links))
        }
        section.facts {
            (fact("Image", short_image(&c.image)))
            (fact("Container ID", &c.id.chars().take(12).collect::<String>()))
            (fact("Status", &c.status))
            (fact(
                "Memory limit",
                &c.mem_limit.map_or_else(|| "—".to_string(), fmt_bytes),
            ))
        }
    }
}

/// Body of a stack detail page: aggregate charts, stack actions, and the
/// stack's containers.
fn stack_detail_main(
    name: &str,
    members: &[ContainerMetrics],
    cpu_count: usize,
    r: &Render,
) -> Markup {
    let host = r.host;
    html! {
        section.detail-head {
            div.detail-title {
                a.back href=(format!("/host/{host}")) { "← Dashboard" }
                h1 { (name) }
                span.count { "(" (members.len()) ")" }
            }
            (stack_action_buttons(host, name, r.read_only))
        }
        (charts_section(
            &format!("{name} · CPU (sum)"),
            &format!("{name} · Memory (sum)"),
        ))
        (io_charts_section())
        // Live region: the `containers` SSE event swaps the member table so its
        // states/metrics track reality.
        div id="containers" { (stack_members_table(&members.iter().collect::<Vec<_>>(), cpu_count, r)) }
        section.panel {
            div.panel-head {
                h3 { "compose.yml" }
                button.refresh type="button"
                    hx-get=(format!("/host/{host}/api/stack/{name}/compose"))
                    hx-target="#compose" hx-swap="innerHTML" { "↻ Refresh" }
            }
            div id="compose"
                hx-get=(format!("/host/{host}/api/stack/{name}/compose"))
                hx-trigger="load" hx-swap="innerHTML" { "Loading…" }
        }
    }
}

/// Start/stop/restart-all buttons for a whole stack.
fn stack_action_buttons(host: &str, name: &str, read_only: bool) -> Markup {
    html! {
        span.actions title=[read_only.then_some("host is read-only")] {
            button.act.start type="button" disabled[read_only]
                hx-post=(format!("/host/{host}/api/stack/{name}/start"))
                hx-target="closest .actions" hx-swap="outerHTML"
                title="Start all" { "▶" }
            button.act.restart type="button" disabled[read_only]
                hx-post=(format!("/host/{host}/api/stack/{name}/restart"))
                hx-target="closest .actions" hx-swap="outerHTML"
                hx-confirm=(format!("Restart all containers in {name}?"))
                title="Restart all" { "⟳" }
            button.act.stop type="button" disabled[read_only]
                hx-post=(format!("/host/{host}/api/stack/{name}/stop"))
                hx-target="closest .actions" hx-swap="outerHTML"
                hx-confirm=(format!("Stop all containers in {name}?"))
                title="Stop all" { "■" }
        }
    }
}

/// A labelled fact for the detail meta grid.
fn fact(label: &str, value: &str) -> Markup {
    html! {
        div.fact {
            span.fact-label { (label) }
            span.fact-value { (value) }
        }
    }
}

/// The brand mark in the header: the app icon followed by the wordmark. The
/// white icon variant is used because the UI is dark-themed throughout. `alt`
/// is empty — the adjacent "DockDoe" text already names the link.
fn brand() -> Markup {
    html! {
        a.brand href="/" {
            img.brand-icon src="/assets/dockdoe-white.svg" alt="";
            span.brand-name { "Dock" span { "Doe" } }
        }
    }
}

/// The standalone login page — deliberately minimal: the brand, a username and
/// password field, and a submit button. No header, charts, or live machinery,
/// so it needs none of the page scripts. `error` shows the failed-login notice.
fn login_shell(error: bool) -> Markup {
    html! {
        (DOCTYPE)
        html lang="en" {
            head {
                meta charset="utf-8";
                meta name="viewport" content="width=device-width, initial-scale=1";
                title { "DockDoe — Log in" }
                link rel="icon" type="image/svg+xml" href="/assets/dockdoe.svg"
                    media="(prefers-color-scheme: light)";
                link rel="icon" type="image/svg+xml" href="/assets/dockdoe-white.svg"
                    media="(prefers-color-scheme: dark)";
                link rel="stylesheet" href="/assets/dockdoe.css";
            }
            body.login-body {
                form.login method="post" action="/login" {
                    (brand())
                    @if error {
                        p.login-error { "Invalid username or password." }
                    }
                    label."visually-hidden" for="username" { "Username" }
                    input #username type="text" name="username" placeholder="Username"
                        autocomplete="username" autofocus required;
                    label."visually-hidden" for="password" { "Password" }
                    input #password type="password" name="password" placeholder="Password"
                        autocomplete="current-password" required;
                    button type="submit" { "Log in" }
                }
            }
        }
    }
}

/// The inner content of the host header (everything HTMX swaps on each update).
/// `dash` is `None` before the first snapshot arrives — then only the brand and
/// host switcher show, no tally or timestamp.
fn host_header_inner(
    dash: Option<&Dashboard>,
    host: &str,
    hosts: &[String],
    auth_enabled: bool,
) -> Markup {
    html! {
        (brand())
        (host_switch(host, hosts))
        @if let Some(d) = dash {
            (container_counts_metric(&ContainerCounts::of(&d.containers)))
        }
        span.spacer {}
        @if let Some(d) = dash {
            span.generated { "updated " (fmt_age(d.generated_at_unix_ms)) }
        }
        @if auth_enabled {
            (logout_button())
        }
    }
}

/// The header's logout control: a one-button POST form (styled as a link).
/// A GET link would let any cross-site `<img src=".../logout">` end the
/// session; a POST from elsewhere doesn't carry the SameSite=Lax cookie and
/// is turned away by the auth middleware instead.
fn logout_button() -> Markup {
    html! {
        form.logout-form method="post" action="/logout" {
            button.logout type="submit" title="Log out" { "Logout" }
        }
    }
}

/// The host switcher: one link per monitored host, the current one marked.
/// Rendered only when more than one host is configured (single-host setups have
/// nothing to switch between).
fn host_switch(current: &str, hosts: &[String]) -> Markup {
    html! {
        @if hosts.len() > 1 {
            nav.host-switch {
                @for h in hosts {
                    @if h == current {
                        span.host-tab.current { (h) }
                    } @else {
                        a.host-tab href=(format!("/host/{h}")) { (h) }
                    }
                }
            }
        }
    }
}

/// The multi-host landing page at `/`: a simple list of hosts to pick from.
/// Single-host setups never reach this (they redirect straight to the host).
fn host_index_page(hosts: &[String], auth_enabled: bool) -> Markup {
    html! {
        (DOCTYPE)
        html lang="en" {
            head {
                meta charset="utf-8";
                meta name="viewport" content="width=device-width, initial-scale=1";
                title { "DockDoe" }
                link rel="icon" type="image/svg+xml" href="/assets/dockdoe.svg"
                    media="(prefers-color-scheme: light)";
                link rel="icon" type="image/svg+xml" href="/assets/dockdoe-white.svg"
                    media="(prefers-color-scheme: dark)";
                link rel="stylesheet" href="/assets/dockdoe.css";
            }
            body {
                header.host id="host-header" {
                    (brand())
                    span.spacer {}
                    @if auth_enabled {
                        (logout_button())
                    }
                }
                main {
                    section.host-list {
                        h2 { "Hosts" }
                        ul {
                            @for h in hosts {
                                li { a href=(format!("/host/{h}")) { (h) } }
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Container tally for the header, bucketed the way the badge colours are:
/// running (green), exited (red), everything else lumped as "other".
#[derive(Debug, PartialEq, Eq)]
struct ContainerCounts {
    total: usize,
    running: usize,
    exited: usize,
    other: usize,
    unhealthy: usize,
}

impl ContainerCounts {
    fn of(containers: &[ContainerMetrics]) -> Self {
        let mut counts = ContainerCounts {
            total: containers.len(),
            running: 0,
            exited: 0,
            other: 0,
            unhealthy: 0,
        };
        for c in containers {
            match c.state {
                ContainerState::Running => counts.running += 1,
                ContainerState::Exited => counts.exited += 1,
                _ => counts.other += 1,
            }
            if c.health == HealthState::Unhealthy {
                counts.unhealthy += 1;
            }
        }
        counts
    }
}

/// The header's container tally. Zero buckets are omitted ("0 exited" is
/// noise), except running — "0 running" is exactly the alarm worth seeing.
fn container_counts_metric(counts: &ContainerCounts) -> Markup {
    html! {
        div.metric {
            span.label { "Containers" }
            span.value {
                (counts.total)
                span.count-ok { " · " (counts.running) " running" }
                @if counts.exited > 0 {
                    span.count-err { " · " (counts.exited) " exited" }
                }
                @if counts.other > 0 {
                    span.count-idle { " · " (counts.other) " other" }
                }
                @if counts.unhealthy > 0 {
                    span.count-err { " · " (counts.unhealthy) " unhealthy" }
                }
            }
        }
    }
}

/// Render all containers, grouped by compose stack. Standalone containers
/// (no compose project) are grouped last under "Standalone".
fn container_section(containers: &[ContainerMetrics], cpu_count: usize, r: &Render) -> Markup {
    if containers.is_empty() {
        return html! { p.empty { "No containers found." } };
    }

    // BTreeMap keyed so that named stacks sort alphabetically and standalone
    // (None) sorts last; members within each stack are sorted by name.
    let mut groups: BTreeMap<StackKey<'_>, Vec<&ContainerMetrics>> = BTreeMap::new();
    for c in containers {
        let key = match &c.stack {
            Some(name) => StackKey::Named(name),
            None => StackKey::Standalone,
        };
        groups.entry(key).or_default().push(c);
    }
    for members in groups.values_mut() {
        members.sort_by(|a, b| a.name.cmp(&b.name));
    }

    html! {
        @for (key, members) in &groups {
            @let title = match key {
                StackKey::Named(name) => *name,
                StackKey::Standalone => "Standalone",
            };
            section.stack {
                h2 {
                    @match key {
                        StackKey::Named(name) => a.stack-link href=(format!("/host/{}/stack/{name}", r.host)) { (title) },
                        StackKey::Standalone => span { (title) },
                    }
                    " " span.count { "(" (members.len()) ")" }
                }
                table {
                    (container_table_head())
                    tbody {
                        @for c in members { (container_row(c, cpu_count, r)) }
                    }
                }
            }
        }
    }
}

/// One container table row. `cpu_count` scales the CPU bar: container CPU% is
/// per-core cumulative (a busy 4-core container reads 400%), so full bar =
/// the whole host, and the bar matches what the host CPU chart would show.
fn container_row(c: &ContainerMetrics, cpu_count: usize, r: &Render) -> Markup {
    html! {
        tr {
            td.name { a href=(format!("/host/{}/container/{}", r.host, c.id)) { (c.name) } }
            td.image { (short_image(&c.image)) }
            td {
                span.badge.(state_name(c.state)) { (state_name(c.state)) }
                (health_marker(c.health))
                (port_chips(&c.ports, r.links))
            }
            td.num {
                @match c.cpu_percent {
                    Some(pct) => {
                        (format!("{pct:.1}%"))
                        (bar(pct, cpu_count.max(1) as f64 * 100.0))
                    }
                    None => span style="color:var(--muted)" { "–" }
                }
            }
            td.num {
                @match c.mem_used {
                    Some(used) => {
                        (fmt_bytes(used))
                        @if let Some(limit) = c.mem_limit {
                            (bar(used as f64, limit as f64))
                        }
                    }
                    None => span style="color:var(--muted)" { "–" }
                }
            }
            td.actions-cell { (action_buttons(r.host, &c.id, r.read_only)) }
        }
    }
}

/// Start/stop/restart buttons for one container. Stateless so it can be reused
/// verbatim in the row and as the HTMX response after an action. Destructive
/// actions ask for confirmation.
fn action_buttons(host: &str, id: &str, read_only: bool) -> Markup {
    html! {
        span.actions title=[read_only.then_some("host is read-only")] {
            button.act.start type="button" disabled[read_only]
                hx-post=(format!("/host/{host}/api/container/{id}/start"))
                hx-target="closest .actions" hx-swap="outerHTML"
                title="Start" { "▶" }
            button.act.restart type="button" disabled[read_only]
                hx-post=(format!("/host/{host}/api/container/{id}/restart"))
                hx-target="closest .actions" hx-swap="outerHTML"
                hx-confirm="Restart this container?"
                title="Restart" { "⟳" }
            button.act.stop type="button" disabled[read_only]
                hx-post=(format!("/host/{host}/api/container/{id}/stop"))
                hx-target="closest .actions" hx-swap="outerHTML"
                hx-confirm="Stop this container?"
                title="Stop" { "■" }
        }
    }
}

/// A horizontal fill bar; turns amber above 70% and red above 90%.
fn bar(value: f64, max: f64) -> Markup {
    let ratio = if max > 0.0 {
        (value / max).clamp(0.0, 1.0)
    } else {
        0.0
    };
    let pct = ratio * 100.0;
    let class = if ratio >= 0.9 {
        "bar err"
    } else if ratio >= 0.7 {
        "bar warn"
    } else {
        "bar"
    };
    html! {
        span class=(class) { span style=(format!("width:{pct:.1}%")) {} }
    }
}

fn health_marker(health: HealthState) -> Markup {
    let (class, label) = match health {
        HealthState::Healthy => ("healthy", "● healthy"),
        HealthState::Unhealthy => ("unhealthy", "● unhealthy"),
        HealthState::Starting => ("starting", "● starting"),
        HealthState::None => return html! {},
    };
    html! { span class=(format!("health {class}")) { (label) } }
}

/// All ports for the container detail page: every published port as a blue
/// link to the browsing host, every internal-only port as a muted,
/// non-clickable pill. Renders nothing when the container exposes no ports.
fn port_pills(ports: &[Port], links: &PortLinks) -> Markup {
    if ports.is_empty() {
        return html! {};
    }
    html! {
        span.ports {
            @for p in ports {
                @match p.public {
                    Some(_) => (port_link(p, true, links)),
                    None => span.port-pill.muted
                        title="exposed inside Docker, not published to the host" {
                        (p.private) (proto_suffix(&p.proto))
                    },
                }
            }
        }
    }
}

/// Compact published-port chips for the dense tables: the first two host ports
/// as links, any remainder collapsed into a neutral "+N" chip (the rest are
/// listed in its tooltip). Internal-only ports are omitted here — they show on
/// the detail page. Renders nothing without any published port.
fn port_chips(ports: &[Port], links: &PortLinks) -> Markup {
    const SHOWN: usize = 2;
    let published: Vec<&Port> = ports.iter().filter(|p| p.public.is_some()).collect();
    if published.is_empty() {
        return html! {};
    }
    html! {
        span.ports {
            @for p in published.iter().take(SHOWN) { (port_link(p, false, links)) }
            @if published.len() > SHOWN {
                span.port-more title=(overflow_list(&published[SHOWN..])) {
                    "+" (published.len() - SHOWN)
                }
            }
        }
    }
}

/// A blue pill for one published port. `full` adds the "→ container-port" detail
/// used on the container page; the dense tables show only the host port.
///
/// Whether it is a link, and to where, follows the host's [`PortLinks`]:
/// - [`PortLinks::Browser`]: a `localhost` href plus `data-port`, which live.js
///   rewrites to the actual browsing host (the only place the real hostname is
///   known).
/// - [`PortLinks::Host`]: a link straight to the configured host — for when the
///   browsing host is a proxy but the Docker host is reachable directly.
/// - [`PortLinks::Off`]: an unlinked pill, for proxy-only setups where no direct
///   `host:port` link works.
fn port_link(p: &Port, full: bool, links: &PortLinks) -> Markup {
    let public = p.public.expect("port_link called on an unpublished port");
    let title = format!("host {public} → container {}/{}", p.private, p.proto);
    let label = html! {
        (public)
        @if full { " → " (p.private) }
        (proto_suffix(&p.proto))
    };
    match links {
        PortLinks::Off => html! {
            span.port-pill title=(title) { (label) }
        },
        PortLinks::Host(host) => html! {
            a.port-pill target="_blank" rel="noopener"
                href=(format!("http://{host}:{public}")) title=(title) { (label) }
        },
        PortLinks::Browser => html! {
            a.port-pill data-port=(public) target="_blank" rel="noopener"
                href=(format!("http://localhost:{public}")) title=(title) { (label) }
        },
    }
}

/// "/udp" / "/sctp" suffix for non-TCP ports; empty for plain TCP, which is the
/// overwhelming default and would only add noise.
fn proto_suffix(proto: &str) -> String {
    if proto == "tcp" {
        String::new()
    } else {
        format!("/{proto}")
    }
}

/// Space-separated list of published ports for the "+N" chip's tooltip.
fn overflow_list(rest: &[&Port]) -> String {
    rest.iter()
        .filter_map(|p| {
            p.public
                .map(|public| format!("{public}{}", proto_suffix(&p.proto)))
        })
        .collect::<Vec<_>>()
        .join("  ")
}

/// Key that sorts named stacks alphabetically before standalone containers.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
enum StackKey<'a> {
    Named(&'a str),
    Standalone,
}

/// A container state's name, used both as the badge's CSS class and as its
/// visible label (they are the same word for every state).
fn state_name(state: ContainerState) -> &'static str {
    match state {
        ContainerState::Running => "running",
        ContainerState::Exited => "exited",
        ContainerState::Dead => "dead",
        ContainerState::Paused => "paused",
        ContainerState::Restarting => "restarting",
        ContainerState::Stopping => "stopping",
        ContainerState::Removing => "removing",
        ContainerState::Created => "created",
        ContainerState::Unknown => "unknown",
    }
}

/// Strip a registry/tag down to a readable image name.
fn short_image(image: &str) -> &str {
    image.rsplit('/').next().unwrap_or(image)
}

/// Format a byte count with binary units.
fn fmt_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

/// Render how long ago a Unix-ms timestamp was, relative to now. This backs
/// the header's staleness hint, so the far end matters most: a collector that
/// has been stuck for days must read as "3d ago", not "4320m ago".
fn fmt_age(unix_ms: u64) -> String {
    fmt_age_secs(now_unix_ms().saturating_sub(unix_ms) / 1000)
}

fn fmt_age_secs(secs: u64) -> String {
    match secs {
        0 => "just now".to_string(),
        s if s < 60 => format!("{s}s ago"),
        s if s < 3600 => format!("{}m ago", s / 60),
        s if s < 86_400 => format!("{}h ago", s / 3600),
        s => format!("{}d ago", s / 86_400),
    }
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

fn now_unix_secs() -> u64 {
    now_unix_ms() / 1000
}

fn duration_ms(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn port_links_from_config_resolves_each_case() {
        let unix = "unix:///var/run/docker.sock";
        let host = |pub_host, url| match PortLinks::from_config(pub_host, url) {
            PortLinks::Host(h) => h,
            other => panic!("expected Host, got {other:?}"),
        };

        // An explicit public_host wins over the endpoint URL.
        assert!(matches!(
            PortLinks::from_config(Some("off"), unix),
            PortLinks::Off
        ));
        assert!(matches!(
            PortLinks::from_config(Some("OFF"), unix),
            PortLinks::Off
        ));
        assert!(matches!(
            PortLinks::from_config(Some("none"), unix),
            PortLinks::Off
        ));
        assert!(matches!(
            PortLinks::from_config(Some("-"), unix),
            PortLinks::Off
        ));
        assert_eq!(host(Some(" 192.168.1.50 "), unix), "192.168.1.50");

        // No usable public_host: a unix socket falls back to the browsing host.
        assert!(matches!(
            PortLinks::from_config(None, unix),
            PortLinks::Browser
        ));
        assert!(matches!(
            PortLinks::from_config(Some(""), unix),
            PortLinks::Browser
        ));
        assert!(matches!(
            PortLinks::from_config(Some("  "), unix),
            PortLinks::Browser
        ));

        // No public_host: derive from a tcp/http/https endpoint's host.
        assert_eq!(host(None, "tcp://nas.lan:2375"), "nas.lan");
        assert_eq!(
            host(None, "https://docker.example.com:2376/"),
            "docker.example.com"
        );
        assert_eq!(host(None, "http://10.0.0.5:2375"), "10.0.0.5");
        assert_eq!(host(None, "tcp://[::1]:2375"), "::1");
    }

    #[test]
    fn fmt_bytes_uses_binary_units() {
        assert_eq!(fmt_bytes(512), "512 B");
        assert_eq!(fmt_bytes(1024), "1.0 KiB");
        assert_eq!(fmt_bytes(1_572_864), "1.5 MiB");
        assert_eq!(fmt_bytes(2 * 1024 * 1024 * 1024), "2.0 GiB");
    }

    #[test]
    fn strip_ansi_removes_escape_sequences() {
        // Colour codes around text are removed, text kept.
        assert_eq!(strip_ansi("\u{1b}[31mred\u{1b}[0m text"), "red text");
        // Multi-line plain text is untouched.
        assert_eq!(strip_ansi("line1\nline2\n"), "line1\nline2\n");
        // Cursor/clear sequences are removed too.
        assert_eq!(strip_ansi("a\u{1b}[2Kb"), "ab");
        // A bare ESC without CSI doesn't eat following text.
        assert_eq!(strip_ansi("ok"), "ok");
    }

    #[test]
    fn resolve_history_handles_named_ranges_and_custom_windows() {
        // No `Duration::from_hours` on stable; same trade-off as in docker.rs.
        #[allow(clippy::duration_suboptimal_units)]
        let seed_window = Duration::from_secs(3600);
        let named = |range: &str| HistoryQuery {
            range: Some(range.to_string()),
            since_ms: None,
            until_ms: None,
        };
        let window = |since: u64, until: u64| HistoryQuery {
            range: None,
            since_ms: Some(since),
            until_ms: Some(until),
        };

        // 1h fits the raw retention → raw samples, full resolution.
        let w = resolve_history(&named("1h"), seed_window).unwrap();
        assert!(w.raw);
        assert_eq!(w.group_ms, 60_000);
        // 30d exceeds it → trend buckets, downsampled to 30 min groups.
        let w = resolve_history(&named("30d"), seed_window).unwrap();
        assert!(!w.raw);
        assert_eq!(w.group_ms, 1_800_000);
        assert!(w.since <= now_unix_ms() - 29 * 24 * 3_600_000);

        // A recent custom window is served raw, an older one from trends.
        let now = now_unix_ms();
        let w = resolve_history(&window(now - 600_000, now), seed_window).unwrap();
        assert!(w.raw);
        let w = resolve_history(&window(now - 86_400_000, now - 7_200_000), seed_window).unwrap();
        assert!(!w.raw);
        assert_eq!(w.until, now - 7_200_000);

        // Rejected: unknown name, inverted window, range+window mix, nothing.
        assert!(resolve_history(&named("2y"), seed_window).is_none());
        assert!(resolve_history(&window(50, 10), seed_window).is_none());
        let mut mixed = named("1h");
        mixed.since_ms = Some(0);
        mixed.until_ms = Some(10);
        assert!(resolve_history(&mixed, seed_window).is_none());
        assert!(
            resolve_history(
                &HistoryQuery {
                    range: None,
                    since_ms: None,
                    until_ms: None
                },
                seed_window
            )
            .is_none()
        );
    }

    #[test]
    fn group_for_window_targets_whole_buckets() {
        // Up to 24h: one point per 60s trend bucket.
        assert_eq!(group_for_window(3_600_000), 60_000);
        assert_eq!(group_for_window(86_400_000), 60_000);
        // 7d → 7min groups, 30d → 30min groups (≤ ~1440 points each).
        assert_eq!(group_for_window(7 * 86_400_000), 420_000);
        assert_eq!(group_for_window(30 * 86_400_000), 1_800_000);
        // Degenerate windows never yield a zero group.
        assert_eq!(group_for_window(0), 60_000);
    }

    #[test]
    fn fmt_age_scales_units_up_to_days() {
        assert_eq!(fmt_age_secs(0), "just now");
        assert_eq!(fmt_age_secs(1), "1s ago");
        assert_eq!(fmt_age_secs(59), "59s ago");
        assert_eq!(fmt_age_secs(60), "1m ago");
        assert_eq!(fmt_age_secs(3_599), "59m ago");
        // The header hint for a long-stuck collector must not read "4320m ago".
        assert_eq!(fmt_age_secs(3_600), "1h ago");
        assert_eq!(fmt_age_secs(86_399), "23h ago");
        assert_eq!(fmt_age_secs(3 * 86_400), "3d ago");
    }

    #[test]
    fn raw_group_caps_points_but_keeps_short_windows_exact() {
        // The raw path serves full-resolution samples; the group must cap what
        // a window can return even when `raw_retention_secs` is raised to days
        // (a 7d window at a 3s interval would otherwise be ~200k JSON points).
        for window_ms in [3_600_000, 86_400_000, 7 * 86_400_000] {
            let points = window_ms / raw_group_for_window(window_ms);
            assert!(
                points <= 1_441,
                "{window_ms}ms window may yield {points} raw points"
            );
        }
        // A short drill-down window's group stays below any realistic sample
        // interval, so its samples pass through at full resolution.
        assert!(raw_group_for_window(600_000) < 1_000);
        // Degenerate windows never yield a zero group.
        assert_eq!(raw_group_for_window(0), 1);
    }

    #[test]
    fn split_log_timestamp_extracts_daemon_prefix() {
        // The daemon's nanosecond timestamp is recognised and split off.
        assert_eq!(
            split_log_timestamp("2026-06-12T10:15:30.123456789Z hello world"),
            Some(("2026-06-12T10:15:30.123456789Z", "hello world"))
        );
        // Seconds-precision (no fractional part) works too.
        assert_eq!(
            split_log_timestamp("2026-06-12T10:15:30Z msg"),
            Some(("2026-06-12T10:15:30Z", "msg"))
        );
        // Lines without the prefix are left alone.
        assert_eq!(split_log_timestamp("plain log line"), None);
        assert_eq!(split_log_timestamp(""), None);
        // A first word that merely resembles a timestamp is rejected.
        assert_eq!(split_log_timestamp("2026-06-12T10:15:30 msg"), None);
        assert_eq!(split_log_timestamp("not-a-timestamp-atallZ msg"), None);
    }

    #[test]
    fn render_log_lines_wraps_timestamps_in_spans() {
        let html = render_log_lines("2026-06-12T10:15:30.5Z started\nplain line\n").into_string();
        // Timestamp lands in a span with a seconds-precision data-ts…
        assert!(html.contains(r#"<span class="log-ts" data-ts="2026-06-12T10:15:30Z">"#));
        assert!(html.contains("2026-06-12 10:15:30</span> started\n"));
        // …while prefix-less lines pass through untouched.
        assert!(html.contains("plain line\n"));
    }

    #[test]
    fn short_image_strips_registry() {
        assert_eq!(
            short_image("docker.io/library/nginx:latest"),
            "nginx:latest"
        );
        assert_eq!(short_image("redis:7"), "redis:7");
    }

    #[test]
    fn clamp_since_caps_the_lookback_window() {
        // No `Duration::from_hours` on stable; same trade-off as in docker.rs.
        #[allow(clippy::duration_suboptimal_units)]
        let window = Duration::from_secs(3600);
        let floor = now_unix_ms() - 3_600_000;
        // A zero/ancient client value is raised to the window floor…
        assert!(clamp_since(0, window) >= floor);
        // …while a recent value passes through unchanged.
        let recent = now_unix_ms() - 1_000;
        assert_eq!(clamp_since(recent, window), recent);
    }

    #[test]
    fn htmx_header_is_required_verbatim() {
        let mut headers = HeaderMap::new();
        assert!(!is_htmx_request(&headers));
        headers.insert("hx-request", "false".parse().unwrap());
        assert!(!is_htmx_request(&headers));
        headers.insert("hx-request", "true".parse().unwrap());
        assert!(is_htmx_request(&headers));
    }

    #[test]
    fn host_without_port_handles_ipv4_ipv6_and_bare_names() {
        assert_eq!(host_without_port("example.com:8080"), "example.com");
        assert_eq!(host_without_port("example.com"), "example.com");
        assert_eq!(host_without_port("127.0.0.1:8080"), "127.0.0.1");
        assert_eq!(host_without_port("[::1]:8080"), "::1");
        assert_eq!(host_without_port("[2001:db8::1]"), "2001:db8::1");
    }

    #[test]
    fn host_allowlist_permits_localhost_and_configured_names_only() {
        let allowed = normalize_allowed_hosts(&[" DockHost.lan:8080 ".to_string()]);
        let host = |value: &str| {
            let mut headers = HeaderMap::new();
            headers.insert(header::HOST, value.parse().unwrap());
            headers
        };

        // Disabled check lets anything through, even a missing Host header.
        assert!(host_allowed(&HeaderMap::new(), &[]));

        assert!(host_allowed(&host("dockhost.lan"), &allowed));
        assert!(host_allowed(&host("DockHost.lan:9000"), &allowed));
        assert!(host_allowed(&host("localhost:8080"), &allowed));
        assert!(host_allowed(&host("127.0.0.1"), &allowed));
        assert!(host_allowed(&host("[::1]:8080"), &allowed));

        assert!(!host_allowed(&host("attacker.example"), &allowed));
        assert!(!host_allowed(&HeaderMap::new(), &allowed));
    }

    #[test]
    fn container_counts_bucket_by_state_and_health() {
        let c = |state, health| ContainerMetrics {
            id: "x".to_string(),
            name: "c-x".to_string(),
            image: "img".to_string(),
            state,
            status: String::new(),
            health,
            stack: None,
            cpu_percent: None,
            mem_used: None,
            mem_limit: None,
            net_rx_bps: None,
            net_tx_bps: None,
            disk_read_bps: None,
            disk_write_bps: None,
            ports: Vec::new(),
        };
        let containers = [
            c(ContainerState::Running, HealthState::Healthy),
            c(ContainerState::Running, HealthState::Unhealthy),
            c(ContainerState::Exited, HealthState::None),
            c(ContainerState::Paused, HealthState::None),
            c(ContainerState::Created, HealthState::None),
        ];
        assert_eq!(
            ContainerCounts::of(&containers),
            ContainerCounts {
                total: 5,
                running: 2,
                exited: 1,
                other: 2,
                unhealthy: 1,
            }
        );
        assert_eq!(
            ContainerCounts::of(&[]),
            ContainerCounts {
                total: 0,
                running: 0,
                exited: 0,
                other: 0,
                unhealthy: 0,
            }
        );
    }

    #[test]
    fn stack_key_sorts_standalone_last() {
        let mut keys = vec![
            StackKey::Standalone,
            StackKey::Named("alpha"),
            StackKey::Named("beta"),
        ];
        keys.sort();
        assert_eq!(
            keys,
            vec![
                StackKey::Named("alpha"),
                StackKey::Named("beta"),
                StackKey::Standalone,
            ]
        );
    }
}
