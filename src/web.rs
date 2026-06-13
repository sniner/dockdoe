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

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::extract::{Query, Request, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use futures_util::{Stream, StreamExt};
use maud::{DOCTYPE, Markup, html};
use rust_embed::RustEmbed;

use crate::collector::SharedDashboard;
use crate::docker::{Action, DockerHandle};
use crate::model::{ContainerMetrics, ContainerState, Dashboard, HealthState, Port};
use crate::store::{HistoryPoint, MetricPoint, Store};

/// How often the live SSE streams poll the latest snapshot.
const SNAPSHOT_POLL: Duration = Duration::from_secs(1);

/// Shared state for the web layer.
#[derive(Clone)]
pub struct AppState {
    /// The latest snapshot — single source of truth for both the initial render
    /// and the live SSE streams.
    pub shared: SharedDashboard,
    /// Store, for seeding charts with recent history.
    pub store: Store,
    /// Docker handle for lifecycle actions and logs.
    pub docker: DockerHandle,
    /// How far back to seed charts on first load.
    pub seed_window: Duration,
    /// Hostnames the UI may be addressed as (normalized: lowercase, no port).
    /// Empty disables the Host check; localhost forms are always allowed.
    pub allowed_hosts: Arc<[String]>,
}

#[derive(RustEmbed)]
#[folder = "assets/"]
struct Assets;

/// Build the application router.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/", get(dashboard))
        .route("/container/{id}", get(container_detail))
        .route("/stack/{name}", get(stack_detail))
        .route("/events", get(events_dashboard))
        .route("/events/container/{id}", get(events_container))
        .route("/events/stack/{name}", get(events_stack))
        .route("/api/container/{id}/logs", get(container_logs))
        .route("/api/stack/{name}/compose", get(stack_compose))
        .route("/api/metrics/host", get(metrics_host))
        .route("/api/metrics/container/{id}", get(metrics_container))
        .route("/api/metrics/stack/{name}", get(metrics_stack))
        .route("/api/history/host", get(history_host))
        .route("/api/history/container/{id}", get(history_container))
        .route("/api/history/stack/{name}", get(history_stack))
        .route(
            "/api/container/{id}/{action}",
            axum::routing::post(container_action).layer(middleware::from_fn(require_htmx)),
        )
        .route(
            "/api/stack/{name}/{action}",
            axum::routing::post(stack_action).layer(middleware::from_fn(require_htmx)),
        )
        .route("/assets/{*path}", get(asset))
        .layer(middleware::from_fn_with_state(state.clone(), check_host))
        .with_state(state)
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
    axum::extract::Path((id, action)): axum::extract::Path<(String, String)>,
) -> Response {
    let Some(action) = Action::parse(&action) else {
        return (StatusCode::BAD_REQUEST, "unknown action").into_response();
    };
    match state.docker.apply(&id, action).await {
        Ok(()) => {
            tracing::info!(%id, ?action, "applied container action");
            // The next collector cycle refreshes state; echo the buttons back.
            action_buttons(&id).into_response()
        }
        Err(err) => {
            tracing::warn!(%id, ?action, %err, "container action failed");
            (StatusCode::BAD_GATEWAY, format!("action failed: {err}")).into_response()
        }
    }
}

/// Apply an action to every container in a stack, then echo the stack's action
/// buttons back for HTMX to swap.
async fn stack_action(
    State(state): State<AppState>,
    axum::extract::Path((name, action)): axum::extract::Path<(String, String)>,
) -> Response {
    let Some(action) = Action::parse(&action) else {
        return (StatusCode::BAD_REQUEST, "unknown action").into_response();
    };

    // Orchestrate dependency-aware: the docker layer reads the compose
    // depends_on labels and starts/stops members in the right order.
    match state.docker.stack_action(&name, action).await {
        Ok(outcome) => {
            tracing::info!(
                stack = %name, ?action,
                total = outcome.total, failed = outcome.failed,
                "applied stack action"
            );
        }
        Err(err) => {
            tracing::warn!(stack = %name, ?action, %err, "stack action failed");
            return (
                StatusCode::BAD_GATEWAY,
                format!("stack action failed: {err}"),
            )
                .into_response();
        }
    }
    stack_action_buttons(&name).into_response()
}

/// The current snapshot, cloned out of the shared lock.
fn current_snapshot(state: &AppState) -> Option<Dashboard> {
    state.shared.read().ok().and_then(|guard| guard.clone())
}

/// Number of log lines to tail for the logs panel.
const LOG_TAIL_LINES: u32 = 200;

/// Return the tail of a container's logs as an HTML fragment for the logs panel.
async fn container_logs(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Markup {
    match state.docker.logs_tail(&id, LOG_TAIL_LINES).await {
        Ok(text) if text.trim().is_empty() => html! { span.muted { "(no logs)" } },
        Ok(text) => render_log_lines(&strip_ansi(&text)),
        Err(err) => {
            tracing::warn!(%id, %err, "fetching logs failed");
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
    axum::extract::Path(name): axum::extract::Path<String>,
) -> Markup {
    let paths = match state.docker.compose_config_files(&name).await {
        Ok(paths) => paths,
        Err(err) => {
            tracing::warn!(stack = %name, %err, "resolving compose files failed");
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

/// JSON backfill for the host charts: raw points since `since_ms`. `live.js`
/// calls this before (re)opening the SSE stream to fill the gap that built up
/// while the page was hidden, in the bfcache, or suspended.
async fn metrics_host(
    State(state): State<AppState>,
    Query(q): Query<SinceQuery>,
) -> Json<Vec<MetricPoint>> {
    let since = clamp_since(q.since_ms, state.seed_window);
    let store = state.store.clone();
    Json(fetch_points(move || store.recent_host_samples(since)).await)
}

/// JSON backfill for a container detail page's charts.
async fn metrics_container(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
    Query(q): Query<SinceQuery>,
) -> Json<Vec<MetricPoint>> {
    let since = clamp_since(q.since_ms, state.seed_window);
    let store = state.store.clone();
    Json(fetch_points(move || store.recent_container_samples(&id, since)).await)
}

/// JSON backfill for a stack detail page's charts (trend-based, like the seed).
async fn metrics_stack(
    State(state): State<AppState>,
    axum::extract::Path(name): axum::extract::Path<String>,
    Query(q): Query<SinceQuery>,
) -> Json<Vec<MetricPoint>> {
    let since = clamp_since(q.since_ms, state.seed_window);
    let store = state.store.clone();
    Json(fetch_points(move || store.recent_stack_trends(&name, since)).await)
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

const INVALID_RANGE: (StatusCode, &str) = (
    StatusCode::BAD_REQUEST,
    "invalid range: pass ?range=1h|6h|24h|7d|30d or ?since_ms=&until_ms=",
);

/// History for the host charts: median plus min–max envelope over the window.
async fn history_host(State(state): State<AppState>, Query(q): Query<HistoryQuery>) -> Response {
    let Some(w) = resolve_history(&q, state.seed_window) else {
        return INVALID_RANGE.into_response();
    };
    let store = state.store.clone();
    let points = if w.raw {
        fetch_points(move || {
            let samples = store.recent_host_samples(w.since)?;
            Ok(raw_history(&samples, w.until))
        })
        .await
    } else {
        fetch_points(move || store.history_host(w.since, w.until, w.group_ms)).await
    };
    Json(points).into_response()
}

/// History for one container's charts.
async fn history_container(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
    Query(q): Query<HistoryQuery>,
) -> Response {
    let Some(w) = resolve_history(&q, state.seed_window) else {
        return INVALID_RANGE.into_response();
    };
    let store = state.store.clone();
    let points = if w.raw {
        fetch_points(move || {
            let samples = store.recent_container_samples(&id, w.since)?;
            Ok(raw_history(&samples, w.until))
        })
        .await
    } else {
        fetch_points(move || store.history_container(&id, w.since, w.until, w.group_ms)).await
    };
    Json(points).into_response()
}

/// History for a stack's aggregate charts. Stacks have no raw series, so all
/// windows come from the trend buckets.
async fn history_stack(
    State(state): State<AppState>,
    axum::extract::Path(name): axum::extract::Path<String>,
    Query(q): Query<HistoryQuery>,
) -> Response {
    let Some(w) = resolve_history(&q, state.seed_window) else {
        return INVALID_RANGE.into_response();
    };
    let store = state.store.clone();
    Json(fetch_points(move || store.history_stack(&name, w.since, w.until, w.group_ms)).await)
        .into_response()
}

/// Lift raw samples into the history shape, dropping anything past the
/// window's upper bound (the raw queries only filter on `since`).
fn raw_history(samples: &[MetricPoint], until_ms: u64) -> Vec<HistoryPoint> {
    samples
        .iter()
        .filter(|p| p.ts_ms <= until_ms)
        .map(HistoryPoint::from_raw)
        .collect()
}

/// Render the dashboard from the latest snapshot, seeding charts from history.
async fn dashboard(State(state): State<AppState>) -> Markup {
    let snapshot = current_snapshot(&state);
    let since = now_unix_ms().saturating_sub(duration_ms(state.seed_window));
    let store = state.store.clone();
    let seed = fetch_points(move || store.recent_host_samples(since)).await;
    dashboard_page(snapshot.as_ref(), &seed)
}

/// Detail page for a single container.
async fn container_detail(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Response {
    let snapshot = current_snapshot(&state);
    let Some(container) = snapshot
        .as_ref()
        .and_then(|d| d.containers.iter().find(|c| c.id == id).cloned())
    else {
        let body = shell(
            snapshot.as_ref(),
            html! { p.empty { "Container not found." } },
            &[],
            "/events",
            "/api/metrics/host",
        );
        return (StatusCode::NOT_FOUND, body).into_response();
    };

    let since = now_unix_ms().saturating_sub(duration_ms(state.seed_window));
    let store = state.store.clone();
    let seed_id = id.clone();
    let seed = fetch_points(move || store.recent_container_samples(&seed_id, since)).await;

    let live_url = format!("/events/container/{id}");
    let backfill_url = format!("/api/metrics/container/{id}");
    shell(
        snapshot.as_ref(),
        container_detail_main(&container),
        &seed,
        &live_url,
        &backfill_url,
    )
    .into_response()
}

/// Detail page for a compose stack.
async fn stack_detail(
    State(state): State<AppState>,
    axum::extract::Path(name): axum::extract::Path<String>,
) -> Response {
    let snapshot = current_snapshot(&state);
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
            snapshot.as_ref(),
            html! { p.empty { "Stack not found." } },
            &[],
            "/events",
            "/api/metrics/host",
        );
        return (StatusCode::NOT_FOUND, body).into_response();
    }

    // Stack charts stream live aggregates. Raw samples aren't keyed by stack,
    // so we seed from trends: the sum of member medians per bucket, which lines
    // up with the live aggregate (sum of current member values).
    let since = now_unix_ms().saturating_sub(duration_ms(state.seed_window));
    let store = state.store.clone();
    let seed_name = name.clone();
    let seed = fetch_points(move || store.recent_stack_trends(&seed_name, since)).await;

    // Members are non-empty here, so a snapshot exists; 1 is just a fallback.
    let cpu_count = snapshot.as_ref().map_or(1, |d| d.host.cpu_count);
    let live_url = format!("/events/stack/{name}");
    let backfill_url = format!("/api/metrics/stack/{name}");
    shell(
        snapshot.as_ref(),
        stack_detail_main(&name, &members, cpu_count),
        &seed,
        &live_url,
        &backfill_url,
    )
    .into_response()
}

/// Single live stream for the dashboard: `header` + `containers` HTML fragments
/// and a host `metrics` point per snapshot. One SSE connection per page keeps us
/// well under the browser's per-host connection limit.
async fn events_dashboard(
    State(state): State<AppState>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let stream = dashboard_stream(&state).flat_map(|dash| {
        let point = MetricPoint {
            ts_ms: dash.generated_at_unix_ms,
            cpu_percent: f64::from(dash.host.cpu_percent),
            mem_used: Some(dash.host.mem_used),
            // The host series has no per-container I/O.
            net_rx: None,
            net_tx: None,
            disk_read: None,
            disk_write: None,
        };
        futures_util::stream::iter([
            Ok(header_event(&dash)),
            Ok(containers_event(&dash)),
            Ok(metrics_event(&point)),
        ])
    });
    Sse::new(stream).keep_alive(KeepAlive::default())
}

/// Single live stream for a container detail page: `header` plus the
/// container's `metrics` point.
async fn events_container(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let stream = dashboard_stream(&state).flat_map(move |dash| {
        let mut events = vec![Ok(header_event(&dash))];
        if let Some(c) = dash.containers.iter().find(|c| c.id == id) {
            events.push(Ok(detail_event(c)));
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
    Sse::new(stream).keep_alive(KeepAlive::default())
}

/// Single live stream for a stack detail page: `header` plus an aggregate
/// `metrics` point (CPU and memory summed across the stack's containers).
async fn events_stack(
    State(state): State<AppState>,
    axum::extract::Path(name): axum::extract::Path<String>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let stream = dashboard_stream(&state).flat_map(move |dash| {
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
            .data(stack_members_table(&members, dash.host.cpu_count).into_string());
        futures_util::stream::iter([
            Ok(header_event(&dash)),
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
    Sse::new(stream).keep_alive(KeepAlive::default())
}

/// The `detail` event: a container detail page's live region (state + facts).
fn detail_event(c: &ContainerMetrics) -> Event {
    Event::default()
        .event("detail")
        .data(container_detail_live(c).into_string())
}

/// The `header` event: the host header's inner HTML.
fn header_event(dash: &Dashboard) -> Event {
    Event::default()
        .event("header")
        .data(host_header_inner(dash).into_string())
}

/// The `containers` event: the container table's HTML.
fn containers_event(dash: &Dashboard) -> Event {
    Event::default()
        .event("containers")
        .data(container_section(&dash.containers, dash.host.cpu_count).into_string())
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
fn dashboard_stream(state: &AppState) -> impl Stream<Item = Arc<Dashboard>> + use<> {
    let shared = Arc::clone(&state.shared);
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
#[allow(clippy::needless_pass_by_value)]
fn shell(
    snapshot: Option<&Dashboard>,
    main_content: Markup,
    seed: &[MetricPoint],
    live_url: &str,
    backfill_url: &str,
) -> Markup {
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
                link rel="stylesheet" href="/assets/dockdoe.css";
            }
            body {
                header.host id="host-header" {
                    @match snapshot {
                        Some(d) => (host_header_inner(d)),
                        None => (brand()),
                    }
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
                            div.history-ranges id="history-ranges" {
                                button type="button" data-range="1h" { "1h" }
                                button type="button" data-range="6h" { "6h" }
                                button type="button" data-range="24h" { "24h" }
                                button type="button" data-range="7d" { "7d" }
                                button type="button" data-range="30d" { "30d" }
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
            }
        }
    }
}

/// The dashboard body: host charts plus the live container table.
fn dashboard_page(snapshot: Option<&Dashboard>, seed: &[MetricPoint]) -> Markup {
    let main = html! {
        (charts_section("Host CPU", "Host Memory"))
        div id="containers" {
            @match snapshot {
                Some(d) => (container_section(&d.containers, d.host.cpu_count)),
                None => p.empty { "Collecting first metrics sample…" },
            }
        }
    };
    shell(snapshot, main, seed, "/events", "/api/metrics/host")
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
/// CPU/memory charts. Deliberately live-only — no history-expand button yet, so
/// the underlying I/O trends accumulate for a future history view without one.
fn io_charts_section() -> Markup {
    html! {
        section.charts {
            (io_chart_card("Network", "net", "rx", "tx"))
            (io_chart_card("Disk I/O", "disk", "read", "write"))
        }
    }
}

/// One dual-line I/O chart card: a title, a two-key colour legend, a hover
/// readout, and the uPlot mount point (`chart-net` / `chart-disk`).
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
            }
            div id=(format!("chart-{key}")) {}
        }
    }
}

/// Body of a single-container detail page.
fn container_detail_main(c: &ContainerMetrics) -> Markup {
    html! {
        section.detail-head {
            div.detail-title {
                a.back href="/" { "← Dashboard" }
                h1 { (c.name) }
                @if let Some(stack) = &c.stack {
                    a.stack-pill href=(format!("/stack/{stack}")) { (stack) }
                }
            }
            (action_buttons(&c.id))
        }
        // Live region: swapped wholesale by the `detail` SSE event so state and
        // facts track reality. The charts below are left untouched.
        div id="detail-live" { (container_detail_live(c)) }
        (charts_section(&format!("{} · CPU", c.name), &format!("{} · Memory", c.name)))
        (io_charts_section())
        section.panel {
            div.panel-head {
                h3 { "Logs " span.count { "(last " (LOG_TAIL_LINES) " lines)" } }
                button.refresh type="button"
                    hx-get=(format!("/api/container/{}/logs", c.id))
                    hx-target="#logs" hx-swap="innerHTML" { "↻ Refresh" }
            }
            pre.logs id="logs"
                hx-get=(format!("/api/container/{}/logs", c.id))
                hx-trigger="load" hx-swap="innerHTML" { "Loading logs…" }
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
fn stack_members_table(members: &[&ContainerMetrics], cpu_count: usize) -> Markup {
    html! {
        section.stack {
            table {
                (container_table_head())
                tbody {
                    @for c in members { (container_row(c, cpu_count)) }
                }
            }
        }
    }
}

/// The live-updating part of a container detail page: state badge, health, and
/// the facts grid. Re-rendered and pushed via the `detail` SSE event.
fn container_detail_live(c: &ContainerMetrics) -> Markup {
    html! {
        div.status-line {
            span.badge.(state_class(c.state)) { (state_label(c.state)) }
            (health_marker(c.health))
            (port_pills(&c.ports))
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
fn stack_detail_main(name: &str, members: &[ContainerMetrics], cpu_count: usize) -> Markup {
    html! {
        section.detail-head {
            div.detail-title {
                a.back href="/" { "← Dashboard" }
                h1 { (name) }
                span.count { "(" (members.len()) ")" }
            }
            (stack_action_buttons(name))
        }
        (charts_section(
            &format!("{name} · CPU (sum)"),
            &format!("{name} · Memory (sum)"),
        ))
        (io_charts_section())
        // Live region: the `containers` SSE event swaps the member table so its
        // states/metrics track reality.
        div id="containers" { (stack_members_table(&members.iter().collect::<Vec<_>>(), cpu_count)) }
        section.panel {
            div.panel-head {
                h3 { "compose.yml" }
                button.refresh type="button"
                    hx-get=(format!("/api/stack/{name}/compose"))
                    hx-target="#compose" hx-swap="innerHTML" { "↻ Refresh" }
            }
            div id="compose"
                hx-get=(format!("/api/stack/{name}/compose"))
                hx-trigger="load" hx-swap="innerHTML" { "Loading…" }
        }
    }
}

/// Start/stop/restart-all buttons for a whole stack.
fn stack_action_buttons(name: &str) -> Markup {
    html! {
        span.actions {
            button.act.start type="button"
                hx-post=(format!("/api/stack/{name}/start"))
                hx-target="closest .actions" hx-swap="outerHTML"
                title="Start all" { "▶" }
            button.act.restart type="button"
                hx-post=(format!("/api/stack/{name}/restart"))
                hx-target="closest .actions" hx-swap="outerHTML"
                hx-confirm=(format!("Restart all containers in {name}?"))
                title="Restart all" { "⟳" }
            button.act.stop type="button"
                hx-post=(format!("/api/stack/{name}/stop"))
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

/// The inner content of the host header (everything HTMX swaps on each update).
fn host_header_inner(dash: &Dashboard) -> Markup {
    let host = &dash.host;
    html! {
        (brand())
        (metric("CPU", &format!("{:.1}%", host.cpu_percent)))
        (metric(
            "Load 1/5/15",
            &format!(
                "{:.2} {:.2} {:.2}",
                host.load_avg.one, host.load_avg.five, host.load_avg.fifteen
            ),
        ))
        (metric(
            "Memory",
            &format!("{} / {}", fmt_bytes(host.mem_used), fmt_bytes(host.mem_total)),
        ))
        (metric("CPUs", &host.cpu_count.to_string()))
        (container_counts_metric(&ContainerCounts::of(&dash.containers)))
        span.spacer {}
        span.generated { "updated " (fmt_age(dash.generated_at_unix_ms)) }
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

fn metric(label: &str, value: &str) -> Markup {
    html! {
        div.metric {
            span.label { (label) }
            span.value { (value) }
        }
    }
}

/// Render all containers, grouped by compose stack. Standalone containers
/// (no compose project) are grouped last under "Standalone".
fn container_section(containers: &[ContainerMetrics], cpu_count: usize) -> Markup {
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
                        StackKey::Named(name) => a.stack-link href=(format!("/stack/{name}")) { (title) },
                        StackKey::Standalone => span { (title) },
                    }
                    " " span.count { "(" (members.len()) ")" }
                }
                table {
                    (container_table_head())
                    tbody {
                        @for c in members { (container_row(c, cpu_count)) }
                    }
                }
            }
        }
    }
}

/// One container table row. `cpu_count` scales the CPU bar: container CPU% is
/// per-core cumulative (a busy 4-core container reads 400%), so full bar =
/// the whole host, and the bar matches what the host CPU chart would show.
fn container_row(c: &ContainerMetrics, cpu_count: usize) -> Markup {
    html! {
        tr {
            td.name { a href=(format!("/container/{}", c.id)) { (c.name) } }
            td.image { (short_image(&c.image)) }
            td {
                span.badge.(state_class(c.state)) { (state_label(c.state)) }
                (health_marker(c.health))
                (port_chips(&c.ports))
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
            td.actions-cell { (action_buttons(&c.id)) }
        }
    }
}

/// Start/stop/restart buttons for one container. Stateless so it can be reused
/// verbatim in the row and as the HTMX response after an action. Destructive
/// actions ask for confirmation.
fn action_buttons(id: &str) -> Markup {
    html! {
        span.actions {
            button.act.start type="button"
                hx-post=(format!("/api/container/{id}/start"))
                hx-target="closest .actions" hx-swap="outerHTML"
                title="Start" { "▶" }
            button.act.restart type="button"
                hx-post=(format!("/api/container/{id}/restart"))
                hx-target="closest .actions" hx-swap="outerHTML"
                hx-confirm="Restart this container?"
                title="Restart" { "⟳" }
            button.act.stop type="button"
                hx-post=(format!("/api/container/{id}/stop"))
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
fn port_pills(ports: &[Port]) -> Markup {
    if ports.is_empty() {
        return html! {};
    }
    html! {
        span.ports {
            @for p in ports {
                @match p.public {
                    Some(_) => (port_link(p, true)),
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
fn port_chips(ports: &[Port]) -> Markup {
    const SHOWN: usize = 2;
    let published: Vec<&Port> = ports.iter().filter(|p| p.public.is_some()).collect();
    if published.is_empty() {
        return html! {};
    }
    html! {
        span.ports {
            @for p in published.iter().take(SHOWN) { (port_link(p, false)) }
            @if published.len() > SHOWN {
                span.port-more title=(overflow_list(&published[SHOWN..])) {
                    "+" (published.len() - SHOWN)
                }
            }
        }
    }
}

/// A blue, clickable pill for one published port. `full` adds the
/// "→ container-port" detail used on the container page; the dense tables show
/// only the host port. The href targets `localhost` as a sane default for the
/// common "browsing from the Docker host" case and is rewritten to the actual
/// browsing host by live.js, the only place the real hostname is known.
fn port_link(p: &Port, full: bool) -> Markup {
    let public = p.public.expect("port_link called on an unpublished port");
    html! {
        a.port-pill data-port=(public) target="_blank" rel="noopener"
            href=(format!("http://localhost:{public}"))
            title=(format!("host {public} → container {}/{}", p.private, p.proto)) {
            ":" (public)
            @if full { " → " (p.private) }
            (proto_suffix(&p.proto))
        }
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
                .map(|public| format!(":{public}{}", proto_suffix(&p.proto)))
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

fn state_class(state: ContainerState) -> &'static str {
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

fn state_label(state: ContainerState) -> &'static str {
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

/// Render how long ago a Unix-ms timestamp was, relative to now.
fn fmt_age(unix_ms: u64) -> String {
    let secs = now_unix_ms().saturating_sub(unix_ms) / 1000;
    match secs {
        0 => "just now".to_string(),
        1 => "1s ago".to_string(),
        s if s < 60 => format!("{s}s ago"),
        s => format!("{}m ago", s / 60),
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

#[cfg(test)]
mod tests {
    use super::*;

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
