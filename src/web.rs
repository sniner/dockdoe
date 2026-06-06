//! Web layer: axum router, embedded assets, SSE streaming, and HTML rendering.
//!
//! The dashboard is rendered server-side with maud and kept live without a full
//! reload: HTMX's SSE extension swaps the maud-rendered header and container
//! fragments as new samples arrive (`/events`), while uPlot host charts are fed
//! JSON from a second stream (`/events/metrics`) and seeded from the store on
//! first load. Rendering stays in one place (these functions) — the SSE handlers
//! reuse the very same fragments.

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::Router;
use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use futures_util::{Stream, StreamExt};
use maud::{DOCTYPE, Markup, html};
use rust_embed::RustEmbed;
use tokio_stream::wrappers::BroadcastStream;

use crate::collector::{SharedDashboard, SnapshotTx};
use crate::model::{ContainerMetrics, ContainerState, Dashboard, HealthState, HostMetrics};
use crate::store::{HostPoint, Store};

/// Shared state for the web layer.
#[derive(Clone)]
pub struct AppState {
    /// Latest snapshot for the initial server-side render.
    pub shared: SharedDashboard,
    /// Live feed subscribed to by the SSE endpoints.
    pub snapshots: SnapshotTx,
    /// Store, for seeding charts with recent history.
    pub store: Store,
    /// How far back to seed charts on first load.
    pub seed_window: Duration,
}

#[derive(RustEmbed)]
#[folder = "assets/"]
struct Assets;

/// Build the application router.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/", get(dashboard))
        .route("/events", get(events_html))
        .route("/events/metrics", get(events_metrics))
        .route("/assets/{*path}", get(asset))
        .with_state(state)
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

/// Render the dashboard from the latest snapshot, seeding charts from history.
async fn dashboard(State(state): State<AppState>) -> Markup {
    let snapshot = state.shared.read().ok().and_then(|guard| guard.clone());
    let since = now_unix_ms().saturating_sub(duration_ms(state.seed_window));
    let store = state.store.clone();
    let seed = tokio::task::spawn_blocking(move || store.recent_host_samples(since))
        .await
        .ok()
        .and_then(Result::ok)
        .unwrap_or_default();
    page(snapshot.as_ref(), &seed)
}

/// SSE stream of HTML fragments for HTMX: a `header` and a `containers` event
/// per snapshot. The current snapshot is sent immediately so a freshly loaded
/// page doesn't wait a full interval for its first live update.
async fn events_html(
    State(state): State<AppState>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let stream = dashboard_stream(&state).flat_map(|dash| {
        let header = Event::default()
            .event("header")
            .data(host_header_inner(&dash.host, dash.generated_at_unix_ms).into_string());
        let containers = Event::default()
            .event("containers")
            .data(container_section(&dash.containers).into_string());
        futures_util::stream::iter([Ok(header), Ok(containers)])
    });
    Sse::new(stream).keep_alive(KeepAlive::default())
}

/// SSE stream of host metric points (JSON) for the uPlot charts.
async fn events_metrics(
    State(state): State<AppState>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let stream = dashboard_stream(&state).map(|dash| {
        let point = HostPoint {
            ts_ms: dash.generated_at_unix_ms,
            cpu_percent: f64::from(dash.host.cpu_percent),
            mem_used: Some(dash.host.mem_used),
        };
        let data = serde_json::to_string(&point).unwrap_or_else(|_| "{}".to_string());
        Ok(Event::default().data(data))
    });
    Sse::new(stream).keep_alive(KeepAlive::default())
}

/// A stream of snapshots: the current one first (if any), then the live feed.
/// Broadcast lag errors are dropped — a missed intermediate frame is harmless
/// for a live view.
fn dashboard_stream(state: &AppState) -> impl Stream<Item = std::sync::Arc<Dashboard>> + use<> {
    use std::sync::Arc;
    let current = state
        .shared
        .read()
        .ok()
        .and_then(|guard| guard.clone())
        .map(Arc::new);
    let live =
        BroadcastStream::new(state.snapshots.subscribe()).filter_map(|res| async move { res.ok() });
    futures_util::stream::iter(current).chain(live)
}

fn page(snapshot: Option<&Dashboard>, seed: &[HostPoint]) -> Markup {
    let seed_json = serde_json::to_string(seed).unwrap_or_else(|_| "[]".to_string());
    html! {
        (DOCTYPE)
        html lang="en" {
            head {
                meta charset="utf-8";
                meta name="viewport" content="width=device-width, initial-scale=1";
                title { "DockDoe" }
                link rel="stylesheet" href="/assets/vendor/uPlot.min.css";
                link rel="stylesheet" href="/assets/dockdoe.css";
            }
            body hx-ext="sse" sse-connect="/events" {
                header.host id="host-header" sse-swap="header" {
                    @match snapshot {
                        Some(d) => (host_header_inner(&d.host, d.generated_at_unix_ms)),
                        None => span.brand { "Dock" span { "Doe" } },
                    }
                }
                main {
                    (charts_section())
                    div id="containers" sse-swap="containers" {
                        @match snapshot {
                            Some(d) => (container_section(&d.containers)),
                            None => p.empty { "Collecting first metrics sample…" },
                        }
                    }
                }
                script id="seed-data" type="application/json" { (maud::PreEscaped(seed_json)) }
                script src="/assets/vendor/htmx.min.js" {}
                script src="/assets/vendor/sse.js" {}
                script src="/assets/vendor/uPlot.iife.min.js" {}
                script src="/assets/charts.js" {}
            }
        }
    }
}

/// Static markup for the live host charts. Data is supplied by `charts.js`.
fn charts_section() -> Markup {
    html! {
        section.charts {
            div.chart-card {
                span.chart-title { "Host CPU" }
                div id="chart-cpu" {}
            }
            div.chart-card {
                span.chart-title { "Host Memory" }
                div id="chart-mem" {}
            }
        }
    }
}

/// The inner content of the host header (everything HTMX swaps on each update).
fn host_header_inner(host: &HostMetrics, generated_at_unix_ms: u64) -> Markup {
    html! {
        span.brand { "Dock" span { "Doe" } }
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
        span.spacer {}
        span.generated { "updated " (fmt_age(generated_at_unix_ms)) }
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
fn container_section(containers: &[ContainerMetrics]) -> Markup {
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
                h2 { (title) " " span.count { "(" (members.len()) ")" } }
                table {
                    thead {
                        tr {
                            th { "Container" }
                            th { "Image" }
                            th { "State" }
                            th.num { "CPU" }
                            th.num { "Memory" }
                        }
                    }
                    tbody {
                        @for c in members { (container_row(c)) }
                    }
                }
            }
        }
    }
}

fn container_row(c: &ContainerMetrics) -> Markup {
    html! {
        tr {
            td.name { (c.name) }
            td.image { (short_image(&c.image)) }
            td {
                span.badge.(state_class(c.state)) { (state_label(c.state)) }
                (health_marker(c.health))
            }
            td.num {
                @match c.cpu_percent {
                    Some(pct) => {
                        (format!("{pct:.1}%"))
                        (bar(pct, 100.0))
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
    fn short_image_strips_registry() {
        assert_eq!(
            short_image("docker.io/library/nginx:latest"),
            "nginx:latest"
        );
        assert_eq!(short_image("redis:7"), "redis:7");
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
