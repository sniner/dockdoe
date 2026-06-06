//! Web layer: axum router, embedded assets, SSE streaming, and HTML rendering.
//!
//! Pages are rendered server-side with maud and kept live without a full reload
//! over a *single* SSE connection per page (`live.js`). That one stream carries
//! three named events — `header` and `containers` HTML fragments (re-using the
//! same render functions) and a `metrics` JSON point for the uPlot charts.
//! Charts are seeded from the store on first load. One connection per page
//! keeps us comfortably under the browser's per-host HTTP/1.1 connection limit.

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
use crate::docker::{Action, DockerHandle};
use crate::model::{ContainerMetrics, ContainerState, Dashboard, HealthState, HostMetrics};
use crate::store::{MetricPoint, Store};

/// Shared state for the web layer.
#[derive(Clone)]
pub struct AppState {
    /// Latest snapshot for the initial server-side render.
    pub shared: SharedDashboard,
    /// Live feed subscribed to by the SSE endpoints.
    pub snapshots: SnapshotTx,
    /// Store, for seeding charts with recent history.
    pub store: Store,
    /// Docker handle for lifecycle actions and logs.
    pub docker: DockerHandle,
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
        .route("/container/{id}", get(container_detail))
        .route("/stack/{name}", get(stack_detail))
        .route("/events", get(events_dashboard))
        .route("/events/container/{id}", get(events_container))
        .route("/events/stack/{name}", get(events_stack))
        .route("/api/container/{id}/logs", get(container_logs))
        .route("/api/stack/{name}/compose", get(stack_compose))
        .route(
            "/api/container/{id}/{action}",
            axum::routing::post(container_action),
        )
        .route(
            "/api/stack/{name}/{action}",
            axum::routing::post(stack_action),
        )
        .route("/assets/{*path}", get(asset))
        .with_state(state)
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
        Ok(text) => html! { (strip_ansi(&text)) },
        Err(err) => {
            tracing::warn!(%id, %err, "fetching logs failed");
            html! { span.muted { "Could not read logs: " (err) } }
        }
    }
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
        );
        return (StatusCode::NOT_FOUND, body).into_response();
    };

    let since = now_unix_ms().saturating_sub(duration_ms(state.seed_window));
    let store = state.store.clone();
    let seed_id = id.clone();
    let seed = tokio::task::spawn_blocking(move || store.recent_container_samples(&seed_id, since))
        .await
        .ok()
        .and_then(Result::ok)
        .unwrap_or_default();

    let live_url = format!("/events/container/{id}");
    shell(
        snapshot.as_ref(),
        container_detail_main(&container),
        &seed,
        &live_url,
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
        );
        return (StatusCode::NOT_FOUND, body).into_response();
    }

    // Stack charts stream live aggregates; no historical seed (raw samples
    // aren't keyed by stack), so they fill in over the view window.
    let live_url = format!("/events/stack/{name}");
    shell(
        snapshot.as_ref(),
        stack_detail_main(&name, &members),
        &[],
        &live_url,
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
            events.push(Ok(metrics_event(&MetricPoint {
                ts_ms: dash.generated_at_unix_ms,
                cpu_percent: c.cpu_percent.unwrap_or(0.0),
                mem_used: c.mem_used,
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
        let mut cpu = 0.0;
        let mut mem = 0u64;
        for c in dash
            .containers
            .iter()
            .filter(|c| c.stack.as_deref() == Some(name.as_str()))
        {
            cpu += c.cpu_percent.unwrap_or(0.0);
            mem += c.mem_used.unwrap_or(0);
        }
        futures_util::stream::iter([
            Ok(header_event(&dash)),
            Ok(metrics_event(&MetricPoint {
                ts_ms: dash.generated_at_unix_ms,
                cpu_percent: cpu,
                mem_used: Some(mem),
            })),
        ])
    });
    Sse::new(stream).keep_alive(KeepAlive::default())
}

/// The `header` event: the host header's inner HTML.
fn header_event(dash: &Dashboard) -> Event {
    Event::default()
        .event("header")
        .data(host_header_inner(&dash.host, dash.generated_at_unix_ms).into_string())
}

/// The `containers` event: the container table's HTML.
fn containers_event(dash: &Dashboard) -> Event {
    Event::default()
        .event("containers")
        .data(container_section(&dash.containers).into_string())
}

/// The `metrics` event: a metric point as JSON.
fn metrics_event(point: &MetricPoint) -> Event {
    let data = serde_json::to_string(point).unwrap_or_else(|_| "{}".to_string());
    Event::default().event("metrics").data(data)
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

/// Full HTML shell shared by every page: the live host header, the page's main
/// content, and the chart machinery. `metrics_url` is the SSE endpoint the
/// charts subscribe to; `seed` pre-fills them with history.
// `main_content` is moved in by builder convention — callers hand off a
// freshly-built fragment they no longer need.
#[allow(clippy::needless_pass_by_value)]
fn shell(
    snapshot: Option<&Dashboard>,
    main_content: Markup,
    seed: &[MetricPoint],
    live_url: &str,
) -> Markup {
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
            body {
                header.host id="host-header" {
                    @match snapshot {
                        Some(d) => (host_header_inner(&d.host, d.generated_at_unix_ms)),
                        None => a.brand href="/" { "Dock" span { "Doe" } },
                    }
                }
                main { (main_content) }
                script id="seed-data" type="application/json" data-live-url=(live_url) {
                    (maud::PreEscaped(seed_json))
                }
                script src="/assets/vendor/htmx.min.js" {}
                script src="/assets/vendor/uPlot.iife.min.js" {}
                script src="/assets/live.js" {}
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
                Some(d) => (container_section(&d.containers)),
                None => p.empty { "Collecting first metrics sample…" },
            }
        }
    };
    shell(snapshot, main, seed, "/events")
}

/// Markup for a pair of live charts (CPU + memory). Data comes from `live.js`,
/// which reads the seed and live URL off the page.
fn charts_section(cpu_title: &str, mem_title: &str) -> Markup {
    html! {
        section.charts {
            div.chart-card {
                span.chart-title { (cpu_title) }
                div id="chart-cpu" {}
            }
            div.chart-card {
                span.chart-title { (mem_title) }
                div id="chart-mem" {}
            }
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
                span.badge.(state_class(c.state)) { (state_label(c.state)) }
                (health_marker(c.health))
                @if let Some(stack) = &c.stack {
                    a.stack-pill href=(format!("/stack/{stack}")) { (stack) }
                }
            }
            (action_buttons(&c.id))
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
        (charts_section(&format!("{} · CPU", c.name), &format!("{} · Memory", c.name)))
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

/// Body of a stack detail page: aggregate charts, stack actions, and the
/// stack's containers.
fn stack_detail_main(name: &str, members: &[ContainerMetrics]) -> Markup {
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
        section.stack {
            table {
                thead {
                    tr {
                        th { "Container" }
                        th { "Image" }
                        th { "State" }
                        th.num { "CPU" }
                        th.num { "Memory" }
                        th.actions-col { "Actions" }
                    }
                }
                tbody {
                    @for c in members { (container_row(c)) }
                }
            }
        }
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

/// The inner content of the host header (everything HTMX swaps on each update).
fn host_header_inner(host: &HostMetrics, generated_at_unix_ms: u64) -> Markup {
    html! {
        a.brand href="/" { "Dock" span { "Doe" } }
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
                h2 {
                    @match key {
                        StackKey::Named(name) => a.stack-link href=(format!("/stack/{name}")) { (title) },
                        StackKey::Standalone => span { (title) },
                    }
                    " " span.count { "(" (members.len()) ")" }
                }
                table {
                    thead {
                        tr {
                            th { "Container" }
                            th { "Image" }
                            th { "State" }
                            th.num { "CPU" }
                            th.num { "Memory" }
                            th.actions-col { "Actions" }
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
            td.name { a href=(format!("/container/{}", c.id)) { (c.name) } }
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
