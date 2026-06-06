//! Web layer: axum router, embedded assets, and HTML rendering.
//!
//! Milestone 1 renders a static dashboard snapshot. The render functions take
//! the shared model and produce HTML; later milestones add SSE streaming and
//! HTMX partials on top of the same model.

use std::collections::BTreeMap;

use axum::Router;
use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use maud::{DOCTYPE, Markup, html};
use rust_embed::RustEmbed;

use crate::collector::SharedDashboard;
use crate::model::{ContainerMetrics, ContainerState, Dashboard, HealthState, HostMetrics};

#[derive(RustEmbed)]
#[folder = "assets/"]
struct Assets;

/// Build the application router.
pub fn router(shared: SharedDashboard) -> Router {
    Router::new()
        .route("/", get(dashboard))
        .route("/assets/{*path}", get(asset))
        .with_state(shared)
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

/// Render the dashboard from the latest snapshot.
async fn dashboard(State(shared): State<SharedDashboard>) -> Markup {
    let snapshot = shared.read().ok().and_then(|guard| guard.clone());
    page(snapshot.as_ref())
}

fn page(snapshot: Option<&Dashboard>) -> Markup {
    html! {
        (DOCTYPE)
        html lang="en" {
            head {
                meta charset="utf-8";
                meta name="viewport" content="width=device-width, initial-scale=1";
                title { "DockDoe" }
                link rel="stylesheet" href="/assets/dockdoe.css";
            }
            body {
                @match snapshot {
                    Some(dashboard) => {
                        (host_header(&dashboard.host, dashboard.generated_at_unix_ms))
                        main { (container_section(&dashboard.containers)) }
                    }
                    None => {
                        header.host { span.brand { "Dock" span { "Doe" } } }
                        main { p.empty { "Collecting first metrics sample…" } }
                    }
                }
            }
        }
    }
}

fn host_header(host: &HostMetrics, generated_at_unix_ms: u64) -> Markup {
    html! {
        header.host {
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
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0);
    let secs = now.saturating_sub(unix_ms) / 1000;
    match secs {
        0 => "just now".to_string(),
        1 => "1s ago".to_string(),
        s if s < 60 => format!("{s}s ago"),
        s => format!("{}m ago", s / 60),
    }
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
