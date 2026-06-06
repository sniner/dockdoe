//! Serializable data model for the dashboard.
//!
//! These types are the single source of truth: the web layer renders them to
//! HTML, and a future `--json` endpoint can serialize them as-is. Keep them
//! free of presentation concerns.

use serde::Serialize;

/// A full snapshot of host + container metrics at one point in time.
#[derive(Debug, Clone, Serialize)]
pub struct Dashboard {
    /// When this snapshot was collected, as Unix milliseconds.
    pub generated_at_unix_ms: u64,
    pub host: HostMetrics,
    pub containers: Vec<ContainerMetrics>,
}

/// htop-style metrics for the whole host.
#[derive(Debug, Clone, Serialize)]
pub struct HostMetrics {
    /// Overall CPU utilisation across all cores, in percent (0..=100).
    pub cpu_percent: f32,
    pub load_avg: LoadAverage,
    /// Total physical memory in bytes.
    pub mem_total: u64,
    /// Used physical memory in bytes (total minus available).
    pub mem_used: u64,
    /// Number of logical CPUs.
    pub cpu_count: usize,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct LoadAverage {
    pub one: f64,
    pub five: f64,
    pub fifteen: f64,
}

/// Per-container metrics and identity.
#[derive(Debug, Clone, Serialize)]
pub struct ContainerMetrics {
    /// Full container ID.
    pub id: String,
    /// Display name (leading slash stripped).
    pub name: String,
    pub image: String,
    pub state: ContainerState,
    /// Raw Docker status line, e.g. "Up 2 hours (healthy)".
    pub status: String,
    pub health: HealthState,
    /// Compose project this container belongs to, if any
    /// (`com.docker.compose.project` label).
    pub stack: Option<String>,
    /// CPU utilisation in percent; `None` until a second sample is available.
    pub cpu_percent: Option<f64>,
    /// Memory currently in use, in bytes (cache excluded).
    pub mem_used: Option<u64>,
    /// Memory limit in bytes, if the container has one.
    pub mem_limit: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ContainerState {
    Created,
    Running,
    Paused,
    Restarting,
    Exited,
    Removing,
    Dead,
    Stopping,
    Unknown,
}

/// Health as reported by Docker's healthcheck, if one is configured.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum HealthState {
    Healthy,
    Unhealthy,
    Starting,
    /// No healthcheck configured.
    None,
}
