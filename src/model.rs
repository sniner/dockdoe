//! Serializable data model for the dashboard.
//!
//! These types are the single source of truth: the web layer renders them to
//! HTML, and the snapshot API serializes them as-is for a federation hub,
//! which reads them back with the same types. Keep them free of presentation
//! concerns — and keep changes ADDITIVE: the payload is a compatibility
//! contract between DockDoe versions. New fields need `#[serde(default)]`,
//! new enum variants come before the `#[serde(other)]` fallback, and nothing
//! gets renamed or removed.

use serde::{Deserialize, Serialize};

/// A full snapshot of a host's container metrics at one point in time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Dashboard {
    /// When this snapshot was collected, as Unix milliseconds.
    pub generated_at_unix_ms: u64,
    /// Number of logical CPUs on this Docker host, used to scale the container
    /// CPU bars (a full bar is the whole host). Reported by the daemon.
    pub cpu_count: usize,
    pub containers: Vec<ContainerMetrics>,
}

/// Per-container metrics and identity.
#[derive(Debug, Clone, Serialize, Deserialize)]
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
    /// Network receive rate in bytes/second, summed over all interfaces;
    /// `None` until a second sample gives a delta to rate against.
    pub net_rx_bps: Option<f64>,
    /// Network transmit rate in bytes/second; `None` until the second sample.
    pub net_tx_bps: Option<f64>,
    /// Block-device read rate in bytes/second; `None` until the second sample
    /// (or when the runtime reports no block-I/O stats).
    pub disk_read_bps: Option<f64>,
    /// Block-device write rate in bytes/second; `None` until the second sample.
    pub disk_write_bps: Option<f64>,
    /// Network ports the container exposes (published first, then internal),
    /// de-duplicated across host IP families.
    pub ports: Vec<Port>,
}

/// A network port a container exposes. When `public` is set the port is
/// published to the host and reachable from outside; otherwise it is only
/// exposed inside Docker's network.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Port {
    /// Port inside the container.
    pub private: u16,
    /// Host port, if this mapping is published to the host.
    pub public: Option<u16>,
    /// Transport protocol: "tcp", "udp", or "sctp".
    pub proto: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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
    /// Also the deserialization fallback, so a hub on an older DockDoe
    /// tolerates states a newer node may report one day.
    #[serde(other)]
    Unknown,
}

/// Health as reported by Docker's healthcheck, if one is configured.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HealthState {
    Healthy,
    Unhealthy,
    Starting,
    /// No healthcheck configured. Also the deserialization fallback, so a hub
    /// on an older DockDoe tolerates health states a newer node may report.
    #[serde(other)]
    None,
}

/// One monitored host, as listed by `GET /api/hosts` — enough for a federation
/// hub to pick a host and detect version skew.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HostEntry {
    /// The host's config `name` (display name and URL slug).
    pub name: String,
    /// The node's DockDoe version (`CARGO_PKG_VERSION`).
    pub version: String,
}

/// What `GET /host/{host}/api/snapshot` reports to a federation hub.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    /// The node's DockDoe version, for skew detection.
    pub version: String,
    /// Whether the host has latched read-only (lifecycle actions rejected).
    #[serde(default)]
    pub read_only: bool,
    /// The host's current dashboard, exactly as the node renders it locally.
    pub dashboard: Dashboard,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_snapshot() -> Snapshot {
        Snapshot {
            version: "0.10.0".into(),
            read_only: true,
            dashboard: Dashboard {
                generated_at_unix_ms: 1_720_000_000_000,
                cpu_count: 8,
                containers: vec![ContainerMetrics {
                    id: "deadbeef".into(),
                    name: "web".into(),
                    image: "nginx:latest".into(),
                    state: ContainerState::Running,
                    status: "Up 2 hours (healthy)".into(),
                    health: HealthState::Healthy,
                    stack: Some("blog".into()),
                    cpu_percent: Some(12.5),
                    mem_used: Some(64 << 20),
                    mem_limit: None,
                    net_rx_bps: Some(1024.0),
                    net_tx_bps: None,
                    disk_read_bps: None,
                    disk_write_bps: Some(0.0),
                    ports: vec![Port {
                        private: 80,
                        public: Some(8080),
                        proto: "tcp".into(),
                    }],
                }],
            },
        }
    }

    #[test]
    fn snapshot_roundtrips_through_json() {
        let snap = sample_snapshot();
        let json = serde_json::to_string(&snap).expect("serialize");
        let back: Snapshot = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back.version, snap.version);
        assert!(back.read_only);
        let (a, b) = (&back.dashboard.containers[0], &snap.dashboard.containers[0]);
        assert_eq!(a.id, b.id);
        assert_eq!(a.state, b.state);
        assert_eq!(a.health, b.health);
        assert_eq!(a.cpu_percent, b.cpu_percent);
        assert_eq!(a.ports, b.ports);
    }

    #[test]
    fn snapshot_tolerates_additive_changes_from_newer_nodes() {
        // A newer node may add fields and enum variants; an older hub must
        // shrug them off rather than fail the whole snapshot.
        let json = r#"{
            "version": "0.99.0",
            "read_only": false,
            "brand_new_field": {"nested": true},
            "dashboard": {
                "generated_at_unix_ms": 1,
                "cpu_count": 2,
                "future_field": 42,
                "containers": [{
                    "id": "x", "name": "n", "image": "i",
                    "state": "hibernating",
                    "status": "s",
                    "health": "quantum",
                    "stack": null,
                    "cpu_percent": null, "mem_used": null, "mem_limit": null,
                    "net_rx_bps": null, "net_tx_bps": null,
                    "disk_read_bps": null, "disk_write_bps": null,
                    "ports": []
                }]
            }
        }"#;
        let snap: Snapshot = serde_json::from_str(json).expect("tolerant deserialize");
        let c = &snap.dashboard.containers[0];
        assert_eq!(c.state, ContainerState::Unknown);
        assert_eq!(c.health, HealthState::None);
    }

    #[test]
    fn snapshot_defaults_missing_read_only() {
        // read_only arrived with the federation API; keep it defaultable so the
        // field could have been absent — the pattern every future field follows.
        let json = r#"{"version":"0.10.0","dashboard":{"generated_at_unix_ms":1,"cpu_count":1,"containers":[]}}"#;
        let snap: Snapshot = serde_json::from_str(json).expect("deserialize");
        assert!(!snap.read_only);
    }
}
