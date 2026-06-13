//! Apprise notifications for container state changes.
//!
//! When `DOCKDOE_APPRISE_URL` is set, the collector feeds every snapshot into a
//! [`Notifier`], which watches each container's coarse status (up / down /
//! unhealthy) and POSTs an [Apprise] message when a change *sticks*. An
//! incubation delay swallows flapping (a container that restarts and recovers
//! within the delay never alerts), and the very first time a container is seen
//! its status is adopted silently — so a fresh start, or DockDoe's own startup,
//! does not produce a flood.
//!
//! The decision logic lives in [`Tracker`], which is pure and unit-tested; the
//! [`Notifier`] wraps it with the HTTP client and fire-and-forget sending.
//!
//! [Apprise]: https://github.com/caronc/apprise-api

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::time::Duration;

use tracing::{debug, warn};

use crate::model::{ContainerMetrics, ContainerState, HealthState};

/// The coarse, notification-worthy status of a container, collapsed from
/// Docker's finer state/health into the three things a user cares about.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Status {
    /// Running and not failing its healthcheck.
    Up,
    /// Running but the healthcheck reports unhealthy.
    Unhealthy,
    /// Not running (exited, dead, restarting, …).
    Down,
}

/// Map a container's Docker state/health onto our coarse [`Status`].
fn status_of(c: &ContainerMetrics) -> Status {
    match c.state {
        ContainerState::Running => match c.health {
            HealthState::Unhealthy => Status::Unhealthy,
            _ => Status::Up,
        },
        _ => Status::Down,
    }
}

/// A ready-to-send notification: an Apprise message `type` and a body. The
/// title is always "DockDoe", added at send time.
struct Notification {
    /// Apprise message type: `success`, `warning`, or `failure`.
    kind: &'static str,
    body: String,
}

/// Build the notification for a container that has settled into `to`.
fn notification_for(c: &ContainerMetrics, to: Status) -> Notification {
    let who = match &c.stack {
        Some(stack) => format!("Container '{}' (stack {stack})", c.name),
        None => format!("Container '{}'", c.name),
    };
    let (kind, body) = match to {
        Status::Down => ("failure", format!("{who} is down — {}", c.status)),
        Status::Unhealthy => ("warning", format!("{who} is unhealthy — {}", c.status)),
        Status::Up => ("success", format!("{who} recovered — {}", c.status)),
    };
    Notification { kind, body }
}

/// What we know about one container between cycles.
struct Tracked {
    /// The status we last told the user about (the baseline we compare against).
    notified: Status,
    /// A status that differs from `notified` and is waiting out the incubation
    /// delay, with the timestamp it was first observed at.
    pending: Option<(Status, u64)>,
}

/// The pure state machine: given successive snapshots, decide which changes
/// have settled long enough to notify about. Holds no I/O.
struct Tracker {
    /// How long a new status must persist before it is reported.
    delay_ms: u64,
    states: HashMap<String, Tracked>,
}

impl Tracker {
    /// Fold one snapshot in and return the notifications that just became due.
    fn evaluate(&mut self, ts_ms: u64, containers: &[ContainerMetrics]) -> Vec<Notification> {
        let mut out = Vec::new();
        let mut seen: HashSet<&str> = HashSet::with_capacity(containers.len());

        for c in containers {
            seen.insert(c.id.as_str());
            let cur = status_of(c);

            match self.states.entry(c.id.clone()) {
                Entry::Vacant(slot) => {
                    // First sighting: adopt the status silently. No alert for a
                    // newly started container or on DockDoe's own startup.
                    slot.insert(Tracked {
                        notified: cur,
                        pending: None,
                    });
                }
                Entry::Occupied(mut slot) => {
                    let t = slot.get_mut();
                    if cur == t.notified {
                        // Back to (or never left) the reported status: any
                        // in-flight change flapped back, so drop it.
                        t.pending = None;
                        continue;
                    }
                    // A different status. Keep the original `since` while the
                    // candidate target is unchanged; restart the timer when the
                    // target itself moves (e.g. down → unhealthy).
                    let since = match t.pending {
                        Some((p, since)) if p == cur => since,
                        _ => {
                            t.pending = Some((cur, ts_ms));
                            ts_ms
                        }
                    };
                    if ts_ms.saturating_sub(since) >= self.delay_ms {
                        out.push(notification_for(c, cur));
                        t.notified = cur;
                        t.pending = None;
                    }
                }
            }
        }

        // Forget vanished containers so a removed-then-recreated name
        // re-baselines instead of firing a spurious change.
        self.states.retain(|id, _| seen.contains(id.as_str()));
        out
    }
}

/// Watches container statuses and POSTs Apprise notifications when changes
/// settle. Constructed only when `DOCKDOE_APPRISE_URL` is set.
pub struct Notifier {
    url: String,
    client: reqwest::Client,
    tracker: Tracker,
}

impl Notifier {
    /// Build a notifier targeting `url`, requiring a new status to persist for
    /// `delay` before it is reported.
    ///
    /// The HTTP client trusts the Mozilla root set compiled into the binary
    /// (`webpki-root-certs`) rather than a system trust store — the scratch
    /// runtime image has none — so HTTPS Apprise endpoints work out of the box.
    #[must_use]
    pub fn new(url: String, delay: Duration) -> Self {
        let roots = webpki_root_certs::TLS_SERVER_ROOT_CERTS
            .iter()
            .filter_map(|der| reqwest::Certificate::from_der(der.as_ref()).ok());
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .tls_certs_only(roots)
            .build()
            .expect("building the reqwest client for Apprise notifications");
        Self {
            url,
            client,
            tracker: Tracker {
                delay_ms: u64::try_from(delay.as_millis()).unwrap_or(u64::MAX),
                states: HashMap::new(),
            },
        }
    }

    /// Feed one snapshot in and dispatch any notifications that became due.
    pub fn observe(&mut self, ts_ms: u64, containers: &[ContainerMetrics]) {
        for note in self.tracker.evaluate(ts_ms, containers) {
            self.dispatch(note);
        }
    }

    /// Fire one notification on a detached task so a slow or unreachable Apprise
    /// endpoint never stalls the collection loop. Failures are logged, not
    /// retried — the next state change will try again.
    fn dispatch(&self, note: Notification) {
        let client = self.client.clone();
        let url = self.url.clone();
        tokio::spawn(async move {
            let payload = serde_json::json!({
                "title": "DockDoe",
                "body": note.body,
                "type": note.kind,
            });
            match client.post(&url).json(&payload).send().await {
                Ok(resp) if resp.status().is_success() => {
                    debug!(kind = note.kind, "apprise notification sent");
                }
                Ok(resp) => warn!(status = %resp.status(), "apprise rejected the notification"),
                Err(err) => warn!(%err, "sending apprise notification failed"),
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn container(id: &str, state: ContainerState, health: HealthState) -> ContainerMetrics {
        ContainerMetrics {
            id: id.to_string(),
            name: id.to_string(),
            image: "img".to_string(),
            state,
            status: "status".to_string(),
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
        }
    }

    fn up(id: &str) -> ContainerMetrics {
        container(id, ContainerState::Running, HealthState::None)
    }
    fn down(id: &str) -> ContainerMetrics {
        container(id, ContainerState::Exited, HealthState::None)
    }
    fn unhealthy(id: &str) -> ContainerMetrics {
        container(id, ContainerState::Running, HealthState::Unhealthy)
    }

    fn tracker(delay_ms: u64) -> Tracker {
        Tracker {
            delay_ms,
            states: HashMap::new(),
        }
    }

    #[test]
    fn first_sighting_is_silent() {
        let mut t = tracker(0);
        // Even an already-down container on the first snapshot must not alert.
        assert!(t.evaluate(1000, &[down("a"), up("b")]).is_empty());
    }

    #[test]
    fn change_waits_out_the_incubation_delay() {
        let mut t = tracker(30_000);
        assert!(t.evaluate(0, &[up("a")]).is_empty()); // baseline
        assert!(t.evaluate(1_000, &[down("a")]).is_empty()); // pending, too soon
        assert!(t.evaluate(20_000, &[down("a")]).is_empty()); // still too soon

        let notes = t.evaluate(31_000, &[down("a")]); // 31_000 - 1_000 >= 30_000
        assert_eq!(notes.len(), 1);
        assert_eq!(notes[0].kind, "failure");
    }

    #[test]
    fn a_flap_within_the_delay_is_swallowed() {
        let mut t = tracker(30_000);
        t.evaluate(0, &[up("a")]);
        t.evaluate(1_000, &[down("a")]); // pending down
        assert!(t.evaluate(2_000, &[up("a")]).is_empty()); // recovered in time
        // ...and stays up well past the old deadline: nothing fires.
        assert!(t.evaluate(40_000, &[up("a")]).is_empty());
    }

    #[test]
    fn recovery_is_reported_too() {
        let mut t = tracker(0); // immediate, to keep the test short
        t.evaluate(0, &[up("a")]);
        let down_note = t.evaluate(1_000, &[down("a")]);
        assert_eq!(down_note.len(), 1);
        assert_eq!(down_note[0].kind, "failure");

        let up_note = t.evaluate(2_000, &[up("a")]);
        assert_eq!(up_note.len(), 1);
        assert_eq!(up_note[0].kind, "success");
    }

    #[test]
    fn unhealthy_maps_to_a_warning() {
        let mut t = tracker(0);
        t.evaluate(0, &[up("a")]);
        let notes = t.evaluate(1_000, &[unhealthy("a")]);
        assert_eq!(notes.len(), 1);
        assert_eq!(notes[0].kind, "warning");
    }

    #[test]
    fn a_shifting_target_restarts_the_timer() {
        let mut t = tracker(10_000);
        t.evaluate(0, &[up("a")]);
        t.evaluate(1_000, &[down("a")]); // pending down since 1_000
        // Target moves to unhealthy at 5_000 → timer restarts from there.
        assert!(t.evaluate(5_000, &[unhealthy("a")]).is_empty());
        assert!(t.evaluate(12_000, &[unhealthy("a")]).is_empty()); // 12_000 - 5_000 < 10_000

        let notes = t.evaluate(15_001, &[unhealthy("a")]); // 15_001 - 5_000 >= 10_000
        assert_eq!(notes.len(), 1);
        assert_eq!(notes[0].kind, "warning");
    }

    #[test]
    fn a_vanished_container_is_forgotten_and_rebaselines() {
        let mut t = tracker(0);
        t.evaluate(0, &[up("a")]); // baseline
        assert!(t.evaluate(1_000, &[]).is_empty()); // gone
        assert!(t.states.is_empty()); // and forgotten
        // Reappears already down → adopted silently, no spurious alert.
        assert!(t.evaluate(2_000, &[down("a")]).is_empty());
    }
}
