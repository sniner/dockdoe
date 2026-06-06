//! Docker engine access via bollard.
//!
//! The one real trap in the Docker stats API is CPU%: the engine reports raw
//! cumulative counters, not a percentage. We compute it ourselves from the
//! delta between two samples. Rather than rely on the API's `precpu_stats`
//! (which is zeroed for a one-shot read), we keep the previous sample from our
//! own last collection cycle and diff against that — so the delta spans exactly
//! one collection interval.

use std::collections::HashMap;

use anyhow::{Context, Result};
use bollard::Docker;
use bollard::models::{ContainerStatsResponse, ContainerSummary, ContainerSummaryStateEnum};
use bollard::query_parameters::{
    ListContainersOptionsBuilder, LogsOptionsBuilder, StatsOptionsBuilder,
};
use futures_util::StreamExt;
use futures_util::future::join_all;
use tracing::debug;

use crate::model::{ContainerMetrics, ContainerState, HealthState};

/// A lifecycle action that can be applied to a container.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    Start,
    Stop,
    Restart,
}

impl Action {
    /// Parse an action from a URL path segment.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "start" => Some(Action::Start),
            "stop" => Some(Action::Stop),
            "restart" => Some(Action::Restart),
            _ => None,
        }
    }
}

/// A cheap, cloneable handle to the Docker engine for actions and logs, used by
/// the web layer. The collector keeps its own [`DockerClient`] for stateful CPU
/// sampling; `bollard::Docker` is internally reference-counted, so a second
/// handle is essentially free.
#[derive(Clone)]
pub struct DockerHandle {
    docker: Docker,
}

impl DockerHandle {
    /// Connect to the local Docker daemon (honouring `DOCKER_HOST` and the
    /// default socket).
    pub fn connect() -> Result<Self> {
        let docker = Docker::connect_with_defaults().context("connecting to the Docker daemon")?;
        Ok(Self { docker })
    }

    /// Apply a lifecycle action to one container.
    pub async fn apply(&self, id: &str, action: Action) -> Result<()> {
        match action {
            Action::Start => self.docker.start_container(id, None).await,
            Action::Stop => self.docker.stop_container(id, None).await,
            Action::Restart => self.docker.restart_container(id, None).await,
        }
        .with_context(|| format!("{action:?} container {id}"))
    }

    /// The compose config-file paths recorded for a stack, read from the
    /// `com.docker.compose.project.config_files` label of any of its
    /// containers. Empty if the stack has no such label.
    pub async fn compose_config_files(&self, stack: &str) -> Result<Vec<String>> {
        let options = ListContainersOptionsBuilder::new().all(true).build();
        let containers = self
            .docker
            .list_containers(Some(options))
            .await
            .context("listing containers")?;
        for c in containers {
            let Some(labels) = c.labels else { continue };
            if labels.get("com.docker.compose.project").map(String::as_str) != Some(stack) {
                continue;
            }
            let files = labels
                .get("com.docker.compose.project.config_files")
                .map(|v| {
                    v.split(',')
                        .map(|s| s.trim().to_string())
                        .filter(|s| !s.is_empty())
                        .collect()
                })
                .unwrap_or_default();
            return Ok(files);
        }
        Ok(Vec::new())
    }

    /// Tail the last `lines` log lines (stdout + stderr) of a container.
    /// bollard de-multiplexes the Docker log stream, so each chunk's `Display`
    /// is just the message text.
    pub async fn logs_tail(&self, id: &str, lines: u32) -> Result<String> {
        let options = LogsOptionsBuilder::new()
            .stdout(true)
            .stderr(true)
            .tail(&lines.to_string())
            .build();
        let mut stream = self.docker.logs(id, Some(options));
        let mut out = String::new();
        while let Some(chunk) = stream.next().await {
            let log = chunk.with_context(|| format!("reading logs of {id}"))?;
            out.push_str(&log.to_string());
        }
        Ok(out)
    }
}

/// The previous CPU counters for one container, used to compute a delta.
#[derive(Debug, Clone, Copy)]
struct PrevCpu {
    total_usage: u64,
    system_usage: u64,
}

/// A client for the Docker engine that remembers prior CPU samples so it can
/// turn cumulative counters into a percentage.
pub struct DockerClient {
    docker: Docker,
    /// Previous CPU counters keyed by container ID.
    prev_cpu: HashMap<String, PrevCpu>,
}

impl DockerClient {
    /// Connect to the local Docker daemon (honouring `DOCKER_HOST` and the
    /// default socket).
    pub fn connect() -> Result<Self> {
        let docker = Docker::connect_with_defaults().context("connecting to the Docker daemon")?;
        Ok(Self {
            docker,
            prev_cpu: HashMap::new(),
        })
    }

    /// Collect metrics for all containers (running and stopped).
    ///
    /// Stats are only fetched for running containers; stopped ones report
    /// identity and state but no CPU/memory.
    pub async fn collect(&mut self) -> Result<Vec<ContainerMetrics>> {
        let options = ListContainersOptionsBuilder::new().all(true).build();
        let summaries = self
            .docker
            .list_containers(Some(options))
            .await
            .context("listing containers")?;

        // Fetch stats for all running containers concurrently. Reborrow as a
        // shared reference so the per-container futures can each hold it.
        let this: &DockerClient = self;
        let stat_futures = summaries.iter().filter(|s| is_running(s)).map(|s| {
            let id = s.id.clone().unwrap_or_default();
            async move { (id.clone(), this.fetch_stats(&id).await) }
        });
        let stats: HashMap<String, ContainerStatsResponse> = join_all(stat_futures)
            .await
            .into_iter()
            .filter_map(|(id, res)| match res {
                Ok(Some(stats)) => Some((id, stats)),
                Ok(None) => None,
                Err(err) => {
                    debug!(%id, %err, "fetching container stats failed");
                    None
                }
            })
            .collect();

        // Reap prev-CPU entries for containers that no longer exist so the map
        // doesn't grow unbounded.
        let live: std::collections::HashSet<&str> =
            summaries.iter().filter_map(|s| s.id.as_deref()).collect();
        self.prev_cpu.retain(|id, _| live.contains(id.as_str()));

        let mut out = Vec::with_capacity(summaries.len());
        for summary in &summaries {
            out.push(self.build_metrics(summary, stats.get(id_of(summary))));
        }
        Ok(out)
    }

    /// Fetch a single one-shot stats sample for one container.
    async fn fetch_stats(&self, id: &str) -> Result<Option<ContainerStatsResponse>> {
        let options = StatsOptionsBuilder::new()
            .stream(false)
            .one_shot(true)
            .build();
        let mut stream = self.docker.stats(id, Some(options));
        match stream.next().await {
            Some(Ok(stats)) => Ok(Some(stats)),
            Some(Err(err)) => Err(err).context("reading stats stream"),
            None => Ok(None),
        }
    }

    /// Build the metrics for one container, computing CPU% against the
    /// remembered previous sample.
    fn build_metrics(
        &mut self,
        summary: &ContainerSummary,
        sample: Option<&ContainerStatsResponse>,
    ) -> ContainerMetrics {
        let id = id_of(summary).to_string();
        let status = summary.status.clone().unwrap_or_default();

        let (cpu_percent, mem_used, mem_limit) = match sample {
            Some(sample) => {
                let cpu = self.cpu_percent(&id, sample);
                let (used, limit) = memory(sample);
                (cpu, used, limit)
            }
            None => (None, None, None),
        };

        ContainerMetrics {
            id,
            name: display_name(summary),
            image: summary.image.clone().unwrap_or_default(),
            state: map_state(summary.state),
            health: parse_health(&status),
            stack: compose_project(summary),
            status,
            cpu_percent,
            mem_used,
            mem_limit,
        }
    }

    /// Compute CPU utilisation in percent from the delta against the previous
    /// sample. Returns `None` on the first sample (no baseline yet).
    fn cpu_percent(&mut self, id: &str, stats: &ContainerStatsResponse) -> Option<f64> {
        let cpu_stats = stats.cpu_stats.as_ref()?;
        let total_usage = cpu_stats.cpu_usage.as_ref()?.total_usage?;
        let system_usage = cpu_stats.system_cpu_usage?;

        let prev = self.prev_cpu.insert(
            id.to_string(),
            PrevCpu {
                total_usage,
                system_usage,
            },
        );
        let prev = prev?;

        let cpu_delta = total_usage.saturating_sub(prev.total_usage);
        let system_delta = system_usage.saturating_sub(prev.system_usage);
        if cpu_delta == 0 || system_delta == 0 {
            return Some(0.0);
        }

        // online_cpus is omitted on some setups; fall back to the per-core
        // count, then to 1.
        let online_cpus = cpu_stats
            .online_cpus
            .map(f64::from)
            .or_else(|| {
                cpu_stats
                    .cpu_usage
                    .as_ref()
                    .and_then(|u| u.percpu_usage.as_ref())
                    .map(|v| v.len() as f64)
            })
            .filter(|&n| n > 0.0)
            .unwrap_or(1.0);

        Some((cpu_delta as f64 / system_delta as f64) * online_cpus * 100.0)
    }
}

/// Memory used (cache excluded) and limit, both in bytes.
///
/// Docker's `docker stats` subtracts `inactive_file` (cgroup v2) or
/// `total_inactive_file` (cgroup v1) from the raw usage to exclude reclaimable
/// page cache. We do the same.
fn memory(stats: &ContainerStatsResponse) -> (Option<u64>, Option<u64>) {
    let Some(mem) = stats.memory_stats.as_ref() else {
        return (None, None);
    };
    let used = mem.usage.map(|usage| {
        let cache = mem
            .stats
            .as_ref()
            .and_then(|s| {
                s.get("inactive_file")
                    .or_else(|| s.get("total_inactive_file"))
            })
            .copied()
            .unwrap_or(0);
        usage.saturating_sub(cache)
    });
    (used, mem.limit)
}

fn is_running(summary: &ContainerSummary) -> bool {
    summary.state == Some(ContainerSummaryStateEnum::RUNNING)
}

fn id_of(summary: &ContainerSummary) -> &str {
    summary.id.as_deref().unwrap_or("")
}

/// First name with the leading slash stripped, or a short ID fallback.
fn display_name(summary: &ContainerSummary) -> String {
    summary
        .names
        .as_ref()
        .and_then(|names| names.first())
        .map_or_else(
            || id_of(summary).chars().take(12).collect(),
            |n| n.trim_start_matches('/').to_string(),
        )
}

fn compose_project(summary: &ContainerSummary) -> Option<String> {
    summary
        .labels
        .as_ref()?
        .get("com.docker.compose.project")
        .cloned()
}

fn map_state(state: Option<ContainerSummaryStateEnum>) -> ContainerState {
    match state {
        Some(ContainerSummaryStateEnum::CREATED) => ContainerState::Created,
        Some(ContainerSummaryStateEnum::RUNNING) => ContainerState::Running,
        Some(ContainerSummaryStateEnum::PAUSED) => ContainerState::Paused,
        Some(ContainerSummaryStateEnum::RESTARTING) => ContainerState::Restarting,
        Some(ContainerSummaryStateEnum::EXITED) => ContainerState::Exited,
        Some(ContainerSummaryStateEnum::REMOVING) => ContainerState::Removing,
        Some(ContainerSummaryStateEnum::DEAD) => ContainerState::Dead,
        Some(ContainerSummaryStateEnum::STOPPING) => ContainerState::Stopping,
        Some(ContainerSummaryStateEnum::EMPTY) | None => ContainerState::Unknown,
    }
}

/// Derive health from the status line, e.g. "Up 2 hours (healthy)". Docker's
/// container list doesn't expose a structured health field, but it appends the
/// health state to the status string.
fn parse_health(status: &str) -> HealthState {
    if status.contains("(healthy)") {
        HealthState::Healthy
    } else if status.contains("(unhealthy)") {
        HealthState::Unhealthy
    } else if status.contains("(health: starting)") {
        HealthState::Starting
    } else {
        HealthState::None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_health_recognises_states() {
        assert_eq!(parse_health("Up 2 hours (healthy)"), HealthState::Healthy);
        assert_eq!(
            parse_health("Up 1 minute (unhealthy)"),
            HealthState::Unhealthy
        );
        assert_eq!(
            parse_health("Up 5 seconds (health: starting)"),
            HealthState::Starting
        );
        assert_eq!(parse_health("Up 3 days"), HealthState::None);
        assert_eq!(parse_health("Exited (0) 2 hours ago"), HealthState::None);
    }
}
