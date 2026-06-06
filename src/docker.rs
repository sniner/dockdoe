//! Docker engine access via bollard.
//!
//! The one real trap in the Docker stats API is CPU%: the engine reports raw
//! cumulative counters, not a percentage. We compute it ourselves from the
//! delta between two samples. Rather than rely on the API's `precpu_stats`
//! (which is zeroed for a one-shot read), we keep the previous sample from our
//! own last collection cycle and diff against that — so the delta spans exactly
//! one collection interval.

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use bollard::Docker;
use bollard::models::{
    ContainerStatsResponse, ContainerSummary, ContainerSummaryStateEnum, HealthStatusEnum,
};
use bollard::query_parameters::{
    InspectContainerOptionsBuilder, ListContainersOptionsBuilder, LogsOptionsBuilder,
    StatsOptionsBuilder,
};
use futures_util::StreamExt;
use futures_util::future::join_all;
use tracing::{debug, warn};

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

    /// Apply an action to a whole compose stack, honouring the dependency order
    /// recorded in the `com.docker.compose.depends_on` labels.
    ///
    /// Start: dependencies first (topological order); a `service_healthy`
    /// dependency is waited on until its healthcheck reports healthy. Stop:
    /// reverse order. Restart: stop then start.
    pub async fn stack_action(&self, project: &str, action: Action) -> Result<StackOutcome> {
        let members = self.stack_members(project).await?;
        let outcome = match action {
            Action::Start => self.start_in_order(&members).await,
            Action::Stop => self.stop_in_order(&members).await,
            Action::Restart => {
                let stop = self.stop_in_order(&members).await;
                let start = self.start_in_order(&members).await;
                // End state is "started", so report the start failures.
                StackOutcome {
                    failed: start.failed,
                    ..stop
                }
            }
        };
        Ok(outcome)
    }

    /// The containers belonging to a compose project, with their service name
    /// and parsed dependency edges.
    async fn stack_members(&self, project: &str) -> Result<Vec<Member>> {
        let options = ListContainersOptionsBuilder::new().all(true).build();
        let containers = self
            .docker
            .list_containers(Some(options))
            .await
            .context("listing containers")?;
        let mut members = Vec::new();
        for c in containers {
            let Some(labels) = c.labels else { continue };
            if labels.get("com.docker.compose.project").map(String::as_str) != Some(project) {
                continue;
            }
            let Some(id) = c.id else { continue };
            let service = labels
                .get("com.docker.compose.service")
                .cloned()
                .unwrap_or_default();
            let deps = labels
                .get("com.docker.compose.depends_on")
                .map(|s| parse_depends_on(s))
                .unwrap_or_default();
            members.push(Member { id, service, deps });
        }

        // Belt and suspenders: a member may share another's network/ipc/pid
        // namespace (`network_mode: container:X`) without that showing up in a
        // depends_on label — older compose versions don't write the label, and
        // hand-run containers never do. Those are *hard*, daemon-enforced
        // start-order dependencies, so derive them from the runtime config too.
        let service_by_id: HashMap<String, String> = members
            .iter()
            .map(|m| (m.id.clone(), m.service.clone()))
            .collect();
        for m in &mut members {
            for target_id in self.namespace_deps(&m.id).await {
                let Some(service) = resolve_service(&service_by_id, &target_id) else {
                    continue;
                };
                if service != m.service && !m.deps.iter().any(|d| d.service == service) {
                    m.deps.push(Dep {
                        service,
                        condition: DepCondition::Started,
                    });
                }
            }
        }
        Ok(members)
    }

    /// Container IDs this container shares a namespace with (network/ipc/pid set
    /// to `container:<id>`). These bindings are enforced by the daemon — the
    /// dependent can't start until the target runs — regardless of any label.
    async fn namespace_deps(&self, id: &str) -> Vec<String> {
        let options = InspectContainerOptionsBuilder::new().build();
        let Ok(info) = self.docker.inspect_container(id, Some(options)).await else {
            return Vec::new();
        };
        let Some(host) = info.host_config else {
            return Vec::new();
        };
        [host.network_mode, host.ipc_mode, host.pid_mode]
            .into_iter()
            .filter_map(|mode| mode?.strip_prefix("container:").map(str::to_string))
            .collect()
    }

    /// Start members in dependency order, waiting on `service_healthy` /
    /// `service_completed_successfully` conditions before starting a dependent.
    async fn start_in_order(&self, members: &[Member]) -> StackOutcome {
        let id_by_service: HashMap<&str, &str> = members
            .iter()
            .map(|m| (m.service.as_str(), m.id.as_str()))
            .collect();
        let mut failed = 0;
        for m in topo_order(members) {
            for dep in &m.deps {
                let Some(&dep_id) = id_by_service.get(dep.service.as_str()) else {
                    continue;
                };
                match dep.condition {
                    DepCondition::Healthy => self.wait_healthy(dep_id).await,
                    DepCondition::CompletedSuccessfully => self.wait_completed(dep_id).await,
                    DepCondition::Started => {}
                }
            }
            if let Err(err) = self.apply(&m.id, Action::Start).await {
                warn!(id = %m.id, service = %m.service, %err, "starting stack member failed");
                failed += 1;
            }
        }
        StackOutcome {
            total: members.len(),
            failed,
        }
    }

    /// Stop members in reverse dependency order.
    async fn stop_in_order(&self, members: &[Member]) -> StackOutcome {
        let mut ordered = topo_order(members);
        ordered.reverse();
        let mut failed = 0;
        for m in ordered {
            if let Err(err) = self.apply(&m.id, Action::Stop).await {
                warn!(id = %m.id, service = %m.service, %err, "stopping stack member failed");
                failed += 1;
            }
        }
        StackOutcome {
            total: members.len(),
            failed,
        }
    }

    /// Poll a container's health until it reports healthy, or give up after
    /// [`DEP_WAIT`]. A container without a healthcheck can't be waited on, so we
    /// proceed immediately.
    async fn wait_healthy(&self, id: &str) {
        let start = Instant::now();
        loop {
            match self.health_status(id).await {
                Some(HealthStatusEnum::HEALTHY) => return,
                None | Some(HealthStatusEnum::NONE | HealthStatusEnum::EMPTY) => {
                    debug!(%id, "dependency has no healthcheck; not waiting");
                    return;
                }
                _ => {} // STARTING / UNHEALTHY → keep waiting
            }
            if start.elapsed() >= DEP_WAIT {
                warn!(%id, "timed out waiting for dependency to become healthy");
                return;
            }
            tokio::time::sleep(DEP_POLL).await;
        }
    }

    /// Poll until a container has exited, or give up after [`DEP_WAIT`].
    async fn wait_completed(&self, id: &str) {
        let start = Instant::now();
        loop {
            match self.run_state(id).await {
                Some((false, exit)) => {
                    if exit != 0 {
                        warn!(%id, exit, "dependency exited non-zero");
                    }
                    return;
                }
                None => return,
                Some((true, _)) => {} // still running → keep waiting
            }
            if start.elapsed() >= DEP_WAIT {
                warn!(%id, "timed out waiting for dependency to complete");
                return;
            }
            tokio::time::sleep(DEP_POLL).await;
        }
    }

    async fn health_status(&self, id: &str) -> Option<HealthStatusEnum> {
        let options = InspectContainerOptionsBuilder::new().build();
        let info = self
            .docker
            .inspect_container(id, Some(options))
            .await
            .ok()?;
        info.state?.health?.status
    }

    /// `(running, exit_code)` for a container, or `None` if it can't be read.
    async fn run_state(&self, id: &str) -> Option<(bool, i64)> {
        let options = InspectContainerOptionsBuilder::new().build();
        let info = self
            .docker
            .inspect_container(id, Some(options))
            .await
            .ok()?;
        let state = info.state?;
        Some((state.running.unwrap_or(false), state.exit_code.unwrap_or(0)))
    }
}

/// How long to wait for a dependency's condition before giving up and starting
/// the dependent anyway. Seconds read clearer here than the stable-Rust
/// alternatives, and there is no `Duration::from_mins` on stable.
#[allow(clippy::duration_suboptimal_units)]
const DEP_WAIT: Duration = Duration::from_secs(120);
/// How often to poll a dependency's state while waiting.
const DEP_POLL: Duration = Duration::from_secs(1);

/// The outcome of a stack-wide action.
#[derive(Debug, Clone, Copy)]
pub struct StackOutcome {
    pub total: usize,
    pub failed: usize,
}

/// A compose stack member and its dependency edges.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Member {
    id: String,
    service: String,
    deps: Vec<Dep>,
}

/// One `depends_on` edge.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Dep {
    service: String,
    condition: DepCondition,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DepCondition {
    Started,
    Healthy,
    CompletedSuccessfully,
}

/// Parse a `com.docker.compose.depends_on` label, e.g.
/// `db:service_healthy:false,cache:service_started:false`.
fn parse_depends_on(label: &str) -> Vec<Dep> {
    label
        .split(',')
        .filter_map(|entry| {
            let mut parts = entry.trim().splitn(3, ':');
            let service = parts.next()?.trim();
            if service.is_empty() {
                return None;
            }
            let condition = match parts.next() {
                Some("service_healthy") => DepCondition::Healthy,
                Some("service_completed_successfully") => DepCondition::CompletedSuccessfully,
                _ => DepCondition::Started,
            };
            Some(Dep {
                service: service.to_string(),
                condition,
            })
        })
        .collect()
}

/// Resolve a `container:<id>` namespace reference to an in-stack service name.
/// Compose writes full container IDs; we also accept a short-ID prefix. Returns
/// `None` when the referenced container isn't part of this stack.
fn resolve_service(service_by_id: &HashMap<String, String>, target_id: &str) -> Option<String> {
    if let Some(service) = service_by_id.get(target_id) {
        return Some(service.clone());
    }
    service_by_id
        .iter()
        .find(|(id, _)| id.starts_with(target_id) || target_id.starts_with(id.as_str()))
        .map(|(_, service)| service.clone())
}

/// Order members so every container comes after the in-stack dependencies it
/// declares (Kahn's algorithm). On a cycle or a missing dependency the
/// remaining members are appended in their original order as a best effort.
fn topo_order(members: &[Member]) -> Vec<&Member> {
    let in_stack: HashSet<&str> = members.iter().map(|m| m.service.as_str()).collect();
    let mut placed: HashSet<&str> = HashSet::new();
    let mut result: Vec<&Member> = Vec::with_capacity(members.len());
    let mut remaining: Vec<&Member> = members.iter().collect();

    while !remaining.is_empty() {
        let (ready, not_ready): (Vec<&Member>, Vec<&Member>) =
            remaining.into_iter().partition(|m| {
                m.deps.iter().all(|d| {
                    !in_stack.contains(d.service.as_str()) || placed.contains(d.service.as_str())
                })
            });
        if ready.is_empty() {
            warn!("stack dependency cycle or missing dependency; using listing order for the rest");
            result.extend(not_ready);
            break;
        }
        for m in &ready {
            placed.insert(m.service.as_str());
        }
        result.extend(ready);
        remaining = not_ready;
    }
    result
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

    #[test]
    fn parse_depends_on_reads_services_and_conditions() {
        // The format compose writes: `service:condition:restart`.
        let deps = parse_depends_on("side:service_healthy:false,db:service_started:true");
        assert_eq!(
            deps,
            vec![
                Dep {
                    service: "side".to_string(),
                    condition: DepCondition::Healthy,
                },
                Dep {
                    service: "db".to_string(),
                    condition: DepCondition::Started,
                },
            ]
        );
        assert!(parse_depends_on("").is_empty());
        assert_eq!(
            parse_depends_on("init:service_completed_successfully:false")[0].condition,
            DepCondition::CompletedSuccessfully
        );
    }

    fn member(service: &str, deps: &[&str]) -> Member {
        Member {
            id: format!("id-{service}"),
            service: service.to_string(),
            deps: deps
                .iter()
                .map(|s| Dep {
                    service: (*s).to_string(),
                    condition: DepCondition::Started,
                })
                .collect(),
        }
    }

    fn order(members: &[Member]) -> Vec<&str> {
        topo_order(members)
            .iter()
            .map(|m| m.service.as_str())
            .collect()
    }

    #[test]
    fn topo_order_places_dependencies_first() {
        // app depends on side → side must come first regardless of input order.
        let members = vec![member("app", &["side"]), member("side", &[])];
        assert_eq!(order(&members), vec!["side", "app"]);
    }

    #[test]
    fn topo_order_handles_a_chain() {
        // a -> b -> c (a depends on b, b depends on c)
        let members = vec![member("a", &["b"]), member("b", &["c"]), member("c", &[])];
        assert_eq!(order(&members), vec!["c", "b", "a"]);
    }

    #[test]
    fn topo_order_survives_a_cycle() {
        // a <-> b cycle: no panic, both still returned.
        let members = vec![member("a", &["b"]), member("b", &["a"])];
        let got = order(&members);
        assert_eq!(got.len(), 2);
    }

    #[test]
    fn topo_order_ignores_out_of_stack_dependencies() {
        // app depends on "external" which isn't part of this stack → ignored.
        let members = vec![member("app", &["external"])];
        assert_eq!(order(&members), vec!["app"]);
    }

    #[test]
    fn resolve_service_matches_full_and_short_ids() {
        let map: HashMap<String, String> = [
            ("abc123def456".to_string(), "side".to_string()),
            ("999888777".to_string(), "db".to_string()),
        ]
        .into_iter()
        .collect();
        // Full id.
        assert_eq!(
            resolve_service(&map, "abc123def456").as_deref(),
            Some("side")
        );
        // Short-id prefix (what a `container:abc123` reference would carry).
        assert_eq!(resolve_service(&map, "abc123").as_deref(), Some("side"));
        // Unknown container → not in this stack.
        assert_eq!(resolve_service(&map, "deadbeef"), None);
    }
}
