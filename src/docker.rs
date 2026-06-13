//! Docker engine access via bollard.
//!
//! The one real trap in the Docker stats API is CPU%: the engine reports raw
//! cumulative counters, not a percentage. We compute it ourselves from the
//! delta between two samples. Rather than rely on the API's `precpu_stats`
//! (which is zeroed for a one-shot read), we keep the previous sample from our
//! own last collection cycle and diff against that — so the delta spans exactly
//! one collection interval.

use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use bollard::Docker;
use bollard::container::LogOutput;
use bollard::errors::Error as BollardError;
use bollard::exec::{CreateExecOptions, ResizeExecOptions, StartExecOptions, StartExecResults};
use bollard::models::{
    ContainerCpuStats, ContainerStatsResponse, ContainerSummary, ContainerSummaryStateEnum,
    HealthStatusEnum, PortSummaryTypeEnum,
};
use bollard::query_parameters::{
    InspectContainerOptionsBuilder, ListContainersOptionsBuilder, LogsOptionsBuilder,
    StatsOptionsBuilder,
};
use futures_util::future::join_all;
use futures_util::{Stream, StreamExt};
use tokio::io::AsyncWrite;
use tracing::{debug, warn};

use crate::model::{ContainerMetrics, ContainerState, HealthState, Port};

/// An attached interactive `exec` session: the engine's combined output stream
/// (TTY merges stdout/stderr) and input sink, plus the exec id used to resize
/// the pseudo-TTY.
pub struct ExecSession {
    pub exec_id: String,
    pub output: Pin<Box<dyn Stream<Item = Result<LogOutput, BollardError>> + Send>>,
    pub input: Pin<Box<dyn AsyncWrite + Send>>,
}

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
    /// is just the message text. Each line carries the daemon's RFC 3339
    /// timestamp prefix (`2026-06-12T10:15:30.123456789Z message`).
    pub async fn logs_tail(&self, id: &str, lines: u32) -> Result<String> {
        let options = LogsOptionsBuilder::new()
            .stdout(true)
            .stderr(true)
            .timestamps(true)
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

    /// Start an interactive TTY `exec` session running `cmd` inside the
    /// container. Returns the exec id (needed to resize the pseudo-TTY) plus the
    /// attached output stream and input sink, which the web layer bridges to a
    /// WebSocket. `cmd` is split on whitespace into argv (so `bash -l` works);
    /// an empty command falls back to `/bin/sh`.
    pub async fn exec_start(&self, id: &str, cmd: &str) -> Result<ExecSession> {
        let mut argv: Vec<String> = cmd.split_whitespace().map(str::to_string).collect();
        if argv.is_empty() {
            argv.push("/bin/sh".to_string());
        }
        let created = self
            .docker
            .create_exec(
                id,
                CreateExecOptions {
                    cmd: Some(argv),
                    attach_stdin: Some(true),
                    attach_stdout: Some(true),
                    attach_stderr: Some(true),
                    tty: Some(true),
                    ..Default::default()
                },
            )
            .await
            .with_context(|| format!("creating exec in {id}"))?;

        let started = self
            .docker
            .start_exec(
                &created.id,
                Some(StartExecOptions {
                    tty: true,
                    ..Default::default()
                }),
            )
            .await
            .context("starting exec")?;

        match started {
            StartExecResults::Attached { output, input } => Ok(ExecSession {
                exec_id: created.id,
                output,
                input,
            }),
            StartExecResults::Detached => anyhow::bail!("exec started detached"),
        }
    }

    /// Resize an exec session's pseudo-TTY to `cols`×`rows` characters.
    pub async fn exec_resize(&self, exec_id: &str, cols: u16, rows: u16) -> Result<()> {
        self.docker
            .resize_exec(
                exec_id,
                ResizeExecOptions {
                    width: cols,
                    height: rows,
                },
            )
            .await
            .context("resizing exec")
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

    /// Start a whole stack: dependency order does the work, a retry loop is the
    /// safety net.
    ///
    /// Each pass starts every not-yet-running member in topological order,
    /// waiting on any modeled `service_healthy` / `service_completed_successfully`
    /// conditions first. In the normal case everything starts and the loop runs
    /// exactly once. If something still won't start — a dependency we couldn't
    /// model (e.g. a namespace partner outside this stack), or a transient
    /// failure — we retry until everything is running or [`STACK_START_TIMEOUT`]
    /// elapses, so we fall back to brute force only when the principled path
    /// didn't suffice.
    async fn start_in_order(&self, members: &[Member]) -> StackOutcome {
        let total = members.len();
        let ordered = topo_order(members);
        let id_by_service: HashMap<&str, &str> = members
            .iter()
            .map(|m| (m.service.as_str(), m.id.as_str()))
            .collect();
        let deadline = Instant::now() + STACK_START_TIMEOUT;

        loop {
            let mut failed = 0;
            let mut progressed = false;
            for m in &ordered {
                // `start_container` returns once the container is running, so a
                // member that's already running needs no action.
                if self.is_running(&m.id).await {
                    continue;
                }
                for dep in &m.deps {
                    if let Some(&dep_id) = id_by_service.get(dep.service.as_str()) {
                        match dep.condition {
                            DepCondition::Healthy => self.wait_healthy(dep_id).await,
                            DepCondition::CompletedSuccessfully => {
                                self.wait_completed(dep_id).await;
                            }
                            DepCondition::Started => {}
                        }
                    }
                }
                match self.apply(&m.id, Action::Start).await {
                    Ok(()) => progressed = true,
                    Err(err) => {
                        debug!(id = %m.id, service = %m.service, %err, "start attempt failed; will retry");
                        failed += 1;
                    }
                }
            }

            if failed == 0 {
                return StackOutcome { total, failed: 0 };
            }
            if Instant::now() >= deadline {
                warn!(
                    stack_unstarted = failed,
                    "timed out starting all stack members"
                );
                return StackOutcome { total, failed };
            }
            // Nothing progressed this round (likely waiting on something outside
            // the stack) — back off before retrying so we don't spin.
            if !progressed {
                tokio::time::sleep(DEP_POLL).await;
            }
        }
    }

    async fn is_running(&self, id: &str) -> bool {
        self.run_state(id).await.is_some_and(|(running, _)| running)
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
/// Overall budget for getting every member of a stack running before giving up.
#[allow(clippy::duration_suboptimal_units)]
const STACK_START_TIMEOUT: Duration = Duration::from_secs(120);

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

/// The previous I/O counters for one container plus when they were read, so
/// network and block-I/O rates can be derived from the byte deltas over the
/// elapsed wall-clock time. Each counter is optional because the runtime may
/// report some categories and not others (e.g. no block-I/O stats).
#[derive(Debug, Clone, Copy)]
struct PrevIo {
    net_rx: Option<u64>,
    net_tx: Option<u64>,
    disk_read: Option<u64>,
    disk_write: Option<u64>,
    at: Instant,
}

/// A client for the Docker engine that remembers prior CPU and I/O samples so
/// it can turn cumulative counters into rates.
pub struct DockerClient {
    docker: Docker,
    /// Previous CPU counters keyed by container ID.
    prev_cpu: HashMap<String, PrevCpu>,
    /// Previous network/block-I/O counters keyed by container ID.
    prev_io: HashMap<String, PrevIo>,
}

impl DockerClient {
    /// Connect to the local Docker daemon (honouring `DOCKER_HOST` and the
    /// default socket).
    pub fn connect() -> Result<Self> {
        let docker = Docker::connect_with_defaults().context("connecting to the Docker daemon")?;
        Ok(Self {
            docker,
            prev_cpu: HashMap::new(),
            prev_io: HashMap::new(),
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

        // Reap prev-sample entries for containers that no longer exist so the
        // maps don't grow unbounded.
        let live: std::collections::HashSet<&str> =
            summaries.iter().filter_map(|s| s.id.as_deref()).collect();
        self.prev_cpu.retain(|id, _| live.contains(id.as_str()));
        self.prev_io.retain(|id, _| live.contains(id.as_str()));

        // One clock reading for the whole cycle: rates derive from the elapsed
        // time since this container's previous sample.
        let now = Instant::now();
        let mut out = Vec::with_capacity(summaries.len());
        for summary in &summaries {
            out.push(self.build_metrics(summary, stats.get(id_of(summary)), now));
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
        now: Instant,
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
        let io = match sample {
            Some(sample) => self.io_rates(&id, sample, now),
            None => IoRates::default(),
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
            net_rx_bps: io.net_rx,
            net_tx_bps: io.net_tx,
            disk_read_bps: io.disk_read,
            disk_write_bps: io.disk_write,
            ports: ports(summary),
        }
    }

    /// Network and block-I/O rates in bytes/second, derived from the byte
    /// deltas against this container's previous sample over the elapsed time.
    /// Returns all-`None` on the first sample (no baseline) or when no time has
    /// passed. Each category is independent: a metric the runtime doesn't
    /// report stays `None` without suppressing the others.
    fn io_rates(&mut self, id: &str, stats: &ContainerStatsResponse, now: Instant) -> IoRates {
        let (net_rx, net_tx) = network_totals(stats);
        let (disk_read, disk_write) = blkio_totals(stats);
        let prev = self.prev_io.insert(
            id.to_string(),
            PrevIo {
                net_rx,
                net_tx,
                disk_read,
                disk_write,
                at: now,
            },
        );
        let Some(prev) = prev else {
            return IoRates::default();
        };
        let secs = now.saturating_duration_since(prev.at).as_secs_f64();
        if secs <= 0.0 {
            return IoRates::default();
        }
        IoRates {
            net_rx: rate(net_rx, prev.net_rx, secs),
            net_tx: rate(net_tx, prev.net_tx, secs),
            disk_read: rate(disk_read, prev.disk_read, secs),
            disk_write: rate(disk_write, prev.disk_write, secs),
        }
    }

    /// Compute CPU utilisation in percent from the delta against the previous
    /// sample. Returns `None` on the first sample (no baseline yet).
    fn cpu_percent(&mut self, id: &str, stats: &ContainerStatsResponse) -> Option<f64> {
        let cpu_stats = stats.cpu_stats.as_ref()?;
        let total_usage = cpu_stats.cpu_usage.as_ref()?.total_usage?;
        let system_usage = cpu_stats.system_cpu_usage?;

        // Remember this sample as the next baseline; `insert` hands back the
        // previous one, absent on the first sample.
        let prev = self.prev_cpu.insert(
            id.to_string(),
            PrevCpu {
                total_usage,
                system_usage,
            },
        )?;

        Some(cpu_percent_from_deltas(
            prev,
            total_usage,
            system_usage,
            online_cpus(cpu_stats),
        ))
    }
}

/// The CPU% formula: the container's share of the host's total CPU time
/// between two samples, scaled to per-core percent (one busy core = 100%,
/// n busy cores = n·100%) — the convention `docker stats` uses. Counter
/// regressions (e.g. a daemon restart) saturate to 0 instead of wrapping.
fn cpu_percent_from_deltas(
    prev: PrevCpu,
    total_usage: u64,
    system_usage: u64,
    online_cpus: f64,
) -> f64 {
    let cpu_delta = total_usage.saturating_sub(prev.total_usage);
    let system_delta = system_usage.saturating_sub(prev.system_usage);
    if cpu_delta == 0 || system_delta == 0 {
        return 0.0;
    }
    (cpu_delta as f64 / system_delta as f64) * online_cpus * 100.0
}

/// Number of CPUs available to the container. `online_cpus` is omitted on some
/// setups; fall back to the per-core counter list, then to 1.
fn online_cpus(cpu_stats: &ContainerCpuStats) -> f64 {
    cpu_stats
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
        .unwrap_or(1.0)
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

/// Per-second I/O rates for one container, all in bytes/second.
#[derive(Debug, Clone, Copy, Default)]
struct IoRates {
    net_rx: Option<f64>,
    net_tx: Option<f64>,
    disk_read: Option<f64>,
    disk_write: Option<f64>,
}

/// A bytes/second rate from two cumulative counter readings `secs` apart, or
/// `None` if either reading is missing. Counter regressions (e.g. the metric
/// disappearing and reappearing) saturate to 0 rather than wrapping.
#[allow(clippy::cast_precision_loss)] // byte counters; rate precision is moot
fn rate(now: Option<u64>, prev: Option<u64>, secs: f64) -> Option<f64> {
    let (now, prev) = (now?, prev?);
    Some(now.saturating_sub(prev) as f64 / secs)
}

/// Total received and transmitted bytes across every network interface, or
/// `None` when the runtime reports no network stats at all (e.g. a container
/// sharing another's network namespace).
fn network_totals(stats: &ContainerStatsResponse) -> (Option<u64>, Option<u64>) {
    let Some(networks) = stats.networks.as_ref() else {
        return (None, None);
    };
    let mut rx = 0u64;
    let mut tx = 0u64;
    for net in networks.values() {
        rx = rx.saturating_add(net.rx_bytes.unwrap_or(0));
        tx = tx.saturating_add(net.tx_bytes.unwrap_or(0));
    }
    (Some(rx), Some(tx))
}

/// Total bytes read from and written to block devices, summed over devices, or
/// `None` when the runtime reports no `io_service_bytes_recursive` (e.g.
/// Windows containers, or some cgroup setups). The `op` label is matched
/// case-insensitively because Docker has used both "Read"/"Write" and
/// lowercase across versions.
fn blkio_totals(stats: &ContainerStatsResponse) -> (Option<u64>, Option<u64>) {
    let Some(entries) = stats
        .blkio_stats
        .as_ref()
        .and_then(|b| b.io_service_bytes_recursive.as_ref())
    else {
        return (None, None);
    };
    let mut read = 0u64;
    let mut write = 0u64;
    for e in entries {
        let Some(op) = e.op.as_deref() else { continue };
        let value = e.value.unwrap_or(0);
        if op.eq_ignore_ascii_case("read") {
            read = read.saturating_add(value);
        } else if op.eq_ignore_ascii_case("write") {
            write = write.saturating_add(value);
        }
    }
    (Some(read), Some(write))
}

/// Ports a container exposes, taken from its summary. Docker lists each
/// published mapping once per host IP family (0.0.0.0 and ::), so identical
/// entries are de-duplicated. Sorted published-first (by host port), then
/// internal-only (by container port), giving the UI a stable, useful order.
fn ports(summary: &ContainerSummary) -> Vec<Port> {
    let mut seen = HashSet::new();
    let mut ports: Vec<Port> = summary
        .ports
        .iter()
        .flatten()
        .filter_map(|p| {
            let proto = match p.typ {
                Some(PortSummaryTypeEnum::UDP) => "udp",
                Some(PortSummaryTypeEnum::SCTP) => "sctp",
                _ => "tcp",
            }
            .to_string();
            seen.insert((p.public_port, p.private_port, proto.clone()))
                .then_some(Port {
                    private: p.private_port,
                    public: p.public_port,
                    proto,
                })
        })
        .collect();
    ports.sort_by(|a, b| match (a.public, b.public) {
        (Some(x), Some(y)) => x.cmp(&y),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => a.private.cmp(&b.private),
    });
    ports
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
    use bollard::models::ContainerCpuUsage;

    #[test]
    fn cpu_percent_matches_the_docker_stats_formula() {
        let prev = PrevCpu {
            total_usage: 1_000,
            system_usage: 100_000,
        };
        // 500 of 10_000 ns of host CPU time on 2 CPUs → 5% · 2 = 10%.
        let pct = cpu_percent_from_deltas(prev, 1_500, 110_000, 2.0);
        assert!((pct - 10.0).abs() < 1e-9);
    }

    #[test]
    fn cpu_percent_is_zero_without_progress() {
        let prev = PrevCpu {
            total_usage: 5_000,
            system_usage: 100_000,
        };
        // No container CPU consumed; also guards the division by zero when
        // the system counter didn't move.
        assert!(cpu_percent_from_deltas(prev, 5_000, 110_000, 4.0).abs() < f64::EPSILON);
        assert!(cpu_percent_from_deltas(prev, 6_000, 100_000, 4.0).abs() < f64::EPSILON);
    }

    #[test]
    fn cpu_percent_saturates_on_counter_regression() {
        // Counters jumped backwards (daemon restart): saturating deltas read
        // as 0% instead of wrapping to astronomic values.
        let prev = PrevCpu {
            total_usage: 5_000,
            system_usage: 100_000,
        };
        assert!(cpu_percent_from_deltas(prev, 1_000, 110_000, 4.0).abs() < f64::EPSILON);
        assert!(cpu_percent_from_deltas(prev, 6_000, 90_000, 4.0).abs() < f64::EPSILON);
    }

    #[test]
    fn online_cpus_prefers_field_then_percpu_list_then_one() {
        let with_field = ContainerCpuStats {
            online_cpus: Some(3),
            ..Default::default()
        };
        assert!((online_cpus(&with_field) - 3.0).abs() < f64::EPSILON);

        let with_percpu = ContainerCpuStats {
            cpu_usage: Some(ContainerCpuUsage {
                percpu_usage: Some(vec![1, 2]),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!((online_cpus(&with_percpu) - 2.0).abs() < f64::EPSILON);

        assert!((online_cpus(&ContainerCpuStats::default()) - 1.0).abs() < f64::EPSILON);

        // A reported 0 must not zero the percentage — fall back to 1.
        let zero_field = ContainerCpuStats {
            online_cpus: Some(0),
            ..Default::default()
        };
        assert!((online_cpus(&zero_field) - 1.0).abs() < f64::EPSILON);
    }

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

    fn port_summary(
        ip: Option<&str>,
        public: Option<u16>,
        private: u16,
        typ: PortSummaryTypeEnum,
    ) -> bollard::models::PortSummary {
        bollard::models::PortSummary {
            ip: ip.map(str::to_string),
            public_port: public,
            private_port: private,
            typ: Some(typ),
        }
    }

    #[test]
    fn ports_dedupes_and_sorts_published_first() {
        let summary = ContainerSummary {
            ports: Some(vec![
                // An internal-only exposed port.
                port_summary(None, None, 5432, PortSummaryTypeEnum::TCP),
                // The same published mapping over IPv4 and IPv6 → one entry.
                port_summary(Some("0.0.0.0"), Some(8080), 80, PortSummaryTypeEnum::TCP),
                port_summary(Some("::"), Some(8080), 80, PortSummaryTypeEnum::TCP),
                // A second published port, lower host port → sorts first.
                port_summary(Some("0.0.0.0"), Some(443), 443, PortSummaryTypeEnum::TCP),
                // UDP keeps its protocol and is distinct from a same-number TCP.
                port_summary(Some("0.0.0.0"), Some(53), 53, PortSummaryTypeEnum::UDP),
            ]),
            ..Default::default()
        };

        let got = ports(&summary);

        assert_eq!(
            got,
            vec![
                Port {
                    private: 53,
                    public: Some(53),
                    proto: "udp".to_string()
                },
                Port {
                    private: 443,
                    public: Some(443),
                    proto: "tcp".to_string()
                },
                Port {
                    private: 80,
                    public: Some(8080),
                    proto: "tcp".to_string()
                },
                // Internal-only last.
                Port {
                    private: 5432,
                    public: None,
                    proto: "tcp".to_string()
                },
            ]
        );
    }

    #[test]
    fn ports_empty_when_none_reported() {
        assert!(ports(&ContainerSummary::default()).is_empty());
    }

    #[test]
    fn rate_is_delta_over_seconds_and_none_without_both_readings() {
        // 3000 bytes over 2 s → 1500 B/s.
        assert_eq!(rate(Some(5_000), Some(2_000), 2.0), Some(1500.0));
        // A counter regression saturates to 0 instead of wrapping.
        assert_eq!(rate(Some(1_000), Some(5_000), 2.0), Some(0.0));
        // Missing either reading → unknown.
        assert_eq!(rate(None, Some(2_000), 2.0), None);
        assert_eq!(rate(Some(5_000), None, 2.0), None);
    }

    #[test]
    fn network_totals_sum_interfaces_or_none() {
        use bollard::models::ContainerNetworkStats;
        let net = |rx, tx| ContainerNetworkStats {
            rx_bytes: Some(rx),
            tx_bytes: Some(tx),
            ..Default::default()
        };
        let stats = ContainerStatsResponse {
            networks: Some(
                [
                    ("eth0".to_string(), net(100, 10)),
                    ("eth1".to_string(), net(50, 5)),
                ]
                .into_iter()
                .collect(),
            ),
            ..Default::default()
        };
        assert_eq!(network_totals(&stats), (Some(150), Some(15)));
        // No networks reported at all → unknown, not zero.
        assert_eq!(
            network_totals(&ContainerStatsResponse::default()),
            (None, None)
        );
    }

    #[test]
    fn blkio_totals_sum_read_write_case_insensitively() {
        use bollard::models::{ContainerBlkioStatEntry, ContainerBlkioStats};
        let entry = |op: &str, value| ContainerBlkioStatEntry {
            op: Some(op.to_string()),
            value: Some(value),
            ..Default::default()
        };
        let stats = ContainerStatsResponse {
            blkio_stats: Some(ContainerBlkioStats {
                io_service_bytes_recursive: Some(vec![
                    entry("Read", 100), // capitalised (older Docker)
                    entry("read", 50),  // lowercase (newer)
                    entry("write", 30),
                    entry("Async", 999), // unrelated op is ignored
                ]),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(blkio_totals(&stats), (Some(150), Some(30)));
        // No block-I/O stats → unknown.
        assert_eq!(
            blkio_totals(&ContainerStatsResponse::default()),
            (None, None)
        );
    }
}
