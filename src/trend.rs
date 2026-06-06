//! Trend rollups: min/max/median over fixed time buckets.
//!
//! Mirrors the `Entwurf.md` design. Samples are accumulated into a bucket
//! aligned to wall-clock (`bucket_secs`). As soon as a sample arrives that
//! belongs to a *later* bucket, the current bucket is closed and its rollups
//! are emitted — trends are computed immediately when the window completes, not
//! lazily when raw data ages out. Median is preferred over mean: it is robust
//! against spikes, and we keep `max` for the worst case anyway.

use std::collections::HashMap;

use crate::model::{ContainerMetrics, HostMetrics};
use crate::store::{ContainerTrend, HostTrend, Metric};

/// Trend rows produced when a bucket closes. Empty when no boundary was crossed.
#[derive(Debug, Default)]
pub struct Flushed {
    pub host: Vec<HostTrend>,
    pub containers: Vec<ContainerTrend>,
}

impl Flushed {
    pub fn is_empty(&self) -> bool {
        self.host.is_empty() && self.containers.is_empty()
    }
}

/// Accumulates samples and emits rollups at bucket boundaries.
pub struct Bucketer {
    bucket_ms: u64,
    /// Start (Unix ms, bucket-aligned) of the bucket currently filling.
    current_start: Option<u64>,
    host: HashMap<Metric, Vec<f64>>,
    containers: HashMap<(String, Metric), ContainerAcc>,
}

struct ContainerAcc {
    name: String,
    stack: Option<String>,
    values: Vec<f64>,
}

impl Bucketer {
    #[must_use]
    pub fn new(bucket_secs: u64) -> Self {
        Self {
            bucket_ms: bucket_secs.max(1) * 1000,
            current_start: None,
            host: HashMap::new(),
            containers: HashMap::new(),
        }
    }

    /// Feed one collection cycle. Returns any rollups produced by closing the
    /// previous bucket (empty if the sample falls in the still-open bucket).
    pub fn push(
        &mut self,
        ts_ms: u64,
        host: &HostMetrics,
        containers: &[ContainerMetrics],
    ) -> Flushed {
        let start = ts_ms - (ts_ms % self.bucket_ms);
        let flushed = match self.current_start {
            Some(cur) if start > cur => {
                let out = self.close(cur);
                self.current_start = Some(start);
                out
            }
            None => {
                self.current_start = Some(start);
                Flushed::default()
            }
            _ => Flushed::default(),
        };

        self.host
            .entry(Metric::Cpu)
            .or_default()
            .push(f64::from(host.cpu_percent));
        self.host
            .entry(Metric::Mem)
            .or_default()
            .push(host.mem_used as f64);

        for c in containers {
            if let Some(cpu) = c.cpu_percent {
                self.acc(c, Metric::Cpu).push(cpu);
            }
            if let Some(mem) = c.mem_used {
                self.acc(c, Metric::Mem).push(mem as f64);
            }
        }

        flushed
    }

    fn acc(&mut self, c: &ContainerMetrics, metric: Metric) -> &mut Vec<f64> {
        &mut self
            .containers
            .entry((c.id.clone(), metric))
            .or_insert_with(|| ContainerAcc {
                name: c.name.clone(),
                stack: c.stack.clone(),
                values: Vec::new(),
            })
            .values
    }

    /// Close the bucket starting at `start`, draining all accumulators into
    /// trend rows.
    fn close(&mut self, start: u64) -> Flushed {
        let bucket_secs = self.bucket_ms / 1000;
        let mut out = Flushed::default();

        for (metric, values) in self.host.drain() {
            if let Some(stats) = Stats::from(&values) {
                out.host.push(HostTrend {
                    bucket_start_ms: start,
                    bucket_secs,
                    metric: metric.as_str(),
                    min: stats.min,
                    max: stats.max,
                    median: stats.median,
                    samples: stats.count,
                });
            }
        }

        for ((id, metric), acc) in self.containers.drain() {
            if let Some(stats) = Stats::from(&acc.values) {
                out.containers.push(ContainerTrend {
                    bucket_start_ms: start,
                    bucket_secs,
                    id,
                    name: acc.name,
                    stack: acc.stack,
                    metric: metric.as_str(),
                    min: stats.min,
                    max: stats.max,
                    median: stats.median,
                    samples: stats.count,
                });
            }
        }

        out
    }
}

/// Min, max, median and count over a set of samples.
struct Stats {
    min: f64,
    max: f64,
    median: f64,
    count: u32,
}

impl Stats {
    fn from(values: &[f64]) -> Option<Self> {
        if values.is_empty() {
            return None;
        }
        let mut sorted = values.to_vec();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        Some(Self {
            min: sorted[0],
            max: sorted[sorted.len() - 1],
            median: median_of_sorted(&sorted),
            count: u32::try_from(sorted.len()).unwrap_or(u32::MAX),
        })
    }
}

/// Median of an already-sorted, non-empty slice.
fn median_of_sorted(sorted: &[f64]) -> f64 {
    let n = sorted.len();
    if n % 2 == 1 {
        sorted[n / 2]
    } else {
        f64::midpoint(sorted[n / 2 - 1], sorted[n / 2])
    }
}

#[cfg(test)]
mod tests {
    // Exact float equality is intentional here: inputs are exact integers, so
    // the computed min/max/median are exact and comparable.
    #![allow(clippy::float_cmp)]

    use super::*;
    use crate::model::{ContainerState, HealthState, LoadAverage};

    fn host(cpu: f32, mem: u64) -> HostMetrics {
        HostMetrics {
            cpu_percent: cpu,
            load_avg: LoadAverage {
                one: 0.0,
                five: 0.0,
                fifteen: 0.0,
            },
            mem_total: 1000,
            mem_used: mem,
            cpu_count: 4,
        }
    }

    fn container(id: &str, cpu: Option<f64>) -> ContainerMetrics {
        ContainerMetrics {
            id: id.to_string(),
            name: format!("c-{id}"),
            image: "img".to_string(),
            state: ContainerState::Running,
            status: "Up".to_string(),
            health: HealthState::None,
            stack: Some("stack".to_string()),
            cpu_percent: cpu,
            mem_used: Some(100),
            mem_limit: Some(200),
        }
    }

    #[test]
    fn median_odd_and_even() {
        assert_eq!(median_of_sorted(&[1.0, 2.0, 3.0]), 2.0);
        assert_eq!(median_of_sorted(&[1.0, 2.0, 3.0, 4.0]), 2.5);
        assert_eq!(median_of_sorted(&[5.0]), 5.0);
    }

    #[test]
    fn median_is_robust_against_a_spike() {
        // One huge spike must not drag the median the way a mean would.
        let stats = Stats::from(&[1.0, 1.0, 1.0, 1.0, 1000.0]).unwrap();
        assert_eq!(stats.median, 1.0);
        assert_eq!(stats.max, 1000.0);
        assert_eq!(stats.min, 1.0);
        assert_eq!(stats.count, 5);
    }

    #[test]
    fn no_flush_within_one_bucket() {
        let mut b = Bucketer::new(60); // 60s buckets
        assert!(b.push(60_000, &host(10.0, 100), &[]).is_empty());
        assert!(b.push(90_000, &host(20.0, 200), &[]).is_empty());
        assert!(b.push(119_000, &host(30.0, 300), &[]).is_empty());
    }

    #[test]
    fn flush_on_bucket_boundary_emits_rollups() {
        let mut b = Bucketer::new(60);
        // Bucket [60_000, 120_000): three CPU samples 10/20/30.
        b.push(60_000, &host(10.0, 100), &[container("x", Some(2.0))]);
        b.push(90_000, &host(20.0, 200), &[container("x", Some(8.0))]);
        // Crossing into the next bucket closes the previous one.
        let flushed = b.push(125_000, &host(99.0, 999), &[container("x", Some(5.0))]);
        assert!(!flushed.is_empty());

        let host_cpu = flushed
            .host
            .iter()
            .find(|t| t.metric == "cpu")
            .expect("host cpu trend");
        assert_eq!(host_cpu.min, 10.0);
        assert_eq!(host_cpu.max, 20.0);
        assert_eq!(host_cpu.median, 15.0);
        assert_eq!(host_cpu.samples, 2);
        assert_eq!(host_cpu.bucket_start_ms, 60_000);
        assert_eq!(host_cpu.bucket_secs, 60);

        let cont_cpu = flushed
            .containers
            .iter()
            .find(|t| t.metric == "cpu")
            .expect("container cpu trend");
        assert_eq!(cont_cpu.id, "x");
        assert_eq!(cont_cpu.name, "c-x");
        assert_eq!(cont_cpu.median, 5.0); // median of [2, 8]
    }

    #[test]
    fn missing_cpu_samples_are_skipped() {
        let mut b = Bucketer::new(60);
        // First sample has no CPU yet (None) — must not count.
        b.push(60_000, &host(10.0, 100), &[container("x", None)]);
        b.push(90_000, &host(20.0, 200), &[container("x", Some(4.0))]);
        let flushed = b.push(130_000, &host(0.0, 0), &[]);
        let cont_cpu = flushed
            .containers
            .iter()
            .find(|t| t.metric == "cpu")
            .unwrap();
        assert_eq!(cont_cpu.samples, 1); // only the Some(4.0) sample
        assert_eq!(cont_cpu.median, 4.0);
    }
}
