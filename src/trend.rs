//! Trend rollups: min/max/median over fixed time buckets.
//!
//! Mirrors the `Entwurf.md` design. Samples are accumulated into a bucket
//! aligned to wall-clock (`bucket_secs`). As soon as a sample arrives that
//! belongs to a *later* bucket, the current bucket is closed and its rollups
//! are emitted — trends are computed immediately when the window completes, not
//! lazily when raw data ages out. Median is preferred over mean: it is robust
//! against spikes, and we keep `max` for the worst case anyway.

use std::collections::HashMap;

use crate::model::ContainerMetrics;
use crate::store::{ContainerTrend, Metric};

/// Trend rows produced when a bucket closes. Empty when no boundary was crossed.
#[derive(Debug, Default)]
pub struct Flushed {
    pub containers: Vec<ContainerTrend>,
}

impl Flushed {
    pub fn is_empty(&self) -> bool {
        self.containers.is_empty()
    }
}

/// Accumulates samples and emits rollups at bucket boundaries.
pub struct Bucketer {
    bucket_ms: u64,
    /// Start (Unix ms, bucket-aligned) of the bucket currently filling.
    current_start: Option<u64>,
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
            containers: HashMap::new(),
        }
    }

    /// Feed one collection cycle. Returns any rollups produced by closing the
    /// previous bucket (empty if the sample falls in the still-open bucket).
    pub fn push(&mut self, ts_ms: u64, containers: &[ContainerMetrics]) -> Flushed {
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

        for c in containers {
            if let Some(cpu) = c.cpu_percent {
                self.acc(c, Metric::Cpu).push(cpu);
            }
            if let Some(mem) = c.mem_used {
                self.acc(c, Metric::Mem).push(mem as f64);
            }
            if let Some(rx) = c.net_rx_bps {
                self.acc(c, Metric::NetRx).push(rx);
            }
            if let Some(tx) = c.net_tx_bps {
                self.acc(c, Metric::NetTx).push(tx);
            }
            if let Some(read) = c.disk_read_bps {
                self.acc(c, Metric::DiskRead).push(read);
            }
            if let Some(write) = c.disk_write_bps {
                self.acc(c, Metric::DiskWrite).push(write);
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
    use crate::model::{ContainerState, HealthState};

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
            net_rx_bps: None,
            net_tx_bps: None,
            disk_read_bps: None,
            disk_write_bps: None,
            ports: Vec::new(),
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
        assert!(b.push(60_000, &[]).is_empty());
        assert!(b.push(90_000, &[]).is_empty());
        assert!(b.push(119_000, &[]).is_empty());
    }

    #[test]
    fn flush_on_bucket_boundary_emits_rollups() {
        let mut b = Bucketer::new(60);
        // Bucket [60_000, 120_000): two CPU samples 2/8 for container "x".
        b.push(60_000, &[container("x", Some(2.0))]);
        b.push(90_000, &[container("x", Some(8.0))]);
        // Crossing into the next bucket closes the previous one.
        let flushed = b.push(125_000, &[container("x", Some(5.0))]);
        assert!(!flushed.is_empty());

        let cont_cpu = flushed
            .containers
            .iter()
            .find(|t| t.metric == "cpu")
            .expect("container cpu trend");
        assert_eq!(cont_cpu.id, "x");
        assert_eq!(cont_cpu.name, "c-x");
        assert_eq!(cont_cpu.median, 5.0); // median of [2, 8]
        assert_eq!(cont_cpu.bucket_start_ms, 60_000);
        assert_eq!(cont_cpu.bucket_secs, 60);
    }

    #[test]
    fn missing_cpu_samples_are_skipped() {
        let mut b = Bucketer::new(60);
        // First sample has no CPU yet (None) — must not count.
        b.push(60_000, &[container("x", None)]);
        b.push(90_000, &[container("x", Some(4.0))]);
        let flushed = b.push(130_000, &[]);
        let cont_cpu = flushed
            .containers
            .iter()
            .find(|t| t.metric == "cpu")
            .unwrap();
        assert_eq!(cont_cpu.samples, 1); // only the Some(4.0) sample
        assert_eq!(cont_cpu.median, 4.0);
    }
}
