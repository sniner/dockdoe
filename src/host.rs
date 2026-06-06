//! Host-level metrics (CPU, load average, memory) via sysinfo.
//!
//! These come straight from the host (`/proc`), not the Docker socket, which
//! does not expose them. When DockDoe runs inside a container, the host's
//! `/proc` must be visible for these numbers to reflect the real host.

use sysinfo::{MemoryRefreshKind, System};

use crate::model::{HostMetrics, LoadAverage};

/// Samples host metrics. Holds a persistent [`System`] so that CPU usage —
/// which is a delta between two refreshes — is computed across collection
/// cycles rather than from a single point.
pub struct HostSampler {
    system: System,
}

impl Default for HostSampler {
    fn default() -> Self {
        Self::new()
    }
}

impl HostSampler {
    #[must_use]
    pub fn new() -> Self {
        Self {
            system: System::new(),
        }
    }

    /// Take a fresh sample. The CPU percentage reflects usage since the
    /// previous call (the first call after construction reads ~0%).
    pub fn sample(&mut self) -> HostMetrics {
        self.system.refresh_cpu_usage();
        self.system
            .refresh_memory_specifics(MemoryRefreshKind::nothing().with_ram());

        let load = System::load_average();
        let mem_total = self.system.total_memory();
        let mem_available = self.system.available_memory();

        HostMetrics {
            cpu_percent: self.system.global_cpu_usage(),
            load_avg: LoadAverage {
                one: load.one,
                five: load.five,
                fifteen: load.fifteen,
            },
            mem_total,
            mem_used: mem_total.saturating_sub(mem_available),
            cpu_count: self.system.cpus().len(),
        }
    }
}
