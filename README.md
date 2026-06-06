# DockDoe

A single-binary Docker host monitor with an embedded web UI. Shows the vital
metrics of your containers — state, CPU, memory — grouped by compose stack,
with htop-style host stats on top.

> Status: **milestone 1** — live dashboard snapshot. Persistence (SQLite trend
> rollups) and live SSE streaming are next.

## Run

```sh
cargo run
# then open http://127.0.0.1:8080
```

Requires access to the Docker socket (`/var/run/docker.sock`, or `DOCKER_HOST`).
Host CPU/load/memory are read from the host `/proc`; when running DockDoe inside
a container, mount the host `/proc` for those to reflect the real host.

## Configuration

| Env var                 | Default          | Meaning                          |
| ----------------------- | ---------------- | -------------------------------- |
| `DOCKDOE_BIND`          | `127.0.0.1:8080` | Web UI bind address              |
| `DOCKDOE_INTERVAL_SECS` | `3`              | Seconds between metric samples   |
| `DOCKDOE_LOG`           | `info`           | Tracing filter (e.g. `debug`)    |

## How CPU% is computed

The Docker stats API reports raw cumulative CPU counters, not a percentage.
DockDoe computes it from the delta between two samples:

```
cpu% = (cpu_delta / system_delta) * online_cpus * 100
```

using its own previous sample (not the API's zeroed `precpu_stats` on a one-shot
read), so the delta spans exactly one collection interval. Verified against
`docker stats`: a CPU-bound container reads 99.9% vs Docker's 99.96%.

## License

DockDoe is free software, licensed under the GNU General Public License,
version 3 or (at your option) any later version. See [LICENSE](LICENSE) for the
full text.

Copyright © 2026 Stefan Schönberger.
