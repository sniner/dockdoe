# DockDoe

A single-binary Docker host monitor with an embedded web UI. Shows the vital
metrics of your containers — state, CPU, memory — grouped by compose stack,
with htop-style host stats on top. The dashboard updates live (no full reload):
HTMX swaps server-rendered fragments over SSE, and uPlot draws live host charts
seeded from history.

> Status: **milestone 2** — live streaming dashboard with SQLite-backed history
> and trend rollups. Next: per-stack detail pages with logs, compose.yml, and
> start/stop/restart actions.

## Run

```sh
cargo run
# then open http://127.0.0.1:8080
```

Requires access to the Docker socket (`/var/run/docker.sock`, or `DOCKER_HOST`).
Host CPU/load/memory are read from the host `/proc`; when running DockDoe inside
a container, mount the host `/proc` for those to reflect the real host.

## Configuration

| Env var                       | Default            | Meaning                                         |
| ----------------------------- | ------------------ | ----------------------------------------------- |
| `DOCKDOE_BIND`                | `127.0.0.1:8080`   | Web UI bind address                             |
| `DOCKDOE_INTERVAL_SECS`       | `3`                | Seconds between metric samples                  |
| `DOCKDOE_DB_PATH`             | `dockdoe.sqlite`   | SQLite database file                            |
| `DOCKDOE_RAW_RETENTION_SECS`  | `3600`             | How long raw samples are kept ("point A")       |
| `DOCKDOE_TREND_BUCKET_SECS`   | `60`               | Trend rollup window (min/max/median per bucket) |
| `DOCKDOE_TREND_RETENTION_SECS`| `2592000` (30 d)   | How long trend rollups are kept                 |
| `DOCKDOE_LOG`                 | `info`             | Tracing filter (e.g. `debug`)                   |

## Data model

Two layers, mirroring a Zabbix-style approach:

- **Raw samples** — every collected value, kept until "point A"
  (`DOCKDOE_RAW_RETENTION_SECS`), then pruned.
- **Trends** — min/max/median rollups per time bucket, computed the moment a
  bucket completes (not lazily as raw data ages out). Median is preferred over
  mean for robustness against spikes; `max` is kept for the worst case. Trends
  have their own, longer retention and store the container name and stack
  alongside the id, so history survives a `docker compose down && up`.

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
