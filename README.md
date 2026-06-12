# DockDoe

> [!WARNING]
> **Very early version (0.1.0).** DockDoe is brand new and under active
> development. Expect rough edges, bugs, and breaking changes between releases.
> It is not yet battle-tested — use it at your own risk, and don't rely on it as
> your only monitoring just yet. Feedback and bug reports are very welcome.

A single-binary Docker host monitor with an embedded web UI. Shows the vital
metrics of your containers — state, CPU, memory — grouped by compose stack,
with htop-style host stats on top. The dashboard updates live (no full reload):
HTMX swaps server-rendered fragments over SSE, and uPlot draws live charts
seeded from history.

Drill into a container (live CPU/memory charts, facts, logs) or a whole stack
(aggregate charts, the compose.yml, start/stop/restart-all), and start, stop or
restart containers right from the UI.

## Run

```sh
cargo run
# then open http://127.0.0.1:8080

# expose it on the network (reachable from other hosts):
dockdoe --bind 0.0.0.0:8080
```

Requires access to the Docker socket (`/var/run/docker.sock`, or `DOCKER_HOST`).
Host CPU/load/memory are read from `/proc`. Those files (`/proc/meminfo`,
`/proc/stat`, `/proc/loadavg`) are not namespaced, so a container sees the real
host values out of the box — no `/proc` mount or `pid: host` needed.

### With Docker

A prebuilt image is published to GHCR (`ghcr.io/sniner/dockdoe`). The simplest
way to run it is the example [`compose.yml`](compose.yml):

```sh
docker compose up -d
# then open http://127.0.0.1:8080
```

Or directly:

```sh
docker run -d --name dockdoe \
  -p 127.0.0.1:8080:8080 \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v dockdoe-data:/data \
  ghcr.io/sniner/dockdoe:latest
```

Mounting the Docker socket grants full control of the daemon (effectively root
on the host), which DockDoe needs for the start/stop/restart actions.

To show a stack's `compose.yml` in the UI, DockDoe reads the file from the
absolute path the daemon records (the `com.docker.compose.project.config_files`
label, fetched over the socket). That path is a **host** path, so mount the
directory holding your compose projects at the **same** path inside the
container (read-only), e.g. `-v /opt/stacks:/opt/stacks:ro`. Without it the
compose tab just reports that the file can't be read; everything else works.

## Configuration

Run `dockdoe --help` for the full list. Every option is a command-line flag and
also reads from an environment variable; the flag wins when both are set.

| Flag                      | Env var                        | Default          | Meaning                                         |
| ------------------------- | ------------------------------ | ---------------- | ----------------------------------------------- |
| `--bind`                  | `DOCKDOE_BIND`                 | `127.0.0.1:8080` | Web UI bind address (`0.0.0.0:8080` to expose)  |
| `--interval-secs`         | `DOCKDOE_INTERVAL_SECS`        | `3`              | Seconds between metric samples                  |
| `--db-path`               | `DOCKDOE_DB_PATH`              | `dockdoe.sqlite` | SQLite database file                            |
| `--raw-retention-secs`    | `DOCKDOE_RAW_RETENTION_SECS`   | `3600`           | How long raw samples are kept ("point A")       |
| `--trend-bucket-secs`     | `DOCKDOE_TREND_BUCKET_SECS`    | `60`             | Trend rollup window (min/max/median per bucket) |
| `--trend-retention-secs`  | `DOCKDOE_TREND_RETENTION_SECS` | `2592000` (30 d) | How long trend rollups are kept                 |
| `--allowed-hosts`         | `DOCKDOE_ALLOWED_HOSTS`        | *(unset)*        | Host-header allowlist, see below                |
| `--log`                   | `DOCKDOE_LOG`                  | `info`           | Tracing filter (e.g. `dockdoe=debug`)           |

### Request hardening

The start/stop/restart endpoints only accept requests carrying the
`HX-Request` header that htmx sends with every request. A cross-site HTML form
can't set custom headers, so drive-by POSTs from other websites are rejected.

That check can't help against DNS rebinding, where the attacker's page ends up
same-origin. For that, set `--allowed-hosts` (comma-separated, e.g.
`dockhost.lan`): requests whose `Host` header matches neither the list nor a
localhost form (`localhost`, `127.0.0.1`, `::1`) are rejected. Recommended
whenever the UI is exposed beyond localhost.

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
