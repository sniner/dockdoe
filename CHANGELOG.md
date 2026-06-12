# Changelog

All notable, user-facing changes to DockDoe. The format is based on
[Keep a Changelog](https://keepachangelog.com).

## [0.2.1] — 2026-06-12

### Added

- **Charts** show the hovered point's time and value in the chart's title row
  ("21:43:05 · 3.2%") — the cursor line finally tells you something

### Fixed

- **Chart gaps** left by a dropped live connection (DockDoe restarting during a container
  update, a network blip) are now backfilled on reconnect; previously only tab switches and
  navigation triggered the backfill
- **Stretches without data** (e.g. DockDoe downtime) are rendered as holes in the line instead
  of a straight bridge that pretended measurements existed

## [0.2.0] — 2026-06-12

### Breaking changes

- **Action endpoints** (`POST /api/container/{id}/{action}`,
  `POST /api/stack/{name}/{action}`) now require the `HX-Request: true` header as a CSRF guard
  and answer `403` without it. The web UI is unaffected (htmx always sends it); direct callers
  (curl, scripts) must add `-H "HX-Request: true"`

### Added

- **Host allowlist**: new `--allowed-hosts` / `DOCKDOE_ALLOWED_HOSTS` option (comma-separated
  hostnames) rejects requests with a foreign `Host` header — a guard against DNS rebinding.
  Off by default; localhost forms always pass. Recommended when the UI is exposed beyond
  localhost
- **Header bar** shows a live container tally: total, running, and — when present — exited,
  other, and unhealthy counts
- **Error toast**: failed actions and fetches now surface the server's error message in the UI
  instead of failing silently
- **Chart backfill endpoints** (`GET /api/metrics/host`, `…/container/{id}`, `…/stack/{name}`,
  with `?since_ms=`) return recent metric points as JSON; the UI uses them to close chart gaps

### Changed

- **Dashboard charts** no longer show a gap after returning from a detail page, switching tabs,
  or a suspend — missed points are backfilled before the live stream reconnects
- **Container tables** align their columns across all stack sections; long names and image
  references are ellipsized
- **CPU bar** scales to the host's total capacity (`CPUs × 100 %`) instead of a fixed 100 %,
  matching the host CPU chart; the printed percentage is unchanged
- **Charts after a restart** no longer start with a bogus 0 % host-CPU dip; the first collected
  sample is discarded as a priming step (costs one interval of startup delay)

### Fixed

- **`--interval-secs 0`** is rejected at startup instead of silently killing the metrics
  collector while the web UI kept running

## [0.1.1] — 2026-06-06

### Changed

- **Stack detail charts** are seeded from trend history instead of starting empty on every
  visit

## [0.1.0] — 2026-06-06

First release: live dashboard (host stats, containers grouped by compose stack), container and
stack detail pages with charts, logs and compose.yml views, start/stop/restart actions,
SQLite-backed history with trend rollups, single static binary and scratch-based Docker image.
