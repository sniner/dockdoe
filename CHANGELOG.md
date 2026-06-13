# Changelog

All notable, user-facing changes to DockDoe. The format is based on
[Keep a Changelog](https://keepachangelog.com).

## [0.4.1] — 2026-06-13

### Added

- **Web UI**: the app icon now sits at the left of the header bar, before the DockDoe wordmark,
  and a matching favicon appears in the browser tab — both come in a light and a white variant
  that the browser picks by its colour-scheme theme

## [0.4.0] — 2026-06-13

### Added

- **Notifications**: when `DOCKDOE_APPRISE_URL` is set, DockDoe POSTs a message to that
  [Apprise](https://github.com/caronc/apprise-api) endpoint whenever a container's state settles
  into a change — down (`failure`), unhealthy (`warning`), or recovered (`success`). DockDoe only
  sends `{title, body, type}`; which services it reaches (Discord, e-mail, …) is configured in
  Apprise, so no per-service setup or secrets live here. Unset disables notifications
- **`DOCKDOE_NOTIFY_DELAY`** (`--notify-delay-secs`, default 30) sets how long a new state must
  persist before it is reported, swallowing flapping such as restart loops and brief blips

## [0.3.0] — 2026-06-13

### Added

- **History view**: every chart card has an expand button (⤢) that opens a large chart of the
  stored history — median line plus a shaded min–max band, with 1h/6h/24h/7d/30d ranges.
  Ranges beyond the raw retention are served from the 30-day trend rollups, downsampled to a
  chartable density. `#history-cpu-7d`-style URL fragments deep-link into a view
- **Drill-down**: drag a span on any chart — the small live charts or the history view itself —
  to zoom into exactly that window, re-fetched at finer resolution down to raw samples;
  double-click zooms back out step by step. (Dragging on a live chart used to zoom for a
  second and snap back with the next live update)
- **History API**: `GET /api/history/{host,container/{id},stack/{name}}` returns
  median/min/max points, taking `?range=1h|6h|24h|7d|30d` or a free `?since_ms=&until_ms=`
  window
- **Log timestamps**: container log lines now show the Docker daemon's per-line timestamp,
  rendered in the browser's local time so they line up with the chart axes

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
