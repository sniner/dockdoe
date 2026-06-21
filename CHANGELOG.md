# Changelog

All notable, user-facing changes to DockDoe. The format is based on
[Keep a Changelog](https://keepachangelog.com).

## [0.7.0] — 2026-06-21

### Breaking changes

- **Host metrics removed**: the htop-style CPU / load / memory bar in the header is gone, along
  with its two dashboard charts. They can't be shown meaningfully for a remote Docker host (they
  come from the local machine's `/proc`, not the Docker API), so they were dropped for every host
- **URLs are host-scoped**: every page now lives under `/host/{host}/…` (e.g.
  `/host/local/container/abc`); `/` is a host chooser that redirects straight to the only host when
  just one is configured. Old bookmarks to `/container/…` / `/stack/…` need the `/host/{host}` prefix

### Added

- **Multi-host monitoring**: a new `config.toml` (via `--config` / `DOCKDOE_CONFIG`) lists the
  Docker hosts to watch in a `[[host]]` array, each with a `name`, a `docker` endpoint, and optional
  `public_host` / `tls_ca` / `tls_insecure`. A header switcher moves between them. Without a config
  file DockDoe still monitors a single local host configured from the existing flags
- **Remote hosts via socket proxy**: connect over `tcp://` / `http://` (e.g. a `linuxserver/socket-
  proxy`) or `https://` with server-certificate verification — trusting the built-in roots, plus an
  optional private CA (`tls_ca`), or skipping verification (`tls_insecure`) for self-signed proxies
- **Read-only hosts**: when a proxy denies an action or exec (HTTP 403), that host is marked
  read-only — its action buttons disable and the terminal shows a notice, no configuration needed

### Changed

- **Port links** are now resolved per host: an explicit `public_host` wins, otherwise the host from
  a `tcp`/`https` endpoint URL, otherwise the browsing host (the local socket). `--port-host` /
  `DOCKDOE_PORT_HOST` still sets it for the single-host fallback
- **Apprise notifications** now name the host a container is on (e.g. "… on host 'prod' is down")
- **CPU bars** scale to the host's CPU count reported by the Docker daemon (`docker info`) rather
  than the machine DockDoe runs on

### Fixed

- **High idle CPU from retention pruning**: DockDoe pruned aged-out data on every sample cycle, and
  each prune scanned its whole table — no index led with the time column the prune filters on. On a
  long-retained trend table (millions of rows) this dominated CPU: an otherwise idle monitor sat at
  ~10% of a core, almost all of it walking rows it deleted none of. The prune columns are now
  indexed (so a prune seeks to the cutoff instead of scanning), and pruning is batched onto a
  separate, slower cadence — configurable via `--prune-interval-secs` / `DOCKDOE_PRUNE_INTERVAL_SECS`
  (default 1 hour) — instead of running every few seconds. Both apply to existing databases on the
  next start
- **In-place database upgrade**: the on-disk schema gained a host dimension, but a database from an
  earlier build was never migrated — opening it aborted at startup (`no such column: host`) once the
  host-aware indexes were added. The schema is now brought up to date in place: the `host` column is
  backfilled (existing samples are attributed to the synthesised `local` host) before the indexes
  are built, so upgrades keep their history instead of needing the old `dockdoe.sqlite` deleted
- **Login fields and password managers**: the login form's username and password fields now carry
  `id` attributes and associated (visually hidden) labels, so password managers like Proton Pass
  reliably detect and fill them

## [0.6.4] — 2026-06-14

### Fixed

- **Port links behind a reverse proxy**: the published-port pills linked to the
  host the browser was pointed at — which, behind a proxy, is the proxy, where the
  container ports aren't open, so the links went nowhere. The new `--port-host` /
  `DOCKDOE_PORT_HOST` sets the host the ports are actually reachable at (e.g. the
  Docker host's LAN address); set it to `off` to drop the links entirely for
  proxy-only setups. Unset keeps the previous behaviour, right for browsing
  DockDoe directly on the Docker host

## [0.6.3] — 2026-06-14

### Added

- **aarch64 release binary**: each release now also ships a standalone static `aarch64-linux-musl`
  binary alongside the `x86_64` one, for running directly on a 64-bit Raspberry Pi or other ARM
  host without Docker

### Changed

- The release pipeline now cross-compiles the binaries and assembles the multi-arch image from
  them, instead of building the arm64 image under QEMU emulation — same image, much faster builds

## [0.6.2] — 2026-06-13

### Fixed

- **Drag-to-history on the I/O charts**: dragging a range on the small Network or Disk I/O charts now
  opens the history overlay for that window, like the CPU and memory charts — instead of zooming the
  chart's axes and snapping back on the next live update

## [0.6.1] — 2026-06-13

### Added

- **arm64 image**: the published container image is now multi-arch — `linux/arm64` alongside
  `linux/amd64` — so it runs on a 64-bit Raspberry Pi (3/4/5, Zero 2) and other ARM hosts. Docker
  pulls the matching variant automatically

## [0.6.0] — 2026-06-13

### Added

- **Authentication**: set `DOCKDOE_AUTH_USER` and `DOCKDOE_AUTH_PASSWORD` to put the web UI behind
  a login (one credential pair, no user database). Credentials are sent once, not on every request:
  a successful login sets a signed, stateless session cookie that survives restarts and upgrades, so
  you stay logged in, with a logout link in the header. Set `DOCKDOE_COOKIE_SECURE=true` behind a
  TLS proxy. Both unset keeps the UI open as before
- **Container terminal**: each running container's detail page has a Terminal panel that opens an
  interactive shell (`docker exec`, `/bin/sh` by default) inside the container over a WebSocket,
  with a fullscreen toggle. It connects on demand and is gated by authentication when that is on.
  Behind a reverse proxy, the WebSocket upgrade must be allowed (see the README)

### Fixed

- **Graceful shutdown**: DockDoe now handles `SIGTERM` (what `docker stop` sends) and `SIGINT`,
  and exits at once. Running as PID 1 in a container, the process got no default action for these
  signals, so `docker stop` was ignored and fell back to `SIGKILL` after its ~10 s grace period;
  the container now stops in well under a second

## [0.5.1] — 2026-06-13

### Changed

- **Port pills** drop the leading colon: a chip now reads `3030` (and `3030 → 8000` on the detail
  page) instead of `:3030`. The pill styling and tooltip already mark it as a port

## [0.5.0] — 2026-06-13

### Added

- **Exposed ports**: containers now show the ports they expose. The detail page lists every port
  after the state badge — published ones as blue links that open `http://<host>:<port>` in a new
  tab, internal-only ones as muted pills. The dashboard and stack tables show the first two
  published ports under the state badge, collapsing any remainder into a `+N` chip
- **Network & disk I/O charts**: container and stack detail pages now show a Network chart (rx/tx)
  and a Disk I/O chart (read/write) alongside CPU and memory, as live byte-per-second rates
  derived from the Docker stats counters. Stack charts sum the rates across the stack's members.
  Their expand buttons open the history overlay too, with each direction drawn as a median line
  plus its own min–max band

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
