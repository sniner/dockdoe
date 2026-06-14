# DockDoe

> [!WARNING]
> **Early version (0.x).** DockDoe is young and under active development.
> Expect rough edges, bugs, and breaking changes between releases (see
> [CHANGELOG.md](CHANGELOG.md)). It is not yet battle-tested — use it at your
> own risk, and don't rely on it as your only monitoring just yet. Feedback
> and bug reports are very welcome.

A single-binary Docker host monitor with an embedded web UI. Shows the vital
metrics of your containers — state, CPU, memory, network and disk I/O — grouped
by compose stack. The dashboard updates live (no full reload): HTMX swaps
server-rendered fragments over SSE, and uPlot draws live charts seeded from
history.

Drill into a container (live CPU/memory charts, facts, logs) or a whole stack
(aggregate charts, the compose.yml, start/stop/restart-all), and start, stop or
restart containers right from the UI. Point it at one Docker host or
[several](#multiple-hosts) — local socket or remote socket proxies.

## Run

```sh
cargo run
# then open http://127.0.0.1:8080

# expose it on the network (reachable from other hosts):
dockdoe --bind 0.0.0.0:8080
```

Requires access to the Docker socket (`/var/run/docker.sock`, or `DOCKER_HOST`).
To watch several hosts, or a remote one, see [Multiple hosts](#multiple-hosts).

### With Docker

A prebuilt image is published to GHCR (`ghcr.io/sniner/dockdoe`), multi-arch for
`linux/amd64` and `linux/arm64` — so it runs on a 64-bit Raspberry Pi (3/4/5,
Zero 2) just as well as on a regular server; Docker picks the right variant
automatically. The simplest way to run it is the example
[`compose.yml`](compose.yml):

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
| `--config`                | `DOCKDOE_CONFIG`               | *(unset)*        | Path to a multi-host `config.toml`, see below   |
| `--bind`                  | `DOCKDOE_BIND`                 | `127.0.0.1:8080` | Web UI bind address (`0.0.0.0:8080` to expose)  |
| `--interval-secs`         | `DOCKDOE_INTERVAL_SECS`        | `3`              | Seconds between metric samples                  |
| `--db-path`               | `DOCKDOE_DB_PATH`              | `dockdoe.sqlite` | SQLite database file                            |
| `--raw-retention-secs`    | `DOCKDOE_RAW_RETENTION_SECS`   | `3600`           | How long raw samples are kept ("point A")       |
| `--trend-bucket-secs`     | `DOCKDOE_TREND_BUCKET_SECS`    | `60`             | Trend rollup window (min/max/median per bucket) |
| `--trend-retention-secs`  | `DOCKDOE_TREND_RETENTION_SECS` | `2592000` (30 d) | How long trend rollups are kept                 |
| `--allowed-hosts`         | `DOCKDOE_ALLOWED_HOSTS`        | *(unset)*        | Host-header allowlist, see below                |
| `--auth-user`             | `DOCKDOE_AUTH_USER`            | *(unset)*        | Web UI login username, see below                |
| `--auth-password`         | `DOCKDOE_AUTH_PASSWORD`        | *(unset)*        | Web UI login password, see below                |
| `--cookie-secure`         | `DOCKDOE_COOKIE_SECURE`        | `false`          | Mark the session cookie `Secure` (HTTPS only)   |
| `--port-host`             | `DOCKDOE_PORT_HOST`            | *(unset)*        | Host the port pills link to, see below          |
| `--apprise-url`           | `DOCKDOE_APPRISE_URL`          | *(unset)*        | Apprise endpoint for notifications, see below    |
| `--notify-delay-secs`     | `DOCKDOE_NOTIFY_DELAY`         | `30`             | Seconds a state must persist before notifying    |
| `--log`                   | `DOCKDOE_LOG`                  | `info`           | Tracing filter (e.g. `dockdoe=debug`)           |

### Multiple hosts

Without a config file DockDoe monitors a single local host (the socket above).
To watch several hosts — or a remote one — point `--config` / `DOCKDOE_CONFIG`
at a `config.toml` whose `[[host]]` entries each describe one Docker host:

```toml
# Global options (same names as the env vars, minus the DOCKDOE_ prefix) may go
# here too; anything omitted falls back to the flags/environment.
bind = "0.0.0.0:8080"

[[host]]
name   = "local"                       # display name + URL slug, must be unique
docker = "unix:///var/run/docker.sock" # the local socket

[[host]]
name        = "nas"
docker      = "tcp://nas.lan:2375"     # a linuxserver/tecnativa socket proxy
public_host = "nas.lan"                # where this host's published ports are reachable

[[host]]
name   = "prod"
docker = "https://dockerproxy.example:2376"  # TLS-fronted proxy
# tls_ca       = "/certs/ca.pem"        # trust a private CA (self-signed proxy)
# tls_insecure = true                   # or skip verification entirely
```

Each host gets its own dashboard under `/host/<name>`; `/` lists them (and
redirects straight through when there's only one). Per-host keys:

- **`docker`** — the endpoint: `unix:///path` (local socket), `tcp://host:port`
  or `http://host:port` (a plain socket proxy, e.g.
  [`linuxserver/socket-proxy`](https://docs.linuxserver.io/images/docker-socket-proxy/),
  best kept on a trusted network or behind Tailscale), or `https://host:port`
  (TLS — verified against the built-in roots, plus `tls_ca`, or `tls_insecure`)
- **`public_host`** — the host the published-port pills link to (see
  [Port links](#port-links)); defaults to the endpoint's host for `tcp`/`https`
- **`tls_ca`** — a PEM CA certificate to trust for an `https` endpoint
- **`tls_insecure`** — skip TLS verification for an `https` endpoint (handy for a
  self-signed reverse proxy; prefer `tls_ca` when you can)

Remote hosts have two limits: the **compose.yml** tab only works for a host
whose files are on the machine DockDoe runs on, and a proxy that denies actions
or exec (returns 403) makes that host **read-only** — its action buttons and
terminal are disabled automatically.

### Request hardening

The start/stop/restart endpoints only accept requests carrying the
`HX-Request` header that htmx sends with every request. A cross-site HTML form
can't set custom headers, so drive-by POSTs from other websites are rejected.

That check can't help against DNS rebinding, where the attacker's page ends up
same-origin. For that, set `--allowed-hosts` (comma-separated, e.g.
`dockhost.lan`): requests whose `Host` header matches neither the list nor a
localhost form (`localhost`, `127.0.0.1`, `::1`) are rejected. Recommended
whenever the UI is exposed beyond localhost.

### Authentication

Set both `--auth-user` / `DOCKDOE_AUTH_USER` and `--auth-password` /
`DOCKDOE_AUTH_PASSWORD` to put the web UI behind a login. There is one credential
pair — no user database, no sign-up. Leave both unset to keep the UI open;
setting only one is treated as a misconfiguration and refuses to start.

Logging in sets a session cookie, so the credentials are sent only once (not on
every request like HTTP Basic Auth), and you can log out again from the header.
The cookie is a signed token — nothing is stored server-side — valid for 30 days,
and it survives restarts and upgrades, so you stay logged in. The signature uses
a random secret generated once and kept in the database; delete the database (or
its `meta` row) to invalidate all sessions.

Behind a TLS-terminating reverse proxy, also set `--cookie-secure` /
`DOCKDOE_COOKIE_SECURE=true` so the cookie is only ever sent over HTTPS. Leave it
off for plain-http access on a trusted LAN, where it would otherwise stop the
cookie from being sent at all.

### Terminal

Each running container's detail page has a **Terminal** panel: click *Connect* to
open an interactive shell (`docker exec`) inside the container. The command
defaults to `/bin/sh`; change it to e.g. `bash` for images that ship it. The `⛶`
button toggles fullscreen. The session opens only on demand and ends when you
disconnect or leave the page. Put the UI behind [authentication](#authentication)
before exposing it — this is a real shell into your containers.

The terminal uses a **WebSocket**. Direct access needs nothing extra, but behind
a reverse proxy you must allow the WebSocket upgrade for it to work. For example,
in Nginx Proxy Manager tick *Websockets Support* on the proxy host; in a plain
nginx config, forward the upgrade headers:

```nginx
location / {
    proxy_pass http://dockdoe:8080;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
}
```

### Port links

Each published container port shows as a pill; the published ones are links that
open `http://<host>:<port>` in a new tab. By default `<host>` is whatever host
you're browsing DockDoe at — exactly right when you browse it directly on the
Docker host.

Behind a reverse proxy that breaks down: the browsing host is the proxy (on
:443), where the container ports aren't open, so the links would point nowhere.
Set the host the ports are really reachable at — `--port-host` /
`DOCKDOE_PORT_HOST` for the single local host, or a host's `public_host` in the
[config](#multiple-hosts) — to:

- **an IP or hostname** (e.g. `192.168.1.50`) — the links point there instead, so
  they still work when the Docker host is reachable directly even though the UI is
  served through a proxy.
- **`off`** — render the ports as plain pills with no links, for setups reachable
  only through the proxy where no direct `host:port` link would work from your
  browser.

For a remote host, `public_host` defaults to the endpoint URL's host, so a
`tcp://nas.lan:2375` host already links its ports to `nas.lan` without setting
it. Leave it unset for the direct-access local case.

### Notifications

Set `--apprise-url` / `DOCKDOE_APPRISE_URL` to an
[Apprise](https://github.com/caronc/apprise-api) endpoint and DockDoe sends a
message whenever a container's state settles into a change: down (`failure`),
unhealthy (`warning`), or recovered (`success`). DockDoe only ever POSTs
`{title, body, type}` to that one URL — which services it fans out to (Discord,
e-mail, Telegram, …) is configured in Apprise, so no per-service setup or
secrets live in DockDoe. Leave it unset to disable notifications entirely.

Point it at a stateful config key, e.g. `https://apprise.example/notify/<key>`;
the target services then live under that key in Apprise.

To avoid alert storms from flapping, a new state must persist for
`--notify-delay-secs` / `DOCKDOE_NOTIFY_DELAY` (default 30) before it is
reported — a container that restarts and recovers within that window stays
quiet. The state seen at startup is adopted as the baseline, so DockDoe doesn't
fire a burst when it boots.

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
