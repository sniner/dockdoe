// Live updates over a single SSE connection per page. One EventSource carries
// three named events:
//   header     - HTML for the host header (swapped into #host-header)
//   containers - HTML for the container table (swapped into #containers)
//   metrics    - a JSON metric point fed to the uPlot charts
// Using one connection keeps us well under the browser's per-host HTTP/1.1
// connection limit (~6), which two long-lived SSE streams per page could
// otherwise exhaust when several tabs are open.
(function () {
  "use strict";

  var MAX_POINTS = 1200; // cap memory; at a few seconds/sample this is hours

  var cfg = document.getElementById("seed-data");
  var liveUrl = (cfg && cfg.getAttribute("data-live-url")) || "/events";
  var backfillUrl = cfg && cfg.getAttribute("data-backfill-url");

  function readSeed() {
    if (!cfg) return [];
    try {
      return JSON.parse(cfg.textContent || "[]");
    } catch (e) {
      return [];
    }
  }

  // --- Charts (optional: only if the page has chart containers) -------------

  var seed = readSeed();
  var stroke = "#4f9cf9";
  var grid = { stroke: "#2c313c", width: 1 };
  var axisStyle = { stroke: "#8b93a1", grid: grid, ticks: grid, size: 40 };

  function chartOpts(el, label, fmt, readout) {
    return {
      width: el.clientWidth || 320,
      height: 110,
      // Drag selects but never zooms here: uPlot's own zoom would be undone
      // by the next SSE point (setData resets the scales). The selection is
      // handed to history.js instead, which opens the overlay on that window.
      cursor: { y: false, drag: { x: true, y: false, setScale: false } },
      legend: { show: false },
      scales: { x: { time: true } },
      series: [
        {},
        { label: label, stroke: stroke, width: 1.5, fill: "rgba(79,156,249,0.12)",
          value: function (u, v) { return v == null ? "--" : fmt(v); } },
      ],
      axes: [
        Object.assign({}, axisStyle, { size: 30, values: timeFmt }),
        Object.assign({}, axisStyle, {
          size: 52, // room for labels like "378M"; the default 40 clipped them
          values: function (u, vals) { return vals.map(fmt); },
        }),
      ],
      hooks: {
        // The legend is hidden, so surface the hovered point's time and value
        // in the chart's title row instead ("21:43:05 · 3.2%").
        setCursor: [function (u) {
          if (!readout) return;
          var i = u.cursor.idx;
          if (i == null || u.data[0][i] == null) {
            readout.textContent = "";
            return;
          }
          var v = u.data[1][i];
          readout.textContent =
            timeStr(u.data[0][i]) + " · " + (v == null ? "--" : fmt(v));
        }],
        // A completed drag selection drills into the history overlay.
        setSelect: [function (u) {
          if (u.select.width <= 0) return;
          var sinceMs = Math.round(u.posToVal(u.select.left, "x") * 1000);
          var untilMs = Math.round(u.posToVal(u.select.left + u.select.width, "x") * 1000);
          u.setSelect({ left: 0, top: 0, width: 0, height: 0 }, false);
          document.dispatchEvent(new CustomEvent("dockdoe:drill", {
            detail: {
              metric: el.id === "chart-mem" ? "mem" : "cpu",
              sinceMs: sinceMs,
              untilMs: untilMs,
            },
          }));
        }],
      },
    };
  }

  // uPlot's default time axis shows only the second component (":ss") when all
  // ticks fall in the same minute, hiding the hour/minute. Always show HH:MM.
  var pad = function (n) { return n < 10 ? "0" + n : "" + n; };
  var timeStr = function (s) {
    var d = new Date(s * 1000);
    return pad(d.getHours()) + ":" + pad(d.getMinutes()) + ":" + pad(d.getSeconds());
  };
  var timeFmt = function (u, splits) { return splits.map(timeStr); };

  var cpuFmt = function (v) { return v.toFixed(0) + "%"; };
  var memFmt = function (v) {
    return v >= 1024 ? (v / 1024).toFixed(1) + "G" : v.toFixed(0) + "M";
  };
  // Bytes/second, for the network and disk-I/O charts.
  var rateFmt = function (v) {
    if (v == null) return "--";
    if (v >= 1048576) return (v / 1048576).toFixed(1) + " MB/s";
    if (v >= 1024) return (v / 1024).toFixed(1) + " KB/s";
    return Math.round(v) + " B/s";
  };

  var strokeOut = "#3fb950"; // tx / write line (green); rx / read reuses `stroke`

  // A dual-line chart (rx+tx, or read+write). Unlike the CPU/memory charts
  // these are live-only: no drag-to-history hook, since the I/O metrics have no
  // history overlay yet. The hovered values surface in the card's readout row,
  // labelled to match the static legend.
  function ioChartOpts(el, readout, inLabel, outLabel) {
    // Drag drills into history like the CPU/memory charts; without
    // setScale:false uPlot would instead zoom the axes, only to be reset by the
    // next SSE point. The drill metric ("net"/"disk") comes from the element id.
    var metric = el.id === "chart-disk" ? "disk" : "net";
    return {
      width: el.clientWidth || 320,
      height: 110,
      cursor: { y: false, drag: { x: true, y: false, setScale: false } },
      legend: { show: false },
      scales: { x: { time: true } },
      series: [
        {},
        { stroke: stroke, width: 1.5 },
        { stroke: strokeOut, width: 1.5 },
      ],
      axes: [
        Object.assign({}, axisStyle, { size: 30, values: timeFmt }),
        Object.assign({}, axisStyle, {
          size: 62, // room for "1.5 MB/s"
          values: function (u, vals) { return vals.map(rateFmt); },
        }),
      ],
      hooks: {
        setCursor: [function (u) {
          if (!readout) return;
          var i = u.cursor.idx;
          if (i == null || u.data[0][i] == null) {
            readout.textContent = "";
            return;
          }
          readout.textContent =
            timeStr(u.data[0][i]) + " · " +
            inLabel + " " + rateFmt(u.data[1][i]) + " · " +
            outLabel + " " + rateFmt(u.data[2][i]);
        }],
        // A completed drag selection drills into the history overlay.
        setSelect: [function (u) {
          if (u.select.width <= 0) return;
          var sinceMs = Math.round(u.posToVal(u.select.left, "x") * 1000);
          var untilMs = Math.round(u.posToVal(u.select.left + u.select.width, "x") * 1000);
          u.setSelect({ left: 0, top: 0, width: 0, height: 0 }, false);
          document.dispatchEvent(new CustomEvent("dockdoe:drill", {
            detail: { metric: metric, sinceMs: sinceMs, untilMs: untilMs },
          }));
        }],
      },
    };
  }

  var cpuEl = document.getElementById("chart-cpu");
  var memEl = document.getElementById("chart-mem");
  var netEl = document.getElementById("chart-net");
  var diskEl = document.getElementById("chart-disk");
  var cpuChart = null;
  var memChart = null;
  var netChart = null;
  var diskChart = null;

  // A straight line between two points that are minutes apart pretends there
  // was data where there was none (DockDoe restarted, host suspended, page
  // away for too long). Break the line instead: insert a null sample into any
  // spacing wider than ~4 typical sample intervals — uPlot renders null as a
  // hole. The threshold adapts to the series: raw samples arrive every few
  // seconds, stack trend buckets a minute apart.
  var gapSecs = 15;

  function gapFromSpacing(times) {
    if (times.length < 3) return gapSecs;
    var deltas = [];
    for (var i = 1; i < times.length; i++) deltas.push(times[i] - times[i - 1]);
    deltas.sort(function (a, b) { return a - b; });
    return Math.max(15, deltas[Math.floor(deltas.length / 2)] * 4);
  }

  if (cpuEl && memEl && typeof uPlot !== "undefined") {
    var rawTs = seed.map(function (p) { return Math.floor(p.ts_ms / 1000); });
    gapSecs = gapFromSpacing(rawTs);

    var ts = [];
    var cpu = [];
    var mem = [];
    // The I/O series share the timeline and gap handling, so they are seeded in
    // the same pass. They stay empty on pages without the net/disk charts (the
    // dashboard), where the seed carries no I/O fields anyway.
    var netRx = [], netTx = [], diskRead = [], diskWrite = [];
    for (var i = 0; i < seed.length; i++) {
      var t = rawTs[i];
      if (ts.length && t - ts[ts.length - 1] > gapSecs) {
        ts.push(ts[ts.length - 1] + 1);
        cpu.push(null); mem.push(null);
        netRx.push(null); netTx.push(null);
        diskRead.push(null); diskWrite.push(null);
      }
      ts.push(t);
      cpu.push(seed[i].cpu_percent);
      mem.push(seed[i].mem_used != null ? seed[i].mem_used / 1048576 : null); // MiB
      netRx.push(seed[i].net_rx != null ? seed[i].net_rx : null);
      netTx.push(seed[i].net_tx != null ? seed[i].net_tx : null);
      diskRead.push(seed[i].disk_read != null ? seed[i].disk_read : null);
      diskWrite.push(seed[i].disk_write != null ? seed[i].disk_write : null);
    }
    cpuChart = new uPlot(
      chartOpts(cpuEl, "CPU", cpuFmt, document.getElementById("readout-cpu")),
      [ts, cpu], cpuEl);
    memChart = new uPlot(
      chartOpts(memEl, "Memory", memFmt, document.getElementById("readout-mem")),
      [ts.slice(), mem], memEl);
    if (netEl) {
      netChart = new uPlot(
        ioChartOpts(netEl, document.getElementById("readout-net"), "rx", "tx"),
        [ts.slice(), netRx, netTx], netEl);
    }
    if (diskEl) {
      diskChart = new uPlot(
        ioChartOpts(diskEl, document.getElementById("readout-disk"), "read", "write"),
        [ts.slice(), diskRead, diskWrite], diskEl);
    }

    window.addEventListener("resize", function () {
      cpuChart.setSize({ width: cpuEl.clientWidth, height: 110 });
      memChart.setSize({ width: memEl.clientWidth, height: 110 });
      if (netChart) netChart.setSize({ width: netEl.clientWidth, height: 110 });
      if (diskChart) diskChart.setSize({ width: diskEl.clientWidth, height: 110 });
    });
  }

  function push(chart, t, y) {
    var d = chart.data;
    var n = d[0].length;
    if (n && d[0][n - 1] === t) return; // skip duplicate timestamp
    if (n && t - d[0][n - 1] > gapSecs) {
      // Data hole (couldn't be backfilled): break the line, don't bridge it.
      d[0].push(d[0][n - 1] + 1);
      d[1].push(null);
    }
    d[0].push(t);
    d[1].push(y);
    while (d[0].length > MAX_POINTS) {
      d[0].shift();
      d[1].shift();
    }
    chart.setData(d);
  }

  // Append one sample to a two-series (in/out) chart, with the same duplicate
  // and gap handling as push().
  function push2(chart, t, yIn, yOut) {
    if (!chart) return;
    var d = chart.data;
    var n = d[0].length;
    if (n && d[0][n - 1] === t) return;
    if (n && t - d[0][n - 1] > gapSecs) {
      d[0].push(d[0][n - 1] + 1);
      d[1].push(null);
      d[2].push(null);
    }
    d[0].push(t);
    d[1].push(yIn);
    d[2].push(yOut);
    while (d[0].length > MAX_POINTS) {
      d[0].shift();
      d[1].shift();
      d[2].shift();
    }
    chart.setData(d);
  }

  // --- Log timestamps ----------------------------------------------------------
  //
  // The server renders each log line's daemon timestamp as UTC inside a
  // .log-ts span (data-ts = seconds-precision RFC 3339). Rewrite them to the
  // browser's local time so they line up with the chart axes. The attribute is
  // removed afterwards so a span is never converted twice.

  function localizeLogTimestamps(root) {
    if (!root || !root.querySelectorAll) return;
    var spans = root.querySelectorAll(".log-ts[data-ts]");
    for (var i = 0; i < spans.length; i++) {
      var d = new Date(spans[i].getAttribute("data-ts"));
      if (isNaN(d)) continue;
      spans[i].textContent =
        d.getFullYear() + "-" + pad(d.getMonth() + 1) + "-" + pad(d.getDate()) +
        " " + pad(d.getHours()) + ":" + pad(d.getMinutes()) + ":" + pad(d.getSeconds());
      spans[i].removeAttribute("data-ts");
    }
  }

  document.body.addEventListener("htmx:afterSwap", function (e) {
    localizeLogTimestamps(e.target);
  });

  // --- Port links --------------------------------------------------------------
  //
  // Port pills are rendered server-side with a localhost href as a fallback, but
  // the real target is whatever host the browser is pointed at — only known
  // here. Rewrite each pill's href to that host. Re-run after every SSE swap of
  // a live region, since innerHTML replaces the freshly-rendered pills.

  function localizePortLinks(root) {
    if (!root || !root.querySelectorAll) return;
    var links = root.querySelectorAll("a.port-pill[data-port]");
    for (var i = 0; i < links.length; i++) {
      links[i].href =
        "http://" + location.hostname + ":" + links[i].getAttribute("data-port");
    }
  }

  localizePortLinks(document);

  // --- Error toast -----------------------------------------------------------
  //
  // htmx does not swap non-2xx responses, so a failed action (or logs/compose
  // fetch) would be invisible: the click just seems to do nothing. Surface the
  // server's error text in a transient toast instead. The toast element lives
  // outside the live regions, so the periodic SSE swaps can't wipe it.

  var toastTimer = null;

  function showToast(msg) {
    var el = document.getElementById("toast");
    if (!el) return;
    el.textContent = msg;
    el.classList.add("show");
    clearTimeout(toastTimer);
    toastTimer = setTimeout(function () { el.classList.remove("show"); }, 6000);
  }

  document.body.addEventListener("htmx:responseError", function (e) {
    var xhr = e.detail && e.detail.xhr;
    var text = xhr && xhr.responseText;
    showToast(text || "Request failed (HTTP " + (xhr ? xhr.status : "?") + ")");
  });
  document.body.addEventListener("htmx:sendError", function () {
    showToast("Network error: could not reach DockDoe");
  });

  // --- Single live connection ------------------------------------------------
  //
  // The connection is closed whenever the page is hidden or navigated away
  // from, and reopened when it becomes visible again. This matters because a
  // browser allows only ~6 connections per host over HTTP/1.1, and a long-lived
  // EventSource holds one. Without closing on hide, pages kept alive in the
  // back/forward cache or in background tabs each keep a connection open — after
  // a handful of navigations the pool is exhausted and the next page load
  // stalls forever. Closing on hide keeps exactly one live connection at a time.

  var es = null;

  function onHeader(e) {
    var el = document.getElementById("host-header");
    if (el) el.innerHTML = e.data;
  }

  function onContainers(e) {
    var el = document.getElementById("containers");
    if (!el) return;
    el.innerHTML = e.data;
    // Re-bind htmx attributes (action buttons) in the swapped-in markup.
    if (window.htmx) window.htmx.process(el);
    localizePortLinks(el);
  }

  // The live region of a container detail page (state badge + facts).
  function onDetail(e) {
    var el = document.getElementById("detail-live");
    if (!el) return;
    el.innerHTML = e.data;
    localizePortLinks(el);
  }

  // Dashboard overview discs: live.js owns the page's one SSE connection, so
  // it just hands the parsed payload over; overview.js does the rendering.
  function onOverview(e) {
    var p;
    try {
      p = JSON.parse(e.data);
    } catch (err) {
      return;
    }
    document.dispatchEvent(new CustomEvent("dockdoe:overview", { detail: p }));
  }

  function onMetrics(e) {
    if (!cpuChart) return;
    var p;
    try {
      p = JSON.parse(e.data);
    } catch (err) {
      return;
    }
    var t = Math.floor(p.ts_ms / 1000);
    push(cpuChart, t, p.cpu_percent);
    push(memChart, t, p.mem_used != null ? p.mem_used / 1048576 : null);
    push2(netChart, t, p.net_rx != null ? p.net_rx : null, p.net_tx != null ? p.net_tx : null);
    push2(diskChart, t,
      p.disk_read != null ? p.disk_read : null,
      p.disk_write != null ? p.disk_write : null);
  }

  var reconnectTimer = null;

  function openStream() {
    if (es) return;
    es = new EventSource(liveUrl);
    es.addEventListener("header", onHeader);
    es.addEventListener("containers", onContainers);
    es.addEventListener("detail", onDetail);
    es.addEventListener("metrics", onMetrics);
    es.addEventListener("overview", onOverview);
    // On a dropped connection the browser would reconnect on its own — but
    // that bypasses the backfill and leaves a hole in the charts (e.g. when
    // the DockDoe server restarts while the page stays open). Take over:
    // drop the source and go back through connect(), which fetches the
    // missed points before reopening the stream.
    es.onerror = function () {
      disconnect();
      clearTimeout(reconnectTimer);
      reconnectTimer = setTimeout(function () {
        if (!document.hidden) connect();
      }, 3000);
    };
  }

  // While the connection is closed (hidden tab, bfcache, suspend) the charts
  // receive nothing, so reconnecting would leave a gap. Before reopening the
  // stream, fetch the missed points from the backfill endpoint and append
  // them. The stream is opened only after the backfill is in, so chart data
  // stays in timestamp order.
  var backfilling = false;

  function connect() {
    if (es || backfilling) return;
    if (!cpuChart || !backfillUrl) {
      openStream();
      return;
    }
    var xs = cpuChart.data[0];
    var lastTs = xs.length ? xs[xs.length - 1] : 0;
    backfilling = true;
    fetch(backfillUrl + "?since_ms=" + (lastTs * 1000 + 1))
      .then(function (r) { return r.ok ? r.json() : []; })
      .then(function (points) {
        for (var i = 0; i < points.length; i++) {
          var p = points[i];
          var t = Math.floor(p.ts_ms / 1000);
          push(cpuChart, t, p.cpu_percent);
          push(memChart, t, p.mem_used != null ? p.mem_used / 1048576 : null);
          push2(netChart, t, p.net_rx != null ? p.net_rx : null, p.net_tx != null ? p.net_tx : null);
          push2(diskChart, t,
            p.disk_read != null ? p.disk_read : null,
            p.disk_write != null ? p.disk_write : null);
        }
      })
      .catch(function () {}) // a failed backfill must not block going live
      .then(function () {
        backfilling = false;
        // The page may have been hidden again while the fetch was in flight.
        if (!document.hidden) openStream();
      });
  }

  function disconnect() {
    if (!es) return;
    es.close();
    es = null;
  }

  document.addEventListener("visibilitychange", function () {
    if (document.hidden) disconnect();
    else connect();
  });
  // Fires when navigating away (including into the bfcache): release the slot.
  window.addEventListener("pagehide", disconnect);
  // Fires when the page is shown, including restoration from the bfcache.
  window.addEventListener("pageshow", connect);

  if (!document.hidden) connect();
})();
