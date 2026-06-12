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

  function chartOpts(el, label, fmt) {
    return {
      width: el.clientWidth || 320,
      height: 110,
      cursor: { y: false },
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
    };
  }

  // uPlot's default time axis shows only the second component (":ss") when all
  // ticks fall in the same minute, hiding the hour/minute. Always show HH:MM.
  var pad = function (n) { return n < 10 ? "0" + n : "" + n; };
  var timeFmt = function (u, splits) {
    return splits.map(function (s) {
      var d = new Date(s * 1000);
      return pad(d.getHours()) + ":" + pad(d.getMinutes()) + ":" + pad(d.getSeconds());
    });
  };

  var cpuFmt = function (v) { return v.toFixed(0) + "%"; };
  var memFmt = function (v) {
    return v >= 1024 ? (v / 1024).toFixed(1) + "G" : v.toFixed(0) + "M";
  };

  var cpuEl = document.getElementById("chart-cpu");
  var memEl = document.getElementById("chart-mem");
  var cpuChart = null;
  var memChart = null;

  if (cpuEl && memEl && typeof uPlot !== "undefined") {
    var ts = seed.map(function (p) { return Math.floor(p.ts_ms / 1000); });
    var cpu = seed.map(function (p) { return p.cpu_percent; });
    var mem = seed.map(function (p) {
      return p.mem_used != null ? p.mem_used / 1048576 : null; // MiB
    });
    cpuChart = new uPlot(chartOpts(cpuEl, "CPU", cpuFmt), [ts.slice(), cpu.slice()], cpuEl);
    memChart = new uPlot(chartOpts(memEl, "Memory", memFmt), [ts.slice(), mem.slice()], memEl);

    window.addEventListener("resize", function () {
      cpuChart.setSize({ width: cpuEl.clientWidth, height: 110 });
      memChart.setSize({ width: memEl.clientWidth, height: 110 });
    });
  }

  function push(chart, t, y) {
    var d = chart.data;
    var n = d[0].length;
    if (n && d[0][n - 1] === t) return; // skip duplicate timestamp
    d[0].push(t);
    d[1].push(y);
    while (d[0].length > MAX_POINTS) {
      d[0].shift();
      d[1].shift();
    }
    chart.setData(d);
  }

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
  }

  // The live region of a container detail page (state badge + facts).
  function onDetail(e) {
    var el = document.getElementById("detail-live");
    if (el) el.innerHTML = e.data;
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
  }

  function openStream() {
    if (es) return;
    es = new EventSource(liveUrl);
    es.addEventListener("header", onHeader);
    es.addEventListener("containers", onContainers);
    es.addEventListener("detail", onDetail);
    es.addEventListener("metrics", onMetrics);
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
