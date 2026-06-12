// History overlay: a native <dialog> with one large chart, opened from the
// expand button on any chart card. Shows the median as a line and the min-max
// envelope as a shaded band, fetched once per range from the history endpoint
// (no SSE involved - this view is deliberately static and separate from the
// live machinery in live.js).
(function () {
  "use strict";

  var dlg = document.getElementById("history-dialog");
  var cfg = document.getElementById("seed-data");
  var historyUrl = cfg && cfg.getAttribute("data-history-url");
  if (!dlg || !historyUrl || typeof uPlot === "undefined") return;

  var chartEl = document.getElementById("history-chart");
  var titleEl = document.getElementById("history-title");
  var readoutEl = document.getElementById("history-readout");
  var rangesEl = document.getElementById("history-ranges");

  var stroke = "#4f9cf9";
  var band = "rgba(79,156,249,0.18)";
  var edge = "rgba(79,156,249,0.35)";
  var grid = { stroke: "#2c313c", width: 1 };
  var axisStyle = { stroke: "#8b93a1", grid: grid, ticks: grid };

  var pad = function (n) { return n < 10 ? "0" + n : "" + n; };
  var cpuFmt = function (v) { return v.toFixed(1) + "%"; };
  // Readout precision: a container's memory often moves within a few MiB, so
  // the coarse one-decimal format of the live charts would render the whole
  // envelope as the same number.
  var memFmt = function (v) {
    return v >= 1024 ? (v / 1024).toFixed(2) + "G" : v.toFixed(1) + "M";
  };

  // Axis ticks need adaptive precision for the same reason: with a tight
  // value range every tick would round to the same label. Derive the number
  // of decimals from the tick spacing so adjacent labels always differ.
  function memTicks(vals) {
    if (!vals.length) return vals;
    var step = vals.length > 1 ? vals[1] - vals[0] : 1; // MiB
    // Sub-MiB tick spacing would need >3 decimals in G, so stay in MiB then.
    var inG = vals[vals.length - 1] >= 1024 && step >= 1;
    var div = inG ? 1024 : 1;
    var s = step / div;
    var dec = s >= 1 ? 0 : Math.min(3, Math.ceil(-Math.log10(s)));
    return vals.map(function (v) { return (v / div).toFixed(dec) + (inG ? "G" : "M"); });
  }
  function cpuTicks(vals) {
    return vals.map(function (v) { return v.toFixed(1) + "%"; });
  }

  var metric = "cpu"; // which metric the open dialog shows
  var range = "24h";
  var chart = null;
  var loadSeq = 0; // ignore out-of-order fetch responses after rapid clicks

  // Long ranges need the day in the axis labels and the readout; within a day
  // the clock alone is enough.
  function timeStr(s, withDate) {
    var d = new Date(s * 1000);
    var clock = pad(d.getHours()) + ":" + pad(d.getMinutes());
    if (!withDate) return clock;
    return pad(d.getMonth() + 1) + "-" + pad(d.getDate()) + " " + clock;
  }

  function showsDate() { return range === "7d" || range === "30d"; }

  // Same idea as in live.js: break the line over stretches without data
  // instead of bridging them. Threshold adapts to the series spacing (raw
  // seconds vs downsampled buckets).
  function gapFromSpacing(times) {
    if (times.length < 3) return 15;
    var deltas = [];
    for (var i = 1; i < times.length; i++) deltas.push(times[i] - times[i - 1]);
    deltas.sort(function (a, b) { return a - b; });
    return Math.max(15, deltas[Math.floor(deltas.length / 2)] * 4);
  }

  function chartOpts(fmt, ticks) {
    return {
      width: chartEl.clientWidth || 800,
      height: 420,
      cursor: { y: false },
      legend: { show: false },
      scales: { x: { time: true } },
      // Data layout: [ts, median, max, min]. The band fills from series 2
      // (upper edge, max) down to series 3 (lower edge, min).
      series: [
        {},
        { stroke: stroke, width: 1.5 },
        { stroke: edge, width: 1 },
        { stroke: edge, width: 1 },
      ],
      bands: [{ series: [2, 3], fill: band }],
      axes: [
        Object.assign({}, axisStyle, {
          size: 30,
          values: function (u, splits) {
            var withDate = showsDate();
            return splits.map(function (s) { return timeStr(s, withDate); });
          },
        }),
        Object.assign({}, axisStyle, {
          // Tight memory ranges produce long labels ("1299.4M"); a fixed
          // gutter clips their leading digits, so size to the longest label.
          size: function (u, values) {
            var longest = (values || []).reduce(function (a, b) {
              return b.length > a.length ? b : a;
            }, "");
            return Math.max(56, 16 + longest.length * 8);
          },
          values: function (u, vals) { return ticks(vals); },
        }),
      ],
      hooks: {
        setCursor: [function (u) {
          var i = u.cursor.idx;
          if (i == null || u.data[0][i] == null || u.data[1][i] == null) {
            readoutEl.textContent = "";
            return;
          }
          var med = u.data[1][i];
          var hi = u.data[2][i];
          var lo = u.data[3][i];
          var spread = lo != null && hi != null && hi - lo > 1e-9
            ? "  (" + fmt(lo) + " – " + fmt(hi) + ")"
            : "";
          readoutEl.textContent =
            timeStr(u.data[0][i], showsDate()) + " · " + fmt(med) + spread;
        }],
      },
    };
  }

  function render(points) {
    var isCpu = metric === "cpu";
    var fmt = isCpu ? cpuFmt : memFmt;
    var ticks = isCpu ? cpuTicks : memTicks;
    var val = function (p, part) {
      var v = p[(isCpu ? "cpu_" : "mem_") + part];
      if (v == null) return null;
      return isCpu ? v : v / 1048576; // bytes -> MiB
    };

    var rawTs = points.map(function (p) { return Math.floor(p.ts_ms / 1000); });
    var gapSecs = gapFromSpacing(rawTs);
    var ts = [], med = [], hi = [], lo = [];
    for (var i = 0; i < points.length; i++) {
      if (ts.length && rawTs[i] - ts[ts.length - 1] > gapSecs) {
        ts.push(ts[ts.length - 1] + 1);
        med.push(null); hi.push(null); lo.push(null);
      }
      ts.push(rawTs[i]);
      med.push(val(points[i], "med"));
      hi.push(val(points[i], "max"));
      lo.push(val(points[i], "min"));
    }

    if (chart) { chart.destroy(); chart = null; }
    chartEl.textContent = points.length ? "" : "No data for this range.";
    if (points.length) {
      chart = new uPlot(chartOpts(fmt, ticks), [ts, med, hi, lo], chartEl);
    }
  }

  function load(r) {
    range = r;
    readoutEl.textContent = "";
    var btns = rangesEl.querySelectorAll("button");
    for (var i = 0; i < btns.length; i++) {
      btns[i].classList.toggle("active", btns[i].getAttribute("data-range") === r);
    }
    var seq = ++loadSeq;
    fetch(historyUrl + "?range=" + encodeURIComponent(r))
      .then(function (resp) {
        if (!resp.ok) throw new Error("HTTP " + resp.status);
        return resp.json();
      })
      .then(function (points) {
        if (seq === loadSeq) render(points);
      })
      .catch(function () {
        if (seq !== loadSeq) return;
        if (chart) { chart.destroy(); chart = null; }
        chartEl.textContent = "Could not load history.";
      });
  }

  rangesEl.addEventListener("click", function (e) {
    var btn = e.target.closest("button[data-range]");
    if (btn) load(btn.getAttribute("data-range"));
  });

  // Open from any chart card's expand button. The dialog title borrows the
  // card's title ("Host CPU", "Memory", ...), which already names the metric.
  document.body.addEventListener("click", function (e) {
    var btn = e.target.closest(".chart-zoom");
    if (!btn) return;
    metric = btn.getAttribute("data-metric") || "cpu";
    var head = btn.closest(".chart-head");
    var title = head && head.querySelector(".chart-title");
    titleEl.textContent = (title ? title.textContent : metric) + " — history";
    dlg.showModal();
    load(range);
  });

  document.getElementById("history-close").addEventListener("click", function () {
    dlg.close();
  });
  // Native <dialog>: a click on the backdrop registers on the dialog element
  // itself (the padded content area is covered by .history-body).
  dlg.addEventListener("click", function (e) {
    if (e.target === dlg) dlg.close();
  });

  window.addEventListener("resize", function () {
    if (chart && dlg.open) {
      chart.setSize({ width: chartEl.clientWidth, height: 420 });
    }
  });

  // Deep link: #history-cpu / #history-mem-7d opens the overlay on load, so a
  // view can be bookmarked or shared (and exercised by headless UI checks).
  var hash = /^#history-(cpu|mem)(?:-(1h|6h|24h|7d|30d))?$/.exec(location.hash);
  if (hash) {
    if (hash[2]) range = hash[2];
    var zoom = document.querySelector('.chart-zoom[data-metric="' + hash[1] + '"]');
    if (zoom) zoom.click();
  }
})();
