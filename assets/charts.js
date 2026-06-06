// Live host charts: seed from server-rendered history, then append points from
// the /events/metrics SSE stream. HTMX handles the header/table fragments; this
// file only drives the two uPlot charts.
(function () {
  "use strict";

  var MAX_POINTS = 1200; // cap memory; at a few seconds/sample this is hours

  function readSeed() {
    var el = document.getElementById("seed-data");
    if (!el) return [];
    try {
      return JSON.parse(el.textContent || "[]");
    } catch (e) {
      return [];
    }
  }

  var seed = readSeed();
  var ts = seed.map(function (p) { return Math.floor(p.ts_ms / 1000); });
  var cpu = seed.map(function (p) { return p.cpu_percent; });
  var mem = seed.map(function (p) {
    return p.mem_used != null ? p.mem_used / 1048576 : null; // MiB
  });

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
        Object.assign({}, axisStyle, { size: 30 }),
        Object.assign({}, axisStyle, {
          values: function (u, vals) { return vals.map(fmt); },
        }),
      ],
    };
  }

  var cpuEl = document.getElementById("chart-cpu");
  var memEl = document.getElementById("chart-mem");
  if (!cpuEl || !memEl || typeof uPlot === "undefined") return;

  var cpuFmt = function (v) { return v.toFixed(0) + "%"; };
  var memFmt = function (v) {
    return v >= 1024 ? (v / 1024).toFixed(1) + "G" : v.toFixed(0) + "M";
  };

  var cpuChart = new uPlot(chartOpts(cpuEl, "CPU", cpuFmt), [ts.slice(), cpu.slice()], cpuEl);
  var memChart = new uPlot(chartOpts(memEl, "Memory", memFmt), [ts.slice(), mem.slice()], memEl);

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

  var es = new EventSource("/events/metrics");
  es.onmessage = function (e) {
    var p;
    try {
      p = JSON.parse(e.data);
    } catch (err) {
      return;
    }
    var t = Math.floor(p.ts_ms / 1000);
    push(cpuChart, t, p.cpu_percent);
    push(memChart, t, p.mem_used != null ? p.mem_used / 1048576 : null);
  };

  window.addEventListener("resize", function () {
    cpuChart.setSize({ width: cpuEl.clientWidth, height: 110 });
    memChart.setSize({ width: memEl.clientWidth, height: 110 });
  });
})();
