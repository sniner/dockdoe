// Dashboard overview: two radial "phosphor" discs (CPU + memory), one sector
// per container. The radius encodes the current value; the last samples stay
// visible as a fading trail (drawn from a ring buffer, so a scale never mixes
// frames — everything is redrawn per tick). Sector order mirrors the container
// table: stacks alphabetically, standalone last, names within; stack borders
// get a slightly wider gap. Data arrives as `dockdoe:overview` CustomEvents
// re-dispatched by live.js from the dashboard SSE stream, seeded from the
// inline JSON that the server renders into the page.
(function () {
  var root = document.getElementById("overview");
  if (!root) return;

  var containerUrl = root.dataset.containerUrl;
  var cpuScale = root.dataset.cpuScale; // linear | sqrt | log
  var memScale = root.dataset.memScale;
  var memCap = Number(root.dataset.memCap);

  var MiB = 1048576;
  var GiB = 1073741824;
  var CPU_FLOOR = 0.1; // log scale's lower bound, in percent
  var MEM_FLOOR = 16 * MiB; // keep in sync with OVERVIEW_MEM_FLOOR (config.rs)
  var TRAIL = 24; // samples of history per sector
  var DECAY = 0.84; // per-sample alpha falloff of the trail
  var GAP = 0.012; // radians between sectors
  var GROUP_GAP = 0.06; // radians between stacks

  // The UI theme is a fixed set of CSS variables; read them once.
  var css = getComputedStyle(document.documentElement);
  var COLOR = {
    bar: css.getPropertyValue("--accent").trim(),
    grid: css.getPropertyValue("--border").trim(),
    label: css.getPropertyValue("--muted").trim(),
    halo: css.getPropertyValue("--panel").trim(),
    hover: css.getPropertyValue("--fg").trim(),
  };

  var list = []; // last payload's containers, in sector order
  var hist = {}; // container id -> ring buffer of {cpu, mem}
  var layout = []; // per sector: {a0, a1} radians, aligned with `list`
  var lastTs = 0;

  function clamp01(v) { return v < 0 ? 0 : v > 1 ? 1 : v; }

  function scaled(mode, frac, logFrac) {
    if (mode === "log") return clamp01(logFrac);
    if (mode === "sqrt") return clamp01(Math.sqrt(frac));
    return clamp01(frac);
  }

  // CPU: the rim is 100% of one core (multi-core values are clamped; the
  // readout shows the real number).
  function scaleCpu(v) {
    var lo = Math.log10(CPU_FLOOR);
    var logFrac = (Math.log10(Math.max(v, CPU_FLOOR)) - lo) / (Math.log10(100) - lo);
    return scaled(cpuScale, v / 100, logFrac);
  }

  // Memory: the rim is the configured cap; the log scale runs from MEM_FLOOR.
  function scaleMem(v) {
    var lo = Math.log2(MEM_FLOOR);
    var logFrac = (Math.log2(Math.max(v, MEM_FLOOR)) - lo) / (Math.log2(memCap) - lo);
    return scaled(memScale, v / memCap, logFrac);
  }

  function cpuTicks() {
    if (cpuScale === "log") return [1, 10, 50];
    if (cpuScale === "sqrt") return [5, 25, 60];
    return [25, 50, 75];
  }

  function memTicks() {
    if (memScale !== "log") return [memCap / 4, memCap / 2, (memCap * 3) / 4];
    // Round binary sizes between the floor and the rim, at most three.
    var out = [];
    for (var v = 128 * MiB; v < memCap * 0.7 && out.length < 3; v *= 8) out.push(v);
    return out;
  }

  function fmtCpu(v) { return (v >= 10 ? v.toFixed(0) : v.toFixed(1)) + "%"; }

  function fmtMem(v) {
    if (v >= GiB) return (v / GiB).toFixed(v >= 10 * GiB ? 0 : 1) + "G";
    return (v / MiB).toFixed(0) + "M";
  }

  /* ----- discs ----- */

  function makeDisc(key, scale, ticks, metric, fmt) {
    var canvas = document.getElementById("overview-" + key);
    var readout = document.getElementById("overview-readout-" + key);
    var disc = {
      canvas: canvas,
      ctx: canvas.getContext("2d"),
      scale: scale,
      ticks: ticks,
      metric: metric, // "cpu" | "mem"
      fmt: fmt,
      readout: readout,
      size: 0,
      hover: -1,
    };

    function resize() {
      var w = canvas.parentElement.clientWidth;
      var size = Math.max(200, Math.min(300, w));
      if (size === disc.size) return;
      disc.size = size;
      var dpr = window.devicePixelRatio || 1;
      canvas.width = size * dpr;
      canvas.height = size * dpr;
      canvas.style.width = size + "px";
      canvas.style.height = size + "px";
      disc.ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      draw(disc);
    }
    new ResizeObserver(resize).observe(canvas.parentElement);
    resize();

    canvas.addEventListener("mousemove", function (e) {
      var r = canvas.getBoundingClientRect();
      var half = disc.size / 2;
      var dx = e.clientX - r.left - half;
      var dy = e.clientY - r.top - half;
      var idx = -1;
      if (Math.sqrt(dx * dx + dy * dy) <= half - 6) {
        idx = hitSector(Math.atan2(dy, dx));
      }
      canvas.style.cursor = idx >= 0 ? "pointer" : "default";
      if (idx !== disc.hover) {
        disc.hover = idx;
        draw(disc);
        updateReadout(disc);
      }
    });
    canvas.addEventListener("mouseleave", function () {
      if (disc.hover === -1) return;
      disc.hover = -1;
      draw(disc);
      updateReadout(disc);
    });
    canvas.addEventListener("click", function () {
      if (disc.hover >= 0) window.location.href = containerUrl + list[disc.hover].id;
    });

    return disc;
  }

  function updateReadout(disc) {
    var c = list[disc.hover];
    if (!c) {
      disc.readout.textContent = "";
      return;
    }
    var v = latest(c.id, disc.metric);
    var name = c.stack ? c.stack + "/" + c.name : c.name;
    disc.readout.textContent = name + " · " + (v == null ? "–" : disc.fmt(v));
  }

  function latest(id, metric) {
    var h = hist[id];
    return h && h.length ? h[h.length - 1][metric] : null;
  }

  // Sector angles for the current `list`: equal widths starting at 12 o'clock,
  // clockwise, with a wider gap wherever the stack changes.
  function computeLayout() {
    var n = list.length;
    layout = [];
    if (!n) return;
    var boundaries = 0;
    var i;
    for (i = 1; i < n; i++) {
      if (list[i].stack !== list[i - 1].stack) boundaries++;
    }
    var width = (2 * Math.PI - boundaries * GROUP_GAP - (n - boundaries) * GAP) / n;
    var a = -Math.PI / 2;
    for (i = 0; i < n; i++) {
      if (i > 0) a += list[i].stack !== list[i - 1].stack ? GROUP_GAP : GAP;
      layout.push({ a0: a, a1: a + width });
      a += width;
    }
  }

  function hitSector(angle) {
    // atan2 covers (-PI, PI]; the layout starts at -PI/2 and runs clockwise,
    // so shift the left half-plane up by a full turn before comparing.
    var a = angle < -Math.PI / 2 ? angle + 2 * Math.PI : angle;
    for (var i = 0; i < layout.length; i++) {
      if (a >= layout[i].a0 && a <= layout[i].a1) return i;
    }
    return -1;
  }

  function sectorPath(ctx, cx, cy, r0, r1, a0, a1) {
    ctx.beginPath();
    ctx.moveTo(cx + r0 * Math.cos(a0), cy + r0 * Math.sin(a0));
    ctx.arc(cx, cy, r1, a0, a1);
    ctx.arc(cx, cy, r0, a1, a0, true);
    ctx.closePath();
  }

  function draw(disc) {
    var ctx = disc.ctx;
    var size = disc.size;
    if (!size) return;
    var cx = size / 2;
    var cy = size / 2;
    var rim = size / 2 - 14; // leave room for the tick labels
    var r0 = 6; // tiny hole so near-zero sectors stay distinguishable
    ctx.clearRect(0, 0, size, size);

    // Reference rings and the rim.
    var ticks = disc.ticks();
    ctx.lineWidth = 1;
    ctx.strokeStyle = COLOR.grid;
    var i;
    for (i = 0; i < ticks.length; i++) {
      ctx.beginPath();
      ctx.arc(cx, cy, r0 + disc.scale(ticks[i]) * (rim - r0), 0, 2 * Math.PI);
      ctx.stroke();
    }
    ctx.beginPath();
    ctx.arc(cx, cy, rim, 0, 2 * Math.PI);
    ctx.stroke();

    // Sectors: oldest sample first, so the fresh one paints on top and
    // anything it no longer covers remains as the fading trail.
    ctx.fillStyle = COLOR.bar;
    for (i = 0; i < list.length; i++) {
      var sec = layout[i];
      var h = hist[list[i].id] || [];
      for (var k = 0; k < h.length; k++) {
        var alpha = Math.pow(DECAY, h.length - 1 - k);
        if (alpha < 0.03) continue;
        var v = h[k][disc.metric];
        if (v == null) continue;
        var r = r0 + disc.scale(v) * (rim - r0);
        if (r <= r0 + 0.5) continue;
        ctx.globalAlpha = alpha;
        sectorPath(ctx, cx, cy, r0, r, sec.a0, sec.a1);
        ctx.fill();
      }
    }
    ctx.globalAlpha = 1;

    if (disc.hover >= 0 && layout[disc.hover]) {
      var hs = layout[disc.hover];
      ctx.strokeStyle = COLOR.hover;
      ctx.lineWidth = 1.5;
      sectorPath(ctx, cx, cy, r0, rim, hs.a0, hs.a1);
      ctx.stroke();
      ctx.lineWidth = 1;
    }

    // Tick labels at 12 o'clock, haloed so they stay readable over sectors.
    ctx.font = "10px system-ui, sans-serif";
    ctx.textAlign = "center";
    for (i = 0; i < ticks.length; i++) {
      var ry = cy - (r0 + disc.scale(ticks[i]) * (rim - r0)) - 2;
      var label = disc.metric === "cpu" ? ticks[i] + "%" : fmtMem(ticks[i]);
      ctx.strokeStyle = COLOR.halo;
      ctx.lineWidth = 3;
      ctx.strokeText(label, cx, ry);
      ctx.fillStyle = COLOR.label;
      ctx.fillText(label, cx, ry);
      ctx.lineWidth = 1;
    }
  }

  /* ----- data flow ----- */

  var cpuDisc = makeDisc("cpu", scaleCpu, cpuTicks, "cpu", fmtCpu);
  var memDisc = makeDisc("mem", scaleMem, memTicks, "mem", function (v) {
    return fmtMem(v) + "iB";
  });

  function apply(payload) {
    if (!payload || !payload.containers || payload.ts_ms === lastTs) return;
    lastTs = payload.ts_ms;
    list = payload.containers;

    var seen = {};
    for (var i = 0; i < list.length; i++) {
      var c = list[i];
      seen[c.id] = true;
      var h = hist[c.id] || (hist[c.id] = []);
      h.push({ cpu: c.cpu, mem: c.mem });
      if (h.length > TRAIL) h.shift();
    }
    // Forget buffers of containers that disappeared from the snapshot.
    for (var id in hist) {
      if (!seen[id]) delete hist[id];
    }

    computeLayout();
    draw(cpuDisc);
    draw(memDisc);
    updateReadout(cpuDisc);
    updateReadout(memDisc);
  }

  document.addEventListener("dockdoe:overview", function (e) { apply(e.detail); });

  var seedEl = document.getElementById("overview-seed");
  if (seedEl) {
    try {
      apply(JSON.parse(seedEl.textContent));
    } catch (err) {
      // A malformed seed just means the discs wait for the first SSE event.
    }
  }
})();
