// Container terminal: an xterm.js session bridged to a docker exec over a
// WebSocket. Deliberately lazy — it connects only when the user clicks Connect,
// since a shell is a real process we don't want to spawn on every page view.
//
// Wire protocol (mirrors the server in web.rs): terminal input is sent as
// binary frames (stdin), a resize is sent as a text frame {cols, rows}, and
// engine output arrives as binary frames written straight to the terminal.
(function () {
  "use strict";

  var panel = document.getElementById("terminal");
  if (!panel || typeof Terminal === "undefined") return;

  var view = document.getElementById("term-view");
  var cmdInput = panel.querySelector(".term-cmd");
  var connectBtn = panel.querySelector(".term-connect");
  var fsBtn = panel.querySelector(".term-fs");
  var wsBase = panel.getAttribute("data-ws");
  if (!view || !connectBtn || !wsBase) return;

  var term = null;
  var fit = null;
  var ws = null;
  var connected = false;
  var encoder = new TextEncoder();

  function wsUrl(cmd) {
    var proto = location.protocol === "https:" ? "wss:" : "ws:";
    return proto + "//" + location.host + wsBase + "?cmd=" + encodeURIComponent(cmd);
  }

  // Refit to the current panel size and tell the engine the new dimensions.
  function syncSize() {
    if (!fit || !term) return;
    try { fit.fit(); } catch (e) { return; }
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({ cols: term.cols, rows: term.rows }));
    }
  }

  function connect() {
    // A fresh terminal each session keeps reconnects clean.
    if (term) { term.dispose(); term = null; }
    view.innerHTML = "";

    term = new Terminal({
      cursorBlink: true,
      fontSize: 13,
      fontFamily: "ui-monospace, SFMono-Regular, Menlo, Consolas, monospace",
      theme: { background: "#15171c", foreground: "#e6e8ec" },
    });
    fit = new FitAddon.FitAddon();
    term.loadAddon(fit);
    term.open(view);
    try { fit.fit(); } catch (e) { /* view may be 0-sized for a tick */ }

    term.onData(function (data) {
      if (ws && ws.readyState === WebSocket.OPEN) ws.send(encoder.encode(data));
    });

    var cmd = (cmdInput && cmdInput.value.trim()) || "/bin/sh";
    ws = new WebSocket(wsUrl(cmd));
    ws.binaryType = "arraybuffer";

    ws.onopen = function () {
      connected = true;
      connectBtn.textContent = "Disconnect";
      syncSize();
      term.focus();
    };
    ws.onmessage = function (e) {
      // Engine output is binary; the server's failure notice is text.
      if (typeof e.data === "string") term.write(e.data);
      else term.write(new Uint8Array(e.data));
    };
    ws.onclose = teardown;
    ws.onerror = teardown;
  }

  function teardown() {
    if (!connected && !ws) return;
    connected = false;
    connectBtn.textContent = "Connect";
    if (ws) {
      ws.onclose = ws.onerror = ws.onmessage = null;
      try { ws.close(); } catch (e) { /* already closing */ }
      ws = null;
    }
    if (term) term.write("\r\n\x1b[90m[disconnected]\x1b[0m\r\n");
  }

  connectBtn.addEventListener("click", function () {
    if (connected) teardown();
    else connect();
  });

  if (fsBtn) {
    fsBtn.addEventListener("click", function () {
      panel.classList.toggle("term-fullscreen");
      // Let the layout settle before measuring the new size.
      setTimeout(syncSize, 50);
    });
  }

  window.addEventListener("resize", function () {
    if (connected) syncSize();
  });

  // Release the exec session when navigating away (incl. into the bfcache).
  window.addEventListener("pagehide", function () {
    if (ws) { try { ws.close(); } catch (e) { /* ignore */ } }
  });
})();
