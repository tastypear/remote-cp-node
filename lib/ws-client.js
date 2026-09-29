"use strict";

const WebSocket = require("ws");
const { EventEmitter } = require("events");
const { Readable, Writable } = require("stream");

class WsExecSession extends EventEmitter {
  constructor(ws) {
    super();
    this._ws = ws;
    this.pid = undefined;
    this.exitCode = null;
    this.signalCode = null;
    this.killed = false;
    this._closed = false;

    this.stdout = new Readable({ read() {} });
    this.stderr = new Readable({ read() {} });

    this.stdin = new Writable({
      write: (chunk, encoding, cb) => {
        if (this._closed || ws.readyState !== WebSocket.OPEN) {
          return cb(new Error("stream closed"));
        }
        const buf = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk, encoding);
        const asUtf8 = buf.toString("utf8");
        let data, enc;
        if (Buffer.from(asUtf8, "utf8").equals(buf)) {
          data = asUtf8; enc = "utf8";
        } else {
          data = buf.toString("base64"); enc = "base64";
        }
        ws.send(JSON.stringify({ type: "stdin", data, encoding: enc }), cb);
      },
      final: (cb) => {
        if (ws.readyState === WebSocket.OPEN) {
          ws.send(JSON.stringify({ type: "stdin_close" }), () => cb());
        } else { cb(); }
      },
    });

    this.stdio = [this.stdin, this.stdout, this.stderr];
  }

  _onMessage(raw, isBinary) {
    // Binary frame: 1-byte prefix (0=stdout, 1=stderr) + raw bytes.
    if (isBinary) {
      const prefix = raw[0];
      const data = raw.subarray(1);
      if (prefix === 0) {
        this.stdout.push(data);
        this.emit("stdoutdata", data);
      } else if (prefix === 1) {
        this.stderr.push(data);
        this.emit("stderrdata", data);
      }
      return;
    }
    let msg;
    try { msg = JSON.parse(raw.toString("utf8")); } catch { return; }

    if (msg.type === "pid") {
      this.pid = msg.pid;
      this.emit("spawn");
    } else if (msg.type === "stdout") {
      const buf = Buffer.from(msg.data, msg.encoding === "base64" ? "base64" : "utf8");
      this.stdout.push(buf);
      this.emit("stdoutdata", buf);
    } else if (msg.type === "stderr") {
      const buf = Buffer.from(msg.data, msg.encoding === "base64" ? "base64" : "utf8");
      this.stderr.push(buf);
      this.emit("stderrdata", buf);
    } else if (msg.type === "exit") {
      this.exitCode = msg.code;
      this.signalCode = msg.signal || null;
      this.stdout.push(null);
      this.stderr.push(null);
      this.emit("exit", msg.code, msg.signal || null);
    } else if (msg.type === "error") {
      this.emit("error", new Error(msg.data));
    }
    // keepalive: ignored
  }

  _onClose() {
    if (this._closed) return;
    this._closed = true;
    if (this.exitCode === null) {
      this.stdout.push(null);
      this.stderr.push(null);
    }
    this.emit("close", this.exitCode, this.signalCode);
  }

  kill(signal) {
    if (this.killed || this.pid == null) return true;
    this.killed = true;
    this.signalCode = signal || "SIGTERM";
    if (this._ws.readyState === WebSocket.OPEN) {
      this._ws.send(JSON.stringify({ type: "kill", signal: signal || "SIGTERM" }));
    }
    return true;
  }

  close() {
    if (this._closed) return;
    try { this._ws.close(); } catch {}
  }

  resize(cols, rows) {
    if (this._ws.readyState === WebSocket.OPEN) {
      this._ws.send(JSON.stringify({ type: "resize", cols, rows }));
    }
  }

  ref() { return this; }
  unref() { return this; }
}

function connect(baseURL, token, startMsg) {
  return new Promise((resolve, reject) => {
    const url = new URL("/ws/exec", baseURL.replace(/^http/, "ws"));
    const headers = {};
    if (token) headers["Authorization"] = "Bearer " + token;

    const ws = new WebSocket(url.href, { handshakeTimeout: 10000, headers });
    let session = null;
    let settled = false;

    const timer = setTimeout(() => {
      if (!settled) {
        settled = true;
        try { ws.close(); } catch {}
        reject(new Error("WebSocket exec: no pid within 30s"));
      }
    }, 30000);

    ws.on("open", () => {
      session = new WsExecSession(ws);
      ws.on("message", (raw, isBinary) => session._onMessage(raw, isBinary));
      ws.on("close", () => session._onClose());

      session.once("spawn", () => {
        if (!settled) { settled = true; clearTimeout(timer); resolve(session); }
      });
      session.once("error", (err) => {
        if (!settled) { settled = true; clearTimeout(timer); reject(err); }
      });

      ws.send(JSON.stringify({ type: "start", binaryFrames: true, ...startMsg }));
    });

    ws.on("error", (err) => {
      if (!settled) { settled = true; clearTimeout(timer); reject(err); }
      else if (session) { session.emit("error", err); }
    });
  });
}

module.exports = { connect, WsExecSession };
