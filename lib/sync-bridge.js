"use strict";

// Sync HTTP bridge — provides synchronous HTTP via a worker thread + SharedArrayBuffer.
// The main thread writes the request to a SharedArrayBuffer, posts a message
// to the worker, and blocks on Atomics.wait. The worker does http.request
// (keep-alive), writes the response, and Atomics.notify wakes the main thread.

const { Worker } = require("worker_threads");
const path = require("path");

let _worker = null;
let _signal = null;
let _dataBuf = null;
let _ready = false;
let _dead = false;

function init(opts) {
  if (_worker) return;
  if (typeof SharedArrayBuffer === "undefined") return false;

  const sabSignal = new SharedArrayBuffer(16);
  const sabData = new SharedArrayBuffer(16 * 1024 * 1024);
  _signal = new Int32Array(sabSignal);
  _dataBuf = Buffer.from(sabData);

  try {
    _worker = new Worker(path.join(__dirname, "sync-worker.js"), {
      workerData: {
        sabSignal,
        sabData,
        baseURL: opts.baseURL,
        token: opts.token,
        tls: opts.baseURL.startsWith("https://"),
      },
    });
  } catch (e) {
    _worker = null;
    return false;
  }

  _worker.on("message", (msg) => {
    if (msg.type === "ready") _ready = true;
  });
  _worker.on("error", () => { _dead = true; });
  _worker.on("exit", () => { _dead = true; });

  try { _worker.unref(); } catch (e) {}

  process.on("exit", () => {
    if (_worker) { try { _worker.terminate(); } catch (e) {} }
  });

  // Wait for worker ready (up to 8s)
  const deadline = Date.now() + 8000;
  while (Date.now() < deadline) {
    if (Atomics.load(_signal, 1) === 1) { _ready = true; break; }
    Atomics.wait(_signal, 1, 0, 500);
  }
  return _ready;
}

function isAvailable() {
  return _worker && _ready && !_dead;
}

// method: 0=GET, 1=POST, 2=PUT
function syncRequest(methodCode, urlPath, body) {
  if (!isAvailable()) return null;

  const pathLen = Buffer.byteLength(urlPath, "utf8");
  _dataBuf.writeInt32LE(methodCode, 0);
  _dataBuf.writeInt32LE(pathLen, 4);
  _dataBuf.write(urlPath, 8, "utf8");
  let off = 8 + pathLen;

  if (body != null) {
    const buf = Buffer.isBuffer(body) ? body : Buffer.from(String(body), "utf8");
    _dataBuf.writeInt32LE(buf.length, off);
    off += 4;
    if (off + buf.length > _dataBuf.length) {
      return null;
    }
    buf.copy(_dataBuf, off);
  } else {
    _dataBuf.writeInt32LE(0, off);
  }

  Atomics.store(_signal, 0, 0);
  _worker.postMessage({ type: "request" });
  // Loop in 30s intervals — long-running commands (grep on large files, builds)
  // can take minutes. A 30s hard timeout would kill them. Up to 20 iterations (10 min).
  let r;
  for (let i = 0; i < 20; i++) {
    r = Atomics.wait(_signal, 0, 0, 30000);
    if (r === "ok" || Atomics.load(_signal, 0) !== 0) break;
  }
  if (Atomics.load(_signal, 0) !== 1) {
    return null;
  }

  const statusCode = _dataBuf.readInt32LE(0);
  const bodyLen = _dataBuf.readInt32LE(4);
  const respBody = bodyLen > 0 ? Buffer.allocUnsafe(bodyLen) : Buffer.alloc(0);
  if (bodyLen > 0) _dataBuf.copy(respBody, 0, 8, 8 + bodyLen);

  return { statusCode, body: respBody };
}

module.exports = { init, isAvailable, syncRequest };
