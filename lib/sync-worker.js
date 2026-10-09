"use strict";

// Sync HTTP worker thread — runs in a Worker created by sync-bridge.js.
// The main thread blocks on Atomics.wait(signal, 0) while this worker makes
// an async http.request with keep-alive. The response is written to the
// SharedArrayBuffer and Atomics.notify wakes the main thread.

const { parentPort, workerData } = require("worker_threads");
const http = require(workerData.tls ? "https" : "http");

const signal = new Int32Array(workerData.sabSignal);
const dataBuf = Buffer.from(workerData.sabData);

const TOKEN = workerData.token;
const BASE_URL = workerData.baseURL;
const HEADERS = { Authorization: "Bearer " + TOKEN };

const agent = new http.Agent({ keepAlive: true, maxSockets: 6, keepAliveMsecs: 1000 });

process.on("uncaughtException", (e) => {
  try { parentPort.postMessage({ type: "error", message: "uncaught: " + (e && e.stack || e) }); } catch (_) {}
});

// Signal ready.
Atomics.store(signal, 1, 1);
Atomics.notify(signal, 1);
parentPort.postMessage({ type: "ready" });

parentPort.on("message", (msg) => {
  if (msg.type !== "request") return;

  // Read request from SAB: method(4) + pathLen(4) + path(pathLen) + bodyLen(4) + body(bodyLen)
  const methodCode = dataBuf.readInt32LE(0);
  const pathLen = dataBuf.readInt32LE(4);
  const urlPath = dataBuf.toString("utf8", 8, 8 + pathLen);
  let off = 8 + pathLen;
  const bodyLen = dataBuf.readInt32LE(off);
  off += 4;
  const body = bodyLen > 0 ? Buffer.from(dataBuf.buffer, off, bodyLen) : null;

  const method = methodCode === 0 ? "GET" : methodCode === 1 ? "POST" : "PUT";
  const headers = { ...HEADERS };
  if (msg.reqId) headers["X-Req-ID"] = msg.reqId;
  if (body) {
    headers["Content-Type"] = method === "POST" ? "application/json" : "application/octet-stream";
  }

  const fullUrl = new URL(urlPath, BASE_URL);
  const req = http.request({
    hostname: fullUrl.hostname,
    port: fullUrl.port,
    path: fullUrl.pathname + fullUrl.search,
    method,
    headers,
    agent,
  }, (res) => {
    const chunks = [];
    res.on("data", (c) => chunks.push(c));
    res.on("end", () => {
      const respBody = Buffer.concat(chunks);
      // Write response: statusCode(4) + bodyLen(4) + body(bodyLen)
      dataBuf.writeInt32LE(res.statusCode, 0);
      dataBuf.writeInt32LE(respBody.length, 4);
      if (respBody.length > 0) respBody.copy(dataBuf, 8);
      Atomics.store(signal, 0, 1);
      Atomics.notify(signal, 0);
    });
  });
  req.on("error", (e) => {
    // statusCode=0 signals a transport error; body is the error message
    const msg = Buffer.from(e.message, "utf8");
    dataBuf.writeInt32LE(0, 0);
    dataBuf.writeInt32LE(msg.length, 4);
    if (msg.length > 0) msg.copy(dataBuf, 8);
    Atomics.store(signal, 0, 1);
    Atomics.notify(signal, 0);
  });
  if (body) req.write(body);
  req.end();
});
