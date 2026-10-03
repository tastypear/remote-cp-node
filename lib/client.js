"use strict";

const http = require("http");
const https = require("https");
const { URL } = require("url");
const { execFileSync } = require("child_process");

const _config = {
  baseURL: "http://127.0.0.1:8765",
  token: "",
  curlPath: null,
};

// Pooled keepAlive agents — reused across async requests (kill, stdin, etc.)
// to avoid a fresh TCP handshake per call. Mirrors remote-fs-node's client.
const _httpAgent = new http.Agent({ keepAlive: true, maxSockets: 16, keepAliveMsecs: 1000 });
const _httpsAgent = new https.Agent({ keepAlive: true, maxSockets: 16, keepAliveMsecs: 1000 });

function configure(opts) {
  Object.assign(_config, opts);
}

function _curl() {
  return _config.curlPath || "curl";
}

// ─── Async HTTP ──────────────────────────────────────────
function _asyncRequest(method, path, { body, headers = {} } = {}) {
  return new Promise((resolve, reject) => {
    const fullUrl = new URL(path, _config.baseURL);
    const lib = fullUrl.protocol === "https:" ? https : http;

    const reqHeaders = { ...headers };
    if (_config.token) {
      reqHeaders["Authorization"] = "Bearer " + _config.token;
    }

    const req = lib.request(
      {
        method,
        hostname: fullUrl.hostname,
        port: fullUrl.port,
        path: fullUrl.pathname + fullUrl.search,
        headers: reqHeaders,
        agent: fullUrl.protocol === "https:" ? _httpsAgent : _httpAgent,
      },
      (res) => {
        const chunks = [];
        res.on("data", (c) => chunks.push(c));
        res.on("end", () => {
          const buf = Buffer.concat(chunks);
          if (res.statusCode >= 400) {
            const err = new Error(`HTTP ${res.statusCode}: ${buf.toString("utf8")}`);
            err.statusCode = res.statusCode;
            err.body = buf;
            reject(err);
          } else {
            resolve({ buffer: buf, statusCode: res.statusCode, headers: res.headers });
          }
        });
      }
    );

    req.on("error", reject);
    req.setTimeout(300000, () => req.destroy(new Error("Request timeout")));

    if (body !== undefined && body !== null) {
      if (Buffer.isBuffer(body) || typeof body === "string") {
        req.write(body);
      } else {
        req.setHeader("Content-Type", "application/json");
        req.write(JSON.stringify(body));
      }
    }
    req.end();
  });
}

// ─── Sync HTTP (via curl) ────────────────────────────────
function _syncRequest(method, path, { body } = {}) {
  const fullUrl = new URL(path, _config.baseURL).href;
  const args = ["-s", "-S", "--max-time", "300", "-w", "\n%{http_code}", "-X", method, fullUrl];

  if (_config.token) {
    args.push("-H", "Authorization: Bearer " + _config.token);
  }
  if (body !== undefined && body !== null) {
    if (Buffer.isBuffer(body)) {
      args.push("--data-binary", "@-");
    } else if (typeof body === "string") {
      args.push("--data-binary", body);
    } else {
      args.push("-H", "Content-Type: application/json");
      args.push("--data", JSON.stringify(body));
    }
  }

  const input = Buffer.isBuffer(body) ? body : undefined;
  const output = execFileSync(_curl(), args, {
    input,
    maxBuffer: 1024 * 1024 * 512,
  });

  const lastNl = output.lastIndexOf(0x0a);
  const httpCode = parseInt(output.subarray(lastNl + 1).toString("utf8").trim(), 10);
  const bodyData = output.subarray(0, lastNl);

  if (httpCode >= 400) {
    const err = new Error(`HTTP ${httpCode}: ${bodyData.toString("utf8")}`);
    err.statusCode = httpCode;
    err.body = bodyData;
    throw err;
  }
  return bodyData;
}

module.exports = {
  configure,
  async postJSON(path, obj) {
    const { buffer } = await _asyncRequest("POST", path, {
      body: JSON.stringify(obj),
      headers: { "Content-Type": "application/json" },
    });
    return JSON.parse(buffer.toString("utf8"));
  },
  postJSONSync(path, obj) {
    const out = _syncRequest("POST", path, { body: obj });
    return JSON.parse(out.toString("utf8"));
  },
  // Batch exec — POST /api/exec/batch
  // cmds: string[], opts: {mode, cwd, env, timeout}
  // Returns {results: [{stdout, stderr, exit_code, duration_ms}, ...]}
  async batch(cmds, opts = {}) {
    const { buffer } = await _asyncRequest("POST", "/api/exec/batch", {
      body: JSON.stringify({ cmds, ...opts }),
      headers: { "Content-Type": "application/json" },
    });
    return JSON.parse(buffer.toString("utf8"));
  },
  batchSync(cmds, opts = {}) {
    const out = _syncRequest("POST", "/api/exec/batch", { body: { cmds, ...opts } });
    return JSON.parse(out.toString("utf8"));
  },
  async postRaw(path, body) {
    const { buffer } = await _asyncRequest("POST", path, {
      body,
      headers: { "Content-Type": "application/octet-stream" },
    });
    return JSON.parse(buffer.toString("utf8"));
  },
  // SSE stream — returns the raw http.IncomingMessage
  async postStream(path, obj) {
    return new Promise((resolve, reject) => {
      const fullUrl = new URL(path, _config.baseURL);
      const lib = fullUrl.protocol === "https:" ? https : http;
      const body = JSON.stringify(obj);
      const reqHeaders = {
        "Content-Type": "application/json",
      };
      if (_config.token) reqHeaders["Authorization"] = "Bearer " + _config.token;

      const req = lib.request(
        {
          method: "POST",
          hostname: fullUrl.hostname,
          port: fullUrl.port,
          path: fullUrl.pathname + fullUrl.search,
          headers: { ...reqHeaders, "Content-Length": Buffer.byteLength(body) },
          agent: fullUrl.protocol === "https:" ? _httpsAgent : _httpAgent,
        },
        (res) => {
          if (res.statusCode >= 400) {
            const chunks = [];
            res.on("data", (c) => chunks.push(c));
            res.on("end", () => {
              const err = new Error(`HTTP ${res.statusCode}: ${Buffer.concat(chunks).toString()}`);
              err.statusCode = res.statusCode;
              reject(err);
            });
            return;
          }
          resolve(res);
        }
      );
      req.on("error", reject);
      req.setTimeout(300000, () => req.destroy(new Error("Stream timeout")));
      req.write(body);
      req.end();
    });
  },
  // Sync kill
  killSync(pid, signal) {
    const out = _syncRequest("POST", `/api/exec/kill?pid=${pid}&signal_name=${signal || "SIGTERM"}`);
    return JSON.parse(out.toString("utf8"));
  },
  // Async kill — returns {ok} even if the pid is unknown (404) or permission
  // denied (403), so callers can treat a missing process as "already gone"
  // rather than catching an exception.
  async kill(pid, signal) {
    try {
      const { buffer } = await _asyncRequest("POST", `/api/exec/kill?pid=${pid}&signal_name=${signal || "SIGTERM"}`);
      return JSON.parse(buffer.toString("utf8"));
    } catch (e) {
      if (e.statusCode === 404 || e.statusCode === 403) {
        return { ok: false, pid, signal: signal || "SIGTERM" };
      }
      throw e;
    }
  },
  // Check if a spawned process is still running (detached processes survive disconnect).
  async status(pid) {
    try {
      const { buffer } = await _asyncRequest("GET", `/api/exec/status?pid=${pid}`);
      return JSON.parse(buffer.toString("utf8"));
    } catch (e) {
      if (e.statusCode === 404) {
        return { pid, running: false, exit_code: null };
      }
      throw e;
    }
  },
  _asyncRequest,
  _syncRequest,
  _config,
};