"use strict";

const { EventEmitter } = require("events");
const { Readable, Writable, PassThrough } = require("stream");
const { promisify } = require("util");
const client = require("./client");
const wsClient = require("./ws-client");

// When true, spawn uses WebSocket transport (streaming stdin + binary-safe).
let _wsTransport = false;
let _wsBaseURL = null;
let _wsToken = null;

// ─── Helpers ─────────────────────────────────────────────
// Default cwd when none is given. SSH exec defaults to the user's home; we
// mirror that. Overridable via configure({ defaultCwd }).
let _defaultCwd = process.env.HOME || "/";

function _setDefaultCwd(v) {
  if (v) _defaultCwd = v;
}

function _setWsTransport(enabled, baseURL, token) {
  _wsTransport = !!enabled;
  // Fall back to the HTTP client's configured baseURL/token when not given
  // explicitly, so configure({ baseURL, token, wsTransport:true }) works in
  // one call as well as separate calls.
  _wsBaseURL = baseURL || client._config.baseURL;
  _wsToken = token || client._config.token;
}

function _normalizeCwd(cwd) {
  const c = cwd || _defaultCwd;
  if (c.includes("\\")) return c.split("\\").join("/");
  return c;
}

function _extractTimeoutMs(opts) {
  // Node timeout is in milliseconds; server expects seconds. Default 0 = no timeout.
  if (!opts.timeout) return 0;
  if (opts.timeout < 1000) return 1;
  return Math.ceil(opts.timeout / 1000);
}

function _execToError(result, cmd) {
  if (result.exit_code !== 0) {
    const err = new Error(`Command failed: ${cmd}`);
    err.cmd = cmd;
    err.stdout = result.stdout || "";
    err.stderr = result.stderr || "";
    err.code = result.exit_code;
    err.killed = result.exit_code === -1;
    return err;
  }
  return null;
}

function _applyEncoding(buf, encoding) {
  if (encoding === "buffer") return Buffer.from(buf);
  return Buffer.from(buf).toString(encoding || "utf8");
}

// Extract raw bytes from a sync /api/exec result. Prefers base64 (binary-safe)
// when the server provided it; falls back to the decoded string for old servers.
function _syncBuf(result, key) {
  const b64 = key === "stdout" ? result.stdout_b64 : result.stderr_b64;
  if (b64) return Buffer.from(b64, "base64");
  return Buffer.from(result[key] || "", "utf8");
}

// When shell:true and args are given, Node.js concatenates cmd+args into the
// shell command string (DEP0190 — not escaped, just joined with spaces).
function _shellCmd(command, args) {
  return args && args.length ? [command, ...args].join(" ") : command;
}

// ─── Spawn error helpers (Node.js parity) ─────────────────
// Map server-side spawn failures to Node.js error shapes so downstream
// `err.code === 'ENOENT'` / `err.code === 'EACCES'` checks work.
const _ERRNO_MAP = { ENOENT: -2, EACCES: -13, ENOTDIR: -20 };

function _spawnError(code, cmd, syscall) {
  const err = new Error(`${syscall} ${cmd} ${code}`);
  err.code = code;
  err.errno = _ERRNO_MAP[code] != null ? _ERRNO_MAP[code] : -1;
  err.syscall = `${syscall} ${cmd}`;
  err.path = cmd;
  err.spawnargs = [];
  return err;
}

// Enrich an error emitted by the server with Node.js spawn error properties.
function _enrichSpawnError(err, cmd, syscall) {
  if (err && err.code && _ERRNO_MAP[err.code] != null) {
    err.errno = _ERRNO_MAP[err.code];
    err.syscall = `${syscall} ${cmd}`;
    err.path = cmd;
    err.spawnargs = [];
  }
  return err;
}

// Map a sync HTTP 404 (command not found) to a Node.js spawn error.
function _mapSyncError(e, cmd, syscall) {
  if (e && e.statusCode === 404 && e.body) {
    try {
      const body = JSON.parse(e.body.toString("utf8"));
      if (body.error_code && _ERRNO_MAP[body.error_code] != null) {
        return _spawnError(body.error_code, cmd, syscall);
      }
    } catch {}
  }
  return e;
}

// ─── AbortSignal support ──────────────────────────────────
function _makeAbortError() {
  const err = new Error("The operation was aborted");
  err.name = "AbortError";
  err.code = "ABORT_ERR";
  return err;
}

// Wire an AbortSignal to a ChildProcess. On abort, kill the child. For
// exec/execFile (buffer mode), deliver an AbortError via the callback if it
// hasn't fired yet. For spawn, the kill produces a normal exit/close.
function _wireAbortSignal(child, signal) {
  if (!signal || typeof signal.addEventListener !== "function") return;
  const onAbort = () => {
    child.aborted = true;
    child.kill();
    if (child._onExitCb) {
      const cb = child._onExitCb;
      child._onExitCb = null;
      try { cb(_makeAbortError(), "", ""); } catch {}
    }
  };
  if (signal.aborted) { onAbort(); return; }
  signal.addEventListener("abort", onAbort, { once: true });
  const cleanup = () => signal.removeEventListener("abort", onAbort);
  child.once("close", cleanup);
  child.once("error", cleanup);
}

// ─── ChildProcess (for spawn) ────────────────────────────
// stdin uses 攒-end semantics: write() buffers chunks; final()/end() flushes
// them as a single POST to /api/exec/stdin. True streaming stdin (write →
// await output → write) is unsupported over HTTP/SSE — throws ERR_STREAM
// if write is called after the stream already flushed.
class ChildProcess extends EventEmitter {
  constructor() {
    super();
    this.pid = undefined;
    this.exitCode = null;
    this.signalCode = null;
    this.killed = false;
    this.aborted = false;
    this.connected = false;
    this.spawnfile = undefined;
    this.spawnargs = [];

    this.stdout = new Readable({ read() {} });
    this.stderr = new Readable({ read() {} });
    this._stdinBuf = [];
    this._stdinClosed = false;
    this.stdin = new Writable({
      write: (chunk, encoding, cb) => {
        if (this._stdinClosed) {
          const er = new Error("write after end");
          er.code = "ERR_STREAM_WRITE_AFTER_END";
          return cb(er);
        }
        this._stdinBuf.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk, encoding));
        cb();
      },
      final: (cb) => {
        this._flushStdin().then(() => cb(), (e) => cb(e));
      },
    });
    // stdio assigned AFTER the streams exist (codex assigned it before — bug).
    this.stdio = [this.stdin, this.stdout, this.stderr];

    this._res = null;
    this._closed = false;
    this._maxBuffer = null;
    this._stdoutLen = 0;
    this._stderrLen = 0;
    // Buffer-collect mode (exec/execFile): stdout/stderr bytes accumulate
    // internally instead of flowing to the readable streams, and the callback
    // fires on exit with (err, stdout, stderr). Lets exec/execFile run over
    // the streaming endpoint so they get a live PID (killable mid-run).
    this._bufferMode = false;
    this._stdoutBuf = [];
    this._stderrBuf = [];
    this._onExitCb = null;
    this._execCmd = "";
    this._execEncoding = null;
  }

  async _flushStdin() {
    if (this._stdinClosed) return;
    this._stdinClosed = true;
    if (this._stdinBuf.length === 0 || this.pid == null) return;
    const data = Buffer.concat(this._stdinBuf).toString("utf8");
    this._stdinBuf = [];
    try {
      await client.postJSON("/api/exec/stdin", { pid: this.pid, data, close: true });
    } catch (e) {
      // Process may have exited before stdin flush — not fatal.
    }
  }

  _attachStream(res) {
    this._res = res;
    let buf = "";

    res.on("data", (chunk) => {
      buf += chunk.toString("utf8");
      const parts = buf.split("\n\n");
      buf = parts.pop();

      for (const part of parts) {
        // SSE frames may be data: lines or : comments (keepalive)
        if (part.startsWith(":")) continue;
        const lines = part.split("\n");
        let dataStr = "";
        for (const line of lines) {
          if (line.startsWith("data: ")) {
            dataStr += line.slice(6);
          }
        }
        if (!dataStr) continue;

        try {
          const event = JSON.parse(dataStr);

          if (event.type === "pid") {
            this.pid = event.pid;
            this.emit("spawn");
            // If stdin was written before pid arrived, flush now.
            if (this._stdinBuf.length > 0 && !this._stdinClosed) {
              this._flushStdin().catch(() => {});
            }
          } else if (event.type === "stdout") {
            const data = Buffer.from(event.data, "utf8");
            this._stdoutLen += data.length;
            if (this._maxBuffer && this._stdoutLen > this._maxBuffer) {
              this.kill(this._killSignal || "SIGTERM");
              this._emitExecError(new Error("maxBuffer size exceeded"));
              return;
            }
            if (this._bufferMode) this._stdoutBuf.push(data);
            else this.stdout.push(data);
          } else if (event.type === "stderr") {
            const data = Buffer.from(event.data, "utf8");
            this._stderrLen += data.length;
            if (this._maxBuffer && this._stderrLen > this._maxBuffer) {
              this.kill(this._killSignal || "SIGTERM");
              this._emitExecError(new Error("maxBuffer size exceeded"));
              return;
            }
            if (this._bufferMode) this._stderrBuf.push(data);
            else this.stderr.push(data);
          } else if (event.type === "exit") {
            this.exitCode = event.code;
            this.signalCode = event.signal || null;
            if (!this._bufferMode) {
              this.stdout.push(null);
              this.stderr.push(null);
            }
            this.emit("exit", event.code, event.signal || null);
            this._fireExecCallback();
          } else if (event.type === "error") {
            const err = new Error(event.data);
            if (event.code) err.code = event.code;
            this._emitExecError(_enrichSpawnError(err, this.spawnfile, "spawn"));
          }
        } catch {}
      }
    });

    res.on("end", () => {
      if (this._closed) return;
      this._closed = true;
      if (this.exitCode === null) {
        // Stream ended without an exit frame — synthesize from connection end.
        if (this.killed) {
          this.exitCode = null;
          this.signalCode = this.signalCode || "SIGTERM";
        } else {
          this.exitCode = 0;
        }
      }
      if (!this._bufferMode) {
        this.stdout.push(null);
        this.stderr.push(null);
      }
      this.emit("close", this.exitCode, this.signalCode);
      this._fireExecCallback();
    });

    res.on("error", (err) => {
      this._emitExecError(err);
    });
  }

  // WS variant of _attachStream — same buffering/callback semantics but driven
  // by a WsExecSession (binary-safe stdout/stderr) instead of an SSE response.
  // Uses the session's synchronous stdoutdata/stderrdata events (not the
  // Readable's async "data" events) so all chunks are collected before "exit".
  _attachWsSession(session) {
    this._wsSession = session;
    this.pid = session.pid;

    // kill() before connect set this.killed but pid was null — fire HTTP kill now.
    if (this.killed) {
      client.kill(this.pid, this._killSignal || "SIGTERM").catch(() => {});
    }

    session.on("stdoutdata", (data) => {
      this._stdoutLen += data.length;
      if (this._maxBuffer && this._stdoutLen > this._maxBuffer) {
        session.kill(this._killSignal || "SIGTERM");
        this._emitExecError(new Error("maxBuffer stdout exceeded"));
        return;
      }
      if (this._bufferMode) this._stdoutBuf.push(data);
      else this.stdout.push(data);
    });

    session.on("stderrdata", (data) => {
      this._stderrLen += data.length;
      if (this._maxBuffer && this._stderrLen > this._maxBuffer) {
        session.kill(this._killSignal || "SIGTERM");
        this._emitExecError(new Error("maxBuffer stderr exceeded"));
        return;
      }
      if (this._bufferMode) this._stderrBuf.push(data);
      else this.stderr.push(data);
    });

    session.on("exit", (code, signal) => {
      this.exitCode = code;
      this.signalCode = signal;
      if (!this._bufferMode) {
        this.stdout.push(null);
        this.stderr.push(null);
      }
      this.emit("exit", code, signal);
      this._fireExecCallback();
    });

    session.on("error", (err) => {
      this._emitExecError(_enrichSpawnError(err, this.spawnfile, "spawn"));
    });

    session.on("close", (code, signal) => {
      if (this._closed) return;
      this._closed = true;
      if (this.exitCode === null) {
        this.exitCode = code != null ? code : (this.killed ? null : 0);
        this.signalCode = signal || this.signalCode;
      }
      if (!this._bufferMode) {
        this.stdout.push(null);
        this.stderr.push(null);
      }
      this.emit("close", this.exitCode, this.signalCode);
      this._fireExecCallback();
    });

    this.emit("spawn");
  }

  // exec/execFile buffer-collect helpers
  _fireExecCallback() {
    if (!this._onExitCb) return;
    const cb = this._onExitCb;
    this._onExitCb = null;
    const stdout = _applyEncoding(Buffer.concat(this._stdoutBuf), this._execEncoding);
    const stderr = _applyEncoding(Buffer.concat(this._stderrBuf), this._execEncoding);
    const err = _execToError({ exit_code: this.exitCode, stdout, stderr }, this._execCmd);
    try { cb(err, stdout, stderr); } catch {}
  }

  _emitExecError(err) {
    // Only emit 'error' if someone is listening — exec/execFile callers that
    // use the callback API don't register an 'error' listener, and emitting
    // 'error' with no listeners throws. The callback below always fires.
    if (this.listenerCount("error") > 0) this.emit("error", err);
    if (this._onExitCb) {
      const cb = this._onExitCb;
      this._onExitCb = null;
      try { cb(err, "", ""); } catch {}
    }
  }

  kill(signal) {
    if (this.killed || this.pid == null) return true;
    const sig = signal || "SIGTERM";
    this.killed = true;
    this.signalCode = sig;

    // Await the kill so we can synthesize exit if the server can't deliver it.
    client
      .kill(this.pid, sig)
      .then((result) => {
        if (!result.ok) {
          // Process already gone or kill failed — synthesize exit so callers
          // don't hang waiting for an exit frame that will never arrive.
          this._synthesizeExit();
        }
      })
      .catch(() => {
        // Network error reaching kill endpoint — synthesize exit.
        this._synthesizeExit();
      });

    return true;
  }

  _synthesizeExit() {
    if (this._closed) return;
    this.exitCode = null;
    this.signalCode = this.signalCode || "SIGTERM";
    if (!this._bufferMode) {
      this.stdout.push(null);
      this.stderr.push(null);
    }
    this.emit("exit", this.exitCode, this.signalCode);
    this._closed = true;
    this.emit("close", this.exitCode, this.signalCode);
    this._fireExecCallback();
  }

  send(message, sendHandle, options, callback) {
    if (typeof sendHandle === "function") {
      callback = sendHandle;
    }
    if (typeof options === "function") {
      callback = options;
    }
    this.stdin.write(typeof message === "string" ? message : JSON.stringify(message));
    if (callback) callback();
    return true;
  }

  disconnect() {
    this.connected = false;
    this.emit("disconnect");
  }

  unref() {
    return this;
  }

  ref() {
    return this;
  }
}

// ─── exec (shell=true, stream-backed) ───────────────────
// Runs over /api/exec/stream so the returned ChildProcess has a live PID and
// supports mid-run kill(). stdout/stderr are buffer-collected internally and
// delivered to the callback on exit (matching Node's exec = spawn + buffer).
// ─── exec (shell=true, stream-backed) ───────────────────
// When wsTransport is on, exec/execFile run over /ws/exec with buffer-collect —
// same callback semantics as SSE, but binary-safe stdout/stderr (base64).
function _execOverWs(child, startMsg, stdinData) {
  child.stdin = new PassThrough();
  child.stdio = [child.stdin, child.stdout, child.stderr];

  // Override kill so it records intent even before pid arrives; _attachWsSession
  // fires the HTTP kill once the pid is known.
  child.kill = (sig) => {
    if (child.killed) return true;
    child.killed = true;
    child.signalCode = sig || "SIGTERM";
    if (child.pid != null) {
      client.kill(child.pid, sig || "SIGTERM")
        .then((r) => { if (!r.ok) child._synthesizeExit(); })
        .catch(() => child._synthesizeExit());
    }
    return true;
  };

  wsClient.connect(_wsBaseURL, _wsToken, startMsg).then((session) => {
    child._attachWsSession(session);
    child.stdin.pipe(session.stdin);
    if (stdinData != null) {
      child.stdin.write(stdinData);
      child.stdin.end();
    }
  }).catch((err) => {
    child._emitExecError(_enrichSpawnError(err, child.spawnfile, "spawn"));
  });
}

function exec(command, options, callback) {
  if (typeof options === "function") {
    callback = options;
    options = {};
  }
  const opts = options || {};
  const cwd = _normalizeCwd(opts.cwd);
  const timeout = _extractTimeoutMs(opts);
  const env = opts.env || {};
  const maxBuffer = opts.maxBuffer || 1024 * 1024;
  const encoding = opts.encoding;
  const killSignal = opts.killSignal || "SIGTERM";

  let stdinData = null;
  if (opts.input) {
    stdinData = typeof opts.input === "string" ? opts.input : opts.input.toString("utf8");
  }

  const child = new ChildProcess();
  child.spawnfile = command;
  child.spawnargs = ["sh", "-c", command];
  child._maxBuffer = maxBuffer;
  child._killSignal = killSignal;
  child._bufferMode = true;
  child._execCmd = command;
  child._execEncoding = encoding;
  child._onExitCb = callback || null;

  if (_wsTransport) {
    _execOverWs(child, { cmd: command, shell: true, cwd, env, timeout }, stdinData);
    if (opts.signal) _wireAbortSignal(child, opts.signal);
    return child;
  }

  client
    .postStream("/api/exec/stream", {
      cmd: command,
      shell: true,
      cwd,
      env,
      timeout,
      stdin: stdinData,
    })
    .then((res) => child._attachStream(res))
    .catch((err) => child._emitExecError(err));

  if (opts.signal) _wireAbortSignal(child, opts.signal);
  return child;
}

// ─── execFile (shell=false, stream-backed) ──────────────
function execFile(file, args, options, callback) {
  if (typeof args === "function") {
    callback = args;
    args = undefined;
    options = {};
  } else if (typeof options === "function") {
    callback = options;
    options = {};
  }
  const opts = options || {};
  const cwd = _normalizeCwd(opts.cwd);
  const timeout = _extractTimeoutMs(opts);
  const env = opts.env || {};
  const maxBuffer = opts.maxBuffer || 1024 * 1024;
  const encoding = opts.encoding;
  const killSignal = opts.killSignal || "SIGTERM";

  let stdinData = null;
  if (opts.input) {
    stdinData = typeof opts.input === "string" ? opts.input : opts.input.toString("utf8");
  }

  const child = new ChildProcess();
  child.spawnfile = file;
  child.spawnargs = [file, ...(args || [])];
  child._maxBuffer = maxBuffer;
  child._killSignal = killSignal;
  child._bufferMode = true;
  child._execCmd = file;
  child._execEncoding = encoding;
  child._onExitCb = callback || null;

  if (_wsTransport) {
    _execOverWs(child, { cmd: file, args: args || [], shell: false, cwd, env, timeout }, stdinData);
    if (opts.signal) _wireAbortSignal(child, opts.signal);
    return child;
  }

  client
    .postStream("/api/exec/stream", {
      cmd: file,
      args: args || [],
      shell: false,
      cwd,
      env,
      timeout,
      stdin: stdinData,
    })
    .then((res) => child._attachStream(res))
    .catch((err) => child._emitExecError(err));

  if (opts.signal) _wireAbortSignal(child, opts.signal);
  return child;
}

// ─── execSync (shell=true) ──────────────────────────────
function execSync(command, options) {
  const opts = options || {};
  const cwd = _normalizeCwd(opts.cwd);
  const timeout = _extractTimeoutMs(opts);
  const env = opts.env || {};
  const encoding = opts.encoding;

  let stdinData = null;
  if (opts.input) {
    stdinData = typeof opts.input === "string" ? opts.input : opts.input.toString("utf8");
  }

  let result;
  try {
    result = client.postJSONSync("/api/exec", {
      cmd: command,
      shell: true,
      cwd,
      env,
      timeout,
      stdin: stdinData,
      binary: true,
    });
  } catch (e) {
    throw _mapSyncError(e, command, "spawnSync");
  }

  const stdoutRaw = _syncBuf(result, "stdout");
  if (opts.maxBuffer && stdoutRaw.length > opts.maxBuffer) {
    const err = new Error("maxBuffer size exceeded");
    err.cmd = command;
    throw err;
  }

  const err = _execToError(result, command);
  if (err) throw err;

  return _applyEncoding(stdoutRaw, encoding);
}

// ─── execFileSync (shell=false) ─────────────────────────
function execFileSync(file, args, options) {
  const opts = options || {};
  const cwd = _normalizeCwd(opts.cwd);
  const timeout = _extractTimeoutMs(opts);
  const env = opts.env || {};
  const encoding = opts.encoding;

  let stdinData = null;
  if (opts.input) {
    stdinData = typeof opts.input === "string" ? opts.input : opts.input.toString("utf8");
  }

  let result;
  try {
    result = client.postJSONSync("/api/exec", {
      cmd: file,
      args: args || [],
      shell: false,
      cwd,
      env,
      timeout,
      stdin: stdinData,
      binary: true,
    });
  } catch (e) {
    throw _mapSyncError(e, file, "spawnSync");
  }

  const stdoutRaw = _syncBuf(result, "stdout");
  if (opts.maxBuffer && stdoutRaw.length > opts.maxBuffer) {
    const err = new Error("maxBuffer size exceeded");
    err.cmd = file;
    throw err;
  }

  const err = _execToError(result, file);
  if (err) throw err;

  return _applyEncoding(stdoutRaw, encoding);
}

// ─── spawn (shell=false unless opts.shell) ──────────────
function spawn(command, args, options) {
  const opts = options || {};
  const cwd = _normalizeCwd(opts.cwd);
  const env = opts.env || {};
  const timeout = _extractTimeoutMs(opts);

  let stdinData = null;
  if (opts.input) {
    stdinData = typeof opts.input === "string" ? opts.input : opts.input.toString("utf8");
  }

  const startMsg = opts.shell
    ? { cmd: _shellCmd(command, args), shell: opts.shell, cwd, env, timeout, stdin: stdinData }
    : { cmd: command, args: args || [], shell: false, cwd, env, timeout, stdin: stdinData };

  if (opts.uid != null) startMsg.uid = opts.uid;
  if (opts.gid != null) startMsg.gid = opts.gid;

  // PTY mode (interactive processes): echo, line editing, terminal control.
  if (opts.pty) {
    startMsg.pty = true;
    startMsg.cols = opts.cols || 80;
    startMsg.rows = opts.rows || 24;
  }
  // Detach: process survives WS disconnect (output drained on the server).
  // Accept both Node.js spelling (detached) and our own (detach).
  const detachable = !!(opts.detach || opts.detached);
  if (detachable) {
    startMsg.detach = true;
  }

  // ─── WebSocket transport: true streaming stdin + binary-safe stdout/stderr ───
  if (_wsTransport) {
    const child = new ChildProcess();
    child.spawnfile = command;
    child.spawnargs = [command, ...(args || [])];
    child._maxBuffer = opts.maxBuffer || null;
    child._killSignal = opts.killSignal || "SIGTERM";

    // PassThrough so listeners can attach immediately and data flows through
    // once the WS session connects (no lost chunks, no ordering race).
    const stdioOpt = opts.stdio || "pipe";
    const stdoutMode = Array.isArray(stdioOpt) ? (stdioOpt[1] || "pipe") : stdioOpt;
    const stderrMode = Array.isArray(stdioOpt) ? (stdioOpt[2] || "pipe") : stdioOpt;
    child.stdout = stdoutMode === "ignore" ? null : new PassThrough();
    child.stderr = stderrMode === "ignore" ? null : new PassThrough();
    child.stdin = new PassThrough();
    child.stdio = [child.stdin, child.stdout, child.stderr];

    let session = null;
    let killedEarly = false;

    child.kill = (sig) => {
      child.killed = true;
      if (session) return session.kill(sig);
      killedEarly = true; // connect not yet resolved — kill once we have it
      return true;
    };
    child.send = (msg) => child.stdin.write(typeof msg === "string" ? msg : JSON.stringify(msg));
    child.disconnect = () => { if (session) session.close(); };
    child.resize = (cols, rows) => { if (session) session.resize(cols, rows); };
    child.ref = () => child;
    child.unref = () => child;

    wsClient.connect(_wsBaseURL, _wsToken, startMsg).then((sess) => {
      session = sess;
      child.pid = sess.pid;

      if (child.stdout) {
        sess.stdout.pipe(child.stdout);
        if (stdoutMode === "inherit") child.stdout.pipe(process.stdout);
      } else {
        sess.stdout.resume();
      }
      if (child.stderr) {
        sess.stderr.pipe(child.stderr);
        if (stderrMode === "inherit") child.stderr.pipe(process.stderr);
      } else {
        sess.stderr.resume();
      }
      child.stdin.pipe(sess.stdin);

      // If kill() was called before connect resolved, kill now.
      if (killedEarly) sess.kill(child._killSignal);

      const maxBuf = opts.maxBuffer || null;
      if (maxBuf) {
        let stdoutLen = 0, stderrLen = 0;
        if (child.stdout) child.stdout.on("data", (chunk) => {
          stdoutLen += chunk.length;
          if (stdoutLen > maxBuf) {
            sess.kill(opts.killSignal || "SIGTERM");
            child.emit("error", new Error("maxBuffer stdout exceeded"));
          }
        });
        if (child.stderr) child.stderr.on("data", (chunk) => {
          stderrLen += chunk.length;
          if (stderrLen > maxBuf) {
            sess.kill(opts.killSignal || "SIGTERM");
            child.emit("error", new Error("maxBuffer stderr exceeded"));
          }
        });
      }

      sess.on("exit", (code, signal) => {
        child.exitCode = code;
        child.signalCode = signal;
        child.emit("exit", code, signal);
      });
      sess.on("close", (code, signal) => {
        if (detachable && child.exitCode === null) {
          // Detached: proc survives on the server — emit 'detach', not 'close'.
          child.emit("detach", child.pid);
        } else {
          child.emit("close", code, signal);
        }
      });
      sess.on("error", (err) => child.emit("error", _enrichSpawnError(err, child.spawnfile, "spawn")));

      child.emit("spawn");
    }).catch((err) => {
      // Connect failed — end stdio so consumers don't hang, then surface error.
      child.stdin.end();
      if (child.stdout) child.stdout.push(null);
      if (child.stderr) child.stderr.push(null);
      child.emit("error", _enrichSpawnError(err, child.spawnfile, "spawn"));
      // Defer close so listeners registered after 'error' can still catch it
      // (matches Node.js, which always emits 'close' asynchronously).
      process.nextTick(() => child.emit("close", child.exitCode, child.signalCode));
    });

    if (opts.signal) _wireAbortSignal(child, opts.signal);
    return child;
  }

  // ─── SSE transport (original path) ───
  const child = new ChildProcess();
  child.spawnfile = command;
  child.spawnargs = [command, ...(args || [])];
  child._maxBuffer = opts.maxBuffer || null;
  child._killSignal = opts.killSignal || "SIGTERM";

  const stdio = opts.stdio || "pipe";
  const _stdoutIgnore = stdio === "ignore" || (Array.isArray(stdio) && stdio[1] === "ignore");
  const _stderrIgnore = stdio === "ignore" || (Array.isArray(stdio) && stdio[2] === "ignore");
  if (_stdoutIgnore) {
    child.stdout = new Readable({ read() {} });
    child.stdout.push(null);
  }
  if (_stderrIgnore) {
    child.stderr = new Readable({ read() {} });
    child.stderr.push(null);
  }
  child.stdio = [child.stdin, child.stdout, child.stderr];
  if (stdio === "inherit" || (Array.isArray(stdio) && stdio[1] === "inherit"))
    child.stdout.pipe(process.stdout);
  if (stdio === "inherit" || (Array.isArray(stdio) && stdio[2] === "inherit"))
    child.stderr.pipe(process.stderr);

  client
    .postStream("/api/exec/stream", startMsg)
    .then((res) => child._attachStream(res))
    .catch((err) => child.emit("error", err));

  if (opts.signal) _wireAbortSignal(child, opts.signal);
  return child;
}

// ─── spawnSync ──────────────────────────────────────────
function spawnSync(command, args, options) {
  const opts = options || {};
  const cwd = _normalizeCwd(opts.cwd);
  const env = opts.env || {};
  const timeout = _extractTimeoutMs(opts);
  const encoding = opts.encoding;

  let stdinData = null;
  if (opts.input) {
    stdinData = typeof opts.input === "string" ? opts.input : opts.input.toString("utf8");
  }

  const reqBody = opts.shell
    ? { cmd: _shellCmd(command, args), shell: opts.shell, cwd, env, timeout, stdin: stdinData, binary: true }
    : { cmd: command, args: args || [], shell: false, cwd, env, timeout, stdin: stdinData, binary: true };
  if (opts.uid != null) reqBody.uid = opts.uid;
  if (opts.gid != null) reqBody.gid = opts.gid;

  let result;
  try {
    result = client.postJSONSync("/api/exec", reqBody);
  } catch (e) {
    // Map 404 (command not found) to Node.js spawn error; otherwise transport error.
    const mapped = _mapSyncError(e, command, "spawnSync");
    return {
      pid: 0,
      output: [null, "", ""],
      stdout: "",
      stderr: "",
      status: null,
      signal: null,
      error: mapped,
    };
  }

  let stdoutBuf = _syncBuf(result, "stdout");
  let stderrBuf = _syncBuf(result, "stderr");

  // Node semantics: maxBuffer exceeded → kill + set signal, not silent truncation.
  if (opts.maxBuffer) {
    if (stdoutBuf.length > opts.maxBuffer) {
      stdoutBuf = stdoutBuf.subarray(0, opts.maxBuffer);
      return {
        pid: result.pid || 0,
        output: [null, _applyEncoding(stdoutBuf, encoding), _applyEncoding(stderrBuf, encoding)],
        stdout: _applyEncoding(stdoutBuf, encoding),
        stderr: _applyEncoding(stderrBuf, encoding),
        status: null,
        signal: opts.killSignal || "SIGTERM",
        error: new Error("maxBuffer size exceeded"),
      };
    }
    if (stderrBuf.length > opts.maxBuffer) {
      stderrBuf = stderrBuf.subarray(0, opts.maxBuffer);
      return {
        pid: result.pid || 0,
        output: [null, _applyEncoding(stdoutBuf, encoding), _applyEncoding(stderrBuf, encoding)],
        stdout: _applyEncoding(stdoutBuf, encoding),
        stderr: _applyEncoding(stderrBuf, encoding),
        status: null,
        signal: opts.killSignal || "SIGTERM",
        error: new Error("maxBuffer size exceeded"),
      };
    }
  }

  const stdout = _applyEncoding(stdoutBuf, encoding);
  const stderr = _applyEncoding(stderrBuf, encoding);

  return {
    pid: result.pid || 0,
    output: [null, stdout, stderr],
    stdout,
    stderr,
    status: result.exit_code,
    signal: result.exit_code === -1 ? (opts.killSignal || "SIGTERM") : null,
    error: result.exit_code !== 0 && result.exit_code !== -1 ? new Error(`Command failed: ${command}`) : null,
  };
}

// ─── fork (shell=false, no injection) ───────────────────
function fork(modulePath, args, options) {
  const opts = options || {};
  // Use "node" (resolved on the remote server's $PATH) rather than
  // process.execPath, which is a client-local path that won't exist on the
  // remote host. modulePath is passed as an argv element (no shell), so
  // metacharacters in it are not interpreted.
  const child = spawn("node", [modulePath, ...(args || [])], {
    ...opts,
    shell: false,
  });
  child.connected = true;
  return child;
}

// ─── Promises API ────────────────────────────────────────
const promises = {
  exec(command, options) {
    let childRef;
    const p = new Promise((resolve, reject) => {
      childRef = exec(command, options, (err, stdout, stderr) => {
        if (err) {
          err.stdout = stdout;
          err.stderr = stderr;
          reject(err);
        } else {
          resolve({ stdout, stderr });
        }
      });
    });
    p.child = childRef;
    return p;
  },
  execFile(file, args, options) {
    let childRef;
    const p = new Promise((resolve, reject) => {
      childRef = execFile(file, args, options, (err, stdout, stderr) => {
        if (err) {
          err.stdout = stdout;
          err.stderr = stderr;
          reject(err);
        } else {
          resolve({ stdout, stderr });
        }
      });
    });
    p.child = childRef;
    return p;
  },
  fork(modulePath, args, options) {
    return Promise.resolve(fork(modulePath, args, options));
  },
};

// util.promisify.custom: make util.promisify(exec/execFile) resolve to
// {stdout, stderr} (object) with .child attached, matching native Node.js.
// Without this, generic promisify resolves to an array [stdout, stderr].
exec[promisify.custom] = promises.exec;
execFile[promisify.custom] = promises.execFile;

// ─── Constants ───────────────────────────────────────────
const constants = {
  UV_PROCESS_DETACHED: 0,
  UV_PROCESS_SETGROUPS: 0,
  UV_PROCESS_SETUID: 0,
  UV_PROCESS_SETGID: 0,
  UV_PROCESS_WINDOWS_HIDE: 0,
  UV_PROCESS_WINDOWS_HIDE_CONSOLE: 0,
  UV_PROCESS_WINDOWS_HIDE_GUI: 0,
  UV_PROCESS_VERBOSITY: 0,
};

// ─── Batch exec ──────────────────────────────────────────
// Returns results array directly (unwrapped from {results: [...]}).
async function batchExec(cmds, opts = {}) {
  const resp = await client.batch(cmds, {
    mode: opts.mode,
    cwd: _normalizeCwd(opts.cwd),
    env: opts.env,
    timeout: opts.timeout,
  });
  return resp.results;
}

function batchExecSync(cmds, opts = {}) {
  const resp = client.batchSync(cmds, {
    mode: opts.mode,
    cwd: _normalizeCwd(opts.cwd),
    env: opts.env,
    timeout: opts.timeout,
  });
  return resp.results;
}

module.exports = {
  exec,
  execFile,
  execSync,
  execFileSync,
  spawn,
  spawnSync,
  fork,
  batchExec,
  batchExecSync,
  ChildProcess,
  constants,
  promises,
  _client: client,
  _setDefaultCwd,
  _setWsTransport,
};
