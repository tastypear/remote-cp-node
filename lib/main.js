"use strict";

const { EventEmitter } = require("events");
const { Readable, Writable } = require("stream");
const client = require("./client");

// ─── Helpers ─────────────────────────────────────────────
// Default cwd when none is given. SSH exec defaults to the user's home; we
// mirror that. Overridable via configure({ defaultCwd }).
let _defaultCwd = process.env.HOME || "/";

function _setDefaultCwd(v) {
  if (v) _defaultCwd = v;
}

function _normalizeCwd(cwd) {
  const c = cwd || _defaultCwd;
  if (c.includes("\\")) return c.split("\\").join("/");
  return c;
}

function _extractTimeoutMs(opts) {
  // Node timeout is in milliseconds; server expects seconds
  if (!opts.timeout) return 30;
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
            this._emitExecError(new Error(event.data));
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
    this.emit("error", err);
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

  const result = client.postJSONSync("/api/exec", {
    cmd: command,
    shell: true,
    cwd,
    env,
    timeout,
    stdin: stdinData,
  });

  if (opts.maxBuffer && result.stdout && result.stdout.length > opts.maxBuffer) {
    const err = new Error("maxBuffer size exceeded");
    err.cmd = command;
    throw err;
  }

  const err = _execToError(result, command);
  if (err) throw err;

  return _applyEncoding(result.stdout || "", encoding);
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

  const result = client.postJSONSync("/api/exec", {
    cmd: file,
    args: args || [],
    shell: false,
    cwd,
    env,
    timeout,
    stdin: stdinData,
  });

  if (opts.maxBuffer && result.stdout && result.stdout.length > opts.maxBuffer) {
    const err = new Error("maxBuffer size exceeded");
    err.cmd = file;
    throw err;
  }

  const err = _execToError(result, file);
  if (err) throw err;

  return _applyEncoding(result.stdout || "", encoding);
}

// ─── spawn (shell=false unless opts.shell) ──────────────
function spawn(command, args, options) {
  const opts = options || {};
  const cwd = _normalizeCwd(opts.cwd);
  const env = opts.env || {};
  const timeout = opts.timeout ? _extractTimeoutMs(opts) : 300;

  const child = new ChildProcess();
  child.spawnfile = command;
  child.spawnargs = [command, ...(args || [])];
  child._maxBuffer = opts.maxBuffer || null;
  child._killSignal = opts.killSignal || "SIGTERM";

  // Handle stdio option
  const stdio = opts.stdio || "pipe";
  if (stdio === "ignore" || (Array.isArray(stdio) && stdio[1] === "ignore")) {
    child.stdout = new Readable({ read() {} });
    child.stdout.push(null);
  }
  if (stdio === "ignore" || (Array.isArray(stdio) && stdio[2] === "ignore")) {
    child.stderr = new Readable({ read() {} });
    child.stderr.push(null);
  }
  // Re-bind stdio if stdout/stderr were replaced above.
  child.stdio = [child.stdin, child.stdout, child.stderr];

  let stdinData = null;
  if (opts.input) {
    stdinData = typeof opts.input === "string" ? opts.input : opts.input.toString("utf8");
  }

  const sseBody = opts.shell
    ? { cmd: command, shell: true, cwd, env, timeout, stdin: stdinData }
    : { cmd: command, args: args || [], shell: false, cwd, env, timeout, stdin: stdinData };

  client
    .postStream("/api/exec/stream", sseBody)
    .then((res) => {
      child._attachStream(res);
    })
    .catch((err) => {
      child.emit("error", err);
    });

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
    ? { cmd: command, shell: true, cwd, env, timeout, stdin: stdinData }
    : { cmd: command, args: args || [], shell: false, cwd, env, timeout, stdin: stdinData };

  let result;
  try {
    result = client.postJSONSync("/api/exec", reqBody);
  } catch (e) {
    // Network/transport error
    return {
      pid: 0,
      output: [null, "", ""],
      stdout: "",
      stderr: "",
      status: null,
      signal: null,
      error: e,
    };
  }

  let stdoutBuf = Buffer.from(result.stdout || "");
  let stderrBuf = Buffer.from(result.stderr || "");

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
    return new Promise((resolve, reject) => {
      exec(command, options, (err, stdout, stderr) => {
        if (err) {
          err.stdout = stdout;
          err.stderr = stderr;
          reject(err);
        } else {
          resolve({ stdout, stderr });
        }
      });
    });
  },
  execFile(file, args, options) {
    return new Promise((resolve, reject) => {
      execFile(file, args, options, (err, stdout, stderr) => {
        if (err) {
          err.stdout = stdout;
          err.stderr = stderr;
          reject(err);
        } else {
          resolve({ stdout, stderr });
        }
      });
    });
  },
  fork(modulePath, args, options) {
    return Promise.resolve(fork(modulePath, args, options));
  },
};

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

module.exports = {
  exec,
  execFile,
  execSync,
  execFileSync,
  spawn,
  spawnSync,
  fork,
  ChildProcess,
  constants,
  promises,
  _client: client,
  _setDefaultCwd,
};
