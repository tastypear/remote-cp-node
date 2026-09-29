# remote-cp-node

Node.js `child_process`-compatible module backed by HTTP. A drop-in replacement for `require("child_process")` that routes `exec`/`spawn`/`fork` to a remote server — a non-interactive SSH exec replacement.

Companion to [remote-fs-node](https://github.com/tastypear/remote-fs-node) (SFTP replacement). Both share the same HTTP server ([remote-ops-server](https://github.com/tastypear/remote-ops-server)), `configure()` shape, and monkey-patch strategy.

## What makes this different

Like remote-fs-node, this patches Node at **three levels**:

1. **JS export layer** — replaces all methods on `require("child_process")`
2. **`process.binding` layer** — not applicable (child_process has no fs-style binding)
3. **`node:child_process` protocol** — patches `Module._resolveFilename` so `require("node:child_process")` is also intercepted

This means any code using `exec`/`spawn`/`execFile`/`fork` — including third-party libraries — works remotely without code changes.

## Quick start

```bash
npm install remote-cp-node
```

### As drop-in replacement

```js
const remoteCp = require("remote-cp-node");
remoteCp.configure({
  baseURL: "http://your-server:8765",
  token: "your-token",
});

const cp = remoteCp;
const { stdout, stderr } = await cp.promises.exec("uname -a");
const r = cp.spawn("ls", ["-la", "/tmp"]);  // streaming stdout
r.stdout.on("data", (d) => console.log(d.toString()));
```

### As monkey-patch

```js
const remoteCp = require("remote-cp-node");
remoteCp.configure({ baseURL: "http://your-server:8765", token: "xxx" });
remoteCp.patch();

// Now ALL code that uses child_process works remotely
const cp = require("child_process");
const { stdout } = cp.execSync("whoami");

// Restore
remoteCp.restore();
```

### Co-existence with remote-fs-node

Both libraries wrap `Module._resolveFilename` to intercept the `node:` protocol. When patching both in the same process, **restore in reverse order** (LIFO) — the last-patched library must restore first, because each saved `_resolveFilename` references the previous wrapper:

```js
remoteFs.patch();   // patches node:fs
remoteCp.patch();   // patches node:child_process (wraps fs's resolver)
// ... use both ...
remoteCp.restore(); // MUST restore cp before fs
remoteFs.restore();
```

Out-of-order restore leaves a dangling wrapper reference (`Cannot read properties of null`). This mirrors how Node's own `Module` hook chains (e.g. ts-node + esbuild) require LIFO teardown.

## API

Mirrors Node's `child_process`:

| Function | Transport | Shell | Notes |
|----------|-----------|-------|-------|
| `exec(cmd, opts, cb)` | SSE or WS (`/ws/exec`) | yes (`sh -c`) | Returns `ChildProcess` with **live PID** (killable mid-run); stdout/stderr buffer-collected, callback gets `(err, stdout, stderr)` on exit. WS transport adds binary-safe output |
| `execFile(file, args, opts, cb)` | SSE or WS | **no** | args as argv array (no injection); live PID, killable mid-run. WS transport adds binary-safe output |
| `execSync(cmd, opts)` | sync curl (`/api/exec`) | yes | Throws on non-zero exit |
| `execFileSync(file, args, opts)` | sync curl | **no** | args as argv |
| `spawn(cmd, args, opts)` | SSE or WS (`/ws/exec`) | no (unless `opts.shell`) | Returns `ChildProcess` with real PID; live stdout/stderr streams. WS transport (`wsTransport:true`) adds streaming stdin + binary-safe output |
| `spawnSync(cmd, args, opts)` | sync curl | no (unless `opts.shell`) | Returns `{pid, stdout, stderr, status, signal}` |
| `fork(modulePath, args, opts)` | SSE | **no** | Uses `"node"` (remote `$PATH`) + modulePath as arg (no shell, no injection) |
| `promises.exec` / `promises.execFile` / `promises.fork` | — | — | Promise wrappers |

`exec`/`execFile` run over the streaming endpoint (like Node's native `exec` = spawn + buffer), so they return a `ChildProcess` with a live PID that supports `kill()` mid-run. Output is buffer-collected up to `maxBuffer` and delivered to the callback on exit — no server-side full-buffer OOM.

### Options

- `cwd` — working directory (defaults to `configure({ defaultCwd })` or `$HOME`, mirroring SSH exec's home default)
- `env` — environment variables (merged over a sanitized server env; secrets like the auth token are never inherited)
- `timeout` — milliseconds (converted to seconds server-side); enforced on **both** sync and stream endpoints
- `maxBuffer` — bytes; exceeded → kill + `error`/`signal` (Node semantics, not silent truncation)
- `encoding` — `"buffer"` returns Buffer, else string (default `utf8`)
- `killSignal` — default `"SIGTERM"`
- `input` — one-shot stdin (string/Buffer), sent with the request and EOF'd before the process runs
- `stdio` — `"pipe"` (default) or `"ignore"`; array form supported for indices 1/2

### ChildProcess

`EventEmitter` with `pid`, `exitCode`, `signalCode`, `killed`, `stdin`, `stdout`, `stderr`, `stdio`. Methods: `kill(signal)`, `send()`, `disconnect()`, `ref()`/`unref()`.

`kill(signal)` contacts `/api/exec/kill` with **ownership verification** — the server only kills PIDs it spawned (whole process group via `os.killpg`). If the kill can't be confirmed (process already exited, network error), an `exit`/`close` is synthesized so callers never hang. Works on `exec`/`execFile`/`spawn`/`fork` (all stream-backed with live PIDs).

## stdin semantics

SSH exec channel supports one-shot stdin (`echo x | ssh host cmd`) and that's the dominant real-world pattern. remote-cp-node supports:

- **One-shot via `opts.input`** — sent in the request body, written before the process runs. Covers ~85% of use cases.
- **Buffered via `child.stdin.write()` + `end()`** — chunks are buffered and flushed as a single POST to `/api/exec/stdin` on `end()`. Covers pipe-style usage (`write(data); end()`). The flush waits for the `pid` SSE event before sending.
- **True streaming stdin** (write → await output → write) — supported via **WebSocket transport** (`configure({ wsTransport: true })`). `spawn()` upgrades to a bidirectional `/ws/exec` session: `child.stdin.write()` streams each chunk as a WS frame, `end()` sends EOF, and stdout/stderr are binary-safe (base64 fallback). `exec`/`execFile`/`spawnSync` still use SSE (buffered semantics). For PTY/resize, see future work.

## WebSocket transport

Enable with `configure({ wsTransport: true })`. `spawn()` then uses a bidirectional WebSocket (`/ws/exec`) instead of SSE, solving two SSE limitations:

- **Streaming stdin** — `child.stdin.write(chunk)` sends immediately; `child.stdin.end()` sends EOF. Interactive write→read→write works.
- **Binary-safe stdout/stderr** — invalid UTF-8 chunks are sent as base64 instead of `errors="replace"` (lossy). Applies to `spawn`, `exec`, and `execFile`.

Auth uses `Authorization: Bearer <token>` header (or `?token=` query fallback). The session is registered in the server's process table, so HTTP `/api/exec/kill` and `/api/exec/stdin` also work on WS-spawned PIDs. `exec`/`execFile` use WS with buffer-collect (same callback semantics, but binary-safe); `spawnSync`/`execSync`/`execFileSync` still use the sync HTTP path.

### PTY mode

`spawn(cmd, args, { pty: true, cols: 80, rows: 24 })` runs the child with a pseudo-terminal — the child sees a real TTY (`isatty()` true), gets echo and line editing, and `TERM=xterm-256color`. Output is merged (stdout+stderr on one stream, as with any PTY). Send `child.resize(cols, rows)` to update the window size. To signal EOF to an interactive process, write `\x04` (Ctrl-D) — `stdin.end()` is a no-op in PTY mode (closing the master kills the session).

### Detach mode

`spawn(cmd, args, { detach: true })` keeps the process alive after the WebSocket disconnects. On disconnect the child emits `'detach'` (with the pid) instead of `'close'` — the process continues running on the server with its output drained. Check status with `await client.status(pid)` (returns `{running, exit_code, ...}`) or kill with `await client.kill(pid)`. Useful for long-running processes that must survive network blips.

## Server backend

remote-cp-node requires an HTTP server implementing these exec endpoints (shared with remote-fs-node; see [remote-ops-server](https://github.com/tastypear/remote-ops-server) for the reference implementation):

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/exec` | POST | Sync exec — returns `{stdout, stderr, exit_code, pid, duration_ms}` |
| `/api/exec/stream` | POST | SSE streaming — emits `pid` → `stdout`/`stderr` → `exit` frames; keepalive comments on idle |
| `/api/exec/kill` | POST | Kill by PID (ownership-checked against server process table) |
| `/api/exec/stdin` | POST | Write to a spawned process's stdin (`{pid, data, close}`) |
| `/ws/exec` | WS | Bidirectional — streaming stdin, binary-safe stdout/stderr (base64), kill, keepalive |

Request body (`shell=true` for `exec`/`execSync`, `shell=false` otherwise):
```json
{ "cmd": "echo", "args": ["hello"], "shell": false, "cwd": "/", "env": {}, "timeout": 30, "stdin": null }
```

Server guarantees:
- **Process registry** — every spawned PID is tracked; kill/stdin verify ownership (no raw `os.kill` on arbitrary PIDs)
- **Env sanitization** — secrets (`*TOKEN*`, `*SECRET*`, `*KEY*`, etc.) stripped from the inherited environment
- **Timeout enforcement** — on both sync and stream endpoints; kills the whole process group (`os.killpg`)
- **SSE keepalive** — idle periods emit `: keepalive` comments to defeat proxy idle timeouts
- **Orphan cleanup** — client disconnect → best-effort process-group kill; background sweeper reaps idle entries

## Test

```bash
# Start a remote-ops server (shared), then:
node test/test.js              # HTTP/SSE transport
node test/test_ws.js           # WebSocket transport
node test/test_pty.js          # PTY mode (interactive processes)
node test/test_integration.js  # remote-fs + remote-cp (HTTP)
node test/test_ws_integration.js  # remote-fs + remote-cp (WS)
```

## Limitations

- True streaming stdin unsupported (see above) — one-shot `input` and buffered `write()+end()` cover the non-interactive exec use cases
- Binary output: stdout/stderr are decoded UTF-8 with `errors="replace"` on the server; non-UTF-8 bytes may be corrupted. Use for text commands, not binary transfer (use remote-fs-node for binary file I/O)
- Sync methods use `curl` subprocess (~50-100ms overhead per call)
- `stdio: "inherit"` not implemented; `detached`/`uid`/`gid` not supported
- IPC channel (`fork`'s `send`/`message` events) writes to stdin, not a real IPC channel
- PTY/interactive terminal not supported (use `spawn` + SSE for live output)
- This mirrors Node's `child_process` API, **not** the `ssh2` library's channel/stream API — downstream code using `ssh2` directly needs adapter work, but code using `child_process` to shell out to `ssh` works transparently after `patch()`

## License

MIT
