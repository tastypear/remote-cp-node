# remote-cp-node

Node.js `child_process`-compatible module backed by HTTP. A drop-in replacement for `require("child_process")` that routes `exec`/`spawn`/`fork` to a remote server — a non-interactive SSH exec replacement.

Companion to [remote-fs-node](https://github.com/tastypear/remote-fs-node) (SFTP replacement). Both share the same HTTP server ([remote-ops-server](https://github.com/tastypear/remote-ops-server)), `configure()` shape, and monkey-patch strategy.

## What makes this different

Patches both `require("child_process")` and `require("node:child_process")` (via `Module._resolveFilename`), so any code using `exec`/`spawn`/`execFile`/`fork` — including third-party libraries — works remotely without code changes.

## Quick start

```bash
npm install remote-cp-node
```

### As drop-in replacement

```js
const remoteCp = require("remote-cp-node");
remoteCp.configure({
  baseURL: "http://your-server:8765",
  token: "my-secret",
});

const cp = remoteCp;
const { stdout, stderr } = await cp.promises.exec("uname -a");
const r = cp.spawn("ls", ["-la", "/tmp"]);  // streaming stdout
r.stdout.on("data", (d) => console.log(d.toString()));
```

### As monkey-patch

```js
const remoteCp = require("remote-cp-node");
remoteCp.configure({ baseURL: "http://your-server:8765", token: "my-secret" });
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

## API

Mirrors Node's `child_process`:

| Function | Transport | Shell | Notes |
|----------|-----------|-------|-------|
| `exec(cmd, opts, cb)` | SSE or WS (`/ws/exec`) | yes (`sh -c`) | Returns `ChildProcess` with **live PID** (killable mid-run); stdout/stderr buffer-collected, callback gets `(err, stdout, stderr)` on exit |
| `execFile(file, args, opts, cb)` | SSE or WS | **no** | args as argv array (no injection); live PID, killable mid-run |
| `execSync(cmd, opts)` | sync (`/api/exec`) | yes | Throws on non-zero exit |
| `execFileSync(file, args, opts)` | sync | **no** | args as argv |
| `spawn(cmd, args, opts)` | SSE or WS (`/ws/exec`) | no (unless `opts.shell`) | Returns `ChildProcess` with real PID; live stdout/stderr streams |
| `spawnSync(cmd, args, opts)` | sync | no (unless `opts.shell`) | Returns `{pid, stdout, stderr, status, signal}` |
| `fork(modulePath, args, opts)` | SSE | **no** | Uses `"node"` (remote `$PATH`) + modulePath as arg (no shell, no injection) |
| `batchExec(cmds, opts)` | async (`/api/exec/batch`) | — | Run multiple commands in one request; returns `{results: [...]}` |
| `batchExecSync(cmds, opts)` | sync | — | Sync variant |
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
- `wsTransport` — set via `configure({ wsTransport: true })`; enables WebSocket transport for `spawn` (streaming stdin + binary-safe output)

### Selective routing

`configure({ shouldRemote: (method, cmd, args, opts) => boolean })` — return `false` to keep a specific call local. Default: everything goes remote. Useful for keeping hot local tools (e.g. `git`, `node`) in-process while shelling out to the remote for everything else.

### ChildProcess

`EventEmitter` with `pid`, `exitCode`, `signalCode`, `killed`, `stdin`, `stdout`, `stderr`, `stdio`. Methods: `kill(signal)`, `send()`, `disconnect()`, `ref()`/`unref()`.

`kill(signal)` contacts `/api/exec/kill` with **ownership verification** — the server only kills PIDs it spawned (whole process group). If the kill can't be confirmed (process already exited, network error), an `exit`/`close` is synthesized so callers never hang. Works on `exec`/`execFile`/`spawn`/`fork` (all stream-backed with live PIDs).

## WebSocket transport

Enable with `configure({ wsTransport: true })`. `spawn()` then uses a bidirectional WebSocket (`/ws/exec`) instead of SSE, adding:

- **Streaming stdin** — `child.stdin.write(chunk)` sends immediately; `child.stdin.end()` sends EOF. Interactive write→read→write works.
- **Binary-safe stdout/stderr** — invalid UTF-8 chunks are sent as base64 instead of `errors="replace"` (lossy). Binary frames (1-byte prefix + raw bytes) are used by default, eliminating the 33% base64 overhead for binary-heavy output.

Auth uses `Authorization: Bearer <token>` header (or `?token=` query fallback). The session is registered in the server's process table, so HTTP `/api/exec/kill` and `/api/exec/stdin` also work on WS-spawned PIDs. `exec`/`execFile` use WS with buffer-collect (same callback semantics, but binary-safe); `spawnSync`/`execSync`/`execFileSync` still use the sync HTTP path.

### PTY mode

`spawn(cmd, args, { pty: true, cols: 80, rows: 24 })` runs the child with a pseudo-terminal — the child sees a real TTY (`isatty()` true), gets echo and line editing, and `TERM=xterm-256color`. Output is merged (stdout+stderr on one stream, as with any PTY). Send `child.resize(cols, rows)` to update the window size. To signal EOF to an interactive process, write `\x04` (Ctrl-D) — `stdin.end()` is a no-op in PTY mode (closing the master kills the session).

### Detach mode

`spawn(cmd, args, { detach: true })` keeps the process alive after the WebSocket disconnects. On disconnect the child emits `'detach'` (with the pid) instead of `'close'` — the process continues running on the server with its output drained. Check status with `await client.status(pid)` (returns `{running, exit_code, ...}`) or kill with `await client.kill(pid)`. Useful for long-running processes that must survive network blips.

## Sync implementation

Sync methods (`execSync`/`execFileSync`/`spawnSync`/`batchExecSync`) use a **worker-thread bridge** (`SharedArrayBuffer` + `Atomics.wait`) — the main thread writes the request to a shared buffer, posts to a worker, and blocks until the response arrives. No subprocess fork per call. Falls back to `curl` if `SharedArrayBuffer` is unavailable (older Node or disabled cross-origin isolation).

## Server backend

Requires [remote-ops-server](https://github.com/tastypear/remote-ops-server) (Go or Python) implementing the exec endpoints: `/api/exec`, `/api/exec/batch`, `/api/exec/stream`, `/api/exec/kill`, `/api/exec/stdin`, `/ws/exec`. See its README for the full API reference.

## Test

```bash
# Start a remote-ops server (shared), then:
node test/test.js              # HTTP/SSE transport
node test/test_ws.js           # WebSocket transport
node test/test_pty.js          # PTY mode (interactive processes)
node test/test_detach.js       # Detach mode
node test/test_batch.js        # Batch exec
node test/test_parity.js       # Node.js child_process API parity
node test/test_integration.js  # remote-fs + remote-cp (HTTP)
node test/test_ws_integration.js  # remote-fs + remote-cp (WS)
```

## Limitations

- Without WS transport, stdout/stderr are decoded UTF-8 with `errors="replace"` on the server; non-UTF-8 bytes may be corrupted. Use `wsTransport: true` for binary-safe output, or remote-fs-node for binary file I/O.
- `stdio: "inherit"` not implemented; `uid`/`gid` not supported
- IPC channel (`fork`'s `send`/`message` events) writes to stdin, not a real IPC channel
- This mirrors Node's `child_process` API, **not** the `ssh2` library's channel/stream API — downstream code using `ssh2` directly needs adapter work, but code using `child_process` to shell out to `ssh` works transparently after `patch()`

## License

MIT
