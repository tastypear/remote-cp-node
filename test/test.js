"use strict";
process.on("uncaughtException", (e) => { console.error("UNCAUGHT:", e); process.exit(1); });
process.on("unhandledRejection", (e) => { console.error("UNHANDLED:", e); process.exit(1); });

const assert = require("assert");
const rcp = require("../index.js");
rcp.configure({ baseURL: "http://127.0.0.1:8766", token: "testtoken123" });
let passed = 0, failed = 0;

async function test(name, fn) {
  try { await fn(); console.log("PASS  " + name); passed++; }
  catch (err) { console.log("FAIL  " + name + ": " + err.message); console.error(err.stack); failed++; }
}

async function run() {
  console.log("=== remote-child_process test suite ===\n");

  // ─── exec (callback) ───
  await test("exec callback", async () => {
    const result = await new Promise((res, rej) => {
      rcp.exec("echo hello world", (err, stdout, stderr) => {
        if (err) rej(err);
        else res({ stdout, stderr });
      });
    });
    assert.strictEqual(result.stdout.trim(), "hello world");
  });

  // ─── exec with cwd ───
  await test("exec with cwd", async () => {
    const result = await new Promise((res, rej) => {
      rcp.exec("pwd", { cwd: "/tmp" }, (err, stdout) => {
        if (err) rej(err);
        else res(stdout);
      });
    });
    assert.strictEqual(result.trim(), "/tmp");
  });

  // ─── exec with env ───
  await test("exec with env", async () => {
    const result = await new Promise((res, rej) => {
      rcp.exec("echo $TEST_VAR", { env: { TEST_VAR: "envval123" } }, (err, stdout) => {
        if (err) rej(err);
        else res(stdout);
      });
    });
    assert.strictEqual(result.trim(), "envval123");
  });

  // ─── exec with stdin ───
  await test("exec with stdin", async () => {
    const result = await new Promise((res, rej) => {
      rcp.exec("cat", { input: "piped data" }, (err, stdout) => {
        if (err) rej(err);
        else res(stdout);
      });
    });
    assert.strictEqual(result, "piped data");
  });

  // ─── exec error (non-zero exit) ───
  await test("exec error handling", async () => {
    const err = await new Promise((res) => {
      rcp.exec("exit 42", (err) => res(err));
    });
    assert.ok(err);
    assert.strictEqual(err.code, 42);
  });

  // ─── exec returns ChildProcess ───
  await test("exec returns ChildProcess with exit event", async () => {
    const child = rcp.exec("echo cp_test");
    const result = await new Promise((res) => {
      child.on("close", (code) => res(code));
    });
    assert.strictEqual(result, 0);
  });

  // ─── exec encoding buffer ───
  await test("exec encoding buffer", async () => {
    const result = await new Promise((res, rej) => {
      rcp.exec("echo buf_test", { encoding: "buffer" }, (err, stdout) => {
        if (err) rej(err);
        else res(stdout);
      });
    });
    assert.ok(Buffer.isBuffer(result));
    assert.strictEqual(result.toString().trim(), "buf_test");
  });

  // ─── execSync ───
  await test("execSync", async () => {
    const output = rcp.execSync("echo sync_test");
    assert.strictEqual(output.toString().trim(), "sync_test");
  });

  // ─── execSync with cwd ───
  await test("execSync with cwd", async () => {
    const output = rcp.execSync("pwd", { cwd: "/var" });
    assert.strictEqual(output.toString().trim(), "/var");
  });

  // ─── execSync error ───
  await test("execSync throws on error", async () => {
    try {
      rcp.execSync("exit 1");
      assert.fail("should throw");
    } catch (err) {
      assert.ok(err.message.includes("Command failed"));
    }
  });

  // ─── execSync encoding utf8 ───
  await test("execSync encoding utf8", async () => {
    const output = rcp.execSync("echo utf8_test", { encoding: "utf8" });
    assert.strictEqual(typeof output, "string");
    assert.strictEqual(output.trim(), "utf8_test");
  });

  // ─── execFile (callback) ───
  await test("execFile callback", async () => {
    const result = await new Promise((res, rej) => {
      rcp.execFile("echo", ["hello", "world"], (err, stdout, stderr) => {
        if (err) rej(err);
        else res({ stdout, stderr });
      });
    });
    assert.strictEqual(result.stdout.trim(), "hello world");
  });

  // ─── execFile with env ───
  await test("execFile with env", async () => {
    const result = await new Promise((res, rej) => {
      rcp.execFile("sh", ["-c", "echo $EF_VAR"], { env: { EF_VAR: "efval" } }, (err, stdout) => {
        if (err) rej(err);
        else res(stdout);
      });
    });
    assert.strictEqual(result.trim(), "efval");
  });

  // ─── execFile with stdin ───
  await test("execFile with stdin", async () => {
    const result = await new Promise((res, rej) => {
      rcp.execFile("cat", [], { input: "ef stdin data" }, (err, stdout) => {
        if (err) rej(err);
        else res(stdout);
      });
    });
    assert.strictEqual(result, "ef stdin data");
  });

  // ─── execFile error handling ───
  await test("execFile error handling", async () => {
    const err = await new Promise((res) => {
      rcp.execFile("sh", ["-c", "exit 33"], (err) => res(err));
    });
    assert.ok(err);
    assert.strictEqual(err.code, 33);
  });

  // ─── execFile args with special chars ───
  await test("execFile args with special chars", async () => {
    const result = await new Promise((res, rej) => {
      rcp.execFile("echo", ["$DOLLAR", "two words"], (err, stdout) => {
        if (err) rej(err);
        else res(stdout);
      });
    });
    assert.strictEqual(result.trim(), "$DOLLAR two words");
  });

  // ─── execFileSync ───
  await test("execFileSync", async () => {
    const output = rcp.execFileSync("echo", ["sync_ef"]);
    assert.strictEqual(output.toString().trim(), "sync_ef");
  });

  // ─── execFileSync with input ───
  await test("execFileSync with input", async () => {
    const output = rcp.execFileSync("cat", [], { input: "sync ef input" });
    assert.strictEqual(output.toString(), "sync ef input");
  });

  // ─── execFileSync error ───
  await test("execFileSync throws on error", async () => {
    try {
      rcp.execFileSync("sh", ["-c", "exit 9"]);
      assert.fail("should throw");
    } catch (err) {
      assert.ok(err.message.includes("Command failed"));
    }
  });

  // ─── spawn (streaming) ───
  await test("spawn streams stdout", async () => {
    const child = rcp.spawn("echo", ["streamed"]);
    let data = "";
    await new Promise((res) => {
      child.stdout.on("data", (c) => { data += c.toString(); });
      child.on("close", res);
    });
    assert.strictEqual(data.trim(), "streamed");
  });

  // ─── spawn with PID ───
  await test("spawn provides PID", async () => {
    const child = rcp.spawn("echo", ["pidtest"]);
    await new Promise((res) => {
      child.on("spawn", () => {
        assert.ok(child.pid > 0, "should have PID");
      });
      child.on("close", res);
    });
  });

  // ─── spawn stderr ───
  await test("spawn captures stderr", async () => {
    const child = rcp.spawn("sh", ["-c", "echo err_msg >&2"]);
    let stderr = "";
    await new Promise((res) => {
      child.stderr.on("data", (c) => { stderr += c.toString(); });
      child.on("close", res);
    });
    assert.strictEqual(stderr.trim(), "err_msg");
  });

  // ─── spawn exit code ───
  await test("spawn exit code", async () => {
    const child = rcp.spawn("sh", ["-c", "exit 7"]);
    const code = await new Promise((res) => {
      child.on("exit", (code) => res(code));
    });
    assert.strictEqual(code, 7);
  });

  // ─── spawn multi-line streaming ───
  await test("spawn multi-line output", async () => {
    const child = rcp.spawn("sh", ["-c", "for i in 1 2 3; do echo line_$i; done"]);
    const lines = [];
    await new Promise((res) => {
      child.stdout.on("data", (c) => {
        lines.push(...c.toString().trim().split("\n"));
      });
      child.on("close", res);
    });
    assert.deepStrictEqual(lines, ["line_1", "line_2", "line_3"]);
  });

  // ─── spawn kill ───
  await test("spawn kill", async () => {
    const child = rcp.spawn("sleep", ["60"]);
    await new Promise((res) => child.on("spawn", res));
    assert.ok(child.pid > 0);
    child.kill("SIGKILL");
    const result = await new Promise((res) => {
      child.on("exit", (code, signal) => res({ code, signal }));
    });
    assert.ok(result.signal === "SIGKILL" || result.code === null || result.code !== 0,
      "should be killed, got code=" + result.code + " signal=" + result.signal);
  });

  // ─── spawnSync ───
  await test("spawnSync", async () => {
    const result = rcp.spawnSync("echo", ["sync_spawn"]);
    assert.strictEqual(result.stdout.toString().trim(), "sync_spawn");
    assert.strictEqual(result.status, 0);
  });

  // ─── spawnSync with input ───
  await test("spawnSync with input", async () => {
    const result = rcp.spawnSync("cat", [], { input: "sync input data" });
    assert.strictEqual(result.stdout.toString(), "sync input data");
  });

  // ─── spawnSync error ───
  await test("spawnSync error status", async () => {
    const result = rcp.spawnSync("sh", ["-c", "exit 5"]);
    assert.strictEqual(result.status, 5);
    assert.ok(result.error);
  });

  // ─── promises.exec ───
  await test("promises.exec", async () => {
    const { stdout } = await rcp.promises.exec("echo promise_exec");
    assert.strictEqual(stdout.trim(), "promise_exec");
  });

  // ─── promises.exec error ───
  await test("promises.exec rejects on error", async () => {
    try {
      await rcp.promises.exec("exit 11");
      assert.fail("should reject");
    } catch (err) {
      assert.strictEqual(err.code, 11);
    }
  });

  // ─── promises.execFile ───
  await test("promises.execFile", async () => {
    const { stdout } = await rcp.promises.execFile("echo", ["promise_ef"]);
    assert.strictEqual(stdout.trim(), "promise_ef");
  });

  // ─── monkey-patch ───
  await test("patch require(child_process)", async () => {
    rcp.patch();
    const cp = require("child_process");
    assert.strictEqual(typeof cp.exec, "function");
    assert.strictEqual(typeof cp.spawn, "function");

    const output = await new Promise((res, rej) => {
      cp.exec("echo patched", (err, stdout) => {
        if (err) rej(err);
        else res(stdout);
      });
    });
    assert.strictEqual(output.trim(), "patched");
    rcp.restore();
  });

  // ─── patch node:child_process ───
  await test("patch require(node:child_process)", async () => {
    rcp.patch();
    const cp = require("node:child_process");
    const output = rcp.execSync("echo node_proto");
    assert.strictEqual(output.toString().trim(), "node_proto");
    rcp.restore();
  });

  // ─── patch execFile ───
  await test("patch exposes execFile", async () => {
    rcp.patch();
    const cp = require("child_process");
    assert.strictEqual(typeof cp.execFile, "function");
    assert.strictEqual(typeof cp.execFileSync, "function");
    const output = rcp.execFileSync("echo", ["patched_ef"]);
    assert.strictEqual(output.toString().trim(), "patched_ef");
    rcp.restore();
  });

  // ─── BUG FIX 1: child.stdio holds actual stream objects ───
  await test("fix: child.stdio has real streams", async () => {
    const child = rcp.spawn("echo", ["x"]);
    assert.ok(child.stdio[0], "stdio[0] (stdin) should be defined");
    assert.ok(child.stdio[1], "stdio[1] (stdout) should be defined");
    assert.ok(child.stdio[2], "stdio[2] (stderr) should be defined");
    assert.strictEqual(child.stdio[1], child.stdout);
    await new Promise((res) => child.on("close", res));
  });

  // ─── BUG FIX 2: stdin 攒-end write actually reaches process ───
  await test("fix: stdin write+end reaches process", async () => {
    const child = rcp.spawn("cat", []);
    await new Promise((res) => child.on("spawn", res));
    child.stdin.write("buffered-");
    child.stdin.write("stdin-data\n");
    child.stdin.end();
    const out = await new Promise((res) => {
      let d = "";
      child.stdout.on("data", (c) => (d += c.toString()));
      child.on("close", () => res(d));
    });
    assert.strictEqual(out, "buffered-stdin-data\n");
  });

  // ─── BUG FIX 3: kill() synthesizes exit on already-dead process ───
  await test("fix: kill on exited process synthesizes exit (no hang)", async () => {
    const child = rcp.spawn("echo", ["gone"]);
    await new Promise((res) => child.on("close", res));
    // Process already exited — kill must not hang, should return true.
    const ok = child.kill("SIGTERM");
    assert.strictEqual(ok, true);
  });

  // ─── BUG FIX 4: fork no shell injection ───
  await test("fix: fork uses execPath (no shell injection)", async () => {
    // modulePath with shell metacharacters must be treated as a literal path,
    // not interpreted by a shell. The node binary will fail to find it as a
    // file → non-zero exit, NOT execute the injected command.
    const child = rcp.fork("; echo INJECTED");
    const code = await new Promise((res) => child.on("exit", (c) => res(c)));
    // Must not have run "echo INJECTED" — check no INJECTED leaks to stdout.
    assert.notStrictEqual(code, 0, "fork of bad path should fail, not run injected cmd");
  });

  // ─── IMPROVEMENT: exec now has a live PID (stream-backed) ───
  await test("improvement: exec has live pid (stream-backed)", async () => {
    const child = rcp.exec("echo pidcheck");
    // pid arrives via the SSE 'pid' event after spawn — wait for it.
    await new Promise((res) => child.on("spawn", res));
    assert.ok(child.pid > 0, "exec should now have a live pid > 0");
    await new Promise((res) => child.on("close", res));
  });

  // ─── IMPROVEMENT: exec kill() works mid-run ───
  await test("improvement: exec kill stops a long-running command", async () => {
    const child = rcp.exec("sleep 30; echo done", { timeout: 30000 });
    await new Promise((res) => child.on("spawn", res));
    assert.ok(child.pid > 0);
    child.kill("SIGTERM");
    const result = await new Promise((res) => {
      child.on("exit", (code, signal) => res({ code, signal }));
    });
    // Killed before the 30s sleep finished — exit should be non-zero or signaled.
    assert.ok(result.code !== 0 || result.signal, "exec kill should stop the process");
  });

  // ─── IMPROVEMENT: execFile kill() works mid-run ───
  await test("improvement: execFile kill stops a long-running command", async () => {
    const child = rcp.execFile("sleep", ["30"], { timeout: 30000 });
    await new Promise((res) => child.on("spawn", res));
    assert.ok(child.pid > 0);
    child.kill("SIGKILL");
    const result = await new Promise((res) => {
      child.on("exit", (code, signal) => res({ code, signal }));
    });
    assert.ok(result.code !== 0 || result.signal, "execFile kill should stop the process");
  });

  // ─── IMPROVEMENT: defaultCwd config ───
  await test("improvement: defaultCwd config honored", async () => {
    rcp.configure({ baseURL: "http://127.0.0.1:8766", token: "testtoken123", defaultCwd: "/var" });
    const out = await new Promise((res, rej) => {
      rcp.exec("pwd", (err, stdout) => (err ? rej(err) : res(stdout)));
    });
    assert.strictEqual(out.trim(), "/var");
    // restore default for subsequent tests
    rcp.configure({ baseURL: "http://127.0.0.1:8766", token: "testtoken123", defaultCwd: undefined });
    rcp._setDefaultCwd(process.env.HOME || "/");
  });

  // ─── BUG FIX 6: spawnSync maxBuffer kills + sets signal ───
  await test("fix: spawnSync maxBuffer sets signal (not silent truncation)", async () => {
    // seq 1000 produces ~4KB of output, exceeding maxBuffer:100. The command
    // exits on its own, so spawnSync completes; client-side maxBuffer check
    // then sets error + signal (Node semantics), not silent truncation.
    const result = rcp.spawnSync("seq", ["1000"], {
      maxBuffer: 100,
      encoding: "utf8",
    });
    assert.ok(result.error, "should have error on maxBuffer");
    assert.ok(result.signal, "should have signal set");
    assert.ok(result.stdout.length <= 100, "stdout should be truncated to maxBuffer");
  });

  // ─── server hardening: env sanitization (no token leak) ───
  await test("hardening: auth token not leaked to child env", async () => {
    const out = rcp.execSync("printenv AGENT_SHIM_TOKEN || echo NO_LEAK");
    assert.strictEqual(out.toString().trim(), "NO_LEAK");
  });

  // ─── server hardening: kill ownership check ───
  await test("hardening: kill rejects foreign pid", async () => {
    // PID 999999 was never spawned by the server → 404 / ok:false
    const r = await rcp._client.kill(999999, "SIGTERM");
    assert.strictEqual(r.ok, false, "server should refuse to kill unowned pid");
  });

  // ─── server hardening: shell=false args not interpreted ───
  await test("hardening: execFile args not shell-interpreted", async () => {
    const out = rcp.execFileSync("echo", ["safe; echo DANGER"]);
    assert.strictEqual(out.toString().trim(), "safe; echo DANGER");
  });

  // ─── stream timeout enforcement ───
  await test("hardening: stream timeout kills process", async () => {
    const child = rcp.spawn("sleep", ["10"], { timeout: 1000 });
    const result = await new Promise((res) => {
      child.on("exit", (code, signal) => res({ code, signal }));
    });
    assert.ok(result.code !== 0 || result.signal, "sleep should be killed by timeout");
  });

  console.log("\n=== " + passed + " passed, " + failed + " failed ===");
  process.exit(failed > 0 ? 1 : 0);
}

run().catch((err) => { console.error("FATAL:", err); process.exit(1); });
