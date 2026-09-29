"use strict";
process.on("uncaughtException", (e) => { console.error("UNCAUGHT:", e); process.exit(1); });
process.on("unhandledRejection", (e) => { console.error("UNHANDLED:", e); process.exit(1); });

const assert = require("assert");
const util = require("util");
const rcp = require("../index.js");

rcp.configure({
  baseURL: "http://127.0.0.1:8766",
  token: "testtoken123",
  wsTransport: true,
});

let passed = 0, failed = 0;

async function test(name, fn) {
  try { await fn(); console.log("PASS  " + name); passed++; }
  catch (err) { console.log("FAIL  " + name + ": " + err.message); console.error(err.stack); failed++; }
}

function once(child, ev) {
  return new Promise((res) => {
    const handler = (...args) => { child.off(ev, handler); res(args); };
    child.on(ev, handler);
  });
}

async function run() {
  console.log("=== Node.js parity test suite ===\n");

  // ─── 1. detached alias (Node.js spelling) ───
  await test("detached: Node.js spelling works as alias", async () => {
    const child = rcp.spawn("sleep", ["30"], { detached: true });
    await once(child, "spawn");
    assert.ok(child.pid > 0, "got pid");
    // Disconnect the WS session — detached process survives on the server.
    child.disconnect();
    const [pid] = await once(child, "detach");
    assert.strictEqual(pid, child.pid, "detach event has pid");
    // Process should still be running on the server.
    await new Promise((r) => setTimeout(r, 500));
    const st = await rcp.client.status(child.pid);
    assert.ok(st.running, "detached process still running");
    // Clean up.
    await rcp.client.kill(child.pid, "SIGKILL").catch(() => {});
  });

  // ─── 2. util.promisify.custom ───
  await test("promisify: util.promisify(exec) returns {stdout, stderr} object", async () => {
    const execP = util.promisify(rcp.exec);
    const result = await execP("echo promisify_test");
    assert.ok(!Array.isArray(result), "should be object, not array");
    assert.strictEqual(typeof result, "object");
    assert.strictEqual(result.stdout.trim(), "promisify_test");
    assert.ok(result.stderr !== undefined, "stderr property exists");
  });

  await test("promisify: util.promisify(execFile) returns {stdout, stderr} object", async () => {
    const execFileP = util.promisify(rcp.execFile);
    const result = await execFileP("echo", ["promisify_ef"]);
    assert.ok(!Array.isArray(result), "should be object, not array");
    assert.strictEqual(result.stdout.trim(), "promisify_ef");
  });

  await test("promisify: .child attached to promise", async () => {
    const execP = util.promisify(rcp.exec);
    const p = execP("echo child_test");
    assert.ok(p.child, "promise has .child");
    assert.ok(typeof p.child.kill === "function", ".child is a ChildProcess");
    await p;
  });

  // ─── 3. Error codes (ENOENT) ───
  await test("error: spawn ENOENT sets err.code", async () => {
    const child = rcp.spawn("nonexistent_cmd_xyz", []);
    const [err] = await once(child, "error");
    assert.strictEqual(err.code, "ENOENT");
    assert.strictEqual(err.errno, -2);
    assert.ok(err.syscall.includes("nonexistent_cmd_xyz"), "syscall has cmd");
    assert.strictEqual(err.path, "nonexistent_cmd_xyz");
    await once(child, "close");
  });

  await test("error: execFile ENOENT via callback", async () => {
    await new Promise((res) => {
      rcp.execFile("nonexistent_cmd_xyz", [], (err) => {
        assert.ok(err, "should have error");
        assert.strictEqual(err.code, "ENOENT");
        res();
      });
    });
  });

  await test("error: execFileSync throws ENOENT", async () => {
    try {
      rcp.execFileSync("nonexistent_cmd_xyz", []);
      assert.fail("should have thrown");
    } catch (err) {
      assert.strictEqual(err.code, "ENOENT");
      assert.strictEqual(err.errno, -2);
    }
  });

  await test("error: spawnSync returns ENOENT in error", async () => {
    const result = rcp.spawnSync("nonexistent_cmd_xyz", []);
    assert.ok(result.error, "should have error");
    assert.strictEqual(result.error.code, "ENOENT");
    assert.strictEqual(result.status, null);
  });

  // ─── 4. timeout default 0 (no timeout) ───
  await test("timeout: no default timeout — sleep 2 completes", async () => {
    const child = rcp.spawn("sleep", ["2"]);
    const [code] = await once(child, "exit");
    assert.strictEqual(code, 0, "sleep should complete, not be killed");
  });

  await test("timeout: explicit timeout still works", async () => {
    const child = rcp.spawn("sleep", ["10"], { timeout: 1000 });
    const [code, sig] = await once(child, "exit");
    assert.ok(code !== 0 || sig !== null, "should be killed by timeout");
  });

  // ─── 5. AbortSignal ───
  await test("abort: spawn killed on signal abort", async () => {
    const ac = new AbortController();
    const child = rcp.spawn("sleep", ["10"], { signal: ac.signal });
    setTimeout(() => ac.abort(), 200);
    const [code, sig] = await once(child, "close");
    assert.ok(child.aborted, "child.aborted is true");
    assert.ok(child.killed, "child was killed");
  });

  await test("abort: exec rejects with AbortError", async () => {
    const ac = new AbortController();
    setTimeout(() => ac.abort(), 200);
    try {
      await rcp.promises.exec("sleep 10", { signal: ac.signal });
      assert.fail("should have rejected");
    } catch (err) {
      assert.strictEqual(err.name, "AbortError");
      assert.strictEqual(err.code, "ABORT_ERR");
    }
  });

  await test("abort: already-aborted signal kills immediately", async () => {
    const ac = new AbortController();
    ac.abort();
    const child = rcp.spawn("sleep", ["5"], { signal: ac.signal });
    const [, sig] = await once(child, "close");
    assert.ok(child.aborted, "child.aborted is true");
  });

  // ─── 6. stdio: 'ignore' ───
  await test("stdio: ignore sets stdout/stderr to null", async () => {
    const child = rcp.spawn("echo", ["ignored"], { stdio: "ignore" });
    assert.strictEqual(child.stdout, null, "stdout is null");
    assert.strictEqual(child.stderr, null, "stderr is null");
    const [code] = await once(child, "close");
    assert.strictEqual(code, 0);
  });

  await test("stdio: ignore array form [pipe, ignore, ignore]", async () => {
    const child = rcp.spawn("echo", ["arr_ignore"], { stdio: ["pipe", "ignore", "ignore"] });
    assert.strictEqual(child.stdout, null, "stdout is null");
    assert.strictEqual(child.stderr, null, "stderr is null");
    const [code] = await once(child, "close");
    assert.strictEqual(code, 0);
  });

  // ─── 6b. stdio: 'inherit' ───
  await test("stdio: inherit still delivers data to child.stdout", async () => {
    const child = rcp.spawn("echo", ["inherited"], { stdio: "inherit" });
    let data = "";
    child.stdout.on("data", (c) => { data += c.toString(); });
    await once(child, "close");
    assert.strictEqual(data.trim(), "inherited", "data flows to child.stdout");
  });

  // ─── 7. shell string ───
  await test("shell: string shell uses specified binary", async () => {
    const child = rcp.spawn("echo $0", [], { shell: "/bin/bash" });
    let data = "";
    child.stdout.on("data", (c) => { data += c.toString(); });
    await once(child, "close");
    assert.ok(data.trim().includes("bash"), "bash -c sets $0 to bash: " + data.trim());
  });

  await test("shell: default shell is sh", async () => {
    const child = rcp.spawn("echo $0", [], { shell: true });
    let data = "";
    child.stdout.on("data", (c) => { data += c.toString(); });
    await once(child, "close");
    assert.ok(data.trim().includes("sh"), "sh -c sets $0 to sh: " + data.trim());
  });

  // ─── 8. uid/gid ───
  await test("uid/gid: spawning with current user's uid/gid works", async () => {
    // Get the server-side uid first.
    const probe = rcp.spawn("id", ["-u"]);
    let uidStr = "";
    probe.stdout.on("data", (c) => { uidStr += c.toString(); });
    await once(probe, "close");
    const uid = parseInt(uidStr.trim(), 10);
    assert.ok(uid > 0, "got valid uid");

    const probe2 = rcp.spawn("id", ["-g"]);
    let gidStr = "";
    probe2.stdout.on("data", (c) => { gidStr += c.toString(); });
    await once(probe2, "close");
    const gid = parseInt(gidStr.trim(), 10);

    // Spawn with explicit uid/gid matching current user — should be a no-op.
    const child = rcp.spawn("id", ["-u"], { uid, gid });
    let out = "";
    child.stdout.on("data", (c) => { out += c.toString(); });
    const [code] = await once(child, "close");
    assert.strictEqual(code, 0, "exits 0");
    assert.strictEqual(parseInt(out.trim(), 10), uid, "uid unchanged");
  });

  console.log("\n=== " + passed + " passed, " + failed + " failed ===");
  process.exit(failed > 0 ? 1 : 0);
}

run().catch((err) => { console.error("FATAL:", err); process.exit(1); });
