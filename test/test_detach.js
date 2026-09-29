"use strict";
process.on("uncaughtException", (e) => { console.error("UNCAUGHT:", e); process.exit(1); });
process.on("unhandledRejection", (e) => { console.error("UNHANDLED:", e); process.exit(1); });

const assert = require("assert");
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

async function run() {
  console.log("=== remote-cp-node detach test suite ===\n");

  // ─── Detached process survives disconnect ───
  await test("detach: process survives WS disconnect", async () => {
    const child = rcp.spawn("sleep", ["30"], { detach: true, timeout: 60 });
    await new Promise((res) => child.on("spawn", res));
    const pid = child.pid;
    assert.ok(pid > 0, "should have a pid");

    // Disconnect the WS session.
    child.disconnect();

    // Wait for detach event.
    const detachedPid = await new Promise((res) => child.on("detach", res));
    assert.strictEqual(detachedPid, pid, "detach event should carry the pid");

    // Process should still be running on the server.
    await new Promise((r) => setTimeout(r, 500));
    const st = await rcp.client.status(pid);
    assert.ok(st.running, "detached process should still be running: " + JSON.stringify(st));

    // Kill it via HTTP.
    const killResult = await rcp.client.kill(pid, "SIGKILL");
    assert.ok(killResult.ok, "should be able to kill detached process");

    // Status should now show not running (or 404).
    await new Promise((r) => setTimeout(r, 500));
    const st2 = await rcp.client.status(pid);
    assert.ok(!st2.running, "killed process should not be running");
  });

  // ─── Detached process produces output before disconnect ───
  await test("detach: output received before disconnect", async () => {
    const child = rcp.spawn("sh", ["-c", "echo before_detach; sleep 20"], { detach: true, timeout: 60 });
    const out = await new Promise((res) => {
      let data = "";
      child.stdout.on("data", (c) => { data += c.toString(); });
      child.stdout.once("data", () => res(data));
    });
    assert.ok(out.includes("before_detach"), "should receive output before disconnect");
    await rcp.client.kill(child.pid, "SIGKILL");
  });

  // ─── Detached process with PTY ───
  await test("detach: PTY process survives disconnect", async () => {
    const child = rcp.spawn("sleep", ["30"], { detach: true, pty: true, timeout: 60 });
    await new Promise((res) => child.on("spawn", res));
    const pid = child.pid;

    child.disconnect();
    await new Promise((res) => child.on("detach", res));
    await new Promise((r) => setTimeout(r, 500));

    const st = await rcp.client.status(pid);
    assert.ok(st.running, "detached PTY process should still be running");

    await rcp.client.kill(pid, "SIGKILL");
  });

  // ─── Non-detached process dies on disconnect ───
  await test("detach: non-detached process dies on disconnect", async () => {
    const child = rcp.spawn("sleep", ["30"], { timeout: 60 });
    await new Promise((res) => child.on("spawn", res));
    const pid = child.pid;

    child.disconnect();
    await new Promise((res) => child.on("close", res));
    await new Promise((r) => setTimeout(r, 500));

    const st = await rcp.client.status(pid);
    assert.ok(!st.running, "non-detached process should be dead after disconnect");
  });

  console.log("\n=== " + passed + " passed, " + failed + " failed ===");
  process.exit(failed > 0 ? 1 : 0);
}

run().catch((err) => { console.error("FATAL:", err); process.exit(1); });
