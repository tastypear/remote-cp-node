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

async function collectOutput(child) {
  const chunks = [];
  await new Promise((res) => {
    child.stdout.on("data", (c) => chunks.push(c));
    child.on("close", res);
  });
  return Buffer.concat(chunks);
}

async function run() {
  console.log("=== remote-cp-node PTY test suite ===\n");

  // ─── PTY: child sees a real terminal ───
  await test("pty: stdin is a TTY", async () => {
    const child = rcp.spawn("test", ["-t", "0"], { pty: true, timeout: 10 });
    const code = await new Promise((res) => child.on("exit", (c) => res(c)));
    assert.strictEqual(code, 0, "test -t 0 should succeed (stdin is a TTY)");
  });

  // ─── PTY: stdout is a TTY ───
  await test("pty: stdout is a TTY", async () => {
    const child = rcp.spawn("test", ["-t", "1"], { pty: true, timeout: 10 });
    const code = await new Promise((res) => child.on("exit", (c) => res(c)));
    assert.strictEqual(code, 0, "test -t 1 should succeed (stdout is a TTY)");
  });

  // ─── PTY: echo output has \r\n line endings ───
  await test("pty: echo output (CRLF line endings)", async () => {
    const child = rcp.spawn("echo", ["hello_pty"], { pty: true, timeout: 10 });
    const out = await collectOutput(child);
    assert.ok(out.toString().includes("hello_pty"), "should contain hello_pty");
    assert.ok(out.includes(Buffer.from("\r\n")), "PTY should produce CRLF line endings");
  });

  // ─── PTY: exit code ───
  await test("pty: exit code", async () => {
    const child = rcp.spawn("sh", ["-c", "exit 7"], { pty: true, timeout: 10 });
    const code = await new Promise((res) => child.on("exit", (c) => res(c)));
    assert.strictEqual(code, 7);
  });

  // ─── PTY: interactive cat with echo ───
  await test("pty: interactive cat (echo + Ctrl-D EOF)", async () => {
    const child = rcp.spawn("cat", [], { pty: true, timeout: 10 });
    await new Promise((res) => child.on("spawn", res));

    const chunks = [];
    child.stdout.on("data", (c) => chunks.push(c));

    child.stdin.write("hello_interactive\n");
    await new Promise((r) => setTimeout(r, 300));
    // Send Ctrl-D (EOT) to signal EOF on the PTY.
    child.stdin.write("\x04");

    await new Promise((res) => child.on("close", res));
    const out = Buffer.concat(chunks).toString();
    // PTY echoes input, so "hello_interactive" appears at least once.
    assert.ok(out.includes("hello_interactive"), "should echo input: " + JSON.stringify(out));
  });

  // ─── PTY: initial window size ───
  await test("pty: initial window size (cols=100)", async () => {
    const child = rcp.spawn("tput", ["cols"], { pty: true, cols: 100, rows: 30, timeout: 10 });
    const out = await collectOutput(child);
    assert.strictEqual(out.toString().trim(), "100", "tput cols should report 100");
  });

  // ─── PTY: resize ───
  await test("pty: resize updates terminal size", async () => {
    const child = rcp.spawn("sh", ["-c", "sleep 0.5; tput cols"], { pty: true, cols: 80, rows: 24, timeout: 10 });
    await new Promise((res) => child.on("spawn", res));
    child.resize(120, 40);
    const out = await collectOutput(child);
    assert.strictEqual(out.toString().trim(), "120", "tput cols should report 120 after resize");
  });

  // ─── PTY: kill ───
  await test("pty: kill stops process", async () => {
    const child = rcp.spawn("sleep", ["30"], { pty: true, timeout: 60 });
    await new Promise((res) => child.on("spawn", res));
    assert.ok(child.pid > 0);
    child.kill("SIGTERM");
    const code = await new Promise((res) => child.on("exit", (c) => res(c)));
    assert.ok(code !== 0, "killed process should not exit 0");
  });

  // ─── PTY: TERM env is set ───
  await test("pty: TERM env is set", async () => {
    const child = rcp.spawn("sh", ["-c", "echo $TERM"], { pty: true, timeout: 10 });
    const out = await collectOutput(child);
    assert.ok(out.toString().trim().length > 0, "TERM should be set");
  });

  console.log("\n=== " + passed + " passed, " + failed + " failed ===");
  process.exit(failed > 0 ? 1 : 0);
}

run().catch((err) => { console.error("FATAL:", err); process.exit(1); });
