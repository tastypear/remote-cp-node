"use strict";
process.on("uncaughtException", (e) => { console.error("UNCAUGHT:", e); process.exit(1); });
process.on("unhandledRejection", (e) => { console.error("UNHANDLED:", e); process.exit(1); });

const assert = require("assert");
const rcp = require("../index.js");

// Configure with WebSocket transport enabled (same server as HTTP tests).
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
  console.log("=== remote-cp-node WebSocket test suite ===\n");

  // ─── Basic spawn via WS ───
  await test("ws: spawn echo", async () => {
    const child = rcp.spawn("echo", ["hello_ws"]);
    let data = "";
    await new Promise((res) => {
      child.stdout.on("data", (c) => { data += c.toString(); });
      child.on("close", res);
    });
    assert.strictEqual(data.trim(), "hello_ws");
  });

  // ─── PID via WS ───
  await test("ws: spawn provides PID", async () => {
    const child = rcp.spawn("echo", ["pidtest"]);
    await new Promise((res) => {
      child.on("spawn", () => assert.ok(child.pid > 0));
      child.on("close", res);
    });
  });

  // ─── stderr via WS ───
  await test("ws: spawn captures stderr", async () => {
    const child = rcp.spawn("sh", ["-c", "echo err_ws >&2"]);
    let stderr = "";
    await new Promise((res) => {
      child.stderr.on("data", (c) => { stderr += c.toString(); });
      child.on("close", res);
    });
    assert.strictEqual(stderr.trim(), "err_ws");
  });

  // ─── exit code via WS ───
  await test("ws: spawn exit code", async () => {
    const child = rcp.spawn("sh", ["-c", "exit 7"]);
    const code = await new Promise((res) => {
      child.on("exit", (code) => res(code));
    });
    assert.strictEqual(code, 7);
  });

  // ─── EDGE CASE 1: True streaming stdin ───
  await test("ws: streaming stdin (write→read→write)", async () => {
    const child = rcp.spawn("cat");
    await new Promise((res) => child.on("spawn", res));

    child.stdin.write("chunk1\n");
    const out1 = await new Promise((res) => {
      child.stdout.once("data", (c) => res(c.toString()));
    });
    assert.strictEqual(out1, "chunk1\n");

    child.stdin.write("chunk2\n");
    const out2 = await new Promise((res) => {
      child.stdout.once("data", (c) => res(c.toString()));
    });
    assert.strictEqual(out2, "chunk2\n");

    child.stdin.end();
    await new Promise((res) => child.on("close", res));
  });

  // ─── EDGE CASE 2: Binary-safe stdout ───
  await test("ws: binary-safe stdout (256 random bytes)", async () => {
    const child = rcp.spawn("head", ["-c", "256", "/dev/urandom"]);
    const chunks = [];
    await new Promise((res) => {
      child.stdout.on("data", (c) => chunks.push(c));
      child.on("close", res);
    });
    const buf = Buffer.concat(chunks);
    assert.strictEqual(buf.length, 256, "should receive exactly 256 bytes");
  });

  // ─── EDGE CASE 2b: Binary stdin ───
  await test("ws: binary stdin (base64 encoded)", async () => {
    const input = Buffer.from([0x00, 0x01, 0x02, 0xff, 0xfe, 0x80, 0x7f, 0x00]);
    const child = rcp.spawn("cat");
    await new Promise((res) => child.on("spawn", res));

    child.stdin.write(input);
    child.stdin.end();

    const chunks = [];
    await new Promise((res) => {
      child.stdout.on("data", (c) => chunks.push(c));
      child.on("close", res);
    });
    const out = Buffer.concat(chunks);
    assert.ok(out.equals(input), "binary stdin should round-trip exactly");
  });

  // ─── EDGE CASE 3: Kill via WS ───
  await test("ws: kill stops long-running process", async () => {
    const child = rcp.spawn("sleep", ["30"]);
    await new Promise((res) => child.on("spawn", res));
    assert.ok(child.pid > 0);
    child.kill("SIGTERM");
    const result = await new Promise((res) => {
      child.on("exit", (code, signal) => res({ code, signal }));
    });
    assert.ok(result.code !== 0 || result.signal, "should be killed");
  });

  // ─── Kill after stdin_close ───
  await test("ws: kill after stdin EOF still works", async () => {
    const child = rcp.spawn("sleep", ["30"]);
    await new Promise((res) => child.on("spawn", res));
    child.stdin.end();
    child.kill("SIGTERM");
    const result = await new Promise((res) => {
      child.on("exit", (code, signal) => res({ code, signal }));
    });
    assert.ok(result.code !== 0 || result.signal, "should be killed after EOF");
  });

  // ─── Multi-line output ───
  await test("ws: multi-line streaming output", async () => {
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

  // ─── Large output ───
  await test("ws: large output (100KB)", async () => {
    const child = rcp.spawn("seq", ["10000"]);
    let total = "";
    await new Promise((res) => {
      child.stdout.on("data", (c) => { total += c.toString(); });
      child.on("close", res);
    });
    const lines = total.trim().split("\n");
    assert.strictEqual(lines.length, 10000);
    assert.strictEqual(lines[0], "1");
    assert.strictEqual(lines[9999], "10000");
  });

  // ─── maxBuffer via WS ───
  await test("ws: maxBuffer triggers error + kill", async () => {
    const child = rcp.spawn("seq", ["10000"], { maxBuffer: 100 });
    let hadError = false;
    await new Promise((res) => {
      child.on("error", (err) => {
        hadError = err.message.includes("maxBuffer");
      });
      child.on("close", res);
    });
    assert.ok(hadError, "should emit maxBuffer error");
  });

  // ─── spawn with shell:true ───
  await test("ws: spawn shell:true", async () => {
    const child = rcp.spawn("echo $HOME", [], { shell: true });
    let data = "";
    await new Promise((res) => {
      child.stdout.on("data", (c) => { data += c.toString(); });
      child.on("close", res);
    });
    assert.ok(data.trim().length > 0, "should expand $HOME");
  });

  // ─── Connection auto-close on process exit ───
  await test("ws: connection closes after process exits", async () => {
    const child = rcp.spawn("echo", ["done"]);
    const result = await new Promise((res) => {
      child.on("close", (code) => res(code));
    });
    assert.strictEqual(result, 0);
  });

  // ─── Multiple concurrent WS sessions ───
  await test("ws: 10 concurrent spawn sessions", async () => {
    const N = 10;
    const results = await Promise.all(Array.from({ length: N }, (_, i) =>
      new Promise((res) => {
        const child = rcp.spawn("echo", [`conc_${i}`]);
        let data = "";
        child.stdout.on("data", (c) => { data += c.toString(); });
        child.on("close", () => res(data.trim()));
      })
    ));
    for (let i = 0; i < N; i++) {
      assert.strictEqual(results[i], `conc_${i}`);
    }
  });

  // ─── Spawn failure sends exit (no hang) ───
  await test("ws: bad command exits cleanly", async () => {
    const child = rcp.spawn("nonexistent_cmd_xyz", []);
    let hadError = false;
    const result = await new Promise((res) => {
      child.on("error", () => { hadError = true; });
      child.on("close", (code) => res(code));
    });
    assert.ok(hadError, "should emit error for bad command");
  });

  console.log("\n=== " + passed + " passed, " + failed + " failed ===");
  process.exit(failed > 0 ? 1 : 0);
}

run().catch((err) => { console.error("FATAL:", err); process.exit(1); });
