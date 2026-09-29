"use strict";
process.on("uncaughtException", (e) => { console.error("UNCAUGHT:", e); process.exit(1); });
process.on("unhandledRejection", (e) => { console.error("UNHANDLED:", e); process.exit(1); });

// Resolve remote-fs-node from sibling directory. Tries the published package
// name (remote-fs-node) first, falls back to the dev working tree (remote-fs-dev).
const path = require("path");
const assert = require("assert");

let remoteFsPath;
for (const candidate of ["../../remote-fs-node", "../../remote-fs-dev"]) {
  const p = path.resolve(__dirname, candidate);
  try { require.resolve(p); remoteFsPath = p; break; } catch {}
}
if (!remoteFsPath) {
  console.error("FATAL: remote-fs-node not found in sibling directory");
  console.error("       clone https://github.com/tastypear/remote-fs-node next to remote-cp-node");
  process.exit(1);
}
const rfs = require(remoteFsPath);
const rcp = require("../index.js");

rfs.configure({ baseURL: "http://127.0.0.1:8766", token: "testtoken123" });
rcp.configure({ baseURL: "http://127.0.0.1:8766", token: "testtoken123", wsTransport: true });

let passed = 0, failed = 0;

async function test(name, fn) {
  try { await fn(); console.log("PASS  " + name); passed++; }
  catch (err) { console.log("FAIL  " + name + ": " + err.message); console.error(err.stack); failed++; }
}

async function run() {
  console.log("=== WS integration: remote-fs + remote-cp (wsTransport) ===\n");

  // write via fs, cat via WS spawn
  await test("fs.writeFile + ws spawn cat", async () => {
    const p = "/tmp/ws_int_1.txt";
    await rfs.fs.promises.writeFile(p, "ws integration data");
    const child = rcp.spawn("cat", [p]);
    let data = "";
    await new Promise((res) => {
      child.stdout.on("data", (c) => { data += c.toString(); });
      child.on("close", res);
    });
    assert.strictEqual(data.trim(), "ws integration data");
    await rfs.fs.promises.unlink(p);
  });

  // Both patched, WS spawn + fs
  await test("both patched: fs.promises + ws spawn", async () => {
    rfs.patch();
    rcp.patch();

    const fs = require("fs");
    const cp = require("child_process");
    const p = "/tmp/ws_int_2.txt";
    await fs.promises.writeFile(p, "patched ws integration");
    const child = cp.spawn("cat", [p]);
    let data = "";
    await new Promise((res) => {
      child.stdout.on("data", (c) => { data += c.toString(); });
      child.on("close", res);
    });
    assert.strictEqual(data.trim(), "patched ws integration");
    await fs.promises.unlink(p);

    rcp.restore();
    rfs.restore();
  });

  // WS spawn writes file, fs reads it
  await test("ws spawn writes, fs reads", async () => {
    rfs.patch();
    rcp.patch();

    const fs = require("fs");
    const cp = require("child_process");
    const p = "/tmp/ws_int_3.txt";
    cp.spawnSync("sh", ["-c", "echo 'ws wrote this' > " + p]);
    const data = await fs.promises.readFile(p, "utf8");
    assert.strictEqual(data.trim(), "ws wrote this");
    await fs.promises.unlink(p);

    rcp.restore();
    rfs.restore();
  });

  // LIFO restore
  await test("LIFO restore with WS transport", async () => {
    rfs.patch();
    rcp.patch();
    rcp.restore();
    rfs.restore();
    assert.ok(true, "no crash on LIFO restore");
  });

  // Binary file via WS spawn, verify with fs
  await test("ws spawn binary output + fs verify", async () => {
    rfs.patch();
    rcp.patch();

    const fs = require("fs");
    const cp = require("child_process");
    const p = "/tmp/ws_int_4.bin";
    const child = cp.spawn("head", ["-c", "256", "/dev/urandom"]);
    const chunks = [];
    await new Promise((res) => {
      child.stdout.on("data", (c) => chunks.push(c));
      child.on("close", res);
    });
    const bin = Buffer.concat(chunks);
    assert.strictEqual(bin.length, 256);
    await fs.promises.writeFile(p, bin);
    const readBack = await fs.promises.readFile(p);
    assert.ok(readBack.equals(bin), "binary round-trip should match");
    await fs.promises.unlink(p);

    rcp.restore();
    rfs.restore();
  });

  console.log("\n=== " + passed + " passed, " + failed + " failed ===");
  process.exit(failed > 0 ? 1 : 0);
}

run().catch((err) => { console.error("FATAL:", err); process.exit(1); });
