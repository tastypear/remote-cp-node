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
rcp.configure({ baseURL: "http://127.0.0.1:8766", token: "testtoken123" });

let passed = 0, failed = 0;

async function test(name, fn) {
  try { await fn(); console.log("PASS  " + name); passed++; }
  catch (err) { console.log("FAIL  " + name + ": " + err.message); console.error(err.stack); failed++; }
}

async function run() {
  console.log("=== remote-fs + remote-child_process integration ===\n");

  // ─── Write file via remote-fs, read via remote-child_process ───
  await test("write with remote-fs, cat with remote-child_process", async () => {
    const testPath = "/tmp/integration_test_1.txt";
    const content = "hello from integration test";
    await rfs.fs.promises.writeFile(testPath, content);

    const output = rcp.execSync("cat " + testPath).toString();
    assert.strictEqual(output.trim(), content);

    await rfs.fs.promises.unlink(testPath);
  });

  // ─── Create dir with remote-fs, ls with remote-child_process ───
  await test("mkdir with remote-fs, ls with remote-child_process", async () => {
    const dir = "/tmp/integration_dir_1";
    await rfs.fs.promises.mkdir(dir, { recursive: true });
    await rfs.fs.promises.writeFile(dir + "/file_a.txt", "aaa");
    await rfs.fs.promises.writeFile(dir + "/file_b.txt", "bbb");

    const listing = rcp.execSync("ls " + dir).toString().trim();
    const files = listing.split("\n").sort();
    assert.deepStrictEqual(files, ["file_a.txt", "file_b.txt"]);

    await rfs.fs.promises.rm(dir, { recursive: true });
  });

  // ─── Both patched (async): fs.writeFile + cp.exec ───
  // Patch order: rfs then rcp. Restore MUST be reverse (LIFO) because both
  // wrap Module._resolveFilename — cp's saved orig points at rfs's wrapper,
  // so rcp must restore before rfs unwraps.
  await test("both patched: fs.promises.writeFile + cp.exec", async () => {
    rfs.patch();
    rcp.patch();

    const fs = require("fs");
    const cp = require("child_process");

    const testPath = "/tmp/integration_test_2.txt";
    await fs.promises.writeFile(testPath, "patched integration");

    const output = cp.execSync("cat " + testPath).toString();
    assert.strictEqual(output.trim(), "patched integration");

    await fs.promises.unlink(testPath);

    rcp.restore();
    rfs.restore();
  });

  // ─── Both patched (sync): fs.writeFileSync + cp.execSync ───
  await test("both patched (patchSync): writeFileSync + execSync", async () => {
    rfs.patch({ patchSync: true });
    rcp.patch();

    const fs = require("fs");
    const cp = require("child_process");

    const testPath = "/tmp/integration_test_3.txt";
    fs.writeFileSync(testPath, "sync patched integration");

    const output = cp.execSync("cat " + testPath).toString();
    assert.strictEqual(output.trim(), "sync patched integration");

    fs.unlinkSync(testPath);

    rcp.restore();
    rfs.restore();
  });

  // ─── node: protocol for both ───
  await test("both patched: node:fs + node:child_process", async () => {
    rfs.patch({ patchSync: true });
    rcp.patch();

    const fs = require("node:fs");
    const cp = require("node:child_process");

    const testPath = "/tmp/integration_test_4.txt";
    fs.writeFileSync(testPath, "node proto integration");

    const output = cp.execSync("cat " + testPath).toString();
    assert.strictEqual(output.trim(), "node proto integration");

    fs.unlinkSync(testPath);

    rcp.restore();
    rfs.restore();
  });

  // ─── spawn + fs.promises.readFile ───
  await test("spawn writes file, fs.promises reads it", async () => {
    rfs.patch();
    rcp.patch();

    const fs = require("fs");
    const cp = require("child_process");

    const testPath = "/tmp/integration_test_5.txt";
    cp.spawnSync("sh", ["-c", "echo 'spawn wrote this' > " + testPath]);

    const data = await fs.promises.readFile(testPath, "utf8");
    assert.strictEqual(data.trim(), "spawn wrote this");

    await fs.promises.unlink(testPath);

    rcp.restore();
    rfs.restore();
  });

  // ─── exec with cwd matching fs ───
  await test("exec cwd matches fs operations", async () => {
    rfs.patch();
    rcp.patch();

    const fs = require("fs");
    const cp = require("child_process");

    const dir = "/tmp";
    const fileName = "cwd_test_file.txt";
    const filePath = dir + "/" + fileName;
    await fs.promises.writeFile(filePath, "cwd content");

    const output = cp.execSync("cat " + fileName, { cwd: dir }).toString();
    assert.strictEqual(output.trim(), "cwd content");

    await fs.promises.unlink(filePath);

    rcp.restore();
    rfs.restore();
  });

  // ─── Re-patch after restore (no stack overflow) ───
  await test("re-patch after restore works", async () => {
    rfs.patch();
    rcp.patch();
    rcp.restore();
    rfs.restore();

    // Re-patch
    rfs.patch();
    rcp.patch();

    const fs = require("fs");
    const cp = require("child_process");

    await fs.promises.writeFile("/tmp/repaint_test.txt", "repaint");
    const out = cp.execSync("cat /tmp/repaint_test.txt").toString();
    assert.strictEqual(out.trim(), "repaint");
    await fs.promises.unlink("/tmp/repaint_test.txt");

    rcp.restore();
    rfs.restore();
  });

  // ─── Reverse restore order ───
  await test("restore in reverse order works", async () => {
    rfs.patch();
    rcp.patch();
    // Restore rcp first, then rfs (reverse of patch order)
    rcp.restore();
    rfs.restore();

    // Local fs should work now
    const fs = require("fs");
    assert.ok(typeof fs.writeFileSync === "function");
  });

  console.log("\n=== " + passed + " passed, " + failed + " failed ===");
  process.exit(failed > 0 ? 1 : 0);
}

run().catch((err) => { console.error("FATAL:", err); process.exit(1); });
