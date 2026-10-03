"use strict";
const cp = require("../index.js");

cp.configure({
  baseURL: "http://127.0.0.1:8765",
  token: "testtoken123",
});

let passed = 0, failed = 0;
function assert(cond, msg) {
  if (cond) { passed++; console.log("  ✓ " + msg); }
  else { failed++; console.log("  ✗ " + msg); }
}

async function main() {
  // 1. batchExecSync — parallel probes
  console.log("\n=== 1. batchExecSync parallel ===");
  const results1 = cp.batchExecSync(
    ["echo hello", "uname -r", "uname -m"],
    { mode: "parallel" }
  );
  assert(results1.length === 3, "3 results for 3 cmds");
  assert(results1[0].stdout.trim() === "hello", "cmd[0] stdout = 'hello'");
  assert(results1[0].exit_code === 0, "cmd[0] exit = 0");
  assert(results1[1].stdout.includes("microsoft"), "cmd[1] has WSL kernel");
  assert(results1[2].stdout.trim() === "x86_64", "cmd[2] arch = x86_64");

  // 2. batchExecSync — sequential shared shell
  console.log("\n=== 2. batchExecSync sequential ===");
  const results2 = cp.batchExecSync(
    ["cd /tmp", "pwd"],
    { mode: "sequential" }
  );
  assert(results2.length === 1, "1 result for sequential");
  assert(results2[0].stdout.trim() === "/tmp", "pwd shows /tmp (shared shell)");

  // 3. batchExec — async parallel
  console.log("\n=== 3. batchExec async parallel ===");
  const results3 = await cp.batchExec(
    ["echo a", "echo b", "echo c"],
    { mode: "parallel" }
  );
  assert(results3.length === 3, "3 results");
  assert(results3[0].stdout.trim() === "a", "cmd[0] = a");
  assert(results3[1].stdout.trim() === "b", "cmd[1] = b");
  assert(results3[2].stdout.trim() === "c", "cmd[2] = c");

  // 4. batchExec — async sequential
  console.log("\n=== 4. batchExec async sequential ===");
  const results4 = await cp.batchExec(
    ["x=42", "echo $x"],
    { mode: "sequential" }
  );
  assert(results4.length === 1, "1 result for sequential");
  assert(results4[0].stdout.trim() === "42", "variable persists across cmds");

  // 5. Error handling — parallel with failing cmd
  console.log("\n=== 5. parallel with error ===");
  const results5 = cp.batchExecSync(
    ["ls /nonexistent", "echo ok"],
    { mode: "parallel" }
  );
  assert(results5.length === 2, "2 results");
  assert(results5[0].exit_code !== 0, "cmd[0] failed");
  assert(results5[1].stdout.trim() === "ok", "cmd[1] still ok");

  // 6. Empty cmds
  console.log("\n=== 6. empty cmds ===");
  const results6 = cp.batchExecSync([], { mode: "parallel" });
  assert(results6.length === 0, "0 results for empty cmds");

  // 7. With cwd
  console.log("\n=== 7. with cwd ===");
  const results7 = cp.batchExecSync(["pwd"], { mode: "parallel", cwd: "/etc" });
  assert(results7[0].stdout.trim() === "/etc", "pwd respects cwd");

  // 8. Semicolon in command (sequential)
  console.log("\n=== 8. semicolon in cmd (sequential) ===");
  const results8 = cp.batchExecSync(
    ["echo hello; echo world", "echo done"],
    { mode: "sequential" }
  );
  assert(results8[0].stdout.includes("hello"), "has hello");
  assert(results8[0].stdout.includes("world"), "has world");
  assert(results8[0].stdout.includes("done"), "has done");

  console.log(`\n${passed} passed, ${failed} failed`);
  process.exit(failed > 0 ? 1 : 0);
}

main().catch((e) => { console.error(e); process.exit(1); });
