// V8 isolate heap of the worker through workerd's inspector (Runtime.getHeapUsage),
// for no app bundle, the Query/Reducer split and the complete bundle: idle
// after load, and after provisioning the schema subset and running Reducers.
// The Workers limit is 128 MB per isolate.
// Writes .artifact/heap-results.json.
import { spawn, execFileSync } from "node:child_process";
import { readFileSync, writeFileSync, rmSync, mkdirSync } from "node:fs";
import { join, resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { createServer } from "node:net";
import { fixture, steps, callOf } from "../test/scenarios.mjs";

const spike = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const freePort = () => new Promise((done) => { const server = createServer().listen(0, "127.0.0.1", () => { const { port } = server.address(); server.close(() => done(port)); }); });
const subset = JSON.parse(readFileSync(join(spike, ".artifact/schema-subset.json"), "utf8"));

async function heap(inspector) {
  const socket = new WebSocket(`ws://127.0.0.1:${inspector}/main`);
  await new Promise((r, j) => { socket.onopen = r; socket.onerror = j; });
  let id = 0;
  const pending = new Map();
  socket.onmessage = (event) => { const message = JSON.parse(event.data); pending.get(message.id)?.(message); };
  const send = (method, params = {}) => new Promise((r) => { const n = ++id; pending.set(n, r); socket.send(JSON.stringify({ id: n, method, params })); });
  // workerd's inspector does not answer HeapProfiler.collectGarbage; usage
  // is read as is (it includes garbage not yet collected).
  const usage = (await send("Runtime.getHeapUsage")).result;
  socket.close();
  return { usedMb: Math.round(usage.usedSize / 1048576 * 10) / 10, totalMb: Math.round(usage.totalSize / 1048576 * 10) / 10 };
}

const results = {};
for (const bundle of ["none", "reducers", "complete"]) {
  const [port, inspector] = [await freePort(), await freePort()];
  const storage = join(spike, ".storage", `heap-${bundle}`);
  rmSync(storage, { recursive: true, force: true }); mkdirSync(storage, { recursive: true });
  execFileSync("node", [join(spike, "scripts/prepare.mjs"), "--bundle", bundle, "--port", String(port), "--storage", storage], { stdio: "ignore" });
  const child = spawn(join(spike, "node_modules/.bin/workerd"), ["serve", "--experimental", `--inspector-addr=127.0.0.1:${inspector}`, join(spike, ".build/config.capnp")], { stdio: "ignore" });
  const base = `http://127.0.0.1:${port}`;
  for (;;) { try { if ((await fetch(`${base}/ping`)).ok) break; } catch { /* starting */ } await new Promise((r) => setTimeout(r, 20)); }
  const idle = await heap(inspector);
  await fetch(`${base}/provision`, { method: "POST", body: JSON.stringify({ statements: subset.statements, rows: [...subset.rows, ...fixture] }) });
  if (bundle === "reducers") for (const step of steps.filter((s) => ["teams.create", "spots.create"].includes(s.path))) await fetch(`${base}/invoke`, { method: "POST", body: JSON.stringify({ call: callOf(step) }) });
  const afterWork = await heap(inspector);
  child.kill();
  await new Promise((r) => child.once("exit", r));
  results[bundle] = { idle, afterProvisionAndReducers: afterWork };
  console.log(bundle, JSON.stringify(results[bundle]));
}
writeFileSync(join(spike, ".artifact/heap-results.json"), JSON.stringify(results, null, 2));
