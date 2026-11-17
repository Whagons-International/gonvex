import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, readdir, rm, writeFile, readFile } from "node:fs/promises";
import { existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import setup, { startManager } from "./global-setup.mjs";

async function directory(t) {
  const root = await mkdtemp(join(tmpdir(), "k20-fixture-test-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  return root;
}
const health = async fixture => (await fetch(`${fixture.url}/healthz`)).json();
const dead = pid => assert.throws(() => process.kill(pid, 0), { code: "ESRCH" });

test("overlapping fixtures own distinct ports; stopping one leaves the other healthy", async t => {
  const tempRoot = await directory(t);
  const fixtures = await Promise.all([1, 2].map(async () => {
    const fixture = await startManager({ tempRoot });
    t.after(fixture.stop);
    return fixture;
  }));
  const [a, b] = fixtures;
  assert.notEqual(a.url, b.url);
  assert.notEqual(a.root, b.root);
  assert.deepEqual(await health(a), { ok: true, worker: "stub" });
  assert.deepEqual(await health(b), { ok: true, worker: "stub" });
  await a.stop();
  dead(a.pid);
  assert.equal(existsSync(a.root), false);
  await assert.rejects(fetch(`${a.url}/healthz`));
  assert.deepEqual(await health(b), { ok: true, worker: "stub" });
  await b.stop();
  dead(b.pid);
  assert.deepEqual(await readdir(tempRoot), []);
});

test("a child that exits before listening fails setup with its stderr and cleans its root", async t => {
  const tempRoot = await directory(t);
  await assert.rejects(startManager({
    tempRoot,
    env: { ...process.env, NODE_OPTIONS: "--require=/deliberately-missing-sandbox-fixture-preload.cjs" },
  }), error => {
    assert.match(error.message, /manager exited before readiness \(code 1/);
    assert.match(error.message, /Cannot find module.*deliberately-missing-sandbox-fixture-preload/);
    return true;
  });
  assert.deepEqual(await readdir(tempRoot), []);
});

test("an unavailable executable fails setup with its spawn error and cleans its root", async t => {
  const tempRoot = await directory(t);
  await assert.rejects(startManager({ tempRoot, executable: join(tempRoot, "absent-node") }), /ENOENT/);
  assert.deepEqual(await readdir(tempRoot), []);
});

test("readiness timeout terminates and awaits a child that never announces readiness", async t => {
  const tempRoot = await directory(t);
  const managerPath = join(tempRoot, "silent.mjs"), pidFile = join(tempRoot, "pid");
  await writeFile(managerPath, `import {writeFileSync} from 'node:fs';writeFileSync(process.env.K20_TEST_PID_FILE,String(process.pid));setInterval(()=>{},1000);`);
  await assert.rejects(startManager({ tempRoot, managerPath, startupTimeoutMs: 1_000,
    env: { ...process.env, K20_TEST_PID_FILE: pidFile } }), /manager readiness timed out after 1000 ms/);
  dead(Number(await readFile(pidFile, "utf8")));
  assert.deepEqual((await readdir(tempRoot)).sort(), ["pid", "silent.mjs"]);
});

test("a listening child with the wrong health capability is refused and cleaned up", async t => {
  const tempRoot = await directory(t), managerPath = join(tempRoot, "bad-health.mjs");
  await writeFile(managerPath, `import {createServer} from 'node:http';const server=createServer((q,r)=>r.end(JSON.stringify({ok:true,worker:'wrong'})));server.listen(0,'127.0.0.1',()=>process.send({type:'sandbox-manager-ready',port:server.address().port}));`);
  await assert.rejects(startManager({ tempRoot, managerPath }), /health check did not match/);
  assert.deepEqual(await readdir(tempRoot), ["bad-health.mjs"]);
});

test("an explicit external manager URL is provided without taking ownership of its process", async () => {
  const previous = process.env.K20_SANDBOX_URL;
  process.env.K20_SANDBOX_URL = "http://127.0.0.1:12345";
  try {
    const provided = [];
    assert.equal(await setup({ provide: (...args) => provided.push(args) }), undefined);
    assert.deepEqual(provided, [["k20SandboxUrl", process.env.K20_SANDBOX_URL]]);
  } finally {
    if (previous === undefined) delete process.env.K20_SANDBOX_URL;
    else process.env.K20_SANDBOX_URL = previous;
  }
});
