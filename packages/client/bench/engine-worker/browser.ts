import { installProfile, resetProfile, readProfile } from "./profile.js";
import { reducerOutboxScope } from "../../src/auth-scope.js";
import { closeEngine } from "../../src/engine/legacy.js";
import { Dexie } from "dexie";
import { entityRecord } from "../../src/indexeddb-read-view.js";
import { GonvexClient, IndexedDBLocalReplicaStorage, browserUpgradeStorage } from "../../src/index.js";
import { createEngineClient, EngineRpcTimeout, type EngineClient } from "../../src/engine/client.js";
import { artifactHash, binding, columns, denyRef, editCollection, editRef, full, row, source, visible } from "./fixture.js";
const profiling = new URL(location.href).searchParams.has("io");
if (profiling) installProfile();
const runtime = `ws://${location.host}/ws`;
const auth = { project: "benchmark", tenant: "tenant", identity: { sub: "account", iss: "fixture" }, fetchToken: async () => "private-fixture-token" };
const storageOptions = { replicaName: "engine-bench", outboxName: "engine-bench-outbox", contract: { version: 1, offlineMaxAgeMs: null }, initialVersion: 1 };
let client: GonvexClient | EngineClient;
let engineWorker: Worker | undefined;
let release: (() => void) | undefined;
let closeStorage: (() => void) | undefined;
const directive = { protocolVersion: 1 as const, scope: "tenant", visibilityScope: "visible", epoch: "fixture" };
const wait = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
const paint = () => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve)));
async function until(test: () => boolean | Promise<boolean>, timeout = 60_000) { const started = performance.now(); while (!await test()) { if (performance.now() - started > timeout) throw new Error("Fixture timed out"); await wait(10); } }
const tasks: number[] = [], frames: number[] = [], clicks: number[] = [];
let observing = false;
const longTaskSupported = PerformanceObserver.supportedEntryTypes.includes("longtask");
if (longTaskSupported) new PerformanceObserver(list => { if (observing) for (const entry of list.getEntries()) tasks.push(entry.duration); }).observe({ entryTypes: ["longtask"] });
function begin() { observing = true; tasks.length = 0; frames.length = 0; clicks.length = 0; }
async function finish(start: number) { await paint(); observing = false; return { wallMs: performance.now() - start, longTasks: [...tasks], longTaskSupported, frames: [...frames], clicks: [...clicks] }; }
function signature(ref: typeof visible) { return `${ref.path}\u0000{}\u0000{"delivery":"replica","live":null,"scope":"tenant"}`; }
async function seed(count = 50_000) {
  const storage = new IndexedDBLocalReplicaStorage(storageOptions.replicaName);
  const existing = new Dexie(storageOptions.replicaName, { cache: "disabled" });
  await storage.loadSession("probe"); await existing.open();
  const existingCount = await existing.table("entities").where("[scope+entity]").equals(["visible", "tasks"]).count();
  const cursor = await existing.table("meta").get(["visible", "cursor"]);
  const windows = await existing.table("windows").where("scope").equals("visible").toArray();
  const revision = Math.max(cursor ? JSON.parse(cursor.value).revision : 0, ...windows.map(window => JSON.parse(window.value).cursor?.revision ?? 0));
  await fetch(`/revision?value=${revision + 1}`);
  existing.close();
  if (existingCount !== count) {
  // Seed preparation writes the adapter's exact record format in bulk. It is
  // outside the measured workload; measured commits still use the SDK adapter.
  await storage.replaceSnapshot({ entities: {}, liveQueries: { [signature(editCollection)]: { signature: signature(editCollection), kind: "replica", entity: "edits", key: "id", ids: [], completeness: "complete", source: "server", columns: ["id", "count"] }, [signature(visible)]: { signature: signature(visible), kind: "replica", entity: "tasks", key: "id", ids: Array.from({ length: 250 }, (_, index) => String(index)), completeness: "complete", source: "server", columns } } }, "visible");
  const database = new Dexie(storageOptions.replicaName, { cache: "disabled" }); await database.open();
  for (let offset = 0; offset < count; offset += 500) {
    const records = Array.from({ length: Math.min(500, count - offset) }, (_, index) => ({ ...entityRecord("visible", "tasks", String(offset + index), row(offset + index)), sequence: 1 }));
    await database.table("entities").bulkPut(records);
    if (offset % 5_000 === 0) console.log(`Seed ${offset + records.length}/${count}`);
  }
  for (let index = 0; index < 5; index++) await database.table("entities").put({ ...entityRecord("visible", `related${index}`, "one", { id: "one", value: index }), sequence: 1 });
  database.close();
  }
  await storage.saveSession(reducerOutboxScope(runtime, auth, "unused"), { directive, artifactHash, identity: { auth: { account: { id: "account" } }, tenant: { id: "tenant" }, member: { id: "member", accountId: "account", permissions: {} } } });
  storage.close(); return { rows: count, initialEdits: await countEdits(), rowBytes: new TextEncoder().encode(JSON.stringify(row(0))).length };
}
async function countEdits() {
  const database = new Dexie(storageOptions.replicaName); await database.open();
  const count = await database.table("entities").where("[scope+entity]").equals(["visible", "edits"]).count(); database.close(); return count;
}
async function resetEdits() {
  // Keep task hydration fixed while removing prior synthetic edits outside
  // measurement. No SDK client or worker has opened these stores yet.
  const database = new Dexie(storageOptions.replicaName); await database.open();
  await database.transaction("rw", [database.table("entities"), database.table("windows")], async () => {
    await database.table("entities").where("[scope+entity]").equals(["visible", "edits"]).delete();
    const key = signature(editCollection);
    await database.table("windows").put({ scope: "visible", signature: key, value: JSON.stringify({ signature: key, kind: "replica", entity: "edits", key: "id", ids: [], completeness: "complete", source: "server", columns: ["id", "count"] }) });
  });
  database.close(); await fetch("/reset-edits", { method: "POST" });
}
async function start(mode: "legacy" | "worker") {
  begin(); const started = performance.now();
  if (mode === "worker") client = await createEngineClient(runtime, { engine: { workerUrl: "/worker.js", artifactHash, workerFactory: () => { engineWorker = new Worker(`/worker.js${profiling ? "?io" : ""}`, { type: "module" }); return engineWorker; } }, auth, storage: storageOptions, onDiagnostic: message => console.log(`Engine ${message}`), fallbackBinding: binding, residency: { maxResidentRows: Number.MAX_SAFE_INTEGER, maxResidentBytes: 200 * 1024 * 1024 } });
  else {
    const stores = browserUpgradeStorage({ ...storageOptions, migrations: [] }); await stores.ready; closeStorage = stores.close;
    client = new GonvexClient(runtime, { ...auth, localRuntime: binding, outbox: { store: stores.store }, localReplica: { storage: stores.storage, maxResidentRows: Number.MAX_SAFE_INTEGER, maxResidentBytes: 200 * 1024 * 1024 } });
  }
  console.log(`Engine started ${mode}`);
  if ("engineMode" in client) client.onError(error => console.log(`Engine error ${error}`));
  const watch = client.watchReplica(visible, {}, { deferStart: true });
  release = watch.onUpdate(() => { const rows = watch.localReplicaResult(); if (rows) document.querySelector("main")!.textContent = `${rows.length}:${(rows[0] as any)?.id}`; });
  await until(() => watch.localReplicaResult()?.length === 250);
  const result = await finish(started);
  console.log("Engine cached paint ready");
  return { ...result, mode: "engineMode" in client ? client.engineMode : "legacy" };
}
async function prepare() {
  // Exact-ID subscriptions retain 1,000 rows in both modes.
  const releases: Array<() => void> = [];
  let readyEntities = 0;
  let sourceReady: Promise<void>;
  if ("engineMode" in client) { const retain = client.retainCollection(source); releases.push(retain); sourceReady = retain.ready; }
  else sourceReady = new Promise(resolve => releases.push((client as GonvexClient).subscribeReplica(source, {}, message => { if (message.type === "replica.ready") resolve(); })));
  for (let index = 0; index < 1_000; index++) {
    if ("engineMode" in client) { const entity = client.watchEntities("tasks", [String(index)]); let ready = false; releases.push(entity.onUpdate(() => { if (!ready && entity.getSnapshot().status === "ready") { ready = true; readyEntities++; } })); }
    else { releases.push(client.retainReplicaEntities("tasks", [String(index)])); releases.push(client.localReplica.subscribe(() => undefined, { entity: "tasks", ids: [String(index)] })); }
  }
  const prior = release; release = () => { prior?.(); releases.forEach(close => close()); };
  client.connect(); await until(() => client.localSessionState() === "authenticated");
  console.log("Engine authenticated");
  await Promise.race([sourceReady, wait(60_000).then(() => { throw new Error("Source subscription did not become ready"); })]);
  if ("engineMode" in client) await until(() => readyEntities === 1_000);
  await wait(200);
}
async function burst() {
  await fetch("/reset-burst", { method: "POST" });
  begin(); const started = performance.now(); let last = performance.now(); let running = true;
  const frame = (now: number) => { frames.push(now - last); last = now; if (running) requestAnimationFrame(frame); }; requestAnimationFrame(frame);
  const button = document.querySelector("button")!;
  let scheduled = performance.now() + 50;
  const handler = () => { const sent = scheduled; scheduled += 50; button.textContent = String(sent); requestAnimationFrame(() => requestAnimationFrame(() => clicks.push(performance.now() - sent))); };
  button.addEventListener("click", handler);
  const interval = setInterval(() => button.click(), 50);
  await fetch("/burst", { method: "POST" }); await wait(500);
  clearInterval(interval); running = false; button.removeEventListener("click", handler); return finish(started);
}
async function snapshot() {
  begin(); const started = performance.now();
  if ("engineMode" in client) {
    const retained = client.retainCollection(full); await retained.ready;
    const result = await finish(started); retained(); return result;
  }
  const watch = client.watchReplica(full, {}, { deferStart: true });
  let resolve!: () => void; const ready = new Promise<void>(done => { resolve = done; });
  const stop = watch.onUpdate(() => { if (watch.status().isUpToDate) resolve(); });
  await ready; const result = await finish(started); stop(); return result;
}
async function workerProfile(operation: "reset" | "read") {
  if (!engineWorker || !("engineMode" in client) || client.engineMode !== "worker") return readProfile();
  return new Promise<any>(resolve => {
    const receive = ({ data }: MessageEvent) => {
      if (data?.benchProfile === operation) { engineWorker!.removeEventListener("message", receive); resolve(data.value); }
    };
    engineWorker!.addEventListener("message", receive); engineWorker!.postMessage({ benchProfile: operation });
  });
}
async function drain() {
  // Queue in offline mode, then reconnect the same client and original IDs.
  await fetch("/offline", { method: "POST" }); await until(() => !client.connectionState().isWebSocketConnected);
  await fetch("/reset-wire-profile", { method: "POST" });
  if (profiling) { resetProfile(); await workerProfile("reset"); }
  begin(); const started = performance.now();
  const latencies: number[] = [];
  const admit = async () => {
    const called = performance.now();
    await client.reducer(editRef, { id: crypto.randomUUID() }, { timeoutMs: 0 });
    latencies.push(performance.now() - called);
  };
  if (new URL(location.href).searchParams.has("pipeline")) await Promise.all(Array.from({ length: 500 }, admit));
  else for (let index = 0; index < 500; index++) await admit();
  const admissionMs = performance.now() - started;
  const admissionProfile = profiling ? await workerProfile("read") : undefined;
  await fetch("/online", { method: "POST" }); client.connect(); await until(async () => {
    const entries = "engineMode" in client ? await (client as any).listIntents() : await client.listIntents();
    return entries.length === 0;
  }, 120_000);
  return { ...await finish(started), admissionMs, admissionLatenciesMs: latencies, wireProfile: await fetch("/wire-profile").then(response => response.json()), ...(profiling ? { admissionProfile, drainProfile: await workerProfile("read") } : {}) };
}
async function parity() {
  if (!("engineMode" in client)) throw new Error("Worker fixture required");
  for (const api of ["fetch", "WebSocket", "XMLHttpRequest"]) {
    let denied = false; try { await client.reducer(denyRef, { api }); } catch (error) { denied = String(error).includes("External I/O"); }
    if (!denied) throw new Error(`Reducer ${api} was not denied`);
  }
  await fetch("/offline", { method: "POST" }); await until(() => !client.connectionState().isWebSocketConnected);
  const command = client.createCommand();
  const first = await client.reducer(editRef, { id: "retry" }, { engineCommand: command });
  engineWorker!.terminate(); await client.recover();
  const second = await client.reducer(editRef, { id: "retry" }, { engineCommand: command });
  if (JSON.stringify(first) !== JSON.stringify(second)) throw new Error("Command retry changed result");
  const queued = await client.listIntents();
  if (queued.filter(entry => entry.id === command.id).length !== 1) throw new Error("Crash recovery duplicated admission");
  const batchCommands = Array.from({ length: 32 }, () => client.createCommand());
  const calls = batchCommands.map((engineCommand, index) => client.reducer(editRef, { id: `crash-${index}` }, { engineCommand, timeoutMs: 0 }));
  const interrupted = Promise.allSettled(calls);
  await Promise.race(calls); engineWorker!.terminate(); await client.recover(); await interrupted;
  const prefix = (await client.listIntents()).filter(entry => batchCommands.some(command => command.id === entry.id)).length;
  if (prefix < 1 || prefix >= batchCommands.length) throw new Error("Crash did not interrupt a partially durable batch");
  await Promise.all(batchCommands.map((engineCommand, index) => client.reducer(editRef, { id: `crash-${index}` }, { engineCommand, timeoutMs: 0 })));
  const replayed = (await client.listIntents()).filter(entry => batchCommands.some(command => command.id === entry.id));
  if (replayed.length !== 32 || new Set(replayed.map(entry => entry.id)).size !== 32) throw new Error("Batch crash retry duplicated or lost a command");
  await fetch("/online", { method: "POST" }); client.connect(); await until(() => client.connectionState().isWebSocketConnected); await until(() => client.localSessionState() === "authenticated");
  await until(async () => (await client.listIntents()).length === 0);
  await fetch("/disconnect", { method: "POST" }); await until(() => !client.connectionState().isWebSocketConnected);
  await until(() => client.connectionState().isWebSocketConnected, 15_000); await until(() => client.localSessionState() === "authenticated");
  await until(async () => (await client.listIntents()).length === 0);
  const deniedCount = await fetch("/denied-count").then(response => response.json()); if (deniedCount !== 0) throw new Error("Reducer network escaped");
  return { deniedApis: 3, retryResult: second, durableCrashRecovery: true, batchCrashRecovery: { durablePrefix: prefix, commands: 32, sameIds: true }, reconnect: true };
}
async function stop() { release?.(); release = undefined; if ("engineMode" in client) await client.close(); else await closeEngine(client); closeStorage?.(); closeStorage = undefined; await wait(100); }
(window as any).benchmark = { seed, resetEdits, start, prepare, burst, snapshot, drain, parity, stop, mode: () => (client as any).engineMode };
