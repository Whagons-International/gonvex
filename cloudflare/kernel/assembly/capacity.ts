import { checkedCapacitySample, WINDOW_FIELDS, SAMPLE_MS, type CapacitySample, type DrainResult } from "../gateway/placement";
import { gatewayShards } from "../session/placement/gateway";
import { rpc } from "../session/rpc";
import { CAPACITY_LIMITS as LIMITS, type CapacityPage, type CapacityQuery, type CapacityRead, type CapacitySlot,
  type CapacityObservation, type CapacityEvent, type CapacityIdentity, type CapacityError, type ReadFailure } from "./capacity-contract";

export interface CapacityEnv {
  GATEWAY: DurableObjectNamespace<any>; GATEWAY_CONTROLLER?: DurableObjectNamespace<any>;
  GONVEX_GATEWAY_SHARDS?: string; GONVEX_RUNTIME_VERSION?: string; WORKER_VERSION?: WorkerVersionMetadata;
}
interface Catalog { version?: string; releases: Record<string, { project: string; hash: string; generation: number }> }
interface StatusPort { status(): Promise<unknown> }
interface HolderPort { capacity(epoch: number): Promise<unknown>; drainStatus(): Promise<unknown> }
type RecordValue = Record<string, unknown>;
const integer = (n: unknown, min = 0, max = Number.MAX_SAFE_INTEGER): n is number => typeof n === "number" && Number.isSafeInteger(n) && n >= min && n <= max;
const finite = (n: unknown): n is number => typeof n === "number" && Number.isFinite(n) && n >= 0 && n <= Number.MAX_SAFE_INTEGER;
const label = (s: unknown): s is string => typeof s === "string" && /^[A-Za-z0-9._-]{1,128}$/.test(s);
function object(v: unknown, required: readonly string[], optional: readonly string[] = []): v is RecordValue {
  return v !== null && typeof v === "object" && !Array.isArray(v) && required.every(k => Object.hasOwn(v, k))
    && Object.keys(v).length <= required.length + optional.length && Object.keys(v).every(k => required.includes(k) || optional.includes(k));
}
const bytes = (v: unknown) => new TextEncoder().encode(JSON.stringify(v)).byteLength;
async function digest(v: unknown): Promise<string> {
  return Array.from(new Uint8Array(await crypto.subtle.digest("SHA-256", new TextEncoder().encode(JSON.stringify(v)))), b => b.toString(16).padStart(2, "0")).join("");
}
export function capacityQuery(url: URL): CapacityQuery | null {
  if (url.search.length > 512) return null;
  const allowed = ["cell", "cursor", "limit", "eventCursor", "eventLimit", "fresh", "runId"];
  for (const key of url.searchParams.keys()) if (!allowed.includes(key) || url.searchParams.getAll(key).length !== 1) return null;
  const read = (key: string, fallback: number | null, min: number, max: number): number | null => {
    const value = url.searchParams.get(key);
    if (value === null) return fallback;
    if (!/^(0|[1-9][0-9]*)$/.test(value)) return null;
    const n = Number(value); return integer(n, min, max) ? n : null;
  };
  const cell = read("cell", null, 0, 31), cursor = read("cursor", 0, 0, 64), limit = read("limit", 4, 1, LIMITS.slots);
  const eventCursor = read("eventCursor", 0, 0, 64), eventLimit = read("eventLimit", 16, 1, LIMITS.events), fresh = read("fresh", 0, 0, 1);
  const runId = url.searchParams.get("runId");
  if ([cell, cursor, limit, eventCursor, eventLimit, fresh].some(n => n === null) || (runId !== null && !/^[A-Za-z0-9_-]{1,64}$/.test(runId))) return null;
  return { cell: cell!, cursor: cursor!, limit: limit!, eventCursor: eventCursor!, eventLimit: eventLimit!, fresh: fresh === 1, runId };
}
const sampleFields = ["v", "epoch", "at", "sockets", "framesPerSecond", "bytesPerSecond", "deliveryMs", "queueAgeMs", "rpcMs"];
function sample(v: unknown): CapacitySample | undefined {
  if (!object(v, sampleFields, ["currentPressure", ...WINDOW_FIELDS]) || !integer(v.at)) return undefined;
  // Validate the shape at its ORIGINAL source time: stale cached samples remain observable.
  return checkedCapacitySample(v as unknown as CapacitySample, v.at);
}
function observation(s: CapacitySample | null, now: number): CapacityObservation {
  const state = !s ? "UNKNOWN" : s.at > now + 1000 ? "CLOCK_SKEW" : now - s.at > LIMITS.cachedStaleMs ? "STALE" : "FRESH";
  const windowState = s?.windowEnd === undefined ? "UNKNOWN" : s.windowEnd > now ? "CLOCK_SKEW" : now - s.windowEnd > 2 * SAMPLE_MS ? "STALE" : "FRESH";
  return { sample: s, state, ageMs: !s || state === "CLOCK_SKEW" ? null : Math.max(0, now - s.at), windowState };
}
function holderName(name: unknown, cell: number): name is string {
  if (typeof name !== "string") return false;
  const match = /^auto-gw-(0|[1-9][0-9]*)-(0|[1-9][0-9]*)$/.exec(name);
  return !!match && Number(match[1]) === cell && integer(Number(match[2]), 0, 63);
}
interface Status { v: 1; epoch: number; pending: number; slots: CapacitySlot[]; events: CapacityEvent[] }
function status(input: unknown, cell: number, now: number): CapacityRead<Status> {
  if (!object(input, ["v", "epoch", "pending", "slots", "events"]) || input.v !== 1 || !integer(input.epoch)
    || !integer(input.pending, 0, 256) || !Array.isArray(input.slots) || input.slots.length > 64 || !Array.isArray(input.events) || input.events.length > 64) return { status: "MALFORMED" };
  const slots: CapacitySlot[] = [], names = new Set<string>();
  for (const s of input.slots) {
    if (!object(s, ["name", "epoch", "mode", "changedAt", "pressuredSince", "replacement", "sample"], ["nextCheck", "pressureEvidence"])
      || !holderName(s.name, cell) || names.has(s.name) || !integer(s.epoch, 1, input.epoch)
      || !["active", "draining", "retired"].includes(s.mode as string) || !integer(s.changedAt)
      || !(s.pressuredSince === null || integer(s.pressuredSince)) || !(s.replacement === null || holderName(s.replacement, cell))
      || !(s.nextCheck === undefined || integer(s.nextCheck))) return { status: "MALFORMED" };
    const cached = s.sample === null ? null : sample(s.sample);
    if (cached === undefined || (cached && cached.epoch !== s.epoch)) return { status: "MALFORMED" };
    let evidence: CapacitySlot["pressureEvidence"] = null;
    if (s.pressureEvidence !== undefined) {
      const e = s.pressureEvidence;
      if (!object(e, ["epoch", "window", "since"]) || !integer(e.epoch, 1) || e.epoch !== s.epoch || !integer(e.since)
        || !Array.isArray(e.window) || e.window.length !== WINDOW_FIELDS.length || !e.window.every(finite)) return { status: "MALFORMED" };
      evidence = { epoch: e.epoch, window: [...e.window], since: e.since };
    }
    names.add(s.name);
    slots.push({ name: s.name, epoch: s.epoch, mode: s.mode as CapacitySlot["mode"], changedAt: s.changedAt,
      pressuredSince: s.pressuredSince, replacement: s.replacement, nextCheck: s.nextCheck ?? null, pressureEvidence: evidence,
      cached: observation(cached, now), fresh: { capacity: { status: "NOT_REQUESTED" }, drain: { status: "NOT_REQUESTED" } } } as CapacitySlot);
  }
  const events: CapacityEvent[] = [];
  for (const e of input.events) {
    if (!object(e, ["at", "action", "gateway", "epoch"]) || !integer(e.at) || !integer(e.epoch, 1, input.epoch)
      || !holderName(e.gateway, cell) || !["allocate", "drain", "retire"].includes(e.action as string)) return { status: "MALFORMED" };
    events.push({ at: e.at, action: e.action as CapacityEvent["action"], gateway: e.gateway, epoch: e.epoch });
  }
  if (bytes(input) > LIMITS.controllerReplyBytes) return { status: "OVERSIZED" };
  return { status: "OK", value: { v: 1, epoch: input.epoch, pending: input.pending, slots, events } };
}
const blockedReasons = ["inflight", "queued", "direct-action", "session-hook-unavailable", "replacement-or-admission-unavailable"];
function drain(input: unknown): DrainResult | undefined {
  if (!object(input, ["closed", "remaining", "blocked"]) || !integer(input.closed) || !integer(input.remaining)
    || !object(input.blocked, [], blockedReasons) || !Object.values(input.blocked).every(integerValue => integer(integerValue))) return undefined;
  return { closed: input.closed, remaining: input.remaining, blocked: { ...input.blocked } as Record<string, number> };
}
class Refusal { constructor(readonly status: ReadFailure) {} }
async function bounded<T>(call: () => Promise<T>, signal: AbortSignal, deadline: number, ms: number): Promise<CapacityRead<T>> {
  if (signal.aborted) return { status: "ABORTED" };
  const left = Math.min(ms, deadline - Date.now());
  if (left <= 0) return { status: "TIMEOUT" };
  let timer: ReturnType<typeof setTimeout> | undefined, onAbort: () => void = () => {};
  try {
    return { status: "OK", value: await Promise.race([Promise.resolve().then(() => {
      if (signal.aborted) throw new Refusal("ABORTED");
      if (Date.now() >= deadline) throw new Refusal("TIMEOUT");
      return call();
    }), new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Refusal("TIMEOUT")), left);
      onAbort = () => reject(new Refusal("ABORTED")); signal.addEventListener("abort", onAbort, { once: true });
    })]) };
  } catch (error) { return { status: error instanceof Refusal ? error.status : "RPC_FAILED" }; }
  finally { if (timer !== undefined) clearTimeout(timer); signal.removeEventListener("abort", onAbort); }
}
async function identity(env: CapacityEnv, catalog: Catalog): Promise<CapacityIdentity> {
  const tuples: [string, string, number][] = [];
  let valid = label(catalog.version) && Object.keys(catalog.releases).length <= 2048;
  for (const [key, r] of Object.entries(catalog.releases).slice(0, 2048)) {
    if (!label(key) || !r || r.project !== key || !label(r.hash) || !integer(r.generation, 1)) { valid = false; break; }
    tuples.push([key, r.hash, r.generation]);
  }
  tuples.sort((a, b) => a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0);
  const config = { controllerBindingPresent: !!env.GATEWAY_CONTROLLER, staticGatewayShards: gatewayShards(env) };
  const w = env.WORKER_VERSION;
  return { originService: "kernel-http", worker: w && label(w.id) && (w.tag === "" || label(w.tag)) && typeof w.timestamp === "string" && w.timestamp.length <= 64
    ? { id: w.id, tag: w.tag, timestamp: w.timestamp } : null,
    runtimeLabel: label(env.GONVEX_RUNTIME_VERSION) ? env.GONVEX_RUNTIME_VERSION : null,
    catalog: valid ? { status: "OK", value: { version: catalog.version!, projects: tuples.length, tuplesSha256: await digest(tuples) } } : { status: "MALFORMED" },
    config: { ...config, factsSha256: await digest(config) }, objectCodeVersions: "UNAVAILABLE_NO_OBJECT_VERSION_PORT" };
}
const unavailable = Object.freeze([
  { metric: "cpu,isolateMemory,cost", status: "UNAVAILABLE" as const, sourceInterface: "GatewayController.status/Gateway.capacity", reason: "NO_PROVIDER_METER_PORT_OR_RUN_COST_JOIN" },
  { metric: "sql,Control,auth,limiter", status: "UNAVAILABLE" as const, sourceInterface: "GatewayController.status/Gateway.capacity", reason: "NO_MEASURED_SERVICE_PORT_PLACEHOLDER_METRICS_NOT_USED" },
  { metric: "holderCodeVersion,drainResultAge,eventLoss", status: "UNAVAILABLE" as const, sourceInterface: "Gateway.capacity/drainStatus/Controller.status", reason: "SOURCE_INTERFACES_DO_NOT_EXPORT_THESE_FACTS" },
]);
function json(value: CapacityPage | CapacityError, statusCode = 200): Response {
  let text = JSON.stringify(value);
  if (new TextEncoder().encode(text).byteLength > LIMITS.responseBytes) { text = JSON.stringify({ v: 1, error: "RESPONSE_BUDGET" }); statusCode = 503; }
  return new Response(text, { status: statusCode, headers: { "content-type": "application/json", "cache-control": "no-store", ...(statusCode === 405 ? { allow: "GET" } : {}) } });
}
/** Called ONLY after fixtureControl's existing bearer gate. Never acquires/observes/drains. */
export async function capacityPage(request: Request, env: CapacityEnv, catalog: Catalog): Promise<Response> {
  if (request.method !== "GET") return json({ v: 1, error: "METHOD" }, 405);
  const q = capacityQuery(new URL(request.url));
  if (!q) return json({ v: 1, error: "BAD_QUERY" }, 400);
  if (request.signal.aborted) return json({ v: 1, error: "ABORTED" }, 499);
  const at = Date.now(), deadline = at + LIMITS.requestMs;
  const result: CapacityPage = { v: 1, at, runId: q.runId, mode: q.fresh ? "fresh" : "cached", pageStatus: "PARTIAL", identity: await identity(env, catalog),
    controller: { status: "NOT_BOUND" }, coverage: null, slots: [], events: [], unavailable,
    provenance: { clock: "UNIX_MS_SERVICE_CLOCKS_NOT_SYNCHRONIZED", controllerStatusCalls: 0, capacityCalls: 0, drainStatusCalls: 0,
      controllerReadMayWake: true, freshMayWakeHolders: false, metricsSnapshotMayAdvance: false, socketSampleMayBeRecorded: false,
      drainResult: "LAST_TRACKED_RESULT_NO_RESULT_TIMESTAMP", cancellation: "STOPS_FURTHER_CALLS_INFLIGHT_RPC_NOT_CANCELLED",
      runId: "CORRELATION_ONLY_NOT_COST_ATTRIBUTION", history: "LAST_64_EVENTS_NO_COMPLETE_TIMELINE_OR_LOSS_COUNTER",
      windowHistory: "LATEST_COMPLETED_BUCKET_ONLY_GAPS_REQUIRE_CLIENT_COMPARISON", sockets: "CURRENT_GAUGE_AND_BUCKET_SAMPLED_MIN_NOT_CONTINUOUS_OCCUPANCY" } };
  if (!env.GATEWAY_CONTROLLER) return json(result);
  const read = await bounded(() => {
    result.provenance.controllerStatusCalls++;
    return rpc<StatusPort>(env.GATEWAY_CONTROLLER!, `gateway-cell-${q.cell}`).status();
  }, request.signal, deadline, LIMITS.controllerMs);
  const checked = read.status === "OK" ? status(read.value, q.cell, Date.now()) : read;
  if (checked.status !== "OK") { result.controller = checked; return json(result); }
  const s = checked.value;
  if (q.cursor > s.slots.length || q.eventCursor > s.events.length) return json({ v: 1, error: "BAD_CURSOR" }, 400);
  // Hash source facts only. Derived ages and HTTP capture time cannot make an unchanged page look new.
  result.controller = { status: "OK", value: { cell: q.cell, epoch: s.epoch, pending: s.pending, readAt: Date.now(), snapshotSha256: await digest(read.status === "OK" ? read.value : null) } };
  const coverage = (cursor: number, limit: number, total: number) => {
    const returned = Math.min(limit, total - cursor), hasMore = cursor + returned < total;
    return { cursor, limit, total, returned, hasMore, nextCursor: hasMore && returned ? cursor + returned : null };
  };
  result.coverage = { cell: q.cell, totalCells: 32, fleet: "ONE_CELL_PAGE_ONLY", slots: coverage(q.cursor, q.limit, s.slots.length),
    events: { ...coverage(q.eventCursor, q.eventLimit, s.events.length), sourceLimit: 64, loss: "UNKNOWN", oldestAt: s.events[0]?.at ?? null, newestAt: s.events.at(-1)?.at ?? null },
    pagination: "COMPARE_CONTROLLER_SNAPSHOT_SHA256" };
  result.slots = s.slots.slice(q.cursor, q.cursor + q.limit); result.events = s.events.slice(q.eventCursor, q.eventCursor + q.eventLimit);
  result.pageStatus = "COMPLETE";
  if (!q.fresh) return json(result);
  let next = 0, stopped: "TIMEOUT" | "ABORTED" | null = null;
  const stop = (read: CapacityRead<unknown>) => {
    if (read.status === "TIMEOUT" || read.status === "ABORTED") stopped = read.status;
  };
  const worker = async () => {
    for (;;) {
      const slot = result.slots[next++]; if (!slot) return;
      if (slot.mode === "retired") { slot.fresh = { capacity: { status: "RETIRED" }, drain: { status: "RETIRED" } }; continue; }
      if (stopped) { slot.fresh = { capacity: { status: stopped }, drain: { status: "NOT_SELECTED" } }; result.pageStatus = "PARTIAL"; continue; }
      slot.fresh = { capacity: { status: "NOT_SELECTED" }, drain: { status: "NOT_SELECTED" } };
      const readSample = await bounded(() => { result.provenance.capacityCalls++; return rpc<HolderPort>(env.GATEWAY, slot.name).capacity(slot.epoch); }, request.signal, deadline, LIMITS.holderMs);
      stop(readSample);
      if (readSample.status !== "OK") { slot.fresh.capacity = readSample; result.pageStatus = "PARTIAL"; continue; }
      const value = sample(readSample.value);
      if (!value) slot.fresh.capacity = { status: "MALFORMED" };
      else if (bytes(value) > LIMITS.holderReplyBytes) slot.fresh.capacity = { status: "OVERSIZED" };
      else if (value.epoch !== slot.epoch) slot.fresh.capacity = { status: "EPOCH_MISMATCH" };
      else {
        const observed = observation(value, Date.now());
        slot.fresh.capacity = { status: "OK", value: observed };
        if (observed.state !== "FRESH") result.pageStatus = "PARTIAL";
      }
      if (slot.fresh.capacity.status !== "OK") { result.pageStatus = "PARTIAL"; continue; }
      if (stopped) { slot.fresh.drain = { status: stopped }; result.pageStatus = "PARTIAL"; continue; }
      const readDrain = await bounded(() => { result.provenance.drainStatusCalls++; return rpc<HolderPort>(env.GATEWAY, slot.name).drainStatus(); }, request.signal, deadline, LIMITS.holderMs);
      stop(readDrain);
      const valueDrain = readDrain.status === "OK" ? drain(readDrain.value) : undefined;
      slot.fresh.drain = readDrain.status !== "OK" ? readDrain : valueDrain ? { status: "OK", value: valueDrain } : { status: "MALFORMED" };
      if (slot.fresh.drain.status !== "OK") result.pageStatus = "PARTIAL";
    }
  };
  await Promise.all(Array.from({ length: LIMITS.concurrency }, worker));
  result.provenance.freshMayWakeHolders = result.provenance.metricsSnapshotMayAdvance = result.provenance.socketSampleMayBeRecorded = result.provenance.capacityCalls > 0;
  return json(result);
}
