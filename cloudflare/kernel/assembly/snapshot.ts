// Object snapshots: a consistent, checksummed copy of one SQLite-backed Durable Object (its tables and its KV) in R2,
// and the stepwise restore of such a snapshot into a FRESH object. Used by the nightly backups (backups.ts) and to
// move a stack's objects to another stack (for example into a jurisdiction), without the data leaving Cloudflare.
//
// Layout under `<prefix>` (no trailing slash):
//   manifest.json          written last: its presence marks a complete snapshot
//   kv.json.gz             every KV entry, values in a tagged JSON (see `encodeValue`)
//   part-000001.json.gz …  `{ pages: [{ table, columns, rows }] }` in P03 wire format, at most PART_BYTES of JSON each
// Every part's SHA-256 is in the manifest (and R2 verifies it on upload); every table's row count and content SHA-256
// (P03 RowHasher, the same digest `/__tenant-transfer` verifies) is in the manifest and checked after a restore.
//
// Consistency: the whole snapshot is taken inside `blockConcurrencyWhile`, so no other event of the object runs (and
// writes) between its first and last read. Restore refuses an object that holds any row, so it can never overwrite data.

import { TenantTransfer } from "../../tools/tenant-transfer/src/transport.mjs";
import { schemaPhases, readSchema } from "../../tools/tenant-transfer/src/sql.mjs";
import { idJurisdiction } from "./residency";

export const SNAPSHOT_FORMAT = "gonvex-object-snapshot";
/** Uncompressed JSON per part; one part is one restore step (one transaction) and stays far below 30 s of CPU. */
export const PART_BYTES = 4 * 1024 * 1024;
/** KV keys that only mean something on the stack (and sockets) that wrote them; a restore skips them. */
export const STACK_LOCAL_KV = ["gateway:", "control/watchers/", "session-recovery", "autoscale-"] as const;
/** The restore's own progress (never part of a snapshot). */
export const RESTORE_KEY = "gonvex/snapshot-restore";

type Checksum = { rows: number; sha256: string };
type SchemaRow = { type: string; name: string; tblName: string; sql: string | null; kind: string };

export interface SnapshotInfo { kind: string; name: string; source?: string }

export interface SnapshotManifest {
  v: 1;
  format: typeof SNAPSHOT_FORMAT;
  object: { kind: string; name: string; id: string; jurisdiction: string | null };
  source: string | null;
  takenAt: string;
  schema: SchemaRow[];
  sequences: Record<string, unknown>;
  tables: Record<string, Checksum>;
  kv: { key: string; entries: number; bytes: number; sha256: string };
  parts: Array<{ key: string; bytes: number; sha256: string; rows: number }>;
  totals: { tables: number; rows: number; storedBytes: number; databaseSize: number };
  ms: number;
}

// ------------------------------------------------------------------ values

const TAGS = ["$u", "$n", "$b", "$bytes", "$ab", "$d", "$map", "$set", "$o"] as const;
const toBase64 = (bytes: Uint8Array) => { let s = ""; for (let i = 0; i < bytes.length; i += 0x8000) s += String.fromCharCode(...bytes.subarray(i, i + 0x8000)); return btoa(s); };
const fromBase64 = (text: string) => Uint8Array.from(atob(text), (c) => c.charCodeAt(0));

/** Structured-clone value → JSON. Anything this cannot represent exactly throws (a snapshot never drops a value). */
export function encodeValue(value: unknown, path = "value"): unknown {
  if (value === undefined) return { $u: 1 };
  if (value === null || typeof value === "string" || typeof value === "boolean") return value;
  if (typeof value === "number") return Number.isFinite(value) && !Object.is(value, -0) ? value : { $n: Object.is(value, -0) ? "-0" : String(value) };
  if (typeof value === "bigint") return { $b: value.toString() };
  if (value instanceof Uint8Array && value.constructor === Uint8Array) return { $bytes: toBase64(value) };
  if (value instanceof ArrayBuffer) return { $ab: toBase64(new Uint8Array(value)) };
  if (value instanceof Date) return { $d: value.getTime() };
  if (value instanceof Map) return { $map: [...value].map(([k, v], i) => [encodeValue(k, `${path}<key ${i}>`), encodeValue(v, `${path}<${i}>`)]) };
  if (value instanceof Set) return { $set: [...value].map((v, i) => encodeValue(v, `${path}{${i}}`)) };
  if (Array.isArray(value)) return Array.from(value, (v, i) => encodeValue(v, `${path}[${i}]`));
  if (typeof value === "object") {
    const proto = Object.getPrototypeOf(value);
    if (proto !== Object.prototype && proto !== null) throw new Error(`${path}: ${proto?.constructor?.name ?? "object"} values are not supported`);
    const out: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(value)) out[k] = encodeValue(v, `${path}.${k}`);
    return Object.keys(out).some((k) => k.startsWith("$")) ? { $o: out } : out;
  }
  throw new Error(`${path}: ${typeof value} values are not supported`);
}

export function decodeValue(value: unknown): unknown {
  if (value === null || typeof value !== "object") return value;
  if (Array.isArray(value)) return value.map(decodeValue);
  const o = value as Record<string, unknown>;
  const keys = Object.keys(o);
  if (keys.length === 1 && (TAGS as readonly string[]).includes(keys[0]!)) {
    const v = o[keys[0]!];
    switch (keys[0]) {
      case "$u": return undefined;
      case "$n": return v === "-0" ? -0 : Number(v);
      case "$b": return BigInt(v as string);
      case "$bytes": return fromBase64(v as string);
      case "$ab": return fromBase64(v as string).buffer;
      case "$d": return new Date(v as number);
      case "$map": return new Map((v as [unknown, unknown][]).map(([k, x]) => [decodeValue(k), decodeValue(x)]));
      case "$set": return new Set((v as unknown[]).map(decodeValue));
      case "$o": return Object.fromEntries(Object.entries(v as Record<string, unknown>).map(([k, x]) => [k, decodeValue(x)]));
    }
  }
  return Object.fromEntries(keys.map((k) => [k, decodeValue(o[k])]));
}

// ------------------------------------------------------------------ bytes

async function sha256Hex(bytes: Uint8Array | ArrayBuffer): Promise<string> {
  return [...new Uint8Array(await crypto.subtle.digest("SHA-256", bytes))].map((b) => b.toString(16).padStart(2, "0")).join("");
}
async function gzip(text: string): Promise<Uint8Array> {
  return new Uint8Array(await new Response(new Blob([text]).stream().pipeThrough(new CompressionStream("gzip"))).arrayBuffer());
}
async function gunzip(bytes: ArrayBuffer): Promise<string> {
  return new Response(new Blob([bytes]).stream().pipeThrough(new DecompressionStream("gzip"))).text();
}
const manifestKey = (prefix: string) => `${prefix}/manifest.json`;
const kvKey = (prefix: string) => `${prefix}/kv.json.gz`;
const partKey = (prefix: string, n: number) => `${prefix}/part-${String(n).padStart(6, "0")}.json.gz`;

/** Reads `key`, checks its SHA-256 against the manifest's, and returns the decompressed JSON. */
async function readVerified<T>(bucket: R2Bucket, key: string, sha256: string): Promise<T> {
  const object = await bucket.get(key);
  if (!object) throw new Error(`snapshot object ${key} is missing`);
  const bytes = await object.arrayBuffer();
  const actual = await sha256Hex(bytes);
  if (actual !== sha256) throw new Error(`snapshot object ${key}: SHA-256 ${actual.slice(0, 12)}… differs from the manifest's ${sha256.slice(0, 12)}…`);
  return JSON.parse(await gunzip(bytes)) as T;
}

function stackLocal(key: string): boolean { return key === RESTORE_KEY || STACK_LOCAL_KV.some((prefix) => key.startsWith(prefix)); }

// ------------------------------------------------------------------ snapshot

/**
 * Writes a consistent snapshot of the object behind `ctx` to `bucket` under `prefix` and returns its manifest.
 * The object accepts no other event until the manifest is stored.
 */
export async function snapshotObject(ctx: DurableObjectState, bucket: R2Bucket, prefix: string, info: SnapshotInfo): Promise<SnapshotManifest> {
  if (!/^[A-Za-z0-9._\/-]+$/.test(prefix) || prefix.includes("..") || prefix.endsWith("/")) throw new Error("invalid snapshot prefix");
  if (await bucket.head(manifestKey(prefix))) throw new Error(`a snapshot already exists at ${prefix}`);
  return ctx.blockConcurrencyWhile(async () => {
    const started = Date.now();
    if (ctx.storage.kv.get(RESTORE_KEY) !== undefined) throw new Error("the object is being restored; no snapshot");
    const transfer = new TenantTransfer(ctx, { budgetMs: Number.MAX_SAFE_INTEGER });
    const catalog = transfer.op_catalog() as { schema: SchemaRow[]; plans: Array<{ name: string }>; sequences: Record<string, unknown> };
    const parts: SnapshotManifest["parts"] = [];
    let rows = 0;
    let storedBytes = 0;
    let queue = catalog.plans.map((p) => ({ table: p.name, after: null as unknown }));
    while (queue.length) {
      const { pages } = transfer.op_read({ tables: queue, maxBytes: PART_BYTES }) as { pages: Array<{ table: string; columns: string[]; rows: unknown[][]; next: unknown }> };
      if (!pages.length) throw new Error("snapshot read returned no pages");
      const last = pages.at(-1)!;
      queue = queue.slice(pages.length);
      if (last.next) queue.unshift({ table: last.table, after: last.next });
      const bytes = await gzip(JSON.stringify({ pages: pages.map(({ table, columns, rows }) => ({ table, columns, rows })) }));
      const sha256 = await sha256Hex(bytes);
      const key = partKey(prefix, parts.length + 1);
      await bucket.put(key, bytes, { sha256, httpMetadata: { contentType: "application/json", contentEncoding: "gzip" } });
      const n = pages.reduce((sum, page) => sum + page.rows.length, 0);
      parts.push({ key, bytes: bytes.byteLength, sha256, rows: n });
      rows += n;
      storedBytes += bytes.byteLength;
    }
    const tables = (await transfer.op_checksum({ tables: null, cursor: null }) as { checksums: Record<string, Checksum> }).checksums;
    if (Object.values(tables).reduce((n, t) => n + t.rows, 0) !== rows) throw new Error("snapshot row count differs from the checksum pass");
    const entries: Array<[string, unknown]> = [];
    for (const [key, value] of ctx.storage.kv.list()) if (key !== RESTORE_KEY) entries.push([key, encodeValue(value, `kv ${key}`)]);
    const kvText = JSON.stringify({ entries });
    const kvBytes = await gzip(kvText);
    const kvSha = await sha256Hex(kvBytes);
    await bucket.put(kvKey(prefix), kvBytes, { sha256: kvSha, httpMetadata: { contentType: "application/json", contentEncoding: "gzip" } });
    storedBytes += kvBytes.byteLength;
    const manifest: SnapshotManifest = {
      v: 1, format: SNAPSHOT_FORMAT,
      object: { kind: info.kind, name: info.name, id: ctx.id.toString(), jurisdiction: idJurisdiction(ctx.id) ?? null },
      source: info.source ?? null, takenAt: new Date(started).toISOString(),
      schema: catalog.schema, sequences: catalog.sequences, tables,
      kv: { key: kvKey(prefix), entries: entries.length, bytes: kvBytes.byteLength, sha256: kvSha },
      parts, totals: { tables: Object.keys(tables).length, rows, storedBytes, databaseSize: ctx.storage.sql.databaseSize }, ms: 0,
    };
    manifest.ms = Date.now() - started;
    await bucket.put(manifestKey(prefix), JSON.stringify(manifest), { httpMetadata: { contentType: "application/json" } });
    return manifest;
  });
}

/** The manifest of a complete snapshot (`null` when there is none at `prefix`). */
export async function readManifest(bucket: R2Bucket, prefix: string): Promise<SnapshotManifest | null> {
  const object = await bucket.get(manifestKey(prefix));
  if (!object) return null;
  const manifest = await object.json<SnapshotManifest>();
  if (manifest.format !== SNAPSHOT_FORMAT || manifest.v !== 1) throw new Error(`${prefix} is not a ${SNAPSHOT_FORMAT} v1`);
  return manifest;
}

// ------------------------------------------------------------------ restore

interface RestoreState {
  prefix: string;
  /** The in-memory restore session that wrote the last step: a restarted object begins again from the wipe. */
  session: string;
  phase: "schema" | "rows" | "finish" | "verify" | "done" | "failed";
  schemaAt: number;
  part: number;
  error?: string;
  report?: RestoreReport;
}

export interface RestoreReport {
  prefix: string;
  object: SnapshotManifest["object"];
  takenAt: string;
  tables: number;
  rows: number;
  kvEntries: number;
  kvSkipped: string[];
  mismatches: Array<{ table: string; expected: Checksum | null; actual: Checksum | null }>;
  quickCheck: string;
  foreignKeyViolations: number;
  fts: Record<string, string>;
}

export interface RestoreStep { phase: RestoreState["phase"]; part?: number; parts?: number; report?: RestoreReport; error?: string }

const sessions = new WeakMap<object, string>();
const sessionOf = (ctx: DurableObjectState) => { let s = sessions.get(ctx); if (!s) { s = crypto.randomUUID(); sessions.set(ctx, s); } return s; };

/** True when no ordinary table of the object holds a row (tables an object creates for itself at construction may exist). */
export function objectIsEmpty(ctx: DurableObjectState): boolean {
  const tables = ctx.storage.sql.exec<{ name: string }>("SELECT name FROM sqlite_schema WHERE type='table' AND sql NOT LIKE 'CREATE VIRTUAL%' AND name NOT LIKE '\\_cf\\_%' ESCAPE '\\' AND name NOT LIKE 'sqlite\\_%' ESCAPE '\\' AND name NOT LIKE '\\_\\_miniflare%' ESCAPE '\\'").toArray();
  return tables.every(({ name }) => ctx.storage.sql.exec<{ n: number }>(`SELECT EXISTS(SELECT 1 FROM "${name.replaceAll('"', '""')}") AS n`).one().n === 0);
}

/**
 * One step of restoring the snapshot at `prefix` into this object. Call it until it answers `done` (or `failed`), each
 * call in its own invocation, then reset the object (its in-memory state predates the restore). The first step refuses
 * an object that holds any row; a restore in progress (or failed) may be begun again with the same prefix.
 */
export async function restoreStep(ctx: DurableObjectState, bucket: R2Bucket, prefix: string): Promise<RestoreStep> {
  let state = ctx.storage.kv.get<RestoreState>(RESTORE_KEY);
  if (state && state.prefix !== prefix) return { phase: "failed", error: `a restore of ${state.prefix} is in progress here` };
  if (state?.phase === "done") return { phase: "done", report: state.report! };
  const manifest = await readManifest(bucket, prefix);
  if (!manifest) return { phase: "failed", error: `no complete snapshot at ${prefix}` };
  const session = sessionOf(ctx);
  if (!state || state.phase === "failed" || state.session !== session) {
    if (!state && !objectIsEmpty(ctx)) return { phase: "failed", error: "the object holds data; a restore only fills an empty object" };
    await ctx.storage.deleteAlarm();
    await ctx.storage.deleteAll();
    state = { prefix, session, phase: "schema", schemaAt: 0, part: 0 };
    ctx.storage.kv.put(RESTORE_KEY, state);
  }
  const save = () => ctx.storage.kv.put(RESTORE_KEY, state);
  const fail = (error: string): RestoreStep => { state!.phase = "failed"; state!.error = error; save(); return { phase: "failed", error }; };
  const transfer = new TenantTransfer(ctx);
  const { before, after, fts } = schemaPhases(manifest.schema);
  try {
    if (state.phase === "schema") {
      const statements = before.slice(state.schemaAt).map(({ name, sql }) => ({ name, sql: sql ?? "" }));
      const result = await transfer.op_schema({ statements }) as { failed: Array<{ name: string; error: string }>; consumed: number };
      if (result.failed.length) return fail(`schema statements refused: ${result.failed.map((f) => `${f.name}: ${f.error}`).join("; ").slice(0, 800)}`);
      state.schemaAt += result.consumed;
      if (state.schemaAt >= before.length) state.phase = "rows";
      save();
      return { phase: state.phase, part: 0, parts: manifest.parts.length };
    }
    if (state.phase === "rows") {
      const part = manifest.parts[state.part];
      if (part) {
        const { pages } = await readVerified<{ pages: Array<{ table: string; columns: string[]; rows: unknown[][] }> }>(bucket, part.key, part.sha256);
        const result = transfer.op_insert({ parts: pages }) as { rows: number };
        if (result.rows !== part.rows) return fail(`${part.key}: ${result.rows} rows inserted, manifest says ${part.rows}`);
        state.part++;
      }
      if (state.part >= manifest.parts.length) state.phase = "finish";
      save();
      return { phase: state.phase, part: state.part, parts: manifest.parts.length };
    }
    if (state.phase === "finish") {
      if (Object.keys(manifest.sequences).length) transfer.op_sequence({ sequences: manifest.sequences });
      const triggers = await new TenantTransfer(ctx, { budgetMs: Number.MAX_SAFE_INTEGER }).op_schema({ statements: [
        ...after.map(({ name, sql }) => ({ name, sql: sql ?? "" })),
        ...fts.map((name) => { const q = `"${name.replaceAll('"', '""')}"`; return { name: `${name} rebuild`, sql: `INSERT INTO ${q}(${q}) VALUES ('rebuild')` }; }),
      ] }) as { failed: Array<{ name: string; error: string }>; consumed: number };
      if (triggers.failed.length || triggers.consumed !== after.length + fts.length) return fail(`trigger/FTS statements: ${triggers.failed.map((f) => `${f.name}: ${f.error}`).join("; ").slice(0, 800) || "budget exceeded"}`);
      const { entries } = await readVerified<{ entries: Array<[string, unknown]> }>(bucket, manifest.kv.key, manifest.kv.sha256);
      if (entries.length !== manifest.kv.entries) return fail("KV entry count differs from the manifest");
      const skipped: string[] = [];
      for (const [key, value] of entries) {
        if (stackLocal(key)) { skipped.push(key); continue; }
        ctx.storage.kv.put(key, decodeValue(value));
      }
      for (const [key, value] of entries) {
        if (stackLocal(key)) continue;
        if (JSON.stringify(encodeValue(ctx.storage.kv.get(key))) !== JSON.stringify(value)) return fail(`KV ${key} does not read back as written`);
      }
      state.report = { prefix, object: manifest.object, takenAt: manifest.takenAt, tables: 0, rows: 0, kvEntries: entries.length - skipped.length, kvSkipped: skipped,
        mismatches: [], quickCheck: "", foreignKeyViolations: 0, fts: {} };
      state.phase = "verify";
      save();
      return { phase: state.phase };
    }
    if (state.phase === "verify") {
      transfer.op_pragma({ sql: "PRAGMA foreign_keys = ON" });
      const verifier = new TenantTransfer(ctx, { budgetMs: Number.MAX_SAFE_INTEGER });
      const actual = (await verifier.op_checksum({ tables: null, cursor: null }) as { checksums: Record<string, Checksum> }).checksums;
      const names = [...new Set([...Object.keys(manifest.tables), ...Object.keys(actual)])].sort();
      const report = state.report!;
      report.mismatches = names.filter((t) => actual[t]?.rows !== manifest.tables[t]?.rows || actual[t]?.sha256 !== manifest.tables[t]?.sha256)
        .map((table) => ({ table, expected: manifest.tables[table] ?? null, actual: actual[table] ?? null }));
      const checks = await verifier.op_check({ fts, cursor: null }) as { quickCheck: string; foreignKeyViolations: number; fts: Record<string, string> };
      report.tables = names.length;
      report.rows = Object.values(actual).reduce((n, t) => n + t.rows, 0);
      report.quickCheck = checks.quickCheck; report.foreignKeyViolations = checks.foreignKeyViolations; report.fts = checks.fts;
      // The live schema must be the snapshot's (shadow tables and SQLite's own objects aside).
      const fingerprint = (rows: SchemaRow[]) => rows.filter((o) => o.kind !== "shadow" && !o.name.startsWith("sqlite_")).map((o) => `${o.type} ${o.name} ${o.sql ?? ""}`).sort().join("\n");
      if (fingerprint(readSchema(ctx.storage.sql) as SchemaRow[]) !== fingerprint(manifest.schema)) return fail("restored schema differs from the snapshot's");
      if (report.mismatches.length) return fail(`table checksums differ: ${report.mismatches.map((m) => m.table).join(", ").slice(0, 800)}`);
      // Equal checksums mean equal rows, so foreign key violations (reported) are exactly the source's own.
      if (checks.quickCheck !== "ok" || Object.values(checks.fts).some((v) => v !== "ok")) {
        return fail(`integrity: quick_check ${checks.quickCheck.slice(0, 200)}, fts ${JSON.stringify(checks.fts).slice(0, 200)}`);
      }
      state.phase = "done";
      save();
      await ctx.storage.sync();
      return { phase: "done", report };
    }
    return { phase: state.phase, ...(state.error ? { error: state.error } : {}) };
  } catch (error) {
    return fail(String((error as Error)?.message ?? error).slice(0, 1000));
  }
}

/** After `done`: drops the restore marker, so the object starts as an ordinary restored object on its next wake. */
export async function finishRestore(ctx: DurableObjectState): Promise<void> {
  const state = ctx.storage.kv.get<RestoreState>(RESTORE_KEY);
  if (state?.phase !== "done") throw new Error("no completed restore to finish");
  ctx.storage.kv.delete(RESTORE_KEY);
  await ctx.storage.sync();
}
