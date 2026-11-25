#!/usr/bin/env node
// Staging-to-Serverless Tenant Migration CLI Orchestrator (Phase 2c).
// Wraps the CPU-budgeted P03 transfer client (tools/tenant-transfer/src/transfer.mjs) with:
//   - Ring-based wave planning (`plan`) inspecting local SQLite DB files before touching remote DOs
//   - Idempotent, resumable, fail-closed wave execution (`run`):
//       pending -> wiping -> seeding -> checking -> verifying -> completing -> flipping -> complete
//   - Automatic checksum & integrity verification gate (routing is NEVER flipped on mismatch)
//   - Single-key instant rollback (`rollback`) back to `previousBackend` ("legacy")
//   - Auditable status summary (`status`) persisted in a JSON state file.
//
// Usage:
//   node scripts/migrate-staging.mjs plan --manifest staging-tenants.json [--ring 1]
//   node scripts/migrate-staging.mjs run --manifest staging-tenants.json --state state.json [--ring 1] [--concurrency 5] [--dry-run]
//   node scripts/migrate-staging.mjs rollback --state state.json [--tenant <id> | --ring <n>]
//   node scripts/migrate-staging.mjs status --state state.json

import { existsSync, mkdirSync, readFileSync, statSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { openFile } from "../tools/tenant-transfer/src/file.mjs";
import { readPlans } from "../tools/tenant-transfer/src/sql.mjs";
import {
  LIMITS,
  ObjectClient,
  fileChecksums,
  objectChecks,
  pushFile,
} from "../tools/tenant-transfer/src/transfer.mjs";

export const DEFAULT_CONCURRENCY = 5;

/**
 * Inspects a local SQLite tenant database file (read-only) to build a migration plan entry.
 */
export function inspectTenantFile(tenant) {
  const absPath = resolve(tenant.dbPath);
  if (!existsSync(absPath)) {
    return {
      tenantId: tenant.tenantId,
      ring: tenant.ring ?? 1,
      dbPath: absPath,
      ok: false,
      error: `database file not found: ${absPath}`,
    };
  }
  const bytes = statSync(absPath).size;
  const file = openFile(absPath, { readOnly: true });
  try {
    const plans = readPlans(file.sql);
    let totalRows = 0;
    for (const plan of plans) {
      const q = `"${plan.name.replaceAll('"', '""')}"`;
      const row = file.sql.exec(`SELECT count(*) AS n FROM ${q}`).raw().next().value;
      totalRows += Number(row?.[0] ?? 0);
    }
    const quick = file.sql.exec("PRAGMA quick_check").raw().next().value?.[0];
    return {
      tenantId: tenant.tenantId,
      cfTenantName: tenant.cfTenantName ?? tenant.tenantId,
      ring: tenant.ring ?? 1,
      dbPath: absPath,
      bytes,
      tables: plans.length,
      totalRows,
      quickCheck: String(quick ?? "unknown"),
      ok: quick === "ok",
      ...(quick === "ok" ? {} : { error: `PRAGMA quick_check failed: ${quick}` }),
    };
  } finally {
    file.close();
  }
}

/**
 * Builds a wave migration plan for all manifest entries matching optional `ring` / `tenantId`.
 */
export function planMigration(manifest, { ring = null, tenantId = null } = {}) {
  const selected = filterTenants(manifest.tenants ?? [], { ring, tenantId });
  const entries = selected.map((t) => inspectTenantFile(t));
  let totalBytes = 0;
  let totalRows = 0;
  let ready = 0;
  for (const e of entries) {
    if (e.ok) {
      ready++;
      totalBytes += e.bytes;
      totalRows += e.totalRows;
    }
  }
  return {
    transferUrl: manifest.transferUrl,
    routingUrl: manifest.routingUrl ?? null,
    selected: entries.length,
    ready,
    blocked: entries.length - ready,
    totalBytes,
    totalRows,
    entries,
  };
}

export function filterTenants(tenants, { ring = null, tenantId = null } = {}) {
  return tenants.filter((t) => {
    if (tenantId !== null && t.tenantId !== tenantId) return false;
    if (ring !== null && (t.ring ?? 1) !== ring) return false;
    return true;
  });
}

export function loadState(statePath) {
  if (!statePath || !existsSync(statePath)) return { v: 1, updatedAt: null, tenants: {} };
  return JSON.parse(readFileSync(statePath, "utf8"));
}

export function saveState(statePath, state) {
  if (!statePath) return;
  state.updatedAt = new Date().toISOString();
  mkdirSync(dirname(resolve(statePath)), { recursive: true });
  writeFileSync(statePath, JSON.stringify(state, null, 2) + "\n");
}

/** Default HTTP routing control-plane client for atomic cutover & instant rollback. */
export function createHttpRouter({ routingUrl, token }) {
  return {
    async setBackend({ tenantId, backend, cfTenantName, wireMode = "dual", txLogMode = "triggers" }) {
      if (!routingUrl) return { skipped: true, tenantId, backend };
      const res = await fetch(`${routingUrl.replace(/\/$/, "")}/tenants/${encodeURIComponent(tenantId)}/routing`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${token}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({ tenantId, backend, cfTenantName, wireMode, txLogMode }),
      });
      const text = await res.text();
      if (!res.ok) throw new Error(`routing flip failed (HTTP ${res.status}): ${text.slice(0, 400)}`);
      return text ? JSON.parse(text) : { ok: true, tenantId, backend };
    },
  };
}

/**
 * Verifies every source table's checksum against the tenant DO via P03 `/verify` in <=60-table pages.
 */
export async function verifyAllChecksums(client, checksums) {
  const names = Object.keys(checksums).sort();
  let verifiedTables = 0;
  for (let i = 0; i < names.length; i += LIMITS.checksumTables) {
    const pageNames = names.slice(i, i + LIMITS.checksumTables);
    const page = {};
    for (const name of pageNames) page[name] = checksums[name];
    for (;;) {
      const res = await client.call("verify", { checksums: page });
      if (res.pending) continue;
      verifiedTables += Number(res.verified ?? pageNames.length);
      break;
    }
  }
  return { verifiedTables, totalTables: names.length };
}

/**
 * Migrates a single tenant end-to-end with fail-closed integrity + checksum verification
 * before flipping the routing pointer.
 */
export async function migrateOneTenant(
  tenant,
  {
    transferUrl,
    transferToken,
    router,
    state,
    statePath = null,
    dryRun = false,
    log = () => {},
    clientFactory = (opts) => new ObjectClient(opts),
  },
) {
  const id = tenant.tenantId;
  const cfTenantName = tenant.cfTenantName ?? id;
  const existing = state.tenants[id];
  if (existing?.phase === "complete") {
    log(`[${id}] already complete; skipping`);
    return existing;
  }

  const record = {
    tenantId: id,
    cfTenantName,
    ring: tenant.ring ?? 1,
    dbPath: resolve(tenant.dbPath),
    previousBackend: tenant.previousBackend ?? "legacy",
    backend: existing?.backend ?? (tenant.previousBackend ?? "legacy"),
    wireMode: tenant.wireMode ?? "dual",
    txLogMode: tenant.txLogMode ?? "triggers",
    phase: "pending",
    startedAt: new Date().toISOString(),
    completedAt: null,
    rows: 0,
    tables: 0,
    error: null,
  };
  const setPhase = (phase, extra = {}) => {
    record.phase = phase;
    Object.assign(record, extra);
    state.tenants[id] = record;
    saveState(statePath, state);
  };

  try {
    const pre = inspectTenantFile(tenant);
    if (!pre.ok) throw new Error(pre.error);
    record.rows = pre.totalRows;
    record.tables = pre.tables;

    if (dryRun) {
      setPhase("dry_run_ok");
      log(`[${id}] dry-run ok (${pre.tables} tables, ${pre.totalRows} rows)`);
      return record;
    }

    const client = clientFactory({ url: transferUrl, token: transferToken, tenant: cfTenantName, log: (m) => log(`[${id}] ${m}`) });

    setPhase("wiping");
    await client.call("wipe");

    setPhase("seeding");
    const pushed = await pushFile(client, record.dbPath, { log: (m) => log(`[${id}] ${m}`) });
    if (pushed.schemaFailures?.length) {
      throw new Error(`schema had ${pushed.schemaFailures.length} refused statement(s)`);
    }

    setPhase("checking");
    const check = await objectChecks(client, pushed.fts ?? []);
    if (check.quickCheck !== "ok" || Number(check.foreignKeyViolations ?? 0) !== 0) {
      throw new Error(`integrity check failed: quickCheck=${check.quickCheck} fkViolations=${check.foreignKeyViolations}`);
    }
    for (const [ftsName, ftsStatus] of Object.entries(check.fts ?? {})) {
      if (ftsStatus !== "ok") throw new Error(`FTS integrity check failed on ${ftsName}: ${ftsStatus}`);
    }

    setPhase("verifying");
    const expected = fileChecksums(record.dbPath);
    await verifyAllChecksums(client, expected);

    setPhase("completing");
    await client.call("complete");

    setPhase("flipping");
    await router.setBackend({
      tenantId: id,
      backend: "cf-serverless",
      cfTenantName,
      wireMode: record.wireMode,
      txLogMode: record.txLogMode,
    });

    setPhase("complete", {
      backend: "cf-serverless",
      completedAt: new Date().toISOString(),
      rows: pushed.rows ?? pre.totalRows,
      requests: client.requests,
    });
    log(`[${id}] COMPLETE (${record.rows} rows, ${record.tables} tables -> cf-serverless)`);
    return record;
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    setPhase("failed", { error: message });
    log(`[${id}] FAILED at ${record.phase}: ${message}`);
    return record;
  }
}

/**
 * Runs a bounded-concurrency migration wave. Halts scheduling new tenants if any tenant fails
 * (`failFast = true`, default) so staging waves pause automatically on parity/integrity issues.
 */
export async function runMigrationWave(
  manifest,
  {
    ring = null,
    tenantId = null,
    concurrency = DEFAULT_CONCURRENCY,
    statePath = null,
    transferToken = process.env.GONVEX_DEV_TRANSFER_TOKEN ?? "",
    routingToken = process.env.GONVEX_ROUTING_TOKEN ?? transferToken,
    router = null,
    dryRun = false,
    failFast = true,
    log = console.log,
    clientFactory,
  } = {},
) {
  const state = loadState(statePath);
  const selected = filterTenants(manifest.tenants ?? [], { ring, tenantId });
  const effectiveRouter = router ?? createHttpRouter({ routingUrl: manifest.routingUrl, token: routingToken });
  const results = [];
  let cursor = 0;
  let halted = false;

  const worker = async () => {
    for (;;) {
      if (halted) return;
      const idx = cursor++;
      if (idx >= selected.length) return;
      const tenant = selected[idx];
      const rec = await migrateOneTenant(tenant, {
        transferUrl: manifest.transferUrl,
        transferToken,
        router: effectiveRouter,
        state,
        statePath,
        dryRun,
        log,
        ...(clientFactory ? { clientFactory } : {}),
      });
      results.push(rec);
      if (failFast && rec.phase === "failed") halted = true;
    }
  };

  const workers = Array.from({ length: Math.min(Math.max(1, concurrency), Math.max(1, selected.length)) }, () => worker());
  await Promise.all(workers);
  const failed = results.filter((r) => r.phase === "failed").length;
  return { total: selected.length, processed: results.length, failed, halted, results, state };
}

/**
 * Rolls back one tenant or an entire ring to its stored `previousBackend` ("legacy").
 */
export async function rollbackTenants(
  {
    statePath,
    routingUrl = null,
    routingToken = process.env.GONVEX_ROUTING_TOKEN ?? "",
    router = null,
    tenantId = null,
    ring = null,
    log = console.log,
  },
) {
  const state = loadState(statePath);
  const effectiveRouter = router ?? createHttpRouter({ routingUrl, token: routingToken });
  const targets = Object.values(state.tenants).filter((t) => {
    if (tenantId !== null && t.tenantId !== tenantId) return false;
    if (ring !== null && t.ring !== ring) return false;
    return true;
  });

  const rolledBack = [];
  for (const rec of targets) {
    const targetBackend = rec.previousBackend ?? "legacy";
    await effectiveRouter.setBackend({
      tenantId: rec.tenantId,
      backend: targetBackend,
      cfTenantName: rec.cfTenantName,
      wireMode: "json",
      txLogMode: "triggers",
    });
    rec.backend = targetBackend;
    rec.phase = "rolled_back";
    rec.rolledBackAt = new Date().toISOString();
    rolledBack.push(rec);
    log(`[${rec.tenantId}] ROLLED BACK -> ${targetBackend}`);
  }
  saveState(statePath, state);
  return { count: rolledBack.length, rolledBack, state };
}

function parseFlags(argv) {
  const flags = {};
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (!a.startsWith("--")) continue;
    const key = a.slice(2);
    const next = argv[i + 1];
    if (next === undefined || next.startsWith("--")) {
      flags[key] = true;
    } else {
      flags[key] = next;
      i++;
    }
  }
  return flags;
}

async function main() {
  const [cmd, ...rest] = process.argv.slice(2);
  const flags = parseFlags(rest);
  const ring = flags.ring !== undefined ? Number(flags.ring) : null;
  const tenantId = flags.tenant ?? null;

  if (cmd === "plan") {
    const manifest = JSON.parse(readFileSync(flags.manifest, "utf8"));
    const plan = planMigration(manifest, { ring, tenantId });
    console.log(JSON.stringify(plan, null, 2));
    if (plan.blocked > 0) process.exit(1);
    return;
  }

  if (cmd === "run") {
    const manifest = JSON.parse(readFileSync(flags.manifest, "utf8"));
    const res = await runMigrationWave(manifest, {
      ring,
      tenantId,
      concurrency: flags.concurrency ? Number(flags.concurrency) : DEFAULT_CONCURRENCY,
      statePath: flags.state ?? null,
      dryRun: Boolean(flags["dry-run"]),
      failFast: !flags["no-fail-fast"],
    });
    console.log(`wave finished: ${res.processed}/${res.total} processed, ${res.failed} failed${res.halted ? " (halted on failure)" : ""}`);
    if (res.failed > 0) process.exit(1);
    return;
  }

  if (cmd === "rollback") {
    const res = await rollbackTenants({
      statePath: flags.state,
      routingUrl: flags["routing-url"] ?? null,
      tenantId,
      ring,
    });
    console.log(`rolled back ${res.count} tenant(s)`);
    return;
  }

  if (cmd === "status") {
    const state = loadState(flags.state);
    for (const rec of Object.values(state.tenants)) {
      console.log(`${rec.tenantId.padEnd(28)} ring=${rec.ring} phase=${String(rec.phase).padEnd(12)} backend=${rec.backend} rows=${rec.rows}${rec.error ? ` err=${rec.error}` : ""}`);
    }
    return;
  }

  console.error("Usage: node scripts/migrate-staging.mjs <plan|run|rollback|status> [--manifest <file>] [--state <file>] [--ring <n>] [--tenant <id>]");
  process.exit(2);
}

if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) {
  main().catch((err) => {
    console.error(err);
    process.exit(1);
  });
}
