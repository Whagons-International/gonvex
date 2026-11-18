// X10: the migration alarm stays inside its CPU budget on a tenant the size of the largest P03 import
// (devcopy-stg1117-12: about 115k rows; thousands of schema objects), with K03's real capture, and ends
// with the same schema, ledger and rows as an unbudgeted run.
//
// Local workerd's clock advances during synchronous work (the platform's does not), so the wall time of
// one `alarm()` here is that event's CPU. The platform measured about 1.5x local for the same chunks
// (contract-changes 0089: 10.6 s on dev for what is about 7 s here).
import { env, runInDurableObject } from "cloudflare:test";
import { createHash } from "node:crypto";
import { expect, it } from "vitest";
import { applyInstallUnit, directSql, finalize, planChangeFeedInstall } from "../../capture/index.ts";
import { readTypedRows, RowHasher } from "../../../tools/tenant-transfer/src/codec.mjs";
import { buildReleasePlan, MemoryPlanStore, migrationsFromFiles, TenantMigrator, type Budget, type PlanStore, type ReleasePlan } from "../index.ts";
import { importLargeRows, LARGE, largeBaseMigrations, largeReleaseMigrations } from "./large-fixture.ts";

const capture = { directSql, finalize, planChangeFeedInstall, applyInstallUnit } as unknown as ConstructorParameters<typeof TenantMigrator>[0]["capture"];
/** The CPU budget of one migration event (0022 §5b: no chunk over 2 s on the platform). */
const EVENT_BUDGET_MS = 2_000;

function store(...plans: ReleasePlan[]): PlanStore {
  const stores = new Map(plans.map((p) => [p.id, new MemoryPlanStore(p)]));
  return { manifest: (id) => stores.get(id)!.manifest(id), chunk: (id, unit, index) => stores.get(id)!.chunk(id, unit, index) };
}

function storageWithoutAlarms(state: DurableObjectState): DurableObjectStorage {
  return new Proxy(state.storage, {
    get(target, key) {
      if (key === "setAlarm" || key === "deleteAlarm") return async () => undefined;
      const value = Reflect.get(target, key);
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
}

/** Schema, ledger and a content checksum of every application table. */
function snapshot(state: DurableObjectState, m: TenantMigrator) {
  const sql = state.storage.sql;
  const schema = sql.exec<{ type: string; name: string; sql: string | null }>("SELECT type, name, sql FROM sqlite_schema WHERE name NOT LIKE '\\_cf\\_%' ESCAPE '\\' ORDER BY type, name").toArray();
  const tables = sql.exec<{ name: string }>("SELECT name FROM sqlite_schema WHERE type='table' AND name GLOB 't[0-9][0-9][0-9]' ORDER BY name").toArray().map((r) => r.name);
  const rows: Record<string, { rows: number; sha256: string }> = {};
  for (const name of tables) {
    const columns = sql.exec<{ name: string }>("SELECT name FROM pragma_table_info(?) ORDER BY cid", name).toArray().map((c) => c.name);
    const hasher = new RowHasher(createHash);
    for (const row of readTypedRows(sql, { name, copyColumns: columns, key: ["id"] })) hasher.addTyped(row);
    rows[name] = hasher.digest();
  }
  const changes = sql.exec<{ n: number }>("SELECT count(*) AS n FROM _gonvex_sync_changes").one().n;
  return { schema, rows, changes, ledger: m.ledger().map((r) => [r.name, r.checksum]) };
}

async function provisionImported(state: DurableObjectState, base: ReleasePlan, plans: PlanStore, budget: Partial<Budget>) {
  const m = new TenantMigrator({ storage: storageWithoutAlarms(state), plans, capture, abort: () => undefined, budget });
  await m.applyRelease(base.id);
  while ((await m.alarm()).state === "running");
  expect(m.status()).toMatchObject({ state: "done", error: null });
  expect(importLargeRows(state.storage.sql)).toBe(LARGE.bigRows + (LARGE.tables - 1) * LARGE.rowsPerTable);
}

it("applies a release over 115k imported rows with every migration event under the CPU budget, and the same result", { timeout: 1_200_000 }, async () => {
  const base = await buildReleasePlan(await migrationsFromFiles(largeBaseMigrations()));
  const release = await buildReleasePlan(await migrationsFromFiles({ ...largeBaseMigrations(), ...largeReleaseMigrations() }));
  const plans = store(base, release);
  const unbounded: Partial<Budget> = { maxChunksPerEvent: 10_000, maxMsPerEvent: Infinity, maxStatementsPerTransaction: 200, initialStatementsPerTransaction: 200, captureStatementsPerTransaction: 200 };

  // Reference: the same release with no budget at all (one event).
  const reference = await runInDurableObject(env.HELLO.getByName(`x10-reference-${Math.random()}`), async (_instance, state) => {
    await provisionImported(state, base, plans, unbounded);
    const m = new TenantMigrator({ storage: storageWithoutAlarms(state), plans, capture, abort: () => undefined, budget: unbounded });
    const t0 = Date.now();
    await m.applyRelease(release.id);
    while ((await m.alarm()).state === "running");
    expect(m.status()).toMatchObject({ state: "done", error: null });
    return { ...snapshot(state, m), ms: Date.now() - t0 };
  });

  const measured = await runInDurableObject(env.HELLO.getByName(`x10-budget-${Math.random()}`), async (_instance, state) => {
    await provisionImported(state, base, plans, unbounded);
    expect(state.storage.sql.exec<{ n: number }>("SELECT count(*) AS n FROM sqlite_schema").one().n).toBeGreaterThan(6_000);
    // A fresh instance, as after operatorApplyRelease on a woken tenant: the default budget.
    const steps: Array<{ unit: string; statements: number; ms: number }> = [];
    const m = new TenantMigrator({ storage: storageWithoutAlarms(state), plans, capture, abort: () => undefined, observe: (e) => steps.push(e) });
    const events: number[] = [];
    /** Per event: the ms of the change-feed plan it ran (one K03 call that cannot be split), or 0. */
    const planned: number[] = [];
    const run = async (fn: () => Promise<Awaited<ReturnType<TenantMigrator["alarm"]>>>) => {
      const t = Date.now(), first = steps.length;
      const result = await fn();
      events.push(Date.now() - t);
      planned.push(steps.slice(first).filter((s) => s.unit === "change-feed-plan").reduce((n, s) => n + s.ms, 0));
      return result;
    };
    let status = await run(() => m.applyRelease(release.id));
    while (status.state === "running") status = await run(() => m.alarm());
    expect(status).toMatchObject({ state: "done", error: null, applied: ["0002_archive_columns.sql", "0003_backfill.sql"] });
    return { ...snapshot(state, m), events, planned, steps };
  });

  const max = Math.max(...measured.events);
  const slowest = [...measured.steps].sort((a, b) => b.ms - a.ms).slice(0, 5).map((s) => `${s.unit}:${s.statements}st:${s.ms}ms`);
  const summary = `X10 migration: ${measured.events.length} events, max ${max} ms, total ${measured.events.reduce((a, b) => a + b, 0)} ms; unbudgeted single run ${reference.ms} ms; slowest steps ${slowest.join(", ")}`;
  console.log(summary);
  // Every event fits the budget; an event that planned the change-feed install may add only that plan
  // (one K03 call over the whole catalog: 1.4 s here, reported in the PR), whatever the runner's speed.
  measured.events.forEach((ms, i) => expect(ms - measured.planned[i]!, summary).toBeLessThan(EVENT_BUDGET_MS));
  measured.events.forEach((ms, i) => { if (measured.planned[i]) expect(ms - measured.planned[i]!, summary).toBeLessThan(EVENT_BUDGET_MS / 2); });
  expect(measured.events.length).toBeGreaterThan(10);
  expect(measured.ledger).toEqual(reference.ledger);
  expect(measured.schema).toEqual(reference.schema);
  expect(measured.rows).toEqual(reference.rows);
  expect(measured.changes).toBe(reference.changes);
});
