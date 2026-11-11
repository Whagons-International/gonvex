// The catalog cache is K01's (kernel/storage/catalog.ts, ruling 0022 §1).
// K04's part: it is used across invocations, `schemaChanged()` and a new
// generation invalidate it, and DDL that rolls back leaves nothing stale.
import { env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import type { CallReport, TenantCall } from "../src/index.js";
import { TestTenant, fixtureArtifact } from "./worker.js";

let tenants = 0;
function tenant() {
  const stub = env.TENANT.getByName(`catalog-${++tenants}`);
  const within = <T>(run: (instance: TestTenant, state: DurableObjectState) => T | Promise<T>) =>
    runInDurableObject(stub, (instance: TestTenant, state) => run(instance, state));
  const call = (fn: string, kind: TenantCall["kind"], args: unknown): Promise<CallReport> =>
    within((instance) => instance.host.run({ function: fn, kind, args, identity: { tenant: { id: "t" }, account: { id: "a" } }, invocation: { commandId: `c-${Math.random()}` }, now: 1 }));
  return { call, within };
}

describe("catalog cache (K01's, used by the host)", () => {
  it("reads no catalog rows once warm", async () => {
    const t = tenant();
    const first = await t.call("notes.recordAdd", "reducer", { label: "a" });
    const second = await t.call("notes.recordAdd", "reducer", { label: "b" });
    expect(first.ok && second.ok).toBe(true);
    expect(first.stats.catalogStatements).toBeGreaterThan(0);
    expect(second.stats.catalogStatements).toBe(0);
    expect(second.stats.catalogRowsRead).toBe(0);
  });

  it("sees a table created by a committed Reducer", async () => {
    const t = tenant();
    const missing = await t.call("notes.insertInto", "reducer", { table: "extra1", row: { _id: "x" } });
    expect(missing.error?.message).toContain("no such table: extra1");
    expect((await t.call("notes.ddl", "reducer", { table: "extra1" })).ok).toBe(true);
    const inserted = await t.call("notes.insertInto", "reducer", { table: "extra1", row: { _id: "y", label: "z" } });
    expect(JSON.parse(String(inserted.value))).toEqual({ _id: "y", label: "z" });
  });

  it("forgets catalogs read inside a Reducer whose DDL rolled back", async () => {
    const t = tenant();
    expect((await t.call("notes.ddl", "reducer", { table: "extra2", fail: true })).ok).toBe(false);
    const after = await t.call("notes.insertInto", "reducer", { table: "extra2", row: { _id: "x" } });
    expect(after.error?.message).toContain("no such table: extra2");
  });

  it("drops every catalog on schemaChanged() and on a new generation", async () => {
    const t = tenant();
    await t.call("notes.add", "reducer", { id: "a", body: "a" });
    expect((await t.call("notes.add", "reducer", { id: "b", body: "b" })).stats.catalogStatements).toBe(0);
    // Kernel DDL outside the host (a migration): K02 calls schemaChanged().
    await t.within((instance, state) => {
      state.storage.sql.exec("ALTER TABLE notes ADD COLUMN extra JSON");
      instance.host.schemaChanged();
    });
    const migrated = await t.call("notes.add", "reducer", { id: "c", body: "c" });
    expect(migrated.stats.catalogStatements).toBeGreaterThan(0);
    expect(JSON.parse(String(migrated.value))).toHaveProperty("extra", null);
    expect((await t.call("notes.add", "reducer", { id: "d", body: "d" })).stats.catalogStatements).toBe(0);
    await t.within((instance) => { instance.registry.install(fixtureArtifact(2)); });
    expect((await t.call("notes.add", "reducer", { id: "e", body: "e" })).stats.catalogStatements).toBeGreaterThan(0);
  });
});
