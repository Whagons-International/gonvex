// #555 E1: the migrator runs plan statements as raw SQL, outside the storage session's REPLACE check. A
// migration's INSERT OR REPLACE deletes rows with no log row; before the fix nothing marked the commit, so the
// principal cache rolled forward and patched through it (a leaked grant), and the permission index patched by
// row_id. Now every migration that staged a change logs the global marker before its finalize. This drives the real
// TenantMigrator with the tenant object's capture hooks.

import { env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it, vi } from "vitest";
import { applyInstallUnit, CaptureState, ChangeWake, directSql, finalize, invokeCaptured, planChangeFeedInstall, type CaptureSql } from "../../capture/index.ts";
import { REPLACED_MARKER } from "../../capture/replace-marker.ts";
import { buildReleasePlan, MemoryPlanStore, migrationsFromFiles, TenantMigrator, type CaptureHooks, type ReleasePlan } from "../../migrate/index.ts";
import { PermissionBits as ReducerPermissionBits } from "../../reducer/permissions.ts";
import { TenantStorage, type StorageSession } from "../../storage";
import { readClock } from "../feed.ts";
import { parsePlan } from "../plan.ts";
import { resolveMemberSql, resolveWithParents, type VisibilitySession } from "../principal.ts";
import { VisibilityRegistry } from "../tenant.ts";

vi.setConfig({ testTimeout: 60_000 });

let objects = 0;

const V1 = {
  "migrations/0001_app.sql": `-- gonvex:scope tenant
CREATE TABLE claims ("_id" TEXT PRIMARY KEY, "memberId" TEXT, "spotId" TEXT UNIQUE);
CREATE TABLE grants ("_id" TEXT PRIMARY KEY, "memberId" TEXT, "permission" TEXT, "scope" TEXT, UNIQUE ("permission", "scope"));
CREATE TABLE spots ("_id" TEXT PRIMARY KEY);
`,
};
// The reviewer's E1 probe: the UNIQUE spotId conflict deletes m1's claim c1; the grant g2 takes g1's (admin, org).
const V2 = {
  ...V1,
  "migrations/0002_move.sql": `-- gonvex:scope tenant
INSERT OR REPLACE INTO claims VALUES ('c3', 'm3', 's4');
INSERT OR REPLACE INTO grants VALUES ('g2', 'm2', 'admin', 'org');
`,
};
// A migration that stages no change (DDL only) logs no marker.
const V3 = { ...V2, "migrations/0003_ddl.sql": "-- gonvex:scope tenant\nCREATE TABLE extra (id TEXT PRIMARY KEY);\n" };

const plan = parsePlan({
  version: 2, table: "spots", key: "_id", permissionTables: ["claims", "members"],
  sets: { claimedSpots: { kind: "grant", table: "claims", select: "spotId", joins: [], where: [{ table: "claims", column: "memberId", context: "member.id" }] } },
  where: { operator: "inSet", column: "_id", set: "claimedSpots" },
});

const sessionOf = (name: string): VisibilitySession => ({
  identity: { account: { id: `acct-${name}` } },
  route: { tenantId: "t" },
  member: { id: name, accountId: `acct-${name}`, status: "active", role: "member", permissions: {} },
  developerSession: null,
  anonymous: false,
});

describe("#555 E1: a migration's REPLACE marks its commit", () => {
  it("the principal cache and the Reducer's permission index rebuild over a migration's INSERT OR REPLACE", async () => {
    const [v1, v2, v3] = (await Promise.all([V1, V2, V3].map(async (files) => buildReleasePlan(await migrationsFromFiles(files))))) as [ReleasePlan, ReleasePlan, ReleasePlan];
    const stub = env.HELLO.getByName(`x17p-e1-${++objects}-${Math.random()}`);
    await runInDurableObject(stub, async (_instance, state) => {
      const capture = new CaptureState();
      const hooks: CaptureHooks = {
        directSql: (sql) => directSql(sql) as unknown as ReturnType<CaptureHooks["directSql"]>,
        finalize: (sql, provenance) => finalize(sql as CaptureSql, provenance, { state: capture }) as ReturnType<CaptureHooks["finalize"]>,
        planChangeFeedInstall: (sql) => planChangeFeedInstall(sql as CaptureSql) as ReturnType<CaptureHooks["planChangeFeedInstall"]>,
        applyInstallUnit: (sql, unit) => applyInstallUnit(sql as CaptureSql, unit as Parameters<typeof applyInstallUnit>[1]),
      };
      // The test object has no alarm handler: record alarms instead of setting them (as kernel/migrate's tests do).
      const storageProxy = new Proxy(state.storage, {
        get(target, key) {
          if (key === "setAlarm" || key === "deleteAlarm") return async () => undefined;
          if (key === "onNextSessionRestoreBookmark") return async (b: string) => b;
          const value = Reflect.get(target, key);
          return typeof value === "function" ? value.bind(target) : value;
        },
      });
      const storage = new TenantStorage(state.storage);
      const migrator = new TenantMigrator({ storage: storageProxy, plans: new MemoryPlanStore(v1, v2, v3), capture: hooks, abort: (reason) => { throw new Error(`abort: ${reason}`); }, schemaChanged: () => storage.invalidateSchema() });
      const release = async (id: string) => {
        await migrator.applyRelease(id);
        for (let i = 0; i < 500; i += 1) {
          const status = await migrator.alarm();
          if (status.state !== "running") {
            expect(status.state).toBe("done");
            return;
          }
        }
        throw new Error("migration did not finish");
      };
      await release(v1.id);

      const context = { state: capture, wake: new ChangeWake() };
      let commands = 0;
      const commit = async (statements: string[]) => {
        const result = await invokeCaptured(storage, "reducer", { commandId: `c${++commands}` }, async (session) => {
          for (const text of statements) session.run(text, []);
        }, context);
        if (!result.ok) throw result.error;
      };
      const read = async <T,>(body: (session: StorageSession) => T) => {
        const result = await storage.invoke("query", async (session) => body(session));
        if (!result.ok) throw result.error;
        return result.value;
      };
      await commit([
        ...["m1", "m2", "m3"].map((m) => `INSERT INTO members (id, account_id) VALUES ('${m}', 'acct-${m}')`),
        `INSERT INTO spots VALUES ('s4')`,
        `INSERT INTO claims VALUES ('c1', 'm1', 's4')`,
        `INSERT INTO grants VALUES ('g1', 'm1', 'admin', 'org')`,
      ]);

      const registry = new VisibilityRegistry();
      const sql = directSql(state.storage.sql);
      const bits = new ReducerPermissionBits({ table: "grants", member: "memberId", permission: "permission", scope: "scope" });
      const resolve = (session: StorageSession, member: string) => {
        const clock = readClock(session);
        const plans = new Map([["spots", plan]]);
        return resolveWithParents(session, sessionOf(member), plan, clock.revision, plans, registry.acl.ensure(session, plans, [plan]), undefined, { cache: registry.principals, clock });
      };
      const check = (session: StorageSession, label: string) => {
        for (const member of ["m1", "m2", "m3"]) {
          const cached = resolve(session, member);
          const fresh = resolveMemberSql(session, sessionOf(member), plan, readClock(session).revision);
          expect([...cached.sets.get("claimedSpots")!], `${label} ${member}`).toEqual([...fresh.sets.get("claimedSpots")!]);
          expect(cached.fingerprint, `${label} ${member} fingerprint`).toBe(fresh.fingerprint);
        }
      };
      await read((session) => {
        check(session, "before");
        expect([...resolve(session, "m1").sets.get("claimedSpots")!]).toEqual(["s4"]);
      });
      bits.ensure(sql);
      expect(bits.has("m1", "admin", "org")).toBe(true);

      await release(v2.id);
      // The migration's commit holds the replacing rows and one marker.
      const tables = state.storage.sql.exec<{ table_name: string }>("SELECT table_name FROM _gonvex_sync_changes WHERE revision = (SELECT revision FROM _gonvex_sync_clock) ORDER BY ordinal").toArray().map((row) => row.table_name);
      expect(tables).toEqual(["claims", "grants", REPLACED_MARKER]);
      await read((session) => {
        check(session, "after the migration");
        expect([...resolve(session, "m1").sets.get("claimedSpots")!], "m1 lost the claim").toEqual([]);
        expect([...resolve(session, "m3").sets.get("claimedSpots")!]).toEqual(["s4"]);
      });
      expect(registry.principals.stats().invalidated, "each cached member dropped, not patched").toBeGreaterThanOrEqual(1);
      bits.ensure(sql);
      expect(bits.has("m1", "admin", "org"), "m1 lost the grant").toBe(false);
      expect(bits.has("m2", "admin", "org")).toBe(true);
      expect(bits.stats.loads).toBe(2);

      // A DDL-only migration stages nothing: no commit, no marker.
      const markers = () => Number(state.storage.sql.exec<{ n: number }>("SELECT count(*) AS n FROM _gonvex_sync_changes WHERE table_name = ?", REPLACED_MARKER).one().n);
      await release(v3.id);
      expect(markers()).toBe(1);
    });
  });
});
