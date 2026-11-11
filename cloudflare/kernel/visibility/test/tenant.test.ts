// Visibility against a real tenant object: principals resolved from
// permission tables with SQL (K01 kernel SQL), parent ACL indexes loaded by
// the registry and kept current from K03's change log, the principal cache's
// probes, and the permission bits. The corpus principals are rebuilt from
// rows, so the three forms are also checked on resolved (not hand-made)
// principals.

import { env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { CaptureState, ChangeWake, directSql, installChangeFeed, invokeCaptured } from "../../capture/index.ts";
import { TenantStorage, type StorageSession } from "../../storage";
import { memberVisibility, rowValue } from "../class.ts";
import { readChanges, readClock } from "../feed.ts";
import type { JsonValue } from "../json.ts";
import { rowMatches } from "../mask.ts";
import { parsePermissionGrants } from "../permission-bits.ts";
import type { VisibilityPlan } from "../plan.ts";
import { capturedTables } from "../principal-cache.ts";
import { principalChangeConcerns, resolveWithParents, type ResolutionMetrics, type VisibilitySession } from "../principal.ts";
import type { ResolvedVisibility } from "../resolved.ts";
import { compilePredicate } from "../sql.ts";
import { VisibilityRegistry } from "../tenant.ts";
import { createQueryVisibility, maskChangeRelevant } from "../query-port.ts";
import { planToJson } from "../plan.ts";
import { corpus, type Corpus, type Principal } from "./corpus.ts";

let objects = 0;

const SCHEMA = [
  `CREATE TABLE members (id TEXT PRIMARY KEY, account_id TEXT, status TEXT, role TEXT, permissions JSON, see_all_spots BOOLEAN)`,
  `CREATE TABLE "memberTeams" ("_id" TEXT PRIMARY KEY, "memberId" TEXT, "teamId" TEXT)`,
  `CREATE TABLE "workspaceTeams" ("_id" TEXT PRIMARY KEY, "teamId" TEXT, "workspaceId" TEXT)`,
  `CREATE TABLE "memberSpots" ("_id" TEXT PRIMARY KEY, "memberId" TEXT, "spotId" TEXT)`,
  `CREATE TABLE "workspaces" ("_id" TEXT PRIMARY KEY, "type" TEXT, "createdBy" TEXT)`,
  `CREATE TABLE "taskUsers" ("_id" TEXT PRIMARY KEY, "userId" TEXT, "taskId" TEXT)`,
  `CREATE TABLE "tasks" ("_id" TEXT PRIMARY KEY, "workspaceId" TEXT, "spotId" TEXT, "createdBy" TEXT, "createdVia" TEXT, "deletedAt" TEXT)`,
  `CREATE TABLE "taskNotes" ("_id" TEXT PRIMARY KEY, "taskId" TEXT, "authorId" TEXT)`,
  `CREATE TABLE "grants" ("_id" TEXT PRIMARY KEY, "memberId" TEXT, "permission" TEXT, "scope" TEXT, "deletedAt" INTEGER)`,
];

/** The first occurrence of each corpus principal, as rows of the permission tables. */
function fixtureStatements(data: Corpus): Array<[string, unknown[]]> {
  const statements: Array<[string, unknown[]]> = [];
  const seen = new Set<string>();
  let n = 0;
  const id = () => `row-${++n}`;
  for (const workspace of ["w1", "w2", "w3"]) statements.push([`INSERT INTO workspaces VALUES ($1, 'TEAM', NULL)`, [workspace]]);
  statements.push([`INSERT INTO workspaces VALUES ('p1', 'PERSONAL', 'alice')`, []], [`INSERT INTO workspaces VALUES ('p2', 'PERSONAL', 'bob')`, []]);
  for (const principal of data.principals) {
    if (seen.has(principal.name)) continue;
    seen.add(principal.name);
    const sets = principal.sets;
    statements.push([`INSERT INTO members VALUES ($1, $2, 'active', $3, $4, $5)`, [principal.name, `acct-${principal.name}`, principal.role, JSON.stringify(principal.permissions), sets.get("allSpots")!.size > 0]]);
    statements.push([`INSERT INTO "memberTeams" VALUES ($1, $2, $3)`, [id(), principal.name, `team-${principal.name}`]]);
    for (const workspace of sets.get("workspaces")!) statements.push([`INSERT INTO "workspaceTeams" VALUES ($1, $2, $3)`, [id(), `team-${principal.name}`, workspace]]);
    for (const spot of sets.get("spots")!) statements.push([`INSERT INTO "memberSpots" VALUES ($1, $2, $3)`, [id(), principal.name, spot]]);
    for (const task of sets.get("assigned")!) statements.push([`INSERT INTO "taskUsers" VALUES ($1, $2, $3)`, [id(), principal.name, task]]);
  }
  for (const row of data.tasks) {
    const r = row as Record<string, JsonValue>;
    statements.push([`INSERT INTO tasks VALUES ($1, $2, $3, $4, $5, $6)`, [r["_id"], r["workspaceId"], r["spotId"], r["createdBy"], r["createdVia"], r["deletedAt"]]]);
  }
  for (const row of data.notes) {
    const r = row as Record<string, JsonValue>;
    statements.push([`INSERT INTO "taskNotes" VALUES ($1, $2, $3)`, [r["_id"], r["taskId"], r["authorId"]]]);
  }
  return statements;
}

function sessionOf(name: string): VisibilitySession {
  return {
    identity: { account: { id: `acct-${name}` } },
    route: { tenantId: "t" },
    member: { id: name, accountId: `acct-${name}`, status: "active", role: "member", permissions: {} },
    developerSession: null,
    anonymous: false,
  };
}

interface Harness {
  state: DurableObjectState;
  storage: TenantStorage;
  registry: VisibilityRegistry;
  commit(statements: Array<[string, unknown[]]>): Promise<number>;
  read<T>(body: (session: StorageSession) => T): Promise<T>;
}

async function withTenant<T>(body: (harness: Harness) => Promise<T>): Promise<T> {
  const stub = env.HELLO.getByName(`k09-${++objects}-${Math.random()}`);
  return runInDurableObject(stub, async (_instance, state) => {
    const sql = directSql(state.storage.sql);
    state.storage.transactionSync(() => {
      installChangeFeed(sql);
      for (const statement of SCHEMA) sql.run(statement);
      installChangeFeed(sql);
    });
    const storage = new TenantStorage(state.storage);
    const context = { state: new CaptureState(), wake: new ChangeWake() };
    let commands = 0;
    const harness: Harness = {
      state,
      storage,
      registry: new VisibilityRegistry(),
      async commit(statements) {
        const result = await invokeCaptured(storage, "reducer", { commandId: `c${++commands}` }, async (session) => {
          for (const [text, parameters] of statements) session.run(text, parameters);
        }, context);
        if (!result.ok) throw result.error;
        return result.notice?.revision ?? 0;
      },
      async read(body) {
        const result = await storage.invoke("query", async (session) => body(session));
        if (!result.ok) throw result.error;
        return result.value;
      },
    };
    return body(harness);
  });
}

function sqlVisible(session: StorageSession, plan: VisibilityPlan, resolved: ResolvedVisibility): Set<string> {
  const parameters: JsonValue[] = [];
  const predicate = compilePredicate(plan, resolved, "r", parameters);
  return new Set(session.run(`SELECT r."_id" FROM "${plan.table}" AS r WHERE ${predicate}`, parameters).rows.map((row) => String(row[0])));
}

/** Every row of `table` as JSON objects (the change feed's images). */
function tableRows(session: StorageSession, table: string): JsonValue[] {
  const result = session.run(`SELECT * FROM "${table}"`);
  return result.rows.map((row) => Object.fromEntries(result.columnNames.map((name, index) => [name, row[index] as JsonValue])));
}

/** The module's plans as a manifest holds them: one per table (the tasks rule and, when given, one notes plan). */
function plansOf(data: Corpus, child?: VisibilityPlan): Map<string, VisibilityPlan> {
  const plans = new Map([["tasks", data.plans[0]!.plan]]);
  if (child && child.table !== "tasks") plans.set(child.table, child);
  return plans;
}

/** Resolves with the registry and the cache, then checks SQL = interpreter = compiled on every row. */
function checkAll(session: StorageSession, harness: Harness, data: Corpus, name: string, scope: string, metrics?: ResolutionMetrics): Map<string, ResolvedVisibility> {
  const clock = readClock(session);
  const resolutions = new Map<string, ResolvedVisibility>();
  for (const corpusPlan of data.plans) {
    const plan = corpusPlan.plan;
    const plans = plansOf(data, plan);
    const acl = harness.registry.acl.ensure(session, plans, [plan]);
    const resolved = resolveWithParents(session, sessionOf(name), plan, clock.revision, plans, acl, metrics, { cache: harness.registry.principals, clock });
    resolutions.set(corpusPlan.name, resolved);
    const expected = sqlVisible(session, plan, resolved);
    const rows = tableRows(session, plan.table);
    const handle = memberVisibility(plan, resolved, scope);
    if (handle.kind !== "compiled") throw new Error("unexpected fallback");
    for (const row of rows) {
      const id = (row as { _id: string })._id;
      const label = `${corpusPlan.name}/${name}/${id}`;
      expect(rowMatches(plan, resolved, row), `${label} interpreted`).toBe(expected.has(id));
      expect(handle.member.visible(rowValue(row)), `${label} compiled`).toBe(expected.has(id));
    }
    handle.member.release();
  }
  return resolutions;
}

const asArray = (set: ReadonlySet<string> | undefined) => [...(set ?? [])].sort();

describe("resolution from SQL (resolve_with_parents)", () => {
  it("rebuilds the corpus principals from permission rows; all three forms agree on them", async () => {
    const data = corpus();
    await withTenant(async (harness) => {
      await harness.commit(fixtureStatements(data));
      await harness.read((session) => {
        const seen = new Set<string>();
        for (const principal of data.principals) {
          if (seen.has(principal.name)) continue;
          seen.add(principal.name);
          const metrics = { visibilitySqlStatements: 0 };
          const resolutions = checkAll(session, harness, data, principal.name, "sql-principals", metrics);
          const tasks = resolutions.get("tasks")!;
          for (const set of ["workspaces", "spots", "personalWorkspaces", "ownedPersonal", "assigned"]) {
            // Sets a role prunes are never resolved (an admin's decision cannot depend on them).
            if (!tasks.sets.has(set)) continue;
            expect(asArray(tasks.sets.get(set)), `${principal.name}.${set}`).toEqual(asArray(principal.sets.get(set)));
          }
          if (tasks.sets.has("allSpots")) expect(tasks.sets.get("allSpots")!.size > 0, `${principal.name}.allSpots`).toBe(principal.sets.get("allSpots")!.size > 0);
          expect(tasks.direct).toEqual({ "account.id": `acct-${principal.name}`, "member.id": principal.name, "tenant.id": "t" });
          expect(tasks.fingerprint).toMatch(/^[0-9a-f]{64}$/);
        }
        const stats = harness.registry.principals.stats();
        // Plans sharing set definitions share one SQL resolution per account.
        expect(stats.hits).toBeGreaterThan(0);
        expect(stats.uncacheable).toBe(0);
      });
    });
  });

  it("an account without an active member is refused with Rust's text", async () => {
    const data = corpus();
    await withTenant(async (harness) => {
      await harness.read((session) => {
        expect(() => resolveWithParents(session, sessionOf("nobody"), data.plans[0]!.plan, 0, plansOf(data), harness.registry.acl.ensure(session, plansOf(data), [data.plans[0]!.plan]))).toThrow(
          "active tenant member is required for visibility",
        );
      });
    });
  });

  it("a verified developer without a member row gets the ephemeral principal; SQL uses it instead of the members table", async () => {
    const data = corpus();
    await withTenant(async (harness) => {
      await harness.commit(fixtureStatements(data));
      await harness.read((session) => {
        const developer: VisibilitySession = {
          identity: { account: { id: "dev-account" } },
          route: { tenantId: "t" },
          member: { id: "developer:dev-account", accountId: "dev-account", status: "active", role: "system_admin", permissions: {} },
          developerSession: { id: "s" },
          anonymous: false,
        };
        const plan = data.plans[2]!.plan; // roles: admin or tasks.viewAll, not guest
        const resolved = resolveWithParents(session, developer, plan, 0, plansOf(data), harness.registry.acl.ensure(session, plansOf(data), [plan]));
        expect(resolved.ephemeralMember).toBe(true);
        expect(harness.registry.principals.key(developer, plan), "developer sessions are never shared").toBeUndefined();
        const expected = sqlVisible(session, plan, resolved);
        expect(expected.size).toBe(0);
        const admin = { ...developer, member: { ...developer.member, role: "admin" } };
        const adminResolved = resolveWithParents(session, admin, plan, 0, plansOf(data), harness.registry.acl.ensure(session, plansOf(data), [plan]));
        expect(sqlVisible(session, plan, adminResolved).size).toBe(data.tasks.length);
      });
    });
  });
});

describe("feed maintenance", () => {
  it("indexes catch up from the change log; the principal cache probes and invalidates", async () => {
    const data = corpus();
    await withTenant(async (harness) => {
      await harness.commit(fixtureStatements(data));
      const notes = data.plans[6]!.plan; // noteInherits
      const plans = plansOf(data, notes);
      // First resolution: SQL; cached for later snapshots.
      await harness.read((session) => checkAll(session, harness, data, "alice", "feed"));
      const afterFirst = harness.registry.principals.stats();
      const held = await harness.read((session) => harness.registry.acl.ensure(session, plans, [notes]).get("tasks", "_id"));
      const loadsBefore = harness.registry.acl.stats().loads;
      // A data write: a task alice can see moves to w2 (hidden from her); the parent index follows.
      const revision = await harness.commit([[`UPDATE tasks SET "workspaceId" = 'w2' WHERE "_id" = 't-w1-null-alice-web-0'`, []]]);
      await harness.read((session) => {
        const index = harness.registry.acl.ensure(session, plans, [notes]).get("tasks", "_id");
        expect(index, "the same index, caught up").toBe(held);
        expect(index.revision).toBe(revision);
        expect((index.get("t-w1-null-alice-web-0") as Record<string, JsonValue>)["workspaceId"]).toBe("w2");
      });
      expect(harness.registry.acl.stats().loads, "indexes were caught up, not reloaded").toBe(loadsBefore);
      const aliceTasks = await harness.read((session) => {
        const changes = readChanges(session, readClock(session).epoch, ["tasks"], revision - 1, revision);
        expect(changes.length).toBe(1);
        const change = changes[0]!.changes[0]!;
        const resolutions = checkAll(session, harness, data, "alice", "feed");
        // A data write is never a principal change.
        expect(principalChangeConcerns(data.plans[0]!.plan, resolutions.get("tasks")!, change.table, change.operation, change.changedColumns, change.oldValue, change.newValue)).toBe(false);
        // ...but it is relevant: alice saw the old row.
        expect(maskChangeRelevant(data.plans[0]!.plan, resolutions.get("tasks")!, change)).toBe(true);
        expect(maskChangeRelevant(notes, resolutions.get("noteInherits")!, change), "a parent change reaches the children's subscriptions").toBe(true);
        return resolutions.get("tasks")!;
      });
      const afterData = harness.registry.principals.stats();
      expect(afterData.probedHits, "a data write keeps the cached principal (probe found no input change)").toBeGreaterThan(afterFirst.probedHits);
      expect(afterData.invalidated).toBe(afterFirst.invalidated);
      expect(aliceTasks.inherited.size).toBe(0);
      // A permission write naming alice: alice joins w3 through her team.
      const joined = await harness.commit([[`INSERT INTO "workspaceTeams" VALUES ('new-link', 'team-alice', 'w3')`, []]]);
      await harness.read((session) => {
        const change = readChanges(session, readClock(session).epoch, ["workspaceTeams"], joined - 1, joined)[0]!.changes[0]!;
        // workspaceTeams is reached through memberTeams: unanchored, so conservative (Rust's rule).
        expect(principalChangeConcerns(data.plans[0]!.plan, aliceTasks, change.table, change.operation, change.changedColumns, change.oldValue, change.newValue)).toBe(true);
        const resolutions = checkAll(session, harness, data, "alice", "feed");
        expect(asArray(resolutions.get("tasks")!.sets.get("workspaces"))).toEqual(["w1", "w3"]);
      });
      expect(harness.registry.principals.stats().invalidated).toBeGreaterThan(afterData.invalidated);
      // An anchored write naming bob is not alice's principal.
      const bobs = await harness.commit([[`INSERT INTO "memberSpots" VALUES ('bob-spot', 'bob', 's2')`, []]]);
      await harness.read((session) => {
        const change = readChanges(session, readClock(session).epoch, ["memberSpots"], bobs - 1, bobs)[0]!.changes[0]!;
        expect(principalChangeConcerns(data.plans[0]!.plan, aliceTasks, change.table, change.operation, change.changedColumns, change.oldValue, change.newValue)).toBe(false);
      });
      // A deleted parent removes inherited access at once; the pushed transaction and the log agree.
      const deleted = await harness.commit([[`DELETE FROM tasks WHERE "_id" = 't-w1-null-alice-finding-0'`, []]]);
      await harness.read((session) => {
        const index = harness.registry.acl.ensure(session, plans, [notes]).get("tasks", "_id");
        expect(index.revision).toBe(deleted);
        expect(index.get("t-w1-null-alice-finding-0")).toBeUndefined();
        checkAll(session, harness, data, "alice", "feed");
      });
    });
  });

  it("the push path applies transactions in order and poisons on a gap or another epoch", async () => {
    const data = corpus();
    await withTenant(async (harness) => {
      await harness.commit(fixtureStatements(data));
      const notes = data.plans[6]!.plan;
      const plans = plansOf(data, notes);
      const index = await harness.read((session) => harness.registry.acl.ensure(session, plans, [notes]).get("tasks", "_id"));
      const revision = await harness.commit([[`UPDATE tasks SET "spotId" = 's9' WHERE "_id" = 't-w1-null-alice-web-0'`, []]]);
      const transactions = await harness.read((session) => readChanges(session, readClock(session).epoch, ["tasks"], revision - 1, revision));
      harness.registry.apply(transactions[0]!, revision - 1);
      expect(index.revision).toBe(revision);
      expect((index.get("t-w1-null-alice-web-0") as Record<string, JsonValue>)["spotId"]).toBe("s9");
      harness.registry.apply({ ...transactions[0]!, revision: revision + 5 }, revision + 4);
      expect(index.poisoned, "a gap certifies nothing").toBe(true);
      const reloaded = await harness.read((session) => harness.registry.acl.ensure(session, plans, [notes]).get("tasks", "_id"));
      expect(reloaded).not.toBe(index);
      harness.registry.apply({ epoch: "another", revision: revision + 1, changes: [] });
      expect(reloaded.poisoned, "another epoch").toBe(true);
    });
  });
});

describe("capture classes for the principal cache", () => {
  it("entity tables count; record storage and owned tables count only through their record", async () => {
    await withTenant(async (harness) => {
      const triggers = await harness.read((session) => session.run("SELECT name, sql FROM sqlite_schema WHERE type = 'trigger' AND name LIKE '\\_gonvex\\_sync\\_%' ESCAPE '\\'").rows.map((row) => [String(row[0]), String(row[1])] as const));
      const tables = capturedTables(triggers, []);
      for (const table of ["members", "memberTeams", "tasks", "grants"]) expect(tables.has(table), table).toBe(true);
      // Record staging triggers (owned / record storage) write _gonvex_record_pending instead.
      const staged = (table: string) => ["insert", "update", "delete"].map((op) => [`_gonvex_sync_${table}_${op}`, `CREATE TRIGGER x BEFORE INSERT ON ${table} BEGIN INSERT INTO _gonvex_record_pending(table_name,row_id,old_value) SELECT 'tasks', NEW._id, NULL; END`] as const);
      expect(capturedTables([...staged("taskStorage")], [["tasksView", "taskStorage"]])).toEqual(new Set(["tasksView"]));
      expect(capturedTables([...staged("taskStorage").slice(0, 2)], [["tasksView", "taskStorage"]]), "a missing trigger leaves it uncaptured").toEqual(new Set());
    });
  });
});

describe("permission bits from the manifest (permissionGrants)", () => {
  it("bits equal SQL over the projection through commits, soft deletes, moves and a rollback", async () => {
    await withTenant(async (harness) => {
      harness.registry.setPermissionGrants(parsePermissionGrants({ table: "grants", member: "memberId", permission: "permission", scope: "scope", whereNull: ["deletedAt"] }));
      const bits = harness.registry.permissions!;
      expect(bits.has("m1", "edit", "w1"), "not loaded yet").toBe("rebuilding");
      const sqlGrants = (session: StorageSession) =>
        new Set(session.run(`SELECT "memberId", "permission", COALESCE("scope", '') FROM grants WHERE "deletedAt" IS NULL AND "memberId" IS NOT NULL AND "permission" IS NOT NULL`).rows.map((row) => row.join("|")));
      const members = ["m1", "m2", "m3"];
      const permissions = ["edit", "view", "lead"];
      const scopes = ["", "w1", "w2", "nowhere"];
      let checked = 0;
      const check = () =>
        harness.read((session) => {
          const index = bits.sync(session);
          expect(index.revision).toBe(readClock(session).revision);
          const expected = sqlGrants(session);
          for (const member of members) for (const permission of permissions) for (const scope of scopes) {
            expect(bits.has(member, permission, scope), `${member} ${permission}@${scope}`).toBe(expected.has(`${member}|${permission}|${scope}`));
            checked += 1;
          }
        });
      await harness.commit([[`INSERT INTO grants VALUES ('g1', 'm1', 'edit', 'w1', NULL)`, []], [`INSERT INTO grants VALUES ('g2', 'm2', 'view', NULL, NULL)`, []]]);
      await check();
      const steps = [
        `INSERT INTO grants VALUES ('p', 'm1', 'lead', 'w2', NULL)`,
        `UPDATE grants SET "deletedAt" = 1 WHERE "_id" = 'p'`,
        `UPDATE grants SET "deletedAt" = NULL, "permission" = 'edit' WHERE "_id" = 'p'`,
        `INSERT INTO grants VALUES ('dup', 'm1', 'edit', 'w1', NULL)`,
        `DELETE FROM grants WHERE "_id" = 'g1'`,
        `UPDATE grants SET "scope" = 'nowhere' WHERE "_id" = 'p'`,
        `UPDATE grants SET "memberId" = 'm3' WHERE "_id" = 'p'`,
        `DELETE FROM grants WHERE "_id" = 'p'`,
      ];
      for (const step of steps) {
        await harness.commit([[step, []]]);
        await check();
      }
      expect(bits.counters.loads, "loaded once, then caught up").toBe(1);
      // A rolled-back grant never becomes a bit.
      const failed = await harness.storage.invoke("reducer", async (session) => {
        session.run(`INSERT INTO grants VALUES ('phantom', 'm1', 'view', 'w1', NULL)`);
        throw new Error("rolled back");
      });
      expect(failed.ok).toBe(false);
      await check();
      expect(bits.has("m1", "view", "w1")).toBe(false);
      expect(checked).toBeGreaterThan(300);
    });
  });
});

describe("the query engine's port (K08 kernel/query/visibility.ts)", () => {
  it("resolves manifest JSON plans, compiles SQL, routes changes, and counts the resolution's rows", async () => {
    const data = corpus();
    await withTenant(async (harness) => {
      await harness.commit(fixtureStatements(data));
      const port = createQueryVisibility(harness.registry);
      // The manifest as K08 holds it: plain JSON.
      const raw = new Map<string, unknown>([["tasks", planToJson(data.plans[0]!.plan)], ["taskNotes", planToJson(data.plans[6]!.plan)]]);
      const counted = { statements: 0, rowsRead: 0 };
      await harness.read((session) => {
        const db = {
          run(text: string, parameters?: readonly unknown[]) {
            const result = session.run(text, parameters);
            counted.statements += 1;
            counted.rowsRead += result.rowsRead;
            return result;
          },
          catalogOf: (table: string) => session.catalogOf(table),
        };
        const context = { db, session: sessionOf("alice"), plans: raw, revision: readClock(session).revision };
        const notesPlan = raw.get("taskNotes");
        const resolved = port.resolve(context, notesPlan);
        expect(resolved.direct["member.id"]).toBe("alice");
        const first = { ...counted };
        // Another open of the tasks plan in the same snapshot: the principal cache answers.
        const tasksPlan = raw.get("tasks");
        port.resolve(context, tasksPlan);
        const before = { ...counted };
        port.resolve(context, tasksPlan);
        const second = { statements: counted.statements - before.statements, rowsRead: counted.rowsRead - before.rowsRead };
        expect(second.statements, "only the clock read").toBe(1);
        expect(harness.registry.principals.stats().hits).toBeGreaterThan(0);
        const parameters: unknown[] = [];
        const predicate = port.compilePredicate(notesPlan, resolved, "r", parameters);
        const visible = session.run(`SELECT r."_id" FROM "taskNotes" AS r WHERE ${predicate}`, parameters).rows.map((row) => String(row[0])).sort();
        const interpreted = tableRows(session, "taskNotes").filter((row) => rowMatches(data.plans[6]!.plan, resolved as ResolvedVisibility, row)).map((row) => String((row as { _id: string })._id)).sort();
        expect(visible).toEqual(interpreted);
        expect(visible.length).toBeGreaterThan(0);
        expect([...port.dependencyTables(notesPlan, raw)].sort()).toEqual(["memberSpots", "memberTeams", "members", "taskUsers", "tasks", "workspaceTeams", "workspaces"]);
        const unrelated = { table: "grants", rowId: "g", operation: "insert", changedColumns: [], oldValue: null, newValue: { _id: "g", memberId: "alice" } };
        expect(port.changeConcerns(notesPlan, resolved, unrelated)).toBe(false);
        const parentChange = { table: "tasks", rowId: "t-w1-null-alice-web-0", operation: "update", changedColumns: ["workspaceId"], oldValue: { _id: "t-w1-null-alice-web-0", workspaceId: "w1", spotId: null, createdBy: "alice", createdVia: "web", deletedAt: null }, newValue: { _id: "t-w1-null-alice-web-0", workspaceId: "w2", spotId: null, createdBy: "alice", createdVia: "web", deletedAt: null } };
        expect(port.changeConcerns(notesPlan, resolved, parentChange)).toBe(true);
        // Errors carry Rust's Display text.
        expect(() => port.resolve({ ...context, session: sessionOf("nobody") }, notesPlan)).toThrow("active tenant member is required for visibility");
        expect(() => port.resolve(context, { version: 2, table: "x", key: "_id", permissionTables: [], where: { operator: "nope" } })).toThrow('invalid visibility plan: unsupported operator "nope"');
        // Numbers for the PR: the first resolution of a task child (member row, 6 sets, the parent index load).
        expect(first.statements).toBeLessThan(25);
        (globalThis as { __k09Numbers?: unknown }).__k09Numbers = { first, second, rows: data.tasks.length };
      });
    });
  });
});
