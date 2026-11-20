// X17p: the principal cache rolls an entry forward through commits instead of
// resolving the member again. The leak test drives random writes to every
// input of a plan shaped like Whagons' spots.list (grant sets on the written
// table with joins, a join hanging off the grant row, a set joining its own
// base table, a derived set) and checks, after every batch of commits, that
// the cached resolution is the fresh SQL resolution: same sets, same
// fingerprint, same visible rows.

import { env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { CaptureState, ChangeWake, directSql, installChangeFeed, invokeCaptured } from "../../capture/index.ts";
import { TenantStorage, type StorageSession } from "../../storage";
import { readClock } from "../feed.ts";
import type { JsonValue } from "../json.ts";
import { parsePlan, type VisibilityPlan } from "../plan.ts";
import { entityKeyColumn } from "../principal-cache.ts";
import { resolveMemberSql, resolveWithParents, type VisibilitySession } from "../principal.ts";
import type { ResolvedVisibility } from "../resolved.ts";
import { compilePredicate } from "../sql.ts";
import { VisibilityRegistry } from "../tenant.ts";

let objects = 0;

const SCHEMA = [
  `CREATE TABLE members (id TEXT PRIMARY KEY, account_id TEXT, status TEXT, role TEXT, permissions JSON)`,
  `CREATE TABLE "memberTeams" ("_id" TEXT PRIMARY KEY, "memberId" TEXT, "teamId" TEXT, "deletedAt" TEXT)`,
  `CREATE TABLE "workspaceTeams" ("_id" TEXT PRIMARY KEY, "teamId" TEXT, "workspaceId" TEXT, "deletedAt" TEXT)`,
  `CREATE TABLE "workspaces" ("_id" TEXT PRIMARY KEY, "type" TEXT, "deletedAt" TEXT)`,
  `CREATE TABLE "spots" ("_id" TEXT PRIMARY KEY, "parentId" TEXT)`,
  `CREATE TABLE "memberSpots" ("_id" TEXT PRIMARY KEY, "memberId" TEXT, "spotId" TEXT, "deletedAt" TEXT)`,
  // An integer key: the patch compares the typed key from the insert image.
  `CREATE TABLE "tasks" ("_id" INTEGER PRIMARY KEY, "spotId" TEXT, "workspaceId" TEXT, "createdBy" TEXT, "reportedByTeamId" TEXT, "deletedAt" TEXT)`,
  `CREATE TABLE "taskShares" ("_id" TEXT PRIMARY KEY, "taskId" INTEGER, "sharedToTeamId" TEXT, "revokedAt" TEXT, "deletedAt" TEXT)`,
];

const live = (table: string) => ({ table, column: "deletedAt", value: { literal: null } });
const me = (table: string) => ({ table, column: "memberId", context: "member.id" });

/** The shapes of spots.list's sets (module.json), on a reduced schema. */
function spotsPlan(): VisibilityPlan {
  return parsePlan({
    version: 2,
    table: "spots",
    key: "_id",
    permissionTables: ["memberTeams", "workspaceTeams", "workspaces", "spots", "memberSpots", "members"],
    sets: {
      memberSpots: { kind: "principal", table: "memberSpots", select: "spotId", joins: [], where: [me("memberSpots"), live("memberSpots")] },
      memberSpotDescendants: {
        kind: "principal", table: "spots", alias: "spot1", select: "_id",
        joins: [{ table: "spots", alias: "spot2", leftColumn: "parentId", rightColumn: "_id" }, { table: "memberSpots", leftColumn: "parentId", rightColumn: "spotId" }],
        where: [me("memberSpots"), live("memberSpots")],
      },
      createdTaskSpots: { kind: "grant", table: "tasks", select: "spotId", joins: [], where: [{ table: "tasks", column: "createdBy", context: "member.id" }, live("tasks")] },
      reportedTeamTaskSpots: {
        kind: "grant", table: "tasks", select: "spotId",
        joins: [{ table: "memberTeams", leftColumn: "reportedByTeamId", rightColumn: "teamId" }],
        where: [me("memberTeams"), live("tasks"), live("memberTeams")],
      },
      // A later join hangs off the grant row (leftAlias): no join token set.
      personalTeamTaskSpots: {
        kind: "grant", table: "tasks", select: "spotId",
        joins: [
          { table: "workspaces", leftColumn: "workspaceId", rightColumn: "_id" },
          { table: "workspaceTeams", leftAlias: "tasks", leftColumn: "workspaceId", rightColumn: "workspaceId" },
          { table: "memberTeams", leftColumn: "teamId", rightColumn: "teamId" },
        ],
        where: [live("tasks"), { table: "workspaces", column: "type", value: { literal: "PERSONAL" } }, live("workspaces"), live("workspaceTeams"), live("memberTeams"), me("memberTeams")],
      },
      teamWorkspaceTaskSpots: {
        kind: "grant", table: "tasks", select: "spotId",
        joins: [{ table: "workspaceTeams", leftColumn: "workspaceId", rightColumn: "workspaceId" }, { table: "memberTeams", leftColumn: "teamId", rightColumn: "teamId" }],
        where: [me("memberTeams"), live("memberTeams"), live("workspaceTeams"), live("tasks")],
      },
      sharedTeamTaskSpotsInput: {
        kind: "grant", table: "taskShares", select: "taskId",
        joins: [{ table: "memberTeams", leftColumn: "sharedToTeamId", rightColumn: "teamId" }],
        where: [me("memberTeams"), { table: "taskShares", column: "revokedAt", value: { literal: null } }, live("taskShares"), live("memberTeams")],
      },
      sharedTeamTaskSpots: { kind: "derived", from: "sharedTeamTaskSpotsInput", table: "tasks", key: "_id", select: "spotId", where: [{ column: "deletedAt", value: { literal: null } }] },
    },
    where: {
      operator: "or",
      children: ["memberSpots", "memberSpotDescendants", "createdTaskSpots", "reportedTeamTaskSpots", "personalTeamTaskSpots", "teamWorkspaceTaskSpots", "sharedTeamTaskSpots"].map((set) => ({ operator: "inSet", column: "_id", set })),
    },
  });
}

const MEMBERS = ["m1", "m2", "m3"];

function sessionOf(name: string): VisibilitySession {
  return {
    identity: { account: { id: `acct-${name}` } },
    route: { tenantId: "t" },
    member: { id: name, accountId: `acct-${name}`, status: "active", role: "member", permissions: {} },
    developerSession: null,
    anonymous: false,
  };
}

/** Deterministic PRNG (mulberry32). */
function random(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

interface Harness {
  registry: VisibilityRegistry;
  commit(statements: Array<[string, unknown[]]>): Promise<number>;
  read<T>(body: (session: StorageSession) => T): Promise<T>;
}

async function withTenant<T>(body: (harness: Harness) => Promise<T>): Promise<T> {
  const stub = env.HELLO.getByName(`x17p-${++objects}-${Math.random()}`);
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
    return body({
      registry: new VisibilityRegistry(),
      async commit(statements) {
        const result = await invokeCaptured(storage, "reducer", { commandId: `c${++commands}` }, async (session) => {
          for (const [text, parameters] of statements) session.run(text, parameters);
        }, context);
        if (!result.ok) throw result.error;
        return result.notice?.revision ?? 0;
      },
      async read(read) {
        const result = await storage.invoke("query", async (session) => read(session));
        if (!result.ok) throw result.error;
        return result.value;
      },
    });
  });
}

function visibleRows(session: StorageSession, plan: VisibilityPlan, resolved: ResolvedVisibility): string[] {
  const parameters: JsonValue[] = [];
  const predicate = compilePredicate(plan, resolved, "r", parameters);
  return session.run(`SELECT r."_id" FROM "${plan.table}" AS r WHERE ${predicate} ORDER BY 1`, parameters).rows.map((row) => String(row[0]));
}

function setsOf(resolved: ResolvedVisibility): Record<string, string[]> {
  return Object.fromEntries([...resolved.sets].map(([name, tokens]) => [name, [...tokens]]).sort(([a], [b]) => String(a).localeCompare(String(b))));
}

/** The cached resolution of every member against a fresh, uncached SQL resolution at the same snapshot. */
function checkMembers(session: StorageSession, harness: Harness, plan: VisibilityPlan, label: string): void {
  const clock = readClock(session);
  const plans = new Map([["spots", plan]]);
  for (const member of MEMBERS) {
    const acl = harness.registry.acl.ensure(session, plans, [plan]);
    const cached = resolveWithParents(session, sessionOf(member), plan, clock.revision, plans, acl, undefined, { cache: harness.registry.principals, clock });
    const fresh = resolveMemberSql(session, sessionOf(member), plan, clock.revision);
    const where = `${label} ${member} @${clock.revision}`;
    // Sets kept in insertion order by the roll-forward must still iterate sorted, as SQL resolution builds them.
    expect(setsOf(cached), `${where} sets`).toEqual(setsOf(fresh));
    for (const [name, tokens] of cached.sets) expect([...tokens], `${where} ${name} order`).toEqual([...fresh.sets.get(name)!]);
    expect(cached.fingerprint, `${where} fingerprint`).toBe(fresh.fingerprint);
    expect(visibleRows(session, plan, cached), `${where} rows`).toEqual(visibleRows(session, plan, fresh));
  }
}

function seed(): Array<[string, unknown[]]> {
  const statements: Array<[string, unknown[]]> = [];
  for (const member of MEMBERS) {
    statements.push([`INSERT INTO members VALUES ($1, $2, 'active', 'member', '{}')`, [member, `acct-${member}`]]);
    statements.push([`INSERT INTO "memberTeams" VALUES ($1, $2, $3, NULL)`, [`mt-${member}`, member, `team-${member}`]]);
  }
  for (const [id, type] of [["w1", "TEAM"], ["w2", "TEAM"], ["p1", "PERSONAL"], ["p2", "PERSONAL"]] as const) statements.push([`INSERT INTO workspaces VALUES ($1, $2, NULL)`, [id, type]]);
  for (let spot = 1; spot <= 8; spot += 1) statements.push([`INSERT INTO spots VALUES ($1, $2)`, [`s${spot}`, spot > 2 ? `s${Math.ceil(spot / 3)}` : null]]);
  statements.push([`INSERT INTO "workspaceTeams" VALUES ('wt-1', 'team-m1', 'w1', NULL)`, []], [`INSERT INTO "workspaceTeams" VALUES ('wt-2', 'team-m2', 'p1', NULL)`, []]);
  statements.push([`INSERT INTO "memberSpots" VALUES ('ms-1', 'm3', 's1', NULL)`, []]);
  return statements;
}

/** One random write to an input of the plan. Mostly task inserts, as in the create workload. */
function write(next: () => number, ids: { task: number; other: number }): [string, unknown[]] {
  const pick = <T,>(values: readonly T[]): T => values[Math.floor(next() * values.length)]!;
  const spot = () => `s${1 + Math.floor(next() * 8)}`;
  // Spots s9..s14 come and go; parents may name one before it exists (a set joining its own base table).
  const anySpot = () => `s${1 + Math.floor(next() * 14)}`;
  const looseSpot = () => `s${9 + Math.floor(next() * 6)}`;
  const workspace = () => pick(["w1", "w2", "p1", "p2", "w9"]);
  const team = () => pick(["team-m1", "team-m2", "team-m3", "team-x"]);
  const deleted = () => (next() < 0.15 ? "2026-11-20" : null);
  const task = () => 1 + Math.floor(next() * Math.max(ids.task, 1));
  const roll = next();
  if (roll < 0.45) {
    ids.task += 1;
    return [`INSERT INTO tasks VALUES ($1, $2, $3, $4, $5, $6)`, [ids.task, spot(), workspace(), pick([...MEMBERS, "mx"]), pick([team(), null]), deleted()]];
  }
  if (roll < 0.55) return [`UPDATE tasks SET "${pick(["spotId", "workspaceId", "createdBy", "reportedByTeamId", "deletedAt"])}" = $2 WHERE "_id" = $1`, [task(), pick([spot(), workspace(), pick(MEMBERS), team(), null])]];
  if (roll < 0.6) return [`DELETE FROM tasks WHERE "_id" = $1`, [task()]];
  if (roll < 0.66) return [`INSERT INTO "taskShares" VALUES ($1, $2, $3, NULL, $4)`, [`ts-${++ids.other}`, task(), team(), deleted()]];
  if (roll < 0.69) return [`UPDATE "taskShares" SET "revokedAt" = '2026-11-20' WHERE "_id" = (SELECT "_id" FROM "taskShares" ORDER BY random() LIMIT 1)`, []];
  if (roll < 0.74) return [`INSERT INTO "workspaceTeams" VALUES ($1, $2, $3, $4)`, [`wt-x${++ids.other}`, team(), workspace(), deleted()]];
  if (roll < 0.77) return [`UPDATE "workspaceTeams" SET "deletedAt" = '2026-11-20' WHERE "_id" = (SELECT "_id" FROM "workspaceTeams" ORDER BY random() LIMIT 1)`, []];
  if (roll < 0.81) return [`INSERT INTO "memberTeams" VALUES ($1, $2, $3, $4)`, [`mt-x${++ids.other}`, pick(MEMBERS), team(), deleted()]];
  if (roll < 0.84) return [`DELETE FROM "memberTeams" WHERE "_id" = (SELECT "_id" FROM "memberTeams" ORDER BY random() LIMIT 1)`, []];
  if (roll < 0.88) return [`INSERT INTO "memberSpots" VALUES ($1, $2, $3, $4)`, [`ms-x${++ids.other}`, pick(MEMBERS), spot(), deleted()]];
  if (roll < 0.9) return [`INSERT OR IGNORE INTO spots VALUES ($1, $2)`, [looseSpot(), anySpot()]];
  if (roll < 0.92) return [`DELETE FROM spots WHERE "_id" = $1`, [looseSpot()]];
  if (roll < 0.94) return [`UPDATE spots SET "parentId" = $2 WHERE "_id" = $1`, [anySpot(), anySpot()]];
  if (roll < 0.97) return [`UPDATE workspaces SET "type" = $2 WHERE "_id" = $1`, [workspace(), pick(["TEAM", "PERSONAL"])]];
  // A member row change: the entry is dropped, never rolled.
  return [`UPDATE members SET permissions = $2 WHERE id = $1`, [pick(MEMBERS), JSON.stringify({ n: ids.other++ })]];
}

describe("principal cache roll-forward (X17p)", () => {
  it("rolled-forward resolutions equal fresh SQL after random writes to every input (leak test)", async () => {
    const plan = spotsPlan();
    for (const seedValue of [1, 2, 3]) {
      await withTenant(async (harness) => {
        await harness.commit(seed());
        await harness.read((session) => checkMembers(session, harness, plan, `seed ${seedValue} start`));
        const next = random(seedValue);
        const ids = { task: 0, other: 0 };
        for (let round = 0; round < 60; round += 1) {
          // 1 to 3 commits between reads, each of 1 to 3 writes: the roll-forward spans several commits.
          const commits = 1 + Math.floor(next() * 3);
          for (let commit = 0; commit < commits; commit += 1) {
            const statements = Array.from({ length: 1 + Math.floor(next() * 3) }, () => write(next, ids));
            await harness.commit(statements);
          }
          await harness.read((session) => checkMembers(session, harness, plan, `seed ${seedValue} round ${round}`));
        }
        const stats = harness.registry.principals.stats();
        expect(stats.rolledForward, JSON.stringify(stats)).toBeGreaterThan(20);
        expect(stats.setsPatched, JSON.stringify(stats)).toBeGreaterThan(0);
        expect(stats.setsRerun, JSON.stringify(stats)).toBeGreaterThan(0);
        expect(stats.invalidated, "member row changes still drop the entry").toBeGreaterThan(0);
      });
    }
  }, 120_000);

  it("a task insert re-runs no set: the grant sets on tasks gain the new row's spot with one point query each", async () => {
    const plan = spotsPlan();
    await withTenant(async (harness) => {
      await harness.commit(seed());
      await harness.commit([[`INSERT INTO "workspaceTeams" VALUES ('wt-m3', 'team-m3', 'w2', NULL)`, []]]);
      await harness.read((session) => checkMembers(session, harness, plan, "before"));
      const before = harness.registry.principals.stats();
      // m1 creates a task in its team's workspace w1, reported by m3's team.
      await harness.commit([[`INSERT INTO tasks VALUES (100, 's7', 'w1', 'm1', 'team-m3', NULL)`, []]]);
      let statements = 0;
      await harness.read((session) => {
        const counting = { run: (text: string, parameters?: unknown[]) => { statements += 1; return session.run(text, parameters as never); } } as unknown as StorageSession;
        const clock = readClock(session);
        const plans = new Map([["spots", plan]]);
        const acl = harness.registry.acl.ensure(session, plans, [plan]);
        const m1 = resolveWithParents(counting, sessionOf("m1"), plan, clock.revision, plans, acl, undefined, { cache: harness.registry.principals, clock });
        expect([...m1.sets.get("createdTaskSpots")!]).toEqual(["s7"]);
        expect([...m1.sets.get("teamWorkspaceTaskSpots")!]).toEqual(["s7"]);
        checkMembers(session, harness, plan, "after insert");
        const m3 = resolveWithParents(session, sessionOf("m3"), plan, clock.revision, plans, acl, undefined, { cache: harness.registry.principals, clock });
        expect([...m3.sets.get("reportedTeamTaskSpots")!]).toEqual(["s7"]);
      });
      const after = harness.registry.principals.stats();
      expect(after.invalidated).toBe(before.invalidated);
      expect(after.rolledForward - before.rolledForward).toBe(3);
      expect(after.setsRerun).toBe(before.setsRerun);
      // Per member: four grant sets on tasks patched.
      expect(after.setsPatched - before.setsPatched).toBe(12);
      // m1's roll-forward, the registry's first: the change summary, the capture triggers' key columns (read once
      // per schema), four point queries, and the derived set (empty source: no SQL).
      expect(statements).toBe(6);
    });
  });

  it("an insert into a table a set joins to itself re-runs it: an existing row can gain the new row as its parent", async () => {
    const plan = spotsPlan();
    await withTenant(async (harness) => {
      await harness.commit(seed());
      // s13's parent s12 does not exist yet; m3 holds s1.
      await harness.commit([[`INSERT INTO spots VALUES ('s13', 's12')`, []]]);
      await harness.read((session) => checkMembers(session, harness, plan, "before"));
      const before = harness.registry.principals.stats();
      await harness.commit([[`INSERT INTO spots VALUES ('s12', 's1')`, []]]);
      await harness.read((session) => {
        checkMembers(session, harness, plan, "after");
        const clock = readClock(session);
        const plans = new Map([["spots", plan]]);
        const m3 = resolveWithParents(session, sessionOf("m3"), plan, clock.revision, plans, harness.registry.acl.ensure(session, plans, [plan]), undefined, { cache: harness.registry.principals, clock });
        expect(m3.sets.get("memberSpotDescendants")!.has("s13")).toBe(true);
      });
      const after = harness.registry.principals.stats();
      expect(after.rolledForward).toBeGreaterThan(before.rolledForward);
      expect(after.setsRerun - before.setsRerun, "memberSpotDescendants, once per member").toBe(3);
    });
  });

  it("reads the key column of the kernel's entity capture triggers only", () => {
    expect(entityKeyColumn(`CREATE TRIGGER "_gonvex_sync_tasks_insert" AFTER INSERT ON "tasks" BEGIN\n  INSERT INTO _gonvex_sync_changes (table_name, row_id, operation, new_value)\n  VALUES ('tasks', CAST(NEW."_id" AS TEXT), 'insert', json_array(NULL, json_object('_id', NEW."_id"), '[]'));\nEND;`)).toBe("_id");
    expect(entityKeyColumn(`VALUES ('it''s', CAST(NEW."a""b" AS TEXT), 'insert'`), "a key that is not a plain JSON path member").toBeUndefined();
    expect(entityKeyColumn(`INSERT INTO _gonvex_record_pending(table_name,row_id,old_value) SELECT 'r',NEW._id,x`)).toBeUndefined();
  });
});
