// X17p: the principal cache rolls an entry forward through commits instead of
// resolving the member again. The leak test drives random writes to every
// input of a plan shaped like Whagons' spots.list (grant sets on the written
// table with joins, a join hanging off the grant row, a set joining its own
// base table, a derived set) and checks, after every batch of commits, that
// the cached resolution is the fresh SQL resolution: same sets, same
// fingerprint, same visible rows.
//
// #555: REPLACE conflict resolution deletes rows with no log row. A statement
// whose own conflict resolution is REPLACE (S), or any write under a schema
// with a REPLACE hazard (H: a declared ON CONFLICT REPLACE, or a trigger body
// using REPLACE), logs the global marker `_gonvex_replaced:*`, and the cache
// drops the entry. The base schema has no hazard, so its writes take the patch
// path; each REPLACE probe adds only the tables its case needs.

import { env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { CaptureState, ChangeWake, directSql, installChangeFeed, invokeCaptured } from "../../capture/index.ts";
import { REPLACED_MARKER } from "../../capture/replace-marker.ts";
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

/** No REPLACE hazard: no declared ON CONFLICT REPLACE, no trigger using REPLACE. */
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
  // #555 R2 and M13: an untyped key column (BLOB, integer, real or text keys; no affinity converts them).
  `CREATE TABLE "loose" ("_id" PRIMARY KEY, "memberId" TEXT, "spotId" TEXT, "deletedAt" TEXT)`,
  // A UNIQUE spot per claim, without a declared conflict clause (no hazard by itself).
  `CREATE TABLE "claims" ("_id" TEXT PRIMARY KEY, "memberId" TEXT, "spotId" TEXT UNIQUE)`,
];

/** Hazard H: a declared ON CONFLICT REPLACE (a plain INSERT may delete another key's row). */
const DECLARED = [`CREATE TABLE "dclaims" ("_id" TEXT PRIMARY KEY, "memberId" TEXT, "spotId" TEXT UNIQUE ON CONFLICT REPLACE)`];
/** #555 C1: comments inside the declared clause (SQLite keeps them in sqlite_schema.sql); each alone is the hazard. */
const DECLARED_COMMENT_BLOCK = [`CREATE TABLE "dclaims" ("_id" TEXT PRIMARY KEY, "memberId" TEXT, "spotId" TEXT UNIQUE ON /* c */ CONFLICT REPLACE)`];
const DECLARED_COMMENT_LINE = [`CREATE TABLE "dclaims" ("_id" TEXT PRIMARY KEY, "memberId" TEXT, "spotId" TEXT, UNIQUE ("spotId") ON -- c
     CONFLICT REPLACE)`];
/** Hazard H: a trigger body using REPLACE. */
const TRIGGER_REPLACE = [
  `CREATE TABLE "claimRequests" ("_id" TEXT PRIMARY KEY, "memberId" TEXT, "spotId" TEXT)`,
  `CREATE TRIGGER "claimRequests_apply" AFTER INSERT ON "claimRequests" BEGIN INSERT OR REPLACE INTO "claims" VALUES (NEW."_id", NEW."memberId", NEW."spotId"); END`,
];
/** #555 C2: a REPLACE in a trigger reached through a foreign key action (parent delete -> CASCADE -> child trigger). */
const CASCADE_TRIGGER = [
  `CREATE TABLE owners ("_id" TEXT PRIMARY KEY)`,
  `CREATE TABLE "ownerLinks" ("_id" TEXT PRIMARY KEY, "ownerId" TEXT REFERENCES owners("_id") ON DELETE CASCADE, "memberId" TEXT, "spotId" TEXT)`,
  `CREATE TRIGGER "ownerLinks_gone" AFTER DELETE ON "ownerLinks" BEGIN INSERT OR REPLACE INTO claims VALUES (OLD."_id", OLD."memberId", OLD."spotId"); END`,
];
/** #555 D1: plain trigger writes that inherit the outer statement's OR REPLACE. No hazard H in the schema. */
const INHERITED = [
  `CREATE TABLE "reqs" ("_id" TEXT PRIMARY KEY, "memberId" TEXT, "spotId" TEXT)`,
  `CREATE TRIGGER "reqs_apply" AFTER INSERT ON "reqs" BEGIN INSERT INTO "claims" VALUES (NEW."_id", NEW."memberId", NEW."spotId"); END`,
  `CREATE TABLE "moves" ("_id" TEXT PRIMARY KEY, "claimId" TEXT, "spotId" TEXT)`,
  `CREATE TRIGGER "moves_apply" AFTER UPDATE ON "moves" BEGIN UPDATE "claims" SET "spotId" = NEW."spotId" WHERE "_id" = NEW."claimId"; END`,
];
/** #555 D2: a declared conflict's delete runs a parent's ON DELETE CASCADE into a child trigger that replaces. */
const DECLARED_CASCADE = [
  `CREATE TABLE "dparents" ("_id" TEXT PRIMARY KEY, "code" TEXT UNIQUE ON CONFLICT REPLACE)`,
  `CREATE TABLE "dchildren" ("_id" TEXT PRIMARY KEY, "parentId" TEXT REFERENCES dparents("_id") ON DELETE CASCADE, "memberId" TEXT, "spotId" TEXT)`,
  `CREATE TRIGGER "dchildren_gone" AFTER DELETE ON "dchildren" BEGIN INSERT OR REPLACE INTO claims VALUES (OLD."_id", OLD."memberId", OLD."spotId"); END`,
];

const live = (table: string) => ({ table, column: "deletedAt", value: { literal: null } });
const me = (table: string) => ({ table, column: "memberId", context: "member.id" });

/** The shapes of spots.list's sets (module.json), on a reduced schema; `declared` adds the `dclaims` set. */
function spotsPlan(declared = false): VisibilityPlan {
  const extra = declared ? { declaredSpots: { kind: "grant", table: "dclaims", select: "spotId", joins: [], where: [me("dclaims")] } } : {};
  return parsePlan({
    version: 2,
    table: "spots",
    key: "_id",
    permissionTables: ["memberTeams", "workspaceTeams", "workspaces", "spots", "memberSpots", "members", "loose", "claims", ...(declared ? ["dclaims"] : [])],
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
      looseSpots: { kind: "principal", table: "loose", select: "spotId", joins: [], where: [me("loose"), live("loose")] },
      claimedSpots: { kind: "grant", table: "claims", select: "spotId", joins: [], where: [me("claims")] },
      ...extra,
    },
    where: {
      operator: "or",
      children: ["memberSpots", "memberSpotDescendants", "createdTaskSpots", "reportedTeamTaskSpots", "personalTeamTaskSpots", "teamWorkspaceTaskSpots", "sharedTeamTaskSpots", "looseSpots", "claimedSpots", ...(declared ? ["declaredSpots"] : [])]
        .map((set) => ({ operator: "inSet", column: "_id", set })),
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
  /** Direct SQL outside any invocation (log surgery for the R05 probe). */
  raw(text: string, ...binds: unknown[]): unknown[][];
  /** Whether the last commit logged the REPLACE marker (and that it is the only one). */
  marked(): boolean;
}

async function withTenant<T>(body: (harness: Harness) => Promise<T>, extra: readonly string[] = []): Promise<T> {
  const stub = env.HELLO.getByName(`x17p-${++objects}-${Math.random()}`);
  return runInDurableObject(stub, async (_instance, state) => {
    const sql = directSql(state.storage.sql);
    state.storage.transactionSync(() => {
      installChangeFeed(sql);
      for (const statement of [...SCHEMA, ...extra]) sql.run(statement);
      installChangeFeed(sql);
    });
    const storage = new TenantStorage(state.storage);
    const context = { state: new CaptureState(), wake: new ChangeWake() };
    let commands = 0;
    const raw = (text: string, ...binds: unknown[]) => [...state.storage.sql.exec(text, ...(binds as never[])).raw()] as unknown[][];
    return body({
      registry: new VisibilityRegistry(),
      raw,
      marked: () => {
        const count = Number(raw(`SELECT count(*) FROM _gonvex_sync_changes WHERE revision = (SELECT revision FROM _gonvex_sync_clock) AND table_name = ?`, REPLACED_MARKER)[0]![0]);
        expect(count, "at most one marker per transaction").toBeLessThanOrEqual(1);
        return count === 1;
      },
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
function write(next: () => number, ids: { task: number; other: number }, declared: boolean): [string, unknown[]] {
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
  if (roll < 0.95) return [`UPDATE workspaces SET "type" = $2 WHERE "_id" = $1`, [workspace(), pick(["TEAM", "PERSONAL"])]];
  // #555 S: REPLACE on a task (the old row's grant goes without a delete in the log), and on a claim (another
  // member's claim on the spot goes).
  if (roll < 0.96) return [pick([`REPLACE INTO tasks VALUES ($1, $2, $3, $4, $5, $6)`, `INSERT OR REPLACE INTO tasks VALUES ($1, $2, $3, $4, $5, $6)`]), [task(), spot(), workspace(), pick([...MEMBERS, "mx"]), pick([team(), null]), deleted()]];
  if (roll < 0.965) return [`INSERT OR REPLACE INTO claims VALUES ($1, $2, $3)`, [`c-${++ids.other}`, pick(MEMBERS), spot()]];
  // #555 R2 / M13: untyped keys of every storage class.
  if (roll < 0.975) {
    const n = ++ids.other;
    const key = pick([`CAST('k${n}' AS BLOB)`, `${n}`, `${n}.5`, `'k${n}'`]);
    return [`INSERT INTO loose VALUES (${key}, $1, $2, $3)`, [pick(MEMBERS), spot(), deleted()]];
  }
  // #555 H: a plain INSERT into the declared table (a conflict deletes another member's claim).
  if (declared && roll < 0.985) return [`INSERT INTO dclaims VALUES ($1, $2, $3)`, [`d-${++ids.other}`, pick(MEMBERS), spot()]];
  // A member row change: the entry is dropped, never rolled.
  return [`UPDATE members SET permissions = $2 WHERE id = $1`, [pick(MEMBERS), JSON.stringify({ n: ids.other++ })]];
}

/** Resolves every member (caching), runs `write`, then checks every member's cached resolution against fresh SQL. */
async function probe(harness: Harness, plan: VisibilityPlan, label: string, write: Array<[string, unknown[]]>): Promise<void> {
  await harness.read((session) => checkMembers(session, harness, plan, `${label} before`));
  await harness.commit(write);
  await harness.read((session) => checkMembers(session, harness, plan, `${label} after`));
}

function cachedSet(harness: Harness, session: StorageSession, plan: VisibilityPlan, member: string, set: string): string[] {
  const clock = readClock(session);
  const plans = new Map([["spots", plan]]);
  const resolved = resolveWithParents(session, sessionOf(member), plan, clock.revision, plans, harness.registry.acl.ensure(session, plans, [plan]), undefined, { cache: harness.registry.principals, clock });
  return [...(resolved.sets.get(set) ?? [])];
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
            const statements = Array.from({ length: 1 + Math.floor(next() * 3) }, () => write(next, ids, false));
            await harness.commit(statements);
          }
          await harness.read((session) => checkMembers(session, harness, plan, `seed ${seedValue} round ${round}`));
        }
        const stats = harness.registry.principals.stats();
        expect(stats.rolledForward, JSON.stringify(stats)).toBeGreaterThan(20);
        expect(stats.setsPatched, JSON.stringify(stats)).toBeGreaterThan(0);
        expect(stats.setsRerun, JSON.stringify(stats)).toBeGreaterThan(0);
        expect(stats.invalidated, "member row changes and REPLACE markers still drop the entry").toBeGreaterThan(0);
      });
    }
  }, 120_000);

  it("the same leak test under a schema hazard: every write logs the marker and the entry is dropped", async () => {
    const plan = spotsPlan(true);
    await withTenant(async (harness) => {
      await harness.commit(seed());
      const next = random(7);
      const ids = { task: 0, other: 0 };
      for (let round = 0; round < 25; round += 1) {
        await harness.commit(Array.from({ length: 1 + Math.floor(next() * 3) }, () => write(next, ids, true)));
        expect(harness.marked(), `round ${round}: H marks every write`).toBe(true);
        await harness.read((session) => checkMembers(session, harness, plan, `hazard round ${round}`));
      }
      const stats = harness.registry.principals.stats();
      expect(stats.rolledForward, "no roll under the marker").toBe(0);
      expect(stats.setsPatched).toBe(0);
    }, DECLARED);
  }, 120_000);

  it("no hazard, no REPLACE statement: no marker, and a task insert takes the patch path (one point query per set)", async () => {
    const plan = spotsPlan();
    await withTenant(async (harness) => {
      await harness.commit(seed());
      expect(harness.marked()).toBe(false);
      await harness.commit([[`INSERT INTO "workspaceTeams" VALUES ('wt-m3', 'team-m3', 'w2', NULL)`, []]]);
      expect(harness.marked()).toBe(false);
      await harness.read((session) => checkMembers(session, harness, plan, "before"));
      const before = harness.registry.principals.stats();
      // m1 creates a task in its team's workspace w1, reported by m3's team.
      await harness.commit([[`INSERT INTO tasks VALUES (100, 's7', 'w1', 'm1', 'team-m3', NULL)`, []]]);
      expect(harness.marked()).toBe(false);
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
      // per schema), the inserted-keys exactness check (once per base table), four point queries, and the derived
      // set (empty source: no SQL).
      expect(statements).toBe(7);
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

  it("#555 S (P1): REPLACE and INSERT OR REPLACE log one global marker and drop the entry (the reviewer's probe)", async () => {
    const plan = spotsPlan();
    await withTenant(async (harness) => {
      await harness.commit(seed());
      await harness.commit([[`INSERT INTO tasks VALUES (100, 's7', 'w2', 'm1', NULL, NULL)`, []]]);
      const before = harness.registry.principals.stats().invalidated;
      await probe(harness, plan, "REPLACE INTO", [[`REPLACE INTO tasks VALUES (100, 's7', 'w2', 'mx', NULL, NULL)`, []]]);
      await harness.read((session) => expect(cachedSet(harness, session, plan, "m1", "createdTaskSpots")).toEqual([]));
      const logged = harness.raw(`SELECT table_name, operation, row_id, new_value FROM _gonvex_sync_changes WHERE revision = (SELECT revision FROM _gonvex_sync_clock) ORDER BY ordinal`);
      expect(logged.map((row) => [row[0], row[1], row[2]])).toEqual([["tasks", "insert", "100"], [REPLACED_MARKER, "delete", ""]]);
      expect(logged[1]![3]).toBeNull();
      expect(harness.registry.principals.stats().invalidated - before, "each member's entry dropped").toBe(3);
      await harness.commit([[`INSERT INTO tasks VALUES (101, 's6', 'w2', 'm2', NULL, NULL)`, []]]);
      await probe(harness, plan, "INSERT OR REPLACE", [[`INSERT OR REPLACE INTO tasks VALUES (101, 's6', 'w2', 'm3', NULL, NULL)`, [], ], [`REPLACE INTO tasks VALUES (102, 's5', 'w2', 'm3', NULL, NULL)`, []]]);
      expect(harness.marked(), "two REPLACE statements, one marker").toBe(true);
      await harness.read((session) => expect(cachedSet(harness, session, plan, "m2", "createdTaskSpots")).toEqual([]));
    });
  });

  it("#555 H: a declared ON CONFLICT REPLACE marks every write, and the replaced claim leaves its member", async () => {
    const plan = spotsPlan(true);
    await withTenant(async (harness) => {
      await harness.commit(seed());
      expect(harness.marked(), "H: even the seed").toBe(true);
      await harness.commit([[`INSERT INTO dclaims VALUES ('c1', 'm1', 's4')`, []]]);
      // A plain INSERT: the UNIQUE spotId conflict deletes c1 (m1's claim) without a log row.
      await probe(harness, plan, "declared", [[`INSERT INTO dclaims VALUES ('c3', 'm3', 's4')`, []]]);
      expect(harness.marked()).toBe(true);
      await harness.read((session) => {
        expect(cachedSet(harness, session, plan, "m1", "declaredSpots")).toEqual([]);
        expect(cachedSet(harness, session, plan, "m3", "declaredSpots")).toEqual(["s4"]);
      });
      // A write to another table under H is marked too.
      await probe(harness, plan, "unrelated", [[`INSERT INTO tasks VALUES (300, 's3', 'w1', 'm1', NULL, NULL)`, []]]);
      expect(harness.marked()).toBe(true);
    }, DECLARED);
  });

  it("#555 C1: a comment inside a declared ON CONFLICT REPLACE does not hide the hazard (the reviewer's claims2/claims3 probes)", async () => {
    const plan = spotsPlan(true);
    for (const [label, schema] of [["block comment", DECLARED_COMMENT_BLOCK], ["line comment", DECLARED_COMMENT_LINE]] as const) {
      await withTenant(async (harness) => {
        await harness.commit(seed());
        await harness.commit([[`INSERT INTO dclaims VALUES ('c1', 'm1', 's4')`, []]]);
        await probe(harness, plan, label, [[`INSERT INTO dclaims VALUES ('c3', 'm3', 's4')`, []]]);
        expect(harness.marked(), label).toBe(true);
        await harness.read((session) => {
          expect(cachedSet(harness, session, plan, "m1", "declaredSpots"), `${label} m1`).toEqual([]);
          expect(cachedSet(harness, session, plan, "m3", "declaredSpots"), `${label} m3`).toEqual(["s4"]);
        });
      }, schema);
    }
  });

  it("#555 H: a trigger body using REPLACE marks every write, and its replace takes the other member's claim", async () => {
    const plan = spotsPlan();
    await withTenant(async (harness) => {
      await harness.commit(seed());
      await harness.commit([[`INSERT INTO claims VALUES ('c2', 'm2', 's5')`, []]]);
      await probe(harness, plan, "trigger", [[`INSERT INTO "claimRequests" VALUES ('c4', 'm1', 's5')`, []]]);
      expect(harness.marked()).toBe(true);
      await harness.read((session) => {
        expect(cachedSet(harness, session, plan, "m2", "claimedSpots")).toEqual([]);
        expect(cachedSet(harness, session, plan, "m1", "claimedSpots")).toEqual(["s5"]);
      });
    }, TRIGGER_REPLACE);
  });

  it("#555 C2: a REPLACE in a trigger reached through ON DELETE CASCADE (the reviewer's owners probe)", async () => {
    const plan = spotsPlan();
    await withTenant(async (harness) => {
      await harness.commit(seed());
      await harness.commit([
        [`INSERT INTO claims VALUES ('c1', 'm1', 's4')`, []],
        [`INSERT INTO owners VALUES ('o1')`, []],
        [`INSERT INTO "ownerLinks" VALUES ('l1', 'o1', 'm3', 's4')`, []],
      ]);
      // The cascade deletes l1; its trigger's INSERT OR REPLACE takes spot s4 from c1 (m1).
      await probe(harness, plan, "cascade", [[`DELETE FROM owners WHERE "_id" = 'o1'`, []]]);
      expect(harness.raw(`SELECT "_id", "memberId", "spotId" FROM claims ORDER BY 1`)).toEqual([["l1", "m3", "s4"]]);
      expect(harness.marked()).toBe(true);
      await harness.read((session) => {
        expect(cachedSet(harness, session, plan, "m1", "claimedSpots")).toEqual([]);
        expect(cachedSet(harness, session, plan, "m3", "claimedSpots")).toEqual(["s4"]);
      });
    }, CASCADE_TRIGGER);
  });

  it("#555 D1: plain trigger writes inherit the outer OR REPLACE (A, A2 and the UPDATE form), with no schema hazard", async () => {
    const plan = spotsPlan();
    await withTenant(async (harness) => {
      await harness.commit(seed());
      // No hazard in this schema: a plain write logs no marker.
      await harness.commit([[`INSERT INTO reqs VALUES ('r0', 'm2', 's8')`, []]]);
      expect(harness.marked()).toBe(false);
      // A: INSERT OR REPLACE into reqs; the trigger's plain INSERT into claims replaces m1's c1.
      await harness.commit([[`INSERT INTO claims VALUES ('c1', 'm1', 's4')`, []]]);
      await probe(harness, plan, "A", [[`INSERT OR REPLACE INTO reqs VALUES ('r3', 'm3', 's4')`, []]]);
      expect(harness.marked(), "A").toBe(true);
      expect(harness.raw(`SELECT "_id", "memberId" FROM claims WHERE "spotId" = 's4'`)).toEqual([["r3", "m3"]]);
      await harness.read((session) => expect(cachedSet(harness, session, plan, "m1", "claimedSpots")).toEqual([]));
      // A2: REPLACE INTO reqs.
      await harness.commit([[`INSERT INTO claims VALUES ('c5', 'm1', 's6')`, []]]);
      await probe(harness, plan, "A2", [[`REPLACE INTO reqs VALUES ('r5', 'm2', 's6')`, []]]);
      expect(harness.marked(), "A2").toBe(true);
      await harness.read((session) => expect(cachedSet(harness, session, plan, "m1", "claimedSpots")).toEqual([]));
      // UPDATE form: UPDATE OR REPLACE moves; the trigger's plain UPDATE moves c2 onto s7, replacing m1's c6.
      await harness.commit([
        [`INSERT INTO claims VALUES ('c6', 'm1', 's7')`, []],
        [`INSERT INTO claims VALUES ('c2', 'm2', 's2')`, []],
        [`INSERT INTO moves VALUES ('mv2', 'c2', 's2')`, []],
      ]);
      await probe(harness, plan, "UPDATE", [[`UPDATE OR REPLACE moves SET "spotId" = 's7' WHERE "_id" = 'mv2'`, []]]);
      expect(harness.marked(), "UPDATE form").toBe(true);
      expect(harness.raw(`SELECT "_id", "memberId" FROM claims WHERE "spotId" = 's7'`)).toEqual([["c2", "m2"]]);
      await harness.read((session) => {
        expect(cachedSet(harness, session, plan, "m1", "claimedSpots")).toEqual([]);
        expect(cachedSet(harness, session, plan, "m2", "claimedSpots")).toEqual(["s6", "s7", "s8"]);
      });
    }, INHERITED);
  });

  it("#555 D2: a declared conflict's delete cascades into a child trigger that replaces (the reviewer's dparents probe)", async () => {
    const plan = spotsPlan();
    await withTenant(async (harness) => {
      await harness.commit(seed());
      await harness.commit([
        [`INSERT INTO claims VALUES ('c1', 'm1', 's4')`, []],
        [`INSERT INTO dparents VALUES ('p1', 'x')`, []],
        [`INSERT INTO dchildren VALUES ('l1', 'p1', 'm3', 's4')`, []],
      ]);
      // The declared conflict deletes p1 -> the cascade deletes l1 -> the trigger replaces c1.
      await probe(harness, plan, "dparents", [[`INSERT INTO dparents VALUES ('p2', 'x')`, []]]);
      expect(harness.marked()).toBe(true);
      expect(harness.raw(`SELECT "_id", "memberId", "spotId" FROM claims ORDER BY 1`)).toEqual([["l1", "m3", "s4"]]);
      await harness.read((session) => {
        expect(cachedSet(harness, session, plan, "m1", "claimedSpots")).toEqual([]);
        expect(cachedSet(harness, session, plan, "m3", "claimedSpots")).toEqual(["s4"]);
      });
    }, DECLARED_CASCADE);
  });

  it("#555 R2 P2 and M13: inserted untyped keys (BLOB, integer, real, text) patch or re-run exactly", async () => {
    const plan = spotsPlan();
    await withTenant(async (harness) => {
      await harness.commit(seed());
      // A BLOB key: its image is hex text, which matches no stored row; the set re-runs.
      await probe(harness, plan, "blob", [[`INSERT INTO loose VALUES (x'0102', 'm1', 's2', NULL)`, []]]);
      await harness.read((session) => expect(cachedSet(harness, session, plan, "m1", "looseSpots")).toEqual(["s2"]));
      const rerun = harness.registry.principals.stats().setsRerun;
      // An integer and a real key in an untyped column: only the typed image matches (the row_id text '5' or
      // '7.5' does not equal the stored number), and they patch.
      const patched = harness.registry.principals.stats().setsPatched;
      await probe(harness, plan, "integer", [[`INSERT INTO loose VALUES (5, 'm2', 's3', NULL)`, []]]);
      await probe(harness, plan, "real", [[`INSERT INTO loose VALUES (7.5, 'm2', 's8', NULL)`, []]]);
      await probe(harness, plan, "text", [[`INSERT INTO loose VALUES ('k9', 'm2', 's1', NULL)`, []]]);
      await harness.read((session) => expect(cachedSet(harness, session, plan, "m2", "looseSpots")).toEqual(["s1", "s3", "s8"]));
      const stats = harness.registry.principals.stats();
      expect(stats.setsPatched - patched).toBeGreaterThanOrEqual(9);
      expect(stats.setsRerun).toBe(rerun);
    });
  });

  it("#555: an insert without a row image (R05) re-runs the set instead of patching nothing", async () => {
    const plan = spotsPlan();
    await withTenant(async (harness) => {
      await harness.commit(seed());
      await harness.read((session) => checkMembers(session, harness, plan, "before"));
      const revision = await harness.commit([[`INSERT INTO tasks VALUES (200, 's5', 'w2', 'm1', NULL, NULL)`, []]]);
      harness.raw(`UPDATE _gonvex_sync_changes SET new_value = NULL WHERE revision = ? AND table_name = 'tasks'`, revision);
      await harness.read((session) => {
        checkMembers(session, harness, plan, "after");
        expect(cachedSet(harness, session, plan, "m1", "createdTaskSpots")).toEqual(["s5"]);
      });
    });
  });

  it("reads the key column of the kernel's entity capture triggers only", () => {
    expect(entityKeyColumn(`CREATE TRIGGER "_gonvex_sync_tasks_insert" AFTER INSERT ON "tasks" BEGIN\n  INSERT INTO _gonvex_sync_changes (table_name, row_id, operation, new_value)\n  VALUES ('tasks', CAST(NEW."_id" AS TEXT), 'insert', json_array(NULL, json_object('_id', NEW."_id"), '[]'));\nEND;`)).toBe("_id");
    expect(entityKeyColumn(`VALUES ('it''s', CAST(NEW."a""b" AS TEXT), 'insert'`), "a key that is not a plain JSON path member").toBeUndefined();
    expect(entityKeyColumn(`INSERT INTO _gonvex_record_pending(table_name,row_id,old_value) SELECT 'r',NEW._id,x`)).toBeUndefined();
  });
});
