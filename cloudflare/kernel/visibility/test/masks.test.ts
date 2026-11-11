// Ports of visibility.rs `mask_tests`, visibility_index.rs tests,
// visibility_class.rs scalar tests and permission_bits.rs unit tests: the
// pure parts (no SQL).

import { describe, expect, it } from "vitest";
import { GrantIndex, grantPair, ParentAclIndex } from "../acl-index.ts";
import type { JsonValue } from "../json.ts";
import { scalar } from "../json.ts";
import { rowMatches } from "../mask.ts";
import { PermissionIndex, parsePermissionGrants } from "../permission-bits.ts";
import { activeSets, forgetPlanCaches, parseExpression, parsePlan, parseSet, validateManifest, validatePlan, viaIndexName, type VisibilityPlan } from "../plan.ts";
import {
  derivedChangeConcerns,
  grantContribution,
  grantJoinSet,
  parentAdmits,
  principalChangeConcerns,
  recomputeDerived,
} from "../principal.ts";
import { InheritedParent, type ResolvedVisibility } from "../resolved.ts";
import { compilePredicate, compileSet } from "../sql.ts";

function member(id: string, sets: Array<[string, string[]]> = []): ResolvedVisibility {
  return {
    ephemeralMember: false,
    revision: 1,
    direct: { "member.id": id, "account.id": `acct-${id}`, "tenant.id": "t" },
    role: "member",
    permissions: {},
    sets: new Map(sets.map(([name, values]) => [name, new Set(values)])),
    fingerprint: "",
    inherited: new Map(),
    indexes: new Map(),
  };
}

function tasksPlan(): VisibilityPlan {
  return parsePlan({
    version: 2, table: "tasks", key: "_id",
    permissionTables: ["memberTeams", "workspaceTeams"],
    sets: {
      workspaces: { kind: "principal", table: "workspaceTeams", select: "workspaceId",
        joins: [{ table: "memberTeams", leftColumn: "teamId", rightColumn: "teamId" }],
        where: [{ table: "memberTeams", column: "memberId", context: "member.id" }] },
      assigned: { kind: "grant", table: "taskUsers", select: "taskId", joins: [],
        where: [{ table: "taskUsers", column: "userId", context: "member.id" },
                { table: "taskUsers", column: "deletedAt", value: { literal: null } }] },
      teamContexts: { kind: "grant", table: "taskWorkspaceContexts", select: "taskId",
        joins: [{ table: "memberTeams", leftColumn: "targetTeamId", rightColumn: "teamId" }],
        where: [{ table: "memberTeams", column: "memberId", context: "member.id" }] },
    },
    where: { operator: "or", children: [
      { operator: "inSet", column: "workspaceId", set: "workspaces" },
      { operator: "inSet", column: "_id", set: "assigned" },
      { operator: "inSet", column: "_id", set: "teamContexts" },
    ] },
  });
}

function parent(rows: Array<[string, JsonValue]>): InheritedParent {
  return new InheritedParent(tasksPlan(), member("alice", [["workspaces", ["w1"]], ["assigned", ["t-assigned"]], ["teamContexts", []]]), ParentAclIndex.fromSnapshot(1, rows));
}

function compile(plan: VisibilityPlan, resolved: ResolvedVisibility): { sql: string; parameters: JsonValue[] } {
  const parameters: JsonValue[] = [];
  return { sql: compilePredicate(plan, resolved, "r", parameters), parameters };
}

describe("mask tests (visibility.rs mask_tests)", () => {
  it("mask SQL compiles role-pruned sets without resolving them", () => {
    const cases: Array<[JsonValue, boolean]> = [
      [{ operator: "or", children: [{ operator: "role", value: "admin" }, { operator: "inSet", column: "workspaceId", set: "workspaces" }] }, true],
      [{ operator: "and", children: [{ operator: "role", value: "member" }, { operator: "nonEmpty", set: "workspaces" }] }, false],
      [{ operator: "not", children: [{ operator: "and", children: [{ operator: "role", value: "member" }, { operator: "inSet", column: "workspaceId", set: "workspaces" }] }] }, true],
    ];
    for (const [predicate, expected] of cases) {
      const plan = tasksPlan();
      plan.predicate = parseExpression(predicate);
      validatePlan(plan);
      const resolved = member("alice");
      resolved.role = "admin";
      expect(activeSets(plan, resolved.role).size).toBe(0);
      const { sql, parameters } = compile(plan, resolved);
      expect(sql).toBe(expected ? "TRUE" : "FALSE");
      expect(rowMatches(plan, resolved, { workspaceId: "private" })).toBe(expected);
      expect(parameters).toEqual([]);
    }
  });

  it("mask SQL still rejects an unresolved active set", () => {
    const plan = tasksPlan();
    const resolved = member("alice");
    expect(activeSets(plan, resolved.role).has("workspaces")).toBe(true);
    expect(() => compile(plan, resolved)).toThrow(/was not resolved/);
  });

  it("an empty derived set is resolved even without grants", () => {
    const plan = parsePlan({
      version: 2, table: "categories", key: "_id", permissionTables: [],
      sets: {
        assigned: { kind: "grant", table: "taskUsers", select: "taskId", where: [{ table: "taskUsers", column: "memberId", context: "member.id" }] },
        assignedCategories: { kind: "derived", from: "assigned", table: "tasks", key: "_id", select: "categoryId" },
      },
      where: { operator: "inSet", column: "_id", set: "assignedCategories" },
    });
    validatePlan(plan);
    const resolved = member("alice", [["assigned", []]]);
    resolved.indexes.set("assignedCategories", ParentAclIndex.fromSnapshot(1, []));
    expect(recomputeDerived(plan, resolved)).toEqual([]);
    expect(resolved.sets.get("assignedCategories"), "resolved empty differs from not resolved").toEqual(new Set());
    compile(plan, resolved);
    expect(rowMatches(plan, resolved, { _id: "private-category" })).toBe(false);
  });

  it("grant rows are decided from the row and the member alone", () => {
    const plan = tasksPlan();
    const alice = member("alice", [["teamContexts::join", ["team-a"]]]);
    const set = plan.sets.get("assigned")!;
    const row = (user: string) => ({ _id: "tu-1", taskId: "task-1", userId: user, deletedAt: null });
    expect(grantContribution("assigned", set, alice, row("alice"))).toEqual({ kind: "token", token: "task-1" });
    expect(grantContribution("assigned", set, alice, row("bob"))).toEqual({ kind: "none" });
    expect(grantContribution("assigned", set, alice, { _id: "tu-1", taskId: "task-1", userId: "alice", deletedAt: 5 }), "a soft-deleted assignment grants nothing").toEqual({ kind: "none" });
    expect(grantContribution("assigned", set, alice, { taskId: "task-1" }), "a missing constrained column is unknown").toEqual({ kind: "unknown" });
    const team = plan.sets.get("teamContexts")!;
    expect(grantContribution("teamContexts", team, alice, { taskId: "task-2", targetTeamId: "team-a" })).toEqual({ kind: "token", token: "task-2" });
    expect(grantContribution("teamContexts", team, alice, { taskId: "task-2", targetTeamId: "team-b" })).toEqual({ kind: "none" });
  });

  it("a grant join set selects the member's join tokens", () => {
    const join = grantJoinSet(tasksPlan().sets.get("teamContexts")!)!;
    expect(join.table).toBe("memberTeams");
    expect(join.select).toBe("teamId");
    expect(join.joins).toEqual([]);
    expect(join.constraints.length).toBe(1);
    expect(join.constraints[0]!.context).toBe("member.id");
    const { sql, parameters } = compileSet(join, member("alice").direct);
    expect(sql.startsWith('SELECT DISTINCT CAST(v0."teamId" AS TEXT) FROM "memberTeams" AS v0')).toBe(true);
    expect(parameters).toEqual(["alice"]);
  });

  it("only permission rows naming the member rebuild its principal", () => {
    const plan = tasksPlan();
    const alice = member("alice");
    const concerns = (table: string, operation: string, oldRow: JsonValue, newRow: JsonValue) => principalChangeConcerns(plan, alice, table, operation, [], oldRow, newRow);
    expect(concerns("tasks", "insert", null, { _id: "t", workspaceId: "w" })).toBe(false);
    expect(concerns("taskUsers", "insert", null, { userId: "alice", taskId: "t" })).toBe(false);
    expect(concerns("memberTeams", "insert", null, { memberId: "bob", teamId: "x" })).toBe(false);
    expect(concerns("memberTeams", "insert", null, { memberId: "alice", teamId: "x" })).toBe(true);
    expect(concerns("memberTeams", "delete", { memberId: "alice", teamId: "x" }, null)).toBe(true);
    const bobRow = { id: "bob", role: "admin" };
    expect(principalChangeConcerns(plan, alice, "members", "update", ["role"], bobRow, bobRow)).toBe(false);
    const aliceRow = { id: "alice", role: "admin" };
    expect(principalChangeConcerns(plan, alice, "members", "update", ["role"], aliceRow, aliceRow)).toBe(true);
    expect(principalChangeConcerns(plan, alice, "members", "update", ["lastSeenAt"], aliceRow, aliceRow)).toBe(false);
  });

  it("derived input rows do not become unanchored principal changes", () => {
    let plan = tasksPlan();
    plan.sets.set("noted", parseSet({ kind: "derived", from: "assigned", table: "taskNotes", key: "taskId", select: "taskId" }));
    plan.predicate.children.push(parseExpression({ operator: "inSet", column: "_id", set: "noted" }));
    forgetPlanCaches(plan);
    validatePlan(plan);
    const alice = member("alice", [["assigned", ["mine"]]]);
    alice.indexes.set("noted", ParentAclIndex.fromRows(1, [["note", "mine", { taskId: "mine" }]]));
    for (const task of ["mine", "another-members-task"]) {
      const row = { _id: "note", taskId: task };
      expect(principalChangeConcerns(plan, alice, "taskNotes", "insert", [], null, row)).toBe(false);
      expect(derivedChangeConcerns(plan, alice, "taskNotes", "note", null, row), "only a source token of this member routes derived work").toBe(task === "mine");
    }
    const inactive = alice;
    inactive.role = "admin";
    inactive.indexes.set("noted", ParentAclIndex.fromRows(1, [["note", "mine", { taskId: "mine" }]]));
    plan = { ...plan, predicate: parseExpression({ operator: "or", children: [{ operator: "role", value: "admin" }, { operator: "public" }] }) };
    plan.predicate.children[1] = tasksPlan().predicate;
    plan.predicate.children[1]!.children.push(parseExpression({ operator: "inSet", column: "_id", set: "noted" }));
    expect(recomputeDerived(plan, inactive)).toEqual([]);
    expect(inactive.sets.has("noted"), "role-pruned sets stay inactive during feed updates").toBe(false);
  });

  it("inherited rows follow the parent in memory", () => {
    const child = parsePlan({
      version: 2, table: "taskNotes", key: "_id", permissionTables: ["memberTeams", "workspaceTeams"], sets: {},
      where: { operator: "inherits", column: "taskId", table: "tasks", key: "_id", onMissing: { operator: "role", value: "system_admin" } },
    });
    const alice = member("alice");
    alice.inherited.set("tasks", parent([
      ["t-visible", { _id: "t-visible", workspaceId: "w1" }],
      ["t-hidden", { _id: "t-hidden", workspaceId: "w2" }],
      ["t-assigned", { _id: "t-assigned", workspaceId: "w2" }],
    ]));
    const note = (task: JsonValue) => ({ _id: "n", taskId: task });
    expect(rowMatches(child, alice, note("t-visible"))).toBe(true);
    expect(rowMatches(child, alice, note("t-hidden"))).toBe(false);
    expect(rowMatches(child, alice, note("t-assigned")), "a parent grant reaches its children").toBe(true);
    expect(rowMatches(child, alice, note(null)), "a null foreign key denies").toBe(false);
    expect(rowMatches(child, alice, note("t-gone")), "orphans need onMissing").toBe(false);
    alice.role = "system_admin";
    expect(rowMatches(child, alice, note("t-gone")), "onMissing admits orphans").toBe(true);
    expect(rowMatches(child, alice, note("t-hidden")), "onMissing never overrides a hidden parent").toBe(false);
    alice.role = "member";
    alice.inherited.get("tasks")!.index.apply(2, [["t-hidden", { _id: "t-hidden", workspaceId: "w1" }]]);
    expect(rowMatches(child, alice, note("t-hidden"))).toBe(true);
  });

  it("derived sets map grants through the shared index in memory", () => {
    const categories = parsePlan({
      version: 2, table: "categories", key: "_id", permissionTables: ["memberTeams", "workspaceTeams"],
      sets: {
        assignedTasks: { kind: "grant", table: "taskUsers", select: "taskId", joins: [], where: [{ table: "taskUsers", column: "userId", context: "member.id" }] },
        assignedTaskCategories: { kind: "derived", from: "assignedTasks", table: "tasks", key: "_id", select: "categoryId", joins: [], where: [{ column: "deletedAt", value: { literal: null } }] },
      },
      where: { operator: "inSet", column: "_id", set: "assignedTaskCategories" },
    });
    validatePlan(categories);
    const alice = member("alice", [["assignedTasks", ["t1", "t2"]]]);
    const index = ParentAclIndex.fromSnapshot(1, [
      ["t1", { _id: "t1", categoryId: "c1", deletedAt: null }],
      ["t2", { _id: "t2", categoryId: "c2", deletedAt: 7 }],
      ["t3", { _id: "t3", categoryId: "c3", deletedAt: null }],
    ]);
    alice.indexes.set("assignedTaskCategories", index);
    expect(recomputeDerived(categories, alice).length).toBe(1);
    expect(alice.sets.get("assignedTaskCategories"), "a soft-deleted granted task contributes no category").toEqual(new Set(["c1"]));
    expect(rowMatches(categories, alice, { _id: "c1" })).toBe(true);
    expect(rowMatches(categories, alice, { _id: "c3" }), "unassigned tasks grant nothing").toBe(false);
    expect(derivedChangeConcerns(categories, alice, "tasks", "t1", { _id: "t1" }, { _id: "t1" })).toBe(true);
    expect(derivedChangeConcerns(categories, alice, "tasks", "t3", { _id: "t3" }, { _id: "t3" })).toBe(false);
    index.apply(2, [["t1", { _id: "t1", categoryId: "c9", deletedAt: null }]]);
    expect(recomputeDerived(categories, alice)).toEqual([{ set: "assignedTaskCategories", added: new Set(["c9"]), removed: new Set(["c1"]) }]);
    expect(recomputeDerived(categories, alice), "recompute is idempotent").toEqual([]);
  });

  it("derived sets reject unsafe shapes", () => {
    const withSet = (set: JsonValue) =>
      parsePlan({
        version: 2, table: "categories", key: "_id", permissionTables: ["memberTeams"],
        sets: {
          assignedTasks: { kind: "grant", table: "taskUsers", select: "taskId", joins: [], where: [{ table: "taskUsers", column: "userId", context: "member.id" }] },
          derived: set,
        },
        where: { operator: "inSet", column: "_id", set: "derived" },
      });
    const base = { kind: "derived", from: "assignedTasks", table: "tasks", key: "_id", select: "categoryId", joins: [], where: [] as JsonValue[] };
    validatePlan(withSet(base));
    expect(() => validatePlan(withSet({ ...base, table: "categories" })), "a derived set never maps through its own table").toThrow();
    expect(() => validatePlan(withSet({ ...base, from: "missing" }))).toThrow();
    validatePlan(withSet({ ...base, where: [{ column: "createdBy", context: "member.id" }] }));
    expect(() => validatePlan(withSet({ ...base, where: [{ table: "other", column: "x", value: { literal: 1 } }] })), "derived filters name indexed-row columns only").toThrow();
  });

  it("inherited parent filters (F1) and permission self-reads (F2)", () => {
    const child = parsePlan({
      version: 2, table: "crmActivities", key: "_id", permissionTables: ["memberTeams", "workspaceTeams"], sets: {},
      where: { operator: "inherits", column: "taskId", table: "tasks", key: "_id", where: [{ column: "deletedAt", value: { literal: null } }] },
    });
    validatePlan(child);
    const alice = member("alice");
    alice.inherited.set("tasks", parent([
      ["live", { _id: "live", workspaceId: "w1", deletedAt: null }],
      ["gone", { _id: "gone", workspaceId: "w1", deletedAt: 5 }],
    ]));
    expect(rowMatches(child, alice, { taskId: "live" })).toBe(true);
    expect(rowMatches(child, alice, { taskId: "gone" })).toBe(false);
    expect(compile(child, alice).sql).toContain('"deletedAt" IS NULL');
    expect(parentAdmits(child, alice, "tasks", alice.inherited.get("tasks")!, { _id: "gone", workspaceId: "w1", deletedAt: 5 })).toBe(false);
    const spots = (permission: string) =>
      parsePlan({
        version: 2, table: "spots", key: "_id", permissionTables: [permission, "memberSpots"],
        sets: { tree: { kind: "principal", table: "spots", alias: "child", select: "_id",
          joins: [{ table: "memberSpots", leftColumn: "parentId", rightColumn: "spotId" }],
          where: [{ table: "memberSpots", column: "memberId", context: "member.id" }] } },
        where: { operator: "inSet", column: "_id", set: "tree" },
      });
    validatePlan(spots("spots"));
    expect(() => validatePlan(spots("other"))).toThrow();
  });

  it("derived sets correlate rows by a non-unique key (F3)", () => {
    const workspaces = parsePlan({
      version: 2, table: "workspaces", key: "_id", permissionTables: ["memberTeams"],
      sets: {
        teams: { kind: "principal", table: "memberTeams", select: "teamId", joins: [], where: [{ table: "memberTeams", column: "memberId", context: "member.id" }] },
        pending: { kind: "grant", table: "taskApprovalInstances", select: "taskId", joins: [],
          where: [{ table: "taskApprovalInstances", column: "approverUserId", context: "member.id" },
                  { table: "taskApprovalInstances", column: "status", value: { literal: "pending" } }] },
        mine: { kind: "derived", from: "pending", table: "taskWorkspaceContexts", key: "taskId", select: "workspaceId", joins: [],
          where: [{ column: "kind", value: { literal: "approval" } }, { column: "revokedAt", value: { literal: null } }, { column: "targetUserId", context: "member.id" }] },
        team: { kind: "derived", from: "pending", table: "taskWorkspaceContexts", key: "taskId", select: "workspaceId", joins: [],
          where: [{ column: "kind", value: { literal: "approval" } }, { column: "targetTeamId", inSet: "teams" }] },
      },
      where: { operator: "or", children: [{ operator: "inSet", column: "_id", set: "mine" }, { operator: "inSet", column: "_id", set: "team" }] },
    });
    validatePlan(workspaces);
    const alice = member("alice", [["pending", ["t1"]], ["teams", ["approvers"]]]);
    const contexts = ParentAclIndex.fromRows(1, [
      ["c1", "t1", { kind: "approval", revokedAt: null, targetUserId: "alice", workspaceId: "w-alice" }],
      ["c2", "t1", { kind: "approval", revokedAt: null, targetUserId: "bob", workspaceId: "w-bob" }],
      ["c3", "t1", { kind: "approval", targetTeamId: "approvers", workspaceId: "w-team" }],
      ["c4", "t2", { kind: "approval", revokedAt: null, targetUserId: "alice", workspaceId: "w-other" }],
    ]);
    alice.indexes.set("mine", contexts);
    alice.indexes.set("team", contexts);
    recomputeDerived(workspaces, alice);
    expect(alice.sets.get("mine"), "only my context on my pending task").toEqual(new Set(["w-alice"]));
    expect(alice.sets.get("team"), "team targets follow my teams").toEqual(new Set(["w-team"]));
    const broken = structuredClone(workspaces);
    broken.sets.get("team")!.constraints[1]!.inSet = "mine";
    expect(() => validatePlan(broken), "derived inSet names a principal or grant set").toThrow();
  });

  it("inheritsAny admits rows linked to any visible parent (F4)", () => {
    const documents = parsePlan({
      version: 2, table: "documents", key: "_id", permissionTables: ["memberTeams", "workspaceTeams"], sets: {},
      where: { operator: "inheritsAny", column: "_id", table: "tasks", key: "_id", via: { table: "documentAssociations", childColumn: "documentId", parentColumn: "taskId" } },
    });
    validatePlan(documents);
    const alice = member("alice");
    alice.inherited.set("tasks", parent([
      ["t-visible", { _id: "t-visible", workspaceId: "w1" }],
      ["t-hidden", { _id: "t-hidden", workspaceId: "w2" }],
    ]));
    const via = documents.predicate.via!;
    const rows: Array<[string, Record<string, JsonValue>]> = [
      ["a1", { documentId: "d-shared", taskId: "t-hidden" }],
      ["a2", { documentId: "d-shared", taskId: "t-visible" }],
      ["a3", { documentId: "d-private", taskId: "t-hidden" }],
    ];
    for (const column of [via.childColumn, via.parentColumn]) {
      alice.indexes.set(viaIndexName(via, column), ParentAclIndex.fromRows(1, rows.map(([id, row]) => [id, scalar(row[column]), row] as const)));
    }
    expect(rowMatches(documents, alice, { _id: "d-shared" })).toBe(true);
    expect(rowMatches(documents, alice, { _id: "d-private" })).toBe(false);
    expect(rowMatches(documents, alice, { _id: "d-unlinked" })).toBe(false);
    const { sql } = compile(documents, alice);
    expect(sql).toContain('FROM "documentAssociations" AS _gonvex_via_');
    expect(sql).toContain('."documentId" = r."_id"');
  });

  it("an inherited parent predicate uses the child principal (F5)", () => {
    const child = parsePlan({
      version: 2, table: "goalUpdates", key: "_id", permissionTables: ["memberTeams", "workspaceTeams"],
      sets: { myGoals: { kind: "grant", table: "goalParticipants", select: "goalId", joins: [], where: [{ table: "goalParticipants", column: "memberId", context: "member.id" }] } },
      where: { operator: "inherits", column: "goalId", table: "tasks", key: "_id",
        predicate: { operator: "or", children: [{ operator: "inSet", column: "_id", set: "myGoals" }, { operator: "eqContext", column: "ownerId", context: "member.id" }] } },
    });
    validatePlan(child);
    const alice = member("alice", [["myGoals", ["g-joined"]]]);
    alice.inherited.set("tasks", parent([
      ["g-joined", { _id: "g-joined", workspaceId: "w2", ownerId: "bob" }],
      ["g-owned", { _id: "g-owned", workspaceId: "w2", ownerId: "alice" }],
      ["g-workspace", { _id: "g-workspace", workspaceId: "w1", ownerId: "bob" }],
    ]));
    expect(rowMatches(child, alice, { goalId: "g-joined" })).toBe(true);
    expect(rowMatches(child, alice, { goalId: "g-owned" })).toBe(true);
    expect(rowMatches(child, alice, { goalId: "g-workspace" }), "the override replaces the parent collection's rule").toBe(false);
    expect(parentAdmits(child, alice, "tasks", alice.inherited.get("tasks")!, { _id: "g-owned", ownerId: "alice" })).toBe(true);
    expect(compile(child, alice).sql).toMatch(/"ownerId" = \$/);
    const nested = structuredClone(child);
    nested.predicate.predicate = structuredClone(nested.predicate);
    expect(() => validatePlan(nested), "a parent predicate cannot inherit again").toThrow();
  });

  it("inherited SQL mirrors the in-memory rule", () => {
    const child = parsePlan({
      version: 2, table: "taskNotes", key: "_id", permissionTables: ["memberTeams", "workspaceTeams"], sets: {},
      where: { operator: "inherits", column: "taskId", table: "tasks", key: "_id", onMissing: { operator: "role", value: "system_admin" } },
    });
    const alice = member("alice");
    alice.inherited.set("tasks", parent([]));
    const { sql } = compile(child, alice);
    expect(sql).toContain('EXISTS (SELECT 1 FROM "tasks" AS _gonvex_parent_');
    expect(sql).toContain(' = r."taskId"');
    expect(sql).toContain('r."taskId" IS NOT NULL AND NOT EXISTS');
    expect(sql, "parent tokens bind as arrays").toContain(" IN (SELECT value FROM json_each(");
    expect(sql, "no permission-table subqueries per row").not.toContain('"memberTeams"');
  });
});

describe("validation errors keep Rust's texts", () => {
  it("names the problem as visibility.rs does", () => {
    const base = { version: 2, table: "tasks", key: "_id", permissionTables: ["memberTeams"], sets: {}, where: { operator: "public" } };
    const error = (plan: unknown) => {
      try {
        validatePlan(parsePlan(plan));
      } catch (caught) {
        return (caught as Error).message;
      }
      return "ok";
    };
    expect(error(base)).toBe("ok");
    expect(error({ ...base, table: "bad-name" })).toBe('invalid visibility plan: invalid SQL identifier "bad-name"');
    expect(error({ ...base, version: 3 })).toBe("invalid visibility plan: visibility version must be 2");
    expect(error({ ...base, where: { operator: "regex" } })).toBe('invalid visibility plan: unsupported operator "regex"');
    expect(error({ ...base, where: { operator: "inSet", column: "x", set: "nope" } })).toBe('invalid visibility plan: unknown visibility set "nope"');
    expect(error({ ...base, sets: { s: { kind: "principal", table: "data", select: "id" } }, where: { operator: "nonEmpty", set: "s" } })).toBe(
      'invalid visibility plan: set "s": undeclared permission table "data"; use a permission-only principal set, a single-source grant set, or inherits',
    );
    expect(error({ ...base, sets: { s: { kind: "grant", table: "data", select: "id", where: [{ column: "x", value: { literal: 1 } }] } }, where: { operator: "nonEmpty", set: "s" } })).toBe(
      'invalid visibility plan: set "s": grant requires member.id or account.id anchor; use a permission-only principal set, a single-source grant set, or inherits',
    );
    expect(error({ ...base, where: { operator: "not", children: [] } })).toBe("invalid visibility plan: not requires one child");
  });

  it("validates inheritance across the manifest", () => {
    const plan = (table: string, parentTable?: string, key = "_id") =>
      parsePlan({ version: 2, table, key: "_id", permissionTables: [], sets: {}, where: parentTable ? { operator: "inherits", column: "p", table: parentTable, key } : { operator: "public" } });
    const manifest = (...plans: VisibilityPlan[]) => new Map(plans.map((p) => [p.table, p]));
    expect(() => validateManifest(manifest(plan("a", "b"), plan("b")))).not.toThrow();
    expect(() => validateManifest(manifest(plan("a", "missing")))).toThrow('invalid visibility plan: plan "a" inherits unknown parent "missing"');
    expect(() => validateManifest(manifest(plan("a", "b"), plan("b", "a")))).toThrow("inheritance cycle");
    expect(() => validateManifest(manifest(plan("a", "b", "id"), plan("b")))).toThrow('plan "a": inherited key "id" differs from parent key "_id"');
    expect(() => validateManifest(manifest(plan("a", "b"), plan("b", "c"), plan("c", "d"), plan("d", "e"), plan("e", "f"), plan("f")))).toThrow("inheritance exceeds four edges");
  });
});

describe("indexes (visibility_index.rs)", () => {
  const grant = (member: string) => new Set([grantPair(member, "task-1")]);

  it("duplicate grants revoke only after the last contribution", () => {
    const index = new GrantIndex();
    expect(index.replace("a", grant("alice"))).toEqual(new Set(["alice"]));
    expect(index.replace("b", grant("alice"))).toEqual(new Set());
    expect(index.replace("a", new Set())).toEqual(new Set());
    expect(index.contains("alice", "task-1")).toBe(true);
    expect(index.replace("b", new Set())).toEqual(new Set(["alice"]));
    expect(index.contains("alice", "task-1")).toBe(false);
    expect(index.replace("b", new Set())).toEqual(new Set());
  });

  it("reassignment routes to old and new members", () => {
    const index = new GrantIndex();
    index.replace("a", grant("alice"));
    expect(index.replace("a", grant("bob"))).toEqual(new Set(["alice", "bob"]));
    expect(index.contains("alice", "task-1")).toBe(false);
    expect(index.contains("bob", "task-1")).toBe(true);
    expect(index.replace("a", grant("bob"))).toEqual(new Set());
  });

  it("non-unique keys group rows and move with their row", () => {
    const index = ParentAclIndex.fromRows(1, [["c1", "task-1", { workspaceId: "a" }], ["c2", "task-1", { workspaceId: "b" }]]);
    expect(index.getAll("task-1").length).toBe(2);
    expect(index.applyRows(2, [["c2", ["task-2", { workspaceId: "b" }]]])).toBe(true);
    expect(index.getAll("task-1").length).toBe(1);
    expect(index.getAll("task-2").length).toBe(1);
    expect(index.applyRows(3, [["c1", undefined]])).toBe(true);
    expect(index.getAll("task-1")).toEqual([]);
    expect(index.get("task-1")).toBeUndefined();
  });

  it("parent transactions are shared and deletion removes access", () => {
    const index = ParentAclIndex.fromSnapshot(10, [["task-1", { workspaceId: "a" }]]);
    const first = index.get("task-1");
    expect(index.get("task-1")).toBe(first);
    expect(index.apply(11, [["task-1", { workspaceId: "b" }]])).toBe(true);
    expect(index.apply(11, [["task-1", undefined]])).toBe(false);
    expect((index.get("task-1") as { workspaceId: string }).workspaceId).toBe("b");
    expect(index.apply(12, [["task-1", undefined]])).toBe(true);
    expect(index.get("task-1")).toBeUndefined();
    expect(index.apply(10, [["task-1", first!]])).toBe(false);
    expect(index.get("task-1")).toBeUndefined();
  });

  it("completeness advances without marking unchanged ACL rows modified", () => {
    const row = { workspaceId: "a" };
    const index = ParentAclIndex.fromSnapshot(10, [["task", row]]);
    index.applyRows(11, []);
    expect(index.revision).toBe(11);
    expect(index.modifiedRevision).toBe(10);
    index.apply(12, [["task", { ...row }]]);
    expect(index.modifiedRevision).toBe(10);
    index.apply(13, [["task", { workspaceId: "b" }]]);
    expect(index.modifiedRevision).toBe(13);
    index.apply(14, [["missing", undefined]]);
    expect(index.modifiedRevision).toBe(13);
    index.apply(15, [["task", undefined]]);
    expect(index.modifiedRevision).toBe(15);
  });

  it("waiters wake on progress and on poison", async () => {
    const index = ParentAclIndex.maintained("e", 1, []);
    const reached = index.waitFor(3);
    index.applyRows(2, []);
    index.applyRows(3, []);
    expect(await reached).toBe(true);
    const poisoned = index.waitFor(9);
    index.poison();
    expect(await poisoned).toBe(false);
  });
});

describe("token text (visibility_token_tests.rs, scalar)", () => {
  it("membership never confuses null columns with the string null token", () => {
    const plan = parsePlan({
      version: 2, table: "tasks", key: "_id", permissionTables: ["memberTeams"],
      sets: { s: { kind: "principal", table: "memberTeams", select: "teamId", where: [{ column: "memberId", context: "member.id" }] } },
      where: { operator: "inSet", column: "teamId", set: "s" },
    });
    const resolved = member("alice", [["s", ["null"]]]);
    expect(rowMatches(plan, resolved, { teamId: null })).toBe(false);
    expect(rowMatches(plan, resolved, {})).toBe(false);
    expect(rowMatches(plan, resolved, { teamId: "null" })).toBe(true);
  });

  it("scalar is the token text Rust uses for strings, integers, booleans and containers", () => {
    expect([scalar(undefined), scalar(null), scalar("x"), scalar(42), scalar(-1), scalar(true), scalar(1.5), scalar([1, "a"]), scalar({ b: 1, a: [2] })]).toEqual([
      "null", "null", "x", "42", "-1", "true", "1.5", '[1,"a"]', '{"a":[2],"b":1}',
    ]);
  });
});

describe("permission bits (permission_bits.rs)", () => {
  const source = parsePermissionGrants({ table: "grants", member: "memberId", permission: "permission", scope: "scope", whereNull: ["deletedAt"] });
  const change = (id: string, oldValue: JsonValue, newValue: JsonValue) => ({ table: "grants", rowId: id, operation: "update", changedColumns: [], oldValue, newValue });
  const grant = (member: string, permission: string, scope: JsonValue) => ({ memberId: member, permission, scope, deletedAt: null });

  it("grants are bits with reference counts and soft deletes", () => {
    const index = PermissionIndex.fromRows(source, "e", 1, [
      ["g1", grant("m1", "edit-tasks", "w1")],
      ["g2", grant("m1", "edit-tasks", "w1")],
      ["g3", grant("m2", "view", null)],
      ["g4", { memberId: "m2", permission: "edit-tasks", scope: "w1", deletedAt: 5 }],
    ]);
    expect(index.has("m1", "edit-tasks", "w1")).toBe(true);
    expect(index.has("m1", "edit-tasks", "w2")).toBe(false);
    expect(index.has("m2", "edit-tasks", "w1"), "soft-deleted rows grant nothing").toBe(false);
    expect(index.has("m2", "view", ""), "null scope is tenant-wide").toBe(true);
    expect(index.size()).toEqual([2, 2]);
    index.apply(source, 2, [change("g1", grant("m1", "edit-tasks", "w1"), null)]);
    expect(index.has("m1", "edit-tasks", "w1")).toBe(true);
    index.apply(source, 3, [change("g2", grant("m1", "edit-tasks", "w1"), grant("m1", "edit-tasks", "w2"))]);
    expect(index.has("m1", "edit-tasks", "w1")).toBe(false);
    expect(index.has("m1", "edit-tasks", "w2")).toBe(true);
    index.apply(source, 3, [change("g9", null, grant("m3", "x", "y"))]);
    expect(index.has("m3", "x", "y")).toBe(false);
    const deleted = { memberId: "m2", permission: "view", scope: null, deletedAt: 9 };
    index.apply(source, 4, [change("g3", grant("m2", "view", null), deleted)]);
    expect(index.has("m2", "view", "")).toBe(false);
    index.apply(source, 5, [change("g3", deleted, grant("m2", "view", null))]);
    expect(index.has("m2", "view", "")).toBe(true);
    expect(index.revision).toBe(5);
  });

  it("bits beyond one word and numeric member ids", () => {
    const rows: Array<[string, JsonValue]> = Array.from({ length: 70 }, (_, n) => [`g${n}`, { memberId: 7, permission: `p${n}`, scope: "s", deletedAt: null }]);
    const index = PermissionIndex.fromRows(source, "e", 1, rows);
    expect(index.has("7", "p69", "s")).toBe(true);
    expect(index.has("7", "p31", "s")).toBe(true);
    expect(index.has("7", "p70", "s")).toBe(false);
    index.apply(source, 2, [change("g69", rows[69]![1], null)]);
    expect(index.has("7", "p69", "s")).toBe(false);
    expect(index.has("7", "p68", "s")).toBe(true);
  });

  it("the declaration is read strictly, as serde deny_unknown_fields", () => {
    expect(() => parsePermissionGrants({ table: "g", member: "m", permission: "p", scope: "s", extra: 1 })).toThrow(/unknown field `extra`/);
    expect(() => parsePermissionGrants({ table: "g", member: "m", permission: "p" })).toThrow("missing field `scope`");
  });
});
