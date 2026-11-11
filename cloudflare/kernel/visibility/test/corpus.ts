// The shared visibility corpus, ported from rust/crates/runtime/src/visibility_corpus.rs:
// plans × principals × rows, checked against the SQL compiler (on Durable
// Object SQLite through K01), the interpreter and the compiled class
// programs. Same plans, same principals, same rows, same order.

import { ParentAclIndex } from "../acl-index.ts";
import type { JsonValue } from "../json.ts";
import { parsePlan, type VisibilityPlan } from "../plan.ts";
import { InheritedParent, type ResolvedVisibility } from "../resolved.ts";

export const TASK_COLUMNS = ["_id", "workspaceId", "spotId", "createdBy", "createdVia", "deletedAt"] as const;
export const NOTE_COLUMNS = ["_id", "taskId", "authorId"] as const;

export interface CorpusPlan {
  name: string;
  plan: VisibilityPlan;
  /** Whether the compiled evaluator is expected to fall back. */
  fallback: boolean;
}

export interface Principal {
  name: string;
  role: string;
  permissions: JsonValue;
  sets: Map<string, Set<string>>;
}

export interface Corpus {
  plans: CorpusPlan[];
  principals: Principal[];
  tasks: JsonValue[];
  notes: JsonValue[];
}

export const PERMISSION_TABLES = ["memberTeams", "workspaceTeams", "memberSpots", "members", "workspaces"];

export function tasksSets(): Record<string, JsonValue> {
  return {
    workspaces: { kind: "principal", table: "workspaceTeams", select: "workspaceId",
      joins: [{ table: "memberTeams", leftColumn: "teamId", rightColumn: "teamId" }],
      where: [{ table: "memberTeams", column: "memberId", context: "member.id" }] },
    spots: { kind: "principal", table: "memberSpots", select: "spotId", joins: [],
      where: [{ table: "memberSpots", column: "memberId", context: "member.id" }] },
    allSpots: { kind: "principal", table: "members", select: "id", joins: [],
      where: [{ table: "members", column: "id", context: "member.id" },
              { table: "members", column: "see_all_spots", value: { literal: true } }] },
    personalWorkspaces: { kind: "principal", table: "workspaces", select: "_id", joins: [],
      where: [{ table: "workspaces", column: "type", value: { literal: "PERSONAL" } }] },
    ownedPersonal: { kind: "principal", table: "workspaces", select: "_id", joins: [],
      where: [{ table: "workspaces", column: "type", value: { literal: "PERSONAL" } },
              { table: "workspaces", column: "createdBy", context: "member.id" }] },
    assigned: { kind: "grant", table: "taskUsers", select: "taskId", joins: [],
      where: [{ table: "taskUsers", column: "userId", context: "member.id" }] },
  };
}

/** The Whagons task rule, reduced to every operator shape it uses. */
export function tasksPlan(): VisibilityPlan {
  return parsePlan({
    version: 2, table: "tasks", key: "_id",
    permissionTables: PERMISSION_TABLES,
    sets: tasksSets(),
    where: { operator: "or", children: [
      { operator: "inSet", column: "_id", set: "assigned" },
      { operator: "and", children: [
        { operator: "inSet", column: "workspaceId", set: "ownedPersonal" }] },
      { operator: "and", children: [
        { operator: "not", children: [
          { operator: "inSet", column: "workspaceId", set: "personalWorkspaces" }] },
        { operator: "or", children: [
          { operator: "permission", value: "tasks.viewAll" },
          { operator: "role", value: "system_admin" },
          { operator: "and", children: [
            { operator: "eq", column: "spotId", value: { literal: null } },
            { operator: "inSet", column: "workspaceId", set: "workspaces" }] },
          { operator: "and", children: [
            { operator: "eq", column: "spotId", value: { literal: "" } },
            { operator: "inSet", column: "workspaceId", set: "workspaces" }] },
          { operator: "and", children: [
            { operator: "inSet", column: "workspaceId", set: "workspaces" },
            { operator: "inSet", column: "spotId", set: "spots" }] },
          { operator: "and", children: [
            { operator: "inSet", column: "workspaceId", set: "workspaces" },
            { operator: "nonEmpty", set: "allSpots" }] },
          { operator: "and", children: [
            { operator: "eqContext", column: "createdBy", context: "member.id" },
            { operator: "eq", column: "createdVia", value: { literal: "finding" } }] },
        ] },
      ] },
    ] },
  });
}

export function corpus(): Corpus {
  const tasks = (predicate: JsonValue) => parsePlan({ version: 2, table: "tasks", key: "_id", permissionTables: PERMISSION_TABLES, sets: tasksSets(), where: predicate });
  const notes = (predicate: JsonValue) => parsePlan({ version: 2, table: "taskNotes", key: "_id", permissionTables: PERMISSION_TABLES, sets: tasksSets(), where: predicate });
  const plans: CorpusPlan[] = [
    { name: "tasks", plan: tasksPlan(), fallback: false },
    { name: "public", plan: tasks({ operator: "public" }), fallback: false },
    {
      name: "roles",
      plan: tasks({ operator: "and", children: [
        { operator: "or", children: [
          { operator: "role", value: "admin" },
          { operator: "permission", value: "tasks.viewAll" }] },
        { operator: "not", children: [{ operator: "role", value: "guest" }] }] }),
      fallback: false,
    },
    { name: "owner", plan: tasks({ operator: "eqContext", column: "createdBy", context: "member.id" }), fallback: false },
    {
      name: "notOwnerOrDeleted",
      plan: tasks({ operator: "and", children: [
        { operator: "not", children: [
          { operator: "eqContext", column: "createdBy", context: "member.id" }] },
        { operator: "not", children: [
          { operator: "eq", column: "deletedAt", value: { literal: null } }] },
        { operator: "not", children: [{ operator: "not", children: [
          { operator: "inSet", column: "workspaceId", set: "workspaces" }] }] }] }),
      fallback: false,
    },
    {
      name: "emptyNonEmpty",
      plan: tasks({ operator: "or", children: [
        { operator: "not", children: [{ operator: "public" }] },
        { operator: "nonEmpty", set: "spots" }] }),
      fallback: false,
    },
    {
      name: "noteInherits",
      plan: notes({ operator: "or", children: [
        { operator: "inherits", column: "taskId", table: "tasks", key: "_id",
          onMissing: { operator: "or", children: [
            { operator: "role", value: "system_admin" },
            { operator: "inSet", column: "taskId", set: "assigned" }] } },
        { operator: "eqContext", column: "authorId", context: "member.id" }] }),
      fallback: false,
    },
    {
      name: "noteOverride",
      plan: notes({ operator: "inherits", column: "taskId", table: "tasks", key: "_id",
        where: [{ column: "deletedAt", value: { literal: null } }],
        predicate: { operator: "or", children: [
          { operator: "eqContext", column: "createdBy", context: "member.id" },
          { operator: "inSet", column: "_id", set: "assigned" },
          { operator: "and", children: [
            { operator: "inSet", column: "workspaceId", set: "workspaces" },
            { operator: "not", children: [
              { operator: "eq", column: "spotId", value: { literal: "s2" } }] }] }] } }),
      fallback: false,
    },
  ];

  const principal = (name: string, role: string, permissions: JsonValue, workspaces: string[], spots: string[], allSpots: boolean, owned: string[], assigned: string[]): Principal => ({
    name,
    role,
    permissions,
    sets: new Map([
      ["workspaces", new Set(workspaces)],
      ["spots", new Set(spots)],
      ["allSpots", new Set(allSpots ? [name] : [])],
      ["personalWorkspaces", new Set(["p1", "p2"])],
      ["ownedPersonal", new Set(owned)],
      ["assigned", new Set(assigned)],
    ]),
  });
  const principals = [
    principal("alice", "member", {}, ["w1"], ["s1"], false, ["p1"], ["t-w2-s2-bob-web-0"]),
    // Same shared tokens as alice: must share alice's class.
    principal("carol", "member", {}, ["w1"], ["s1"], false, [], []),
    principal("bob", "member", {}, ["w1", "w2"], [], true, ["p2"], []),
    principal("dave", "system_admin", {}, ["w1"], ["s1"], false, [], []),
    principal("erin", "member", { "tasks.viewAll": true }, [], [], false, [], []),
    principal("frank", "member", {}, [], [], false, [], []),
    principal("gina", "guest", { "tasks.viewAll": "1" }, ["w2"], ["s2"], false, [], []),
    principal("hank", "admin", { "tasks.viewAll": false }, ["w2"], ["s1", "s2"], false, [], ["missing-task"]),
    // Principal changes: alice after joining w2, after a new grant, after losing her spot.
    principal("alice", "member", {}, ["w1", "w2"], ["s1"], false, ["p1"], ["t-w2-s2-bob-web-0"]),
    principal("alice", "member", {}, ["w1"], ["s1"], false, ["p1"], ["t-w2-s2-bob-web-0", "t-w3-null-null-web-1"]),
    principal("alice", "member", {}, ["w1"], [], false, ["p1"], []),
    // Another system admin with other teams: team sets cannot change an admin's decision, so both admins share one class.
    principal("ivan", "system_admin", {}, ["w2"], ["s2"], true, [], []),
  ];

  const taskRows: JsonValue[] = [];
  for (const workspace of ["w1", "w2", "w3", "p1", "p2", null]) {
    for (const spot of [null, "", "s1", "s2"]) {
      for (const creator of ["alice", "bob", null]) {
        for (const [via, deleted] of [["finding", 0], ["web", 0], ["web", 1]] as const) {
          const id = `t-${workspace ?? "null"}-${spot === null ? "null" : spot === "" ? "blank" : spot}-${creator ?? "null"}-${via}-${deleted}`;
          taskRows.push({ _id: id, workspaceId: workspace, spotId: spot, createdBy: creator, createdVia: via, deletedAt: deleted === 1 ? "2026-10-01" : null });
        }
      }
    }
  }
  const parents: Array<string | null> = [
    ...taskRows.filter((_, index) => index % 7 === 0).map((row) => (row as { _id: string })._id),
    "missing-task",
    "t-gone",
    null,
  ];
  const noteRows: JsonValue[] = [];
  parents.forEach((parent, index) => {
    for (const author of ["alice", "bob"]) noteRows.push({ _id: `n-${index}-${author}`, taskId: parent, authorId: author });
  });
  return { plans, principals, tasks: taskRows, notes: noteRows };
}

export function tasksIndex(corpus: Corpus): ParentAclIndex {
  return ParentAclIndex.fromSnapshot(1, corpus.tasks.map((row) => [(row as { _id: string })._id, row] as [string, JsonValue]));
}

/** The principal as the resolver would produce it for `plan`. */
export function resolve(corpus: Corpus, principal: Principal, plan: VisibilityPlan, index: ParentAclIndex): ResolvedVisibility {
  const resolved: ResolvedVisibility = {
    ephemeralMember: false,
    revision: 1,
    direct: { "member.id": principal.name, "account.id": `acct-${principal.name}`, "tenant.id": "t" },
    role: principal.role,
    permissions: principal.permissions,
    sets: new Map([...principal.sets].map(([name, tokens]) => [name, new Set(tokens)])),
    fingerprint: "",
    inherited: new Map(),
    indexes: new Map(),
  };
  if (plan.table === "taskNotes") {
    const parentPlan = corpus.plans[0]!.plan;
    resolved.inherited.set("tasks", new InheritedParent(parentPlan, resolve(corpus, principal, parentPlan, index), index));
  }
  return resolved;
}
