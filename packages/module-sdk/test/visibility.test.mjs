import assert from "node:assert/strict";
import test from "node:test";

import { createModule, visibility } from "../dist/index.js";

const validPlan = () => ({
  table: "tasks",
  key: "id",
  sets: {
    teams: {
      table: "memberTeams",
      select: "teamId",
      joins: [{ table: "members", leftColumn: "memberId", rightColumn: "id" }],
      where: [{ table: "members", column: "accountId", context: "account.id" }],
    },
  },
  where: {
    operator: "or",
    children: [
      { operator: "permission", value: "tasks:read" },
      {
        operator: "and",
        children: [
          { operator: "eqContext", column: "tenantId", context: "tenant.id" },
          { operator: "inSet", column: "teamId", set: "teams" },
        ],
      },
    ],
  },
});

test("visibility validates and deeply freezes a plan", () => {
  const plan = visibility(validPlan());

  assert.equal(plan.table, "tasks");
  assert.ok(Object.isFrozen(plan));
  assert.ok(Object.isFrozen(plan.sets));
  assert.ok(Object.isFrozen(plan.sets.teams));
  assert.ok(Object.isFrozen(plan.sets.teams.joins));
  assert.ok(Object.isFrozen(plan.sets.teams.joins[0]));
  assert.ok(Object.isFrozen(plan.where));
  assert.ok(Object.isFrozen(plan.where.children));
});

test("visibility requires an explicit rule and supports intentionally public tables", () => {
  assert.equal(visibility({ table: "statuses", key: "id", sets: {}, where: { operator: "public" } }).where.operator, "public");
  assert.throws(() => visibility({ table: "statuses", key: "id", sets: {} }), /where/);
});

test("visibility rejects unsupported operators, contexts, and incomplete expressions", () => {
  assert.throws(
    () => visibility({ ...validPlan(), where: { operator: "sql", value: "true" } }),
    /operator is unsupported/,
  );
  assert.throws(
    () => visibility({ ...validPlan(), where: { operator: "eqContext", column: "tenantId", context: "user.id" } }),
    /account\.id, member\.id, or tenant\.id/,
  );
  assert.throws(
    () => visibility({ ...validPlan(), where: { operator: "permission" } }),
    /value must be a non-empty string/,
  );
  assert.throws(
    () => visibility({ ...validPlan(), where: { operator: "inSet", column: "teamId", set: "missing" } }),
    /unknown visibility set missing/,
  );
  assert.throws(
    () => visibility({ ...validPlan(), where: { operator: "not", children: [] } }),
    /exactly one child/,
  );
});

test("visibility supports explicit self-join aliases and rejects ambiguous duplicates", () => {
  const plan = visibility({
    table: "userLiveLocations",
    key: "id",
    sets: {
      teammates: {
        table: "memberTeams",
        alias: "viewerTeams",
        select: "memberId",
        selectFrom: "peerTeams",
        joins: [{ table: "memberTeams", alias: "peerTeams", leftAlias: "viewerTeams", leftColumn: "teamId", rightColumn: "teamId" }],
        where: [{ table: "viewerTeams", column: "memberId", context: "member.id" }],
      },
    },
    where: { operator: "inSet", column: "memberId", set: "teammates" },
  });
  assert.equal(plan.sets.teammates.selectFrom, "peerTeams");
  assert.throws(() => visibility({
    ...plan,
    sets: { teammates: { ...plan.sets.teammates, alias: undefined } },
  }), /every occurrence requires an explicit alias/);
});

test("visibility supports literal predicates on source and joined rows", () => {
  const plan = visibility({
    table: "boardMessages",
    key: "id",
    sets: {
      publicMessages: {
        table: "boardMessages",
        select: "id",
        joins: [{ table: "boards", leftColumn: "boardId", rightColumn: "id" }],
        where: [{ table: "boards", column: "visibility", value: { literal: "public" } }],
      },
    },
    where: {
      operator: "or",
      children: [
        { operator: "eq", column: "visibility", value: { literal: "public" } },
        { operator: "inSet", column: "id", set: "publicMessages" },
      ],
    },
  });

  assert.deepEqual(plan.sets.publicMessages.where[0].value, { literal: "public" });
  assert.deepEqual(plan.where.children[0].value, { literal: "public" });
  assert.ok(Object.isFrozen(plan.sets.publicMessages.where[0].value));
  assert.ok(Object.isFrozen(plan.where.children[0].value));
});

test("visibility literal constraints require an explicit literal wrapper", () => {
  assert.throws(() => visibility({
    ...validPlan(),
    where: { operator: "eq", column: "visibility", value: "public" },
  }), /must be an object/);
  assert.throws(() => visibility({
    ...validPlan(),
    sets: {
      teams: {
        ...validPlan().sets.teams,
        where: [{ table: "members", column: "status" }],
      },
    },
  }), /requires exactly one of context or value/);
  assert.throws(() => visibility({
    ...validPlan(),
    sets: {
      teams: {
        ...validPlan().sets.teams,
        where: [{
          table: "members",
          column: "status",
          context: "member.id",
          value: { literal: "active" },
        }],
      },
    },
  }), /requires exactly one of context or value/);
});

test("ModuleBuilder emits plans keyed by their source table", () => {
  const module = createModule({ name: "whagons", version: "1" });
  const plan = module.visibility(validPlan());

  assert.equal(module.manifest().visibility.tasks, plan);
  assert.throws(() => module.visibility(validPlan()), /duplicate visibility plan: tasks/);
});


test("visibility validates principal nonEmpty flags without a row column", () => {
  const where = { operator: "nonEmpty", set: "teams" };
  assert.deepEqual(visibility({ ...validPlan(), where }).where, where);
  assert.throws(() => visibility({ ...validPlan(), where: { operator: "nonEmpty", set: "missing" } }), /unknown visibility set missing/);
  assert.throws(() => visibility({ ...validPlan(), where: { ...where, column: "teamId" } }), /column/);
});


test("version 2 freezes permission declarations and supports inherited masks", () => {
  const plan = visibility({ ...validPlan(), version: 2, permissionTables: ["memberTeams", "members"], sets: { teams: { ...validPlan().sets.teams, kind: "principal" } }, where: { operator: "inherits", column: "taskId", table: "tasks", key: "id" } });
  assert.ok(Object.isFrozen(plan.permissionTables));
  assert.equal(plan.where.table, "tasks");
});

test("version 2 rejects self-reading and data joins with a migration hint", () => {
  const make = (set) => ({ ...validPlan(), version: 2, permissionTables: ["members"], sets: { teams: set } });
  assert.throws(() => visibility(make({ ...validPlan().sets.teams, kind: "principal" })), /sets.teams.*permission.*inherits/);
  assert.throws(() => visibility(make({ ...validPlan().sets.teams, kind: "principal", table: "tasks" })), /sets.teams.*own source.*inherits/);
  assert.throws(() => visibility(make({ ...validPlan().sets.teams, kind: "grant", joins: [{ table: "documents", leftColumn: "id", rightColumn: "id" }] })), /sets.teams.*data table documents.*inherits/);
});

test("version 2 accepts directly anchored grants and rejects global grants", () => {
  const plan = { version: 2, table: "tasks", key: "id", permissionTables: [], sets: { assigned: { kind: "grant", table: "taskUsers", select: "taskId", joins: [], where: [{ table: "taskUsers", column: "userId", context: "member.id" }] } }, where: { operator: "inSet", column: "id", set: "assigned" } };
  assert.equal(visibility(plan).sets.assigned.kind, "grant");
  assert.throws(() => visibility({ ...plan, sets: { assigned: { ...plan.sets.assigned, where: [] } } }), /assigned.*anchor/);
});


test("inherits supports a frozen orphan-only fallback", () => {
  const where = { operator: "inherits", column: "taskId", table: "tasks", key: "id", onMissing: { operator: "role", value: "system_admin" } };
  const plan = visibility({ ...validPlan(), where });
  assert.deepEqual(plan.where, where);
  assert.ok(Object.isFrozen(plan.where.onMissing));
  assert.throws(() => visibility({ ...validPlan(), where: { ...where, onMissing: { operator: "unknown" } } }), /onMissing.operator/);
});

test("module manifests reject inheritance cycles and chains deeper than four edges", () => {
  const make = (table, parent) => ({ version: 2, table, key: "id", permissionTables: [], sets: {}, where: parent ? { operator: "inherits", column: "parentId", table: parent, key: "id" } : { operator: "public" } });
  const cycle = createModule({ name: "cycle", version: "1" });
  cycle.visibility(make("a", "b"));
  cycle.visibility(make("b", "a"));
  assert.throws(() => cycle.manifest(), /inherits cycle:.*a.*b.*a/);
  for (const edges of [4, 5]) {
    const module = createModule({ name: "chain", version: "1" });
    for (let index = 0; index <= edges; index++) module.visibility(make(`t${index}`, index < edges ? `t${index + 1}` : undefined));
    if (edges === 4) assert.doesNotThrow(() => module.manifest());
    else assert.throws(() => module.manifest(), /inherits depth exceeds 4/);
  }
});


test("anchored grants may read their source anywhere, but neither kind may join the plan source", () => {
  const assigned = { kind: "grant", table: "taskUsers", select: "taskId", joins: [], where: [{ table: "taskUsers", column: "userId", context: "member.id" }] };
  const base = { version: 2, table: "taskUsers", key: "_id", permissionTables: [], sets: { assigned }, where: { operator: "inSet", column: "taskId", set: "assigned" } };
  assert.equal(visibility(base).sets.assigned.table, "taskUsers");
  const fallback = { ...base, where: { operator: "inherits", column: "taskId", table: "tasks", key: "_id", onMissing: base.where } };
  assert.equal(visibility(fallback).where.onMissing.set, "assigned");
  assert.throws(() => visibility({ ...base, sets: { assigned: { ...assigned, kind: "principal" } } }), /assigned.*own source/);
  for (const kind of ["principal", "grant"]) {
    assert.throws(() => visibility({ ...base, sets: { assigned: { ...assigned, kind, table: "memberTeams", joins: [{ table: "taskUsers", leftColumn: "memberId", rightColumn: "userId" }] } } }), /assigned.*own source/);
  }
});

test("derived sets map a token set through a foreign keyed index and freeze literal filters", () => {
  const base = validPlan();
  const plan = visibility({ ...base, version: 2, permissionTables: ["memberTeams", "members"], sets: {
    teams: { ...base.sets.teams, kind: "principal" },
    mapped: { kind: "derived", from: "teams", table: "teams", key: "id", select: "workspaceId", where: [{ column: "deletedAt", value: { literal: null } }] },
    twice: { kind: "derived", from: "mapped", table: "workspaces", key: "id", select: "categoryId", joins: [], where: [] },
  }, where: { operator: "nonEmpty", set: "twice" } });
  assert.equal(plan.sets.mapped.from, "teams");
  assert.equal(plan.sets.mapped.joins, undefined);
  assert.ok(Object.isFrozen(plan.sets.mapped.where[0].value));
});

test("derived rejects unknown sources, own-table mappings, joins, contexts, cycles and overdeep chains", () => {
  const derived = { kind: "derived", from: "seed", table: "teams", key: "id", select: "workspaceId", where: [] };
  const seed = { kind: "principal", table: "members", select: "id", joins: [], where: [] };
  const make = (sets) => ({ version: 2, table: "tasks", key: "id", permissionTables: ["members"], sets: { seed, ...sets }, where: { operator: "public" } });
  for (const [patch, message] of [
    [{ from: "missing" }, /unknown set missing/],
    [{ table: "tasks" }, /another table/],
    [{ key: "" }, /key/],
    [{ joins: [{ table: "members", leftColumn: "id", rightColumn: "id" }] }, /joins/],
    [{ where: [{ column: "id", context: "member.id" }] }, /context/],
    [{ where: [{ table: "teams", column: "id", value: { literal: null } }] }, /table/],
    [{ where: [{ column: "id", value: null }] }, /value/],
  ]) assert.throws(() => visibility(make({ mapped: { ...derived, ...patch } })), message);
  assert.throws(() => visibility(make({ a: { ...derived, from: "b" }, b: { ...derived, from: "a" } })), /cycle/);
  assert.throws(() => visibility(make({ a: { ...derived }, b: { ...derived, from: "a" }, c: { ...derived, from: "b" } })), /limited to two/);
});
