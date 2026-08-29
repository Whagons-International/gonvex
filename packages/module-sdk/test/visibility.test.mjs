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
