// The real Whagons visibility plans (the Rust perf fixture,
// rust/crates/runtime/src/perf_fixture/whagons_module.json): the manifest
// validates, every plan compiles (no fallback), the SQL form stays under the
// platform's 100 bound parameters, and SQL, interpreter and compiled agree
// on seeded random principals and rows.

import { describe, expect, it } from "vitest";
import manifest from "../../../../rust/crates/runtime/src/perf_fixture/whagons_module.json" with { type: "json" };
import { objectFor } from "../../storage/test/helpers";
import { ParentAclIndex } from "../acl-index.ts";
import { compiled, EvalMemo, memberVisibility, rowJson, rowValue } from "../class.ts";
import type { JsonValue } from "../json.ts";
import { rowMatches } from "../mask.ts";
import { aclColumns, indexColumns, inheritedTables, parsePlan, validateManifest, type VisibilityExpression, type VisibilityPlan } from "../plan.ts";
import { InheritedParent, type ResolvedVisibility } from "../resolved.ts";
import { compilePredicate } from "../sql.ts";

const plans = new Map(Object.entries((manifest as { visibility: Record<string, unknown> }).visibility).map(([table, plan]) => [table, parsePlan(plan)]));

/** mulberry32: a small seeded generator, so a failure reproduces. */
function random(seed: number): () => number {
  let state = seed >>> 0;
  return () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let t = state;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

const TOKENS = Array.from({ length: 12 }, (_, n) => `t${n + 1}`);
const MEMBERS = ["m1", "m2", "m3", "m4"];
const ROLES = ["member", "admin", "system_admin", "guest", "owner", "tenant_owner"];
const PERMISSIONS = ["tasks.viewAll", "view-users", "manage-broadcasts"];

/** Literals a plan compares each column to, so random rows hit them. */
function literals(plan: VisibilityPlan): Map<string, JsonValue[]> {
  const found = new Map<string, JsonValue[]>();
  const add = (column: string, value: JsonValue | undefined) => {
    if (value === undefined || typeof value !== "object" || value === null || !("literal" in value)) return;
    found.set(column, [...(found.get(column) ?? []), (value as { literal: JsonValue }).literal]);
  };
  const walk = (expression: VisibilityExpression) => {
    if (expression.operator === "eq") add(expression.column, expression.value);
    for (const constraint of expression.parentWhere) add(constraint.column, constraint.value);
    for (const child of expression.children) walk(child);
    if (expression.onMissing) walk(expression.onMissing);
    if (expression.predicate) walk(expression.predicate);
  };
  walk(plan.predicate);
  return found;
}

function value(next: () => number, column: string, hints: Map<string, JsonValue[]>): JsonValue {
  const hint = hints.get(column);
  const roll = next();
  if (hint && roll < 0.3) return hint[Math.floor(next() * hint.length)]!;
  if (roll < 0.4) return null;
  if (column === "createdBy" || column === "id" || column.endsWith("UserId") || column === "memberId" || column === "userId") return MEMBERS[Math.floor(next() * MEMBERS.length)]!;
  return TOKENS[Math.floor(next() * TOKENS.length)]!;
}

function principal(next: () => number, member: string, setNames: readonly string[]): ResolvedVisibility {
  const permissions: Record<string, JsonValue> = {};
  for (const permission of PERMISSIONS) if (next() < 0.2) permissions[permission] = next() < 0.5 ? true : "1";
  return {
    ephemeralMember: false,
    revision: 1,
    direct: { "member.id": member, "account.id": `acct-${member}`, "tenant.id": "t" },
    role: ROLES[Math.floor(next() * ROLES.length)]!,
    permissions,
    sets: new Map(setNames.map((name) => [name, new Set(TOKENS.filter(() => next() < 0.25))])),
    fingerprint: "",
    inherited: new Map(),
    indexes: new Map(),
  };
}

describe("Whagons plans (perf fixture)", () => {
  it("the manifest validates and every plan compiles without fallback", () => {
    expect(plans.size).toBe(17);
    validateManifest(plans);
    for (const [table, plan] of plans) expect(compiled(plan).unsupported, table).toBeUndefined();
  });

  it("SQL, interpreter and compiled agree on random principals and rows; SQL stays under 100 parameters", async () => {
    const next = random(9);
    const setNames = [...new Set([...plans.values()].flatMap((plan) => [...plan.sets.keys()]))];
    const tasks = plans.get("tasks")!;
    const tableColumns = new Map<string, string[]>();
    for (const [table, plan] of plans) {
      const columns = new Set([...aclColumns(plan), ...indexColumns(plans, table, plan.key)]);
      if (table === "members") for (const column of ["id", "account_id", "status", "role", "permissions"]) columns.add(column);
      tableColumns.set(table, [...columns]);
    }
    if (!tableColumns.has("members")) tableColumns.set("members", ["id", "account_id", "status", "role", "permissions"]);
    // Rows: tasks keys are unique (a parent index is keyed by them); other tables use __row as identity.
    const rows = new Map<string, Array<Record<string, JsonValue>>>();
    for (const [table, plan] of plans) {
      const hints = literals(plan);
      const generated: Array<Record<string, JsonValue>> = [];
      for (let n = 0; n < (table === "tasks" ? TOKENS.length + 40 : 120); n++) {
        const row: Record<string, JsonValue> = { __row: `${table}-${n}` };
        for (const column of tableColumns.get(table)!) row[column] = value(next, column, hints);
        if (table === "tasks") row["_id"] = n < TOKENS.length ? TOKENS[n]! : `task-${n}`;
        if (table === "members") Object.assign(row, { account_id: null, status: "invited" });
        generated.push(row);
      }
      rows.set(table, generated);
    }
    const principals = Array.from({ length: 24 }, (_, n) => principal(next, MEMBERS[n % MEMBERS.length]!, setNames));
    // Member rows for the role/permission subqueries: one per principal, replaced per check.
    const index = ParentAclIndex.fromSnapshot(1, rows.get("tasks")!.map((row) => [String(row["_id"]), row] as [string, JsonValue]));
    const object = objectFor();
    const created = await object.invoke("reducer", async (session) => {
      for (const [table, columns] of tableColumns) {
        session.run(`CREATE TABLE "${table}" ("__row", ${columns.filter((c) => c !== "__row").map((c) => `"${c}"`).join(", ")})`);
        for (const row of rows.get(table) ?? []) {
          const names = ["__row", ...columns];
          session.run(`INSERT INTO "${table}" (${names.map((c) => `"${c}"`).join(", ")}) VALUES (${names.map((_, i) => `$${i + 1}`).join(", ")})`, names.map((c) => row[c] ?? null));
        }
      }
    });
    if (!created.ok) throw created.error;
    let checks = 0;
    let maxParameters = 0;
    for (const [number, resolved] of principals.entries()) {
      const member = resolved.direct["member.id"]!;
      const result = await object.invoke("reducer", async (session) => {
        session.run(`DELETE FROM members WHERE account_id IS NOT NULL`);
        session.run(`INSERT INTO members ("__row", id, account_id, status, role, permissions) VALUES ($1, $2, $3, 'active', $4, $5)`, [`member-${number}`, member, `acct-${member}`, resolved.role, JSON.stringify(resolved.permissions)]);
        const memo = new EvalMemo();
        for (const [table, plan] of plans) {
          const own: ResolvedVisibility = { ...resolved, inherited: new Map(), indexes: new Map() };
          // Every inheriting Whagons plan inherits tasks.
          if (inheritedTables(plan).includes("tasks")) own.inherited.set("tasks", new InheritedParent(tasks, { ...resolved, inherited: new Map(), indexes: new Map() }, index));
          const parameters: JsonValue[] = [];
          const predicate = compilePredicate(plan, own, "r", parameters);
          maxParameters = Math.max(maxParameters, parameters.length);
          const expected = new Set(session.run(`SELECT r."__row" FROM "${table}" AS r WHERE ${predicate}`, parameters).rows.map((row) => String(row[0])));
          const handle = memberVisibility(plan, own, `whagons-${table}`);
          if (handle.kind !== "compiled") throw new Error(`${table}: fallback`);
          const all = session.run(`SELECT * FROM "${table}"`);
          for (const row of all.rows.map((cells) => Object.fromEntries(all.columnNames.map((name, i) => [name, cells[i] as JsonValue])))) {
            const id = String(row["__row"]);
            const label = `${table}/${number}/${id}`;
            expect(rowMatches(plan, own, row), `${label} interpreted`).toBe(expected.has(id));
            expect(handle.member.visible(rowValue(row)), `${label} compiled`).toBe(expected.has(id));
            expect(handle.member.visible(rowJson(JSON.stringify(row))), `${label} compiled-json`).toBe(expected.has(id));
            expect(handle.member.visibleMemo(memo, row), `${label} compiled-memo`).toBe(expected.has(id));
            checks += 4;
          }
          handle.member.release();
        }
      });
      if (!result.ok) throw result.error;
    }
    expect(checks).toBeGreaterThan(20_000);
    // Durable Object SQLite binds at most 100 parameters per statement (measured: 48, a task child plan).
    expect(maxParameters).toBeLessThan(100);
  });
});
