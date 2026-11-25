// The two visibility declarations and replica definition from the V01 app
// (source 3ad4fa47, manifest f8336ddf). No identities, rows or credentials.
// The unchanged compiler produced 148 binds for this shape, even with {} args.
import { describe, expect, it } from "vitest";
import fixture from "./fixtures/task-relations-policy.json";
import { parseDefinition } from "../../delivery/definition";
import { scanPlan, scanWindow, STATEMENT_ROWS } from "../../delivery/scan";
import { assignSlots, StatementWriter } from "../../storage/parameters";
import { tokenize } from "../../storage/lexer";
import { scalarParameter, type JsonKind, type StorageSession } from "../../storage";
import { objectFor } from "../../storage/test/helpers";
import { ParentAclIndex } from "../acl-index";
import { memberVisibility, rowValue } from "../class";
import type { JsonObject, JsonValue } from "../json";
import { rowMatches } from "../mask";
import { aclColumns, parsePlan } from "../plan";
import { emptyResolution, InheritedParent } from "../resolved";
import { compilePredicate, compileSet } from "../sql";

const parent = parsePlan(fixture.visibility.tasks);
const plan = parsePlan(fixture.visibility.taskRelations);
const definition = parseDefinition(fixture.replica)!;
const kinds = new Map<string, JsonKind>([["createdAt", "integer"], ["values", "text"]]);

function principal(member = "alice") {
  // Different sets must not be merged just because they overlap or are empty
  // today. Use distinct nonempty sets, plus exact equality with reverse order.
  const resolve = (p: typeof plan) => ({
    ...emptyResolution({ "account.id": `account-${member}`, "member.id": member, "tenant.id": "tenant" }, "admin", {}),
    sets: new Map([...p.sets.keys()].map((name, i) => [name, new Set([`token-${i}`])])),
  });
  const own = resolve(plan), inherited = resolve(parent);
  own.inherited.set("tasks", new InheritedParent(parent, inherited, ParentAclIndex.fromSnapshot(1, parents.map((r) => [String(r._id), r]))));
  own.sets.set("personalContextOwnerTasks", new Set(["missing-granted"]));
  return own;
}

const parents: JsonObject[] = [
  { _id: "allowed", createdBy: "alice", workspaceId: "w", spotId: "s" },
  { _id: "denied", createdBy: "bob", workspaceId: "w", spotId: "s" },
];

const rows: JsonObject[] = Array.from({ length: STATEMENT_ROWS * 5 + 17 }, (_, n) => {
  const [taskId, relatedTaskId] = [
    ["allowed", "denied"], ["denied", "denied"], ["missing-granted", null],
    ["missing-denied", null], [null, null], ["denied", "allowed"],
  ][n % 6]!;
  return {
    _id: `relation-${String(n).padStart(4, "0")}`, taskId: taskId!, relatedTaskId: relatedTaskId!,
    relationType: "linked", customFieldId: null, createdBy: "bob",
    createdAt: n % 11 === 0 ? null : 2 ** 40 + Math.floor(n / 4),
    parentTaskName: null, values: "00ff80", deletedAt: n % 13 === 0 ? "deleted" : null,
  };
});

async function seeded() {
  const object = objectFor();
  const result = await object.invoke("reducer", async (db) => {
    db.run("CREATE TABLE members (account_id TEXT, status TEXT, role TEXT, permissions JSON)");
    db.run("INSERT INTO members VALUES ('account-alice','active','admin','{}'), ('account-bob','active','admin','{}')");
    for (const p of [parent, plan]) {
      const columns = [...new Set([...aclColumns(p), ...(p === plan ? definition.columns : [])])];
      db.run(`CREATE TABLE "${p.table}" (${columns.map((c) => `"${c}" ${c === "createdAt" ? "INTEGER" : "TEXT"}`).join(", ")})`);
      for (const row of p === parent ? parents : rows) {
        db.run(`INSERT INTO "${p.table}" (${columns.map((c) => `"${c}"`).join(", ")}) VALUES (${columns.map((_, i) => `$${i + 1}`).join(", ")})`, columns.map((c) => row[c] ?? null));
      }
    }
  });
  if (!result.ok) throw result.error;
  // A real SQLite BLOB in a selected column; projection must remain hex text.
  await object.sql(`UPDATE taskRelations SET "values" = x'00ff80'`);
  const read = async <T>(body: (db: StorageSession) => T): Promise<T> => {
    const result = await object.invoke("query", async (db) => body(db));
    if (!result.ok) throw result.error;
    expect(result.stats.rowsWritten).toBe(0);
    return result.value;
  };
  return { object, read };
}

function bindCount(sql: string, values: readonly JsonValue[]): number {
  const tokens = tokenize(sql);
  const writer = new StatementWriter(sql, tokens, assignSlots(tokens, values.map(scalarParameter)));
  writer.emit(0, tokens.length - 1);
  return writer.binds.length;
}

describe("visibility parameter reuse on Durable Object SQLite", () => {
  it("runs the V01 inherited policy with exact authorized rows across keyset pages in both directions", async () => {
    const { read, object } = await seeded();
    for (const member of ["alice", "bob"]) {
      const resolved = principal(member);
      const handle = memberVisibility(plan, resolved, `bindings-${member}`);
      expect(handle.kind).toBe("compiled");
      if (handle.kind !== "compiled") throw new Error("unsupported fixture");
      const allowed = rows.filter((row) => row.deletedAt === null && rowMatches(plan, resolved, row));
      expect(allowed.length).toBeGreaterThan(STATEMENT_ROWS);
      expect(allowed.length).toBeLessThan(rows.length - STATEMENT_ROWS);
      for (const row of rows) expect(handle.member.visible(rowValue(row))).toBe(rowMatches(plan, resolved, row));
      handle.member.release();
      for (const direction of ["asc", "desc"]) {
        for (const maxRows of [0, 275]) {
          const def = { ...definition, orderDirection: direction, maxRows };
          const scan = scanPlan(def, plan, resolved, {}, kinds);
          // Count actual emitted SQLite slots, not just the input value array.
          expect(bindCount(`${scan.select} WHERE ${scan.where}`, scan.parameters)).toBeLessThan(100);
          expect(scan.parameters.length).toBeLessThan(100);
          const progress = { statements: 0, rowsRead: 0 };
          const got: JsonObject[] = [];
          const window = await scanWindow(read, def, scan, (row) => { got.push(row.value); }, { progress });
          // Independently sort/filter in SQLite using only explicit expected ids.
          // The authorization oracle is the interpreter + compiled evaluator.
          const expected = await read((db) => db.run(`SELECT _id FROM taskRelations ORDER BY createdAt ${direction}, _id ${direction}`).rows
            .map(([key]) => allowed.find((row) => row._id === key)).filter((row): row is JsonObject => row !== undefined));
          expect(got).toEqual((maxRows ? expected.slice(0, maxRows) : expected));
          expect(window.truncated).toBe(maxRows > 0);
          expect(progress.statements).toBeGreaterThan(1);
          expect(progress.statements).toBeLessThanOrEqual(4);
          expect(progress.rowsRead).toBeGreaterThan(got.length);
        }
      }
    }
    await object.sql("UPDATE members SET status = 'suspended'");
    const got: string[] = [];
    await scanWindow(read, definition, scanPlan(definition, plan, principal(), {}, kinds), (row) => { got.push(row.key); });
    expect(got).toEqual([]); // The membership guard still denies every row.
  });

  it("preserves exact sets, subsets, empty sets, and fresh principal values", async () => {
    const { read } = await seeded();
    for (const tokens of [[], ["allowed"], ["allowed", "denied"], ["denied", "allowed"]]) {
      const resolved = principal();
      const inherited = resolved.inherited.get("tasks")!.resolved;
      const sets = new Map(inherited.sets);
      inherited.sets = sets;
      sets.set("personalContextOwnerTasks", new Set(tokens));
      sets.set("personalContextMemberTasks", new Set(["allowed"]));
      sets.set("personalContextTeamTasks", new Set());
      const parameters: JsonValue[] = ["prefix"];
      const predicate = compilePredicate(plan, resolved, "r", parameters);
      expect(parameters[0]).toBe("prefix");
      const expected = rows.filter((row) => rowMatches(plan, resolved, row)).map((row) => row._id);
      const got = await read((db) => db.run(`SELECT r._id FROM taskRelations AS r WHERE $1 = 'prefix' AND (${predicate}) ORDER BY r._id`, parameters).rows.flat());
      expect(got).toEqual(expected);
    }
  });

  it("reuses typed literals and contexts in legacy joined sets without changing their storage classes", async () => {
    const object = objectFor();
    await object.sql("CREATE TABLE grants (token TEXT, member TEXT, enabled BOOLEAN, n INTEGER, label TEXT, gone TEXT)");
    await object.sql("CREATE TABLE links (token TEXT, member TEXT)");
    await object.sql("INSERT INTO grants VALUES ('yes','alice',1,1099511627777,'1',NULL), ('no','bob',1,1099511627777,'1',NULL)");
    await object.sql("INSERT INTO links VALUES ('yes','alice'), ('no','bob')");
    const raw = {
      table: "items", key: "_id", sets: {
        joined: { table: "grants", select: "token", joins: [{ table: "links", leftColumn: "token", rightColumn: "token" }], where: [
          { table: "grants", column: "member", context: "member.id" }, { table: "links", column: "member", context: "member.id" },
          ...Array.from({ length: 8 }, () => [
            { column: "enabled", value: { literal: true } }, { column: "n", value: { literal: 1099511627777 } },
            { column: "label", value: { literal: "1" } }, { column: "gone", value: { literal: null } },
          ]).flat(),
        ] },
      }, where: { operator: "or", children: [{ operator: "inSet", column: "_id", set: "joined" }, { operator: "nonEmpty", set: "joined" }] },
    };
    const p = parsePlan(raw);
    const direct = { "member.id": "alice" };
    const { sql, parameters } = compileSet(p.sets.get("joined")!, direct);
    expect(parameters).toEqual(["alice", true, 1099511627777, "1"]);
    const result = await object.invoke("query", async (db) => {
      expect(db.run(sql, parameters).rows).toEqual([["yes"]]);
      const resolved = emptyResolution(direct, "member", {});
      const args: JsonValue[] = [];
      const predicate = compilePredicate(p, resolved, "r", args);
      expect(args).toEqual(parameters); // shared across inSet and nonEmpty
      expect(db.run(`SELECT _id FROM (SELECT 'x' AS _id) AS r WHERE ${predicate}`, args).rows).toEqual([["x"]]);
      const typed = compilePredicate(parsePlan({ table: "typed", key: "_id", where: { operator: "and", children: [
        { operator: "eq", column: "a", value: { literal: 1 } }, { operator: "eq", column: "b", value: { literal: true } },
        { operator: "eq", column: "c", value: { literal: "1" } }, { operator: "eq", column: "d", value: { literal: null } },
        { operator: "eq", column: "a", value: { literal: 1 } },
      ] } }), resolved, "r", args);
      expect(args.slice(-3)).toEqual([1, true, "1"]);
      expect(db.run(`SELECT typeof(a), typeof(b), typeof(c), typeof(d), typeof(e), hex(e) FROM (SELECT 1 AS a, 1 AS b, '1' AS c, NULL AS d, x'00ff80' AS e) AS r WHERE ${typed}`, args).rows)
        .toEqual([["integer", "integer", "text", "null", "blob", "00FF80"]]);
      expect(db.run("SELECT typeof($1), typeof($2), typeof($3), typeof($4)", [true, null, 1099511627777, "1"]).rows)
        .toEqual([["integer", "null", "integer", "text"]]);
    });
    if (!result.ok) throw result.error;
  });

  it("emits deterministic SQL aliases across repeated compilePredicate calls for SQLite plan caching", () => {
    const resolved = principal("alice");
    const params1: JsonValue[] = [];
    const params2: JsonValue[] = [];
    const sql1 = compilePredicate(plan, resolved, "r", params1);
    const sql2 = compilePredicate(plan, resolved, "r", params2);
    expect(sql1).toBe(sql2);
    expect(params1).toEqual(params2);
  });
});
