// The Rust visibility corpus (visibility_corpus.rs), all four tests: the SQL
// compiler on Durable Object SQLite (through K01's kernel SQL), the
// interpreter and the compiled class programs (value, raw JSON, memo) must
// agree on every plan × principal × row. Any disagreement is a leaked or a
// missing row.

import { describe, expect, it } from "vitest";
import { objectFor } from "../../storage/test/helpers";
import type { StorageSession } from "../../storage";
import { EvalMemo, memberVisibility, rowJson, rowValue, type MemberVisibility } from "../class.ts";
import type { JsonValue } from "../json.ts";
import { rowMatches } from "../mask.ts";
import { validatePlan, type VisibilityPlan } from "../plan.ts";
import type { ResolvedVisibility } from "../resolved.ts";
import { compilePredicate } from "../sql.ts";
import { corpus, NOTE_COLUMNS, resolve, TASK_COLUMNS, tasksIndex, type Corpus } from "./corpus.ts";

const idOf = (row: JsonValue) => (row as { _id: string })._id;

/** Creates the corpus tables and rows (all TEXT, as Rust's fixture). */
function loadDatabase(session: StorageSession, data: Corpus): void {
  const table = (name: string, columns: readonly string[]) => `CREATE TABLE "${name}" (${columns.map((column) => `"${column}" TEXT`).join(", ")})`;
  session.run(table("tasks", TASK_COLUMNS));
  session.run(table("taskNotes", NOTE_COLUMNS));
  session.run("CREATE TABLE members (id TEXT, account_id TEXT, status TEXT, role TEXT, permissions TEXT)");
  for (const [name, columns, rows] of [["tasks", TASK_COLUMNS, data.tasks], ["taskNotes", NOTE_COLUMNS, data.notes]] as const) {
    for (const row of rows) {
      session.run(`INSERT INTO "${name}" VALUES (${columns.map((_, index) => `$${index + 1}`).join(", ")})`, columns.map((column) => {
        const value = (row as Record<string, JsonValue>)[column];
        return typeof value === "string" ? value : null;
      }));
    }
  }
  const seen = new Set<string>();
  for (const principal of data.principals) {
    if (seen.has(principal.name)) continue;
    seen.add(principal.name);
    session.run("INSERT INTO members VALUES ($1, $2, 'active', $3, $4)", [principal.name, `acct-${principal.name}`, principal.role, JSON.stringify(principal.permissions)]);
  }
}

/** Row keys `plan` admits for `resolved`, decided by the SQL compiler. */
function sqlVisible(session: StorageSession, plan: VisibilityPlan, resolved: ResolvedVisibility): Set<string> {
  const parameters: JsonValue[] = [];
  const predicate = compilePredicate(plan, resolved, "r", parameters);
  const statement = `SELECT r."_id" FROM "${plan.table}" AS r WHERE ${predicate}`;
  try {
    return new Set(session.run(statement, parameters).rows.map((row) => String(row[0])));
  } catch (error) {
    throw new Error(`${statement}: ${(error as Error).message}`);
  }
}

/** Asserts that `visible` agrees with `expected` on every row; returns the number of rows checked. */
function checkCase(label: string, rows: readonly JsonValue[], expected: ReadonlySet<string>, visible: (row: JsonValue) => boolean): number {
  const leaked: string[] = [];
  const missing: string[] = [];
  for (const row of rows) {
    const id = idOf(row);
    const decided = visible(row);
    if (!expected.has(id) && decided) leaked.push(id);
    if (expected.has(id) && !decided) missing.push(id);
  }
  expect({ label, leaked, missing }).toEqual({ label, leaked: [], missing: [] });
  return rows.length;
}

function compiledMember(plan: VisibilityPlan, resolved: ResolvedVisibility, scope: string): MemberVisibility {
  const handle = memberVisibility(plan, resolved, scope);
  if (handle.kind !== "compiled") throw new Error(`${plan.table}: unexpected fallback`);
  return handle.member;
}

describe("visibility corpus (visibility_corpus.rs)", () => {
  it("SQL, interpreted and compiled evaluators agree on the corpus", async () => {
    const data = corpus();
    const index = tasksIndex(data);
    const object = objectFor();
    let checks = 0;
    let sqlCases = 0;
    const loaded = await object.invoke("reducer", async (session) => loadDatabase(session, data));
    expect(loaded.ok).toBe(true);
    const result = await object.invoke("query", async (session) => {
      for (const corpusPlan of data.plans) {
        const plan = corpusPlan.plan;
        validatePlan(plan);
        const rows = plan.table === "tasks" ? data.tasks : data.notes;
        // Scope per plan so classes of different plans never mix.
        const scope = `corpus-${corpusPlan.name}`;
        const memo = new EvalMemo();
        data.principals.forEach((principal, number) => {
          const resolved = resolve(data, principal, plan, index);
          const expected = sqlVisible(session, plan, resolved);
          sqlCases += 1;
          const label = `${corpusPlan.name}/${principal.name}#${number}`;
          checks += checkCase(`${label}/interpreted`, rows, expected, (row) => rowMatches(plan, resolved, row));
          const handle = memberVisibility(plan, resolved, scope);
          if (handle.kind === "fallback") {
            expect(corpusPlan.fallback, `${label}: unexpected fallback`).toBe(true);
            return;
          }
          const member = handle.member;
          checks += checkCase(`${label}/compiled`, rows, expected, (row) => member.visible(rowValue(row)));
          checks += checkCase(`${label}/compiled-json`, rows, expected, (row) => member.visible(rowJson(JSON.stringify(row))));
          checks += checkCase(`${label}/compiled-memo`, rows, expected, (row) => member.visibleMemo(memo, row));
        });
        expect(memo.stats.classEvaluations, `${corpusPlan.name}: ${JSON.stringify(memo.stats)}`).toBeLessThanOrEqual(memo.stats.compiledChecks);
      }
    });
    if (!result.ok) throw result.error;
    expect(sqlCases).toBe(data.plans.length * data.principals.length);
    expect(checks).toBeGreaterThan(50_000);
  });

  it("the compiled evaluator matches the interpreter on irregular rows", () => {
    const data = corpus();
    const index = tasksIndex(data);
    const irregular: JsonValue[] = [
      {},
      { _id: "x" },
      { _id: "x", workspaceId: "w1" },
      { _id: "x", workspaceId: "w1", spotId: "s1" },
      { _id: 5, workspaceId: 5, spotId: true, createdBy: "alice", createdVia: "finding" },
      { _id: "t-w2-s2-bob-web-0", workspaceId: ["w1"], spotId: { a: 1 } },
      { _id: "x", workspaceId: "w1", spotId: null, createdBy: 1.5 },
      { _id: "x", workspaceId: 'w"1', createdBy: "al\\u0069ce", createdVia: "finding" },
      { _id: "n", taskId: "t-w1-null-alice-web-0", authorId: null },
      { _id: "n", taskId: 7, authorId: "alice" },
      null,
      [1, 2],
      "row",
    ];
    for (const corpusPlan of data.plans) {
      for (const principal of data.principals) {
        const resolved = resolve(data, principal, corpusPlan.plan, index);
        const handle = memberVisibility(corpusPlan.plan, resolved, "irregular");
        if (handle.kind !== "compiled") continue;
        for (const row of irregular) {
          const expected = rowMatches(corpusPlan.plan, resolved, row);
          expect(handle.member.visible(rowValue(row)), `${corpusPlan.name}/${principal.name} value ${JSON.stringify(row)}`).toBe(expected);
          expect(handle.member.visible(rowJson(JSON.stringify(row))), `${corpusPlan.name}/${principal.name} json ${JSON.stringify(row)}`).toBe(expected);
        }
      }
    }
    // Escaped keys and values decode from raw text too.
    const plan = data.plans[3]!.plan; // owner
    const member = compiledMember(plan, resolve(data, data.principals[0]!, plan, index), "irregular");
    expect(member.visible(rowJson('{"createdBy":"alice"}'))).toBe(true);
    expect(member.visible(rowJson('{"cre\\u0061tedBy":"\\u0061lice"}'))).toBe(true);
    expect(member.visible(rowJson('{"createdBy":"bob","x":[{"createdBy":"alice"}]}'))).toBe(false);
    expect(member.visible(rowJson("not json"))).toBe(false);
  });

  it("identical principals share a class and principal changes move members", () => {
    const data = corpus();
    const index = tasksIndex(data);
    const plan = data.plans[0]!.plan;
    const bind = (number: number) => compiledMember(plan, resolve(data, data.principals[number]!, plan, index), "classes");
    const alice = bind(0);
    const carol = bind(1);
    const bob = bind(2);
    expect(alice.klass, "grants, contexts and owned personal workspaces are member atoms").toBe(carol.klass);
    expect(alice.klass).not.toBe(bob.klass);
    const joined = bind(8);
    expect(joined.klass, "team change moves alice").not.toBe(alice.klass);
    const granted = bind(9);
    expect(granted.klass, "a new grant changes the member, not the class").toBe(alice.klass);
    const lostSpot = bind(10);
    const again = bind(10);
    expect(again.klass).toBe(lostSpot.klass);
    again.release(); // Rust drops the temporary.
    expect(lostSpot.klass).not.toBe(alice.klass);
    expect(alice.scope.stats().classes, "alice/carol, bob, joined, lost spot").toBe(4);
    joined.release();
    lostSpot.release();
    expect(alice.scope.stats().classes, "dead classes leave the registry").toBe(2);
    // The admin class is ungated (sees rows without tokens); team classes are gated.
    expect(alice.klass.gated).toBe(true);
    const dave = bind(3);
    expect(dave.klass.gated).toBe(false);
    const ivan = bind(11);
    expect(ivan.klass, "admins share a class").toBe(dave.klass);
    expect(dave.klass.stableId).toBe(ivan.klass.stableId);
    expect(alice.klass.stableId).not.toBe(dave.klass.stableId);
    expect(alice.klass.stableId).toMatch(/^[0-9a-f]{16}$/);
  });

  it("candidates cover every member that sees a row", () => {
    const data = corpus();
    const index = tasksIndex(data);
    for (const corpusPlan of data.plans) {
      const plan = corpusPlan.plan;
      const scopeName = `candidates-${corpusPlan.name}`;
      const members = data.principals.flatMap((principal) => {
        const handle = memberVisibility(plan, resolve(data, principal, plan, index), scopeName);
        return handle.kind === "compiled" ? [handle.member] : [];
      });
      const first = members[0];
      if (!first) continue;
      const scope = first.scope;
      for (const member of members) scope.register(member);
      const rows = plan.table === "tasks" ? data.tasks : data.notes;
      let visited = 0;
      let seen = 0;
      for (let at = 0; at + 1 < rows.length; at++) {
        const oldRow = scope.decode(rowValue(rows[at]!));
        const newRow = scope.decode(rowValue(rows[at + 1]!));
        const candidates = scope.candidates(oldRow, newRow);
        visited += candidates.classes.length + candidates.members.length;
        for (const member of members) {
          const inClass = candidates.classes.includes(member.klass);
          const inMembers = candidates.members.includes(member);
          for (const row of [oldRow, newRow]) {
            const visible = member.visibleRow(row);
            seen += visible ? 1 : 0;
            expect(!visible || inMembers || inClass, `${corpusPlan.name}: candidates miss a member that sees the row`).toBe(true);
            if (inClass && !inMembers) expect(visible).toBe(member.klass.visibleDefault(row));
          }
        }
      }
      expect(visited > 0 && seen > 0, corpusPlan.name).toBe(true);
    }
  });
});
