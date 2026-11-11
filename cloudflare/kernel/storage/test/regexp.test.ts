// REGEXP without a custom function: the rewrite must give Rust's
// `Regex::is_match` result for every supported pattern, NULL where sqlx's
// regexp gives NULL, and a clear error for anything else.
import { describe, expect, it } from "vitest";
import { objectFor, outcome } from "./helpers";
import { regexpPredicate } from "../regexp";

const SUPPORTED = [
  "^PR-[0-9]+$", "^PO-[0-9]+$", "^GR-[0-9]+$", "^A-[0-9]$", "abc", "^abc", "abc$", "^a.c$", "a.c", "^[^0-9]+$",
  "^x[a-c]{2}y$", "^x[a-c]{2,}y$", "^x[a-c]{1,3}y$", "^(PR)-[0-9]*$", "^ab?c$", "^[\\]\\-]+$", "^\\.\\*\\?$", "^é+$",
  "^$", "", "[0-9]+", "x*", "^[a-z_][a-z0-9_]*$", "^[A-Z]{2}-[0-9]{3}$", "^.*$", "^.+$", "^[*?\\[]$", "[^a]", "^\\^x$",
];
const VALUES = [
  "", "PR-1", "PR-001", "PR-", "PR-12a", "XPR-1", "PR-1\n", "PO-77", "GR-3", "abc", "xabcx", "ab\nc", "a.c", "a\nc", "x",
  "xaby", "xabcy", "xy", "xabcdy", "é", "éé", "e", "A-1", "A-12", "a-1", "*.", "]-", "-]-", ".*?", "ac", "abbc", "AB-123",
  "AB-12", "snake_case", "Snake", "*", "?", "[", "aaa", "b", "^x", "\n",
];

/** Rust regex semantics in JS: `.` excludes only \n (JS also excludes \r and U+2028/9, absent from VALUES). */
const oracle = (pattern: string, value: string) => (new RegExp(pattern, "u").test(value) ? 1 : 0);

describe("REGEXP rewrite", () => {
  it("matches Rust's is_match for every supported pattern and value", async () => {
    const db = objectFor();
    const cases = SUPPORTED.flatMap((pattern) => VALUES.map((value) => [pattern, value] as const));
    const result = await db.invoke("query", async (session) => {
      const out: number[] = [];
      for (const [pattern, value] of cases) out.push((await session.query("SELECT $1 REGEXP $2 AS m", [value, pattern]))[0]!.m as number);
      return out;
    });
    if (!result.ok) throw result.error;
    const mismatches = cases.filter(([pattern, value], index) => result.value[index] !== oracle(pattern, value));
    expect(mismatches).toEqual([]);
    expect(cases.length).toBe(SUPPORTED.length * VALUES.length);
  });

  it("gives NULL like sqlx when the value or the pattern is not TEXT, and supports NOT REGEXP", async () => {
    const db = objectFor();
    await db.run((_tenant, state) => {
      state.storage.sql.exec("CREATE TABLE docs (id TEXT PRIMARY KEY, code)");
      state.storage.sql.exec("INSERT INTO docs VALUES ('a', 'PR-1'), ('b', 'PR-x'), ('c', NULL), ('d', 12), ('e', ' pr-7 ')");
    });
    const result = await db.invoke("query", async (session) => ({
      values: await session.query("SELECT id, code REGEXP $1 AS m, code NOT REGEXP $1 AS n FROM docs ORDER BY id", ["^PR-[0-9]+$"]),
      nullPattern: await session.query("SELECT 'x' REGEXP $1 AS m, 'x' REGEXP $2 AS i", [null, 5]),
      where: await session.query('SELECT id FROM docs WHERE lower(trim("code")) REGEXP $1 OR id = $2 ORDER BY id', ["^pr-[0-9]+$", "z"]),
      literal: await session.query("SELECT 'GR-9' REGEXP '^GR-[0-9]+$' AS m"),
    }));
    if (!result.ok) throw result.error;
    expect(result.value.values).toEqual([
      { id: "a", m: 1, n: 0 }, { id: "b", m: 0, n: 1 }, { id: "c", m: null, n: null }, { id: "d", m: null, n: null }, { id: "e", m: 0, n: 1 },
    ]);
    expect(result.value.nullPattern).toEqual([{ m: null, i: null }]);
    expect(result.value.where).toEqual([{ id: "a" }, { id: "e" }]);
    expect(result.value.literal).toEqual([{ m: 1 }]);
  });

  it("refuses patterns it cannot translate exactly, failing the statement", async () => {
    const db = objectFor();
    for (const [pattern, reason] of [
      ["a|b", "alternation `|` is not supported"],
      ["^\\d+$", "the escape \\d is not supported"],
      ["^(ab)+$", "a quantified group is not supported"],
      ["^a*b*$", "more than one variable-length part is not supported"],
      ["^PR-[0-9]+-", "a variable-length part needs both ^ and $"],
      ["a$b", "`$` is only supported at the end"],
    ] as const) {
      const result = await db.invoke("query", (session) => session.query("SELECT 'x' REGEXP $1", [pattern]));
      expect(outcome(result)).toBe(`REGEXP pattern ${JSON.stringify(pattern)} cannot run on Durable Object SQLite, which has no custom SQL functions: ${reason}. ` +
        "The storage layer translates anchored literals, character classes and at most one repeated class (for example ^PR-[0-9]+$).");
    }
    const call = await db.invoke("query", (session) => session.query("SELECT regexp('a', 'a')"));
    expect(outcome(call)).toMatch(/^error returned from database: \(code: 1\) no such function: regexp/);
    const shape = await db.invoke("query", (session) => session.query("SELECT 'a' || 'b' REGEXP $1", ["a"]));
    expect(outcome(shape)).toMatch(/^REGEXP on Durable Object SQLite needs the form/);
  });

  it("compiles to GLOB, length and substr only", () => {
    expect(regexpPredicate("^PR-[0-9]+$", "v")).toBe("length(v) >= 4 AND v GLOB 'PR-*' AND substr(v, 4) NOT GLOB '*[^0-9]*'");
    expect(regexpPredicate("^A-[0-9]$", "v")).toBe("v GLOB 'A-[0-9]'");
    expect(regexpPredicate("o'k", "v")).toBe("v GLOB '*o''k*'");
  });
});
