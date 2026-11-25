import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { compare, countsOf, scan, updateBaseline } from "./check-dod.mjs";

describe("cloudflare DOD static gate", () => {
  it("flags sql-in-loop and concat-key repo-wide", () => {
    const src = `
      export function bad(db, ids, map) {
        for (let i = 0; i < ids.length; i++) {
          db.exec("SELECT * FROM t WHERE id = ?", ids[i]);
          map.get(\`k:\${ids[i]}\`);
          map.set("a:" + ids[i], 1);
        }
      }
    `;
    const vs = scan(src, "kernel/example/bad.ts", false);
    const rules = vs.map((v) => v.rule).sort();
    assert.deepEqual(rules, ["concat-key", "concat-key", "sql-in-loop"]);
  });

  it("flags iter-method, for-of, for-in, alloc-in-loop, and json-in-zone in hot zones", () => {
    const src = `
      export function hot(rows) {
        const mapped = rows.map((r) => r.id);
        for (const x of mapped) {
          Object.keys(x);
          new Map();
        }
        for (const k in rows) {
          JSON.stringify(k);
        }
      }
    `;
    const vs = scan(src, "kernel/codec/hot.ts", true);
    const rules = new Set(vs.map((v) => v.rule));
    assert.ok(rules.has("iter-method"));
    assert.ok(rules.has("for-of"));
    assert.ok(rules.has("for-in"));
    assert.ok(rules.has("alloc-in-loop"));
    assert.ok(rules.has("json-in-zone"));
  });

  it("passes clean indexed DOD loops with zero violations", () => {
    const src = `
      export function clean(rows, out) {
        for (let i = 0; i < rows.length; i++) {
          out[i] = rows[i] * 2;
        }
        return out;
      }
    `;
    const vs = scan(src, "kernel/codec/clean.ts", true);
    assert.equal(vs.length, 0);
  });

  it("enforces ratchet semantics: allows drops, blocks raises unless allowRaise is set", () => {
    const base = { "a.ts": { "sql-in-loop": 2 } };
    const dropped = countsOf([{ file: "a.ts", line: 1, rule: "sql-in-loop", msg: "" }]);
    assert.equal(compare(dropped, base).over.length, 0);
    assert.equal(compare(dropped, base).under.length, 1);
    assert.equal(updateBaseline(dropped, base).ok, true);

    const raised = countsOf([
      { file: "a.ts", line: 1, rule: "sql-in-loop", msg: "" },
      { file: "a.ts", line: 2, rule: "sql-in-loop", msg: "" },
      { file: "a.ts", line: 3, rule: "sql-in-loop", msg: "" },
    ]);
    assert.equal(compare(raised, base).over.length, 1);
    assert.equal(updateBaseline(raised, base, false).ok, false);
    assert.equal(updateBaseline(raised, base, true).ok, true);
  });
});
