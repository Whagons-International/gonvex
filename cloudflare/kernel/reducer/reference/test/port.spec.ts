// What the TypeScript port adds to the Rust behavior: paging, batched
// statements, the injected savepoint, and Rust's validation texts.
import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
// @ts-expect-error vite raw import
import fixtureSql from "../../../../../rust/crates/store/tests/reference_integrity/fixture.sql?raw";
import { counting } from "../../../capture/sql.ts";
import { declareReferences, flushReferences, hasStagedReferences, ReferenceIntegrityError } from "../index.ts";
import { objectNamespace, Tenant } from "./harness.ts";

const namespace = objectNamespace();
let objects = 0;

function tenant<T>(run: (tenant: Tenant) => T): Promise<T> {
  return runInDurableObject(namespace.getByName(`port-${++objects}`), (_instance, state) => {
    const t = new Tenant(state.storage);
    t.provision("0001_references.sql", fixtureSql);
    return run(t);
  });
}

describe("reference integrity port", () => {
  it("flushes and inventories many rows a page at a time", async () => {
    await tenant((f) => {
      const values = (prefix: string, count: number, row: (id: string) => string) =>
        Array.from({ length: count }, (_, index) => row(`${prefix}-${index}`)).join(",");
      // 1,200 staged rows: five pages.
      const counters = { statements: 0, rowsRead: 0, rowsWritten: 0 };
      f.storage.transactionSync(() => {
        f.storage.sql.exec(`INSERT INTO teams VALUES ${values("t", 600, (id) => `('${id}',NULL)`)}`);
        f.storage.sql.exec(`INSERT INTO workspaces VALUES ${values("w", 600, (id) => `('${id}','["t-${id.slice(2)}"]',NULL,NULL)`)}`);
        expect(hasStagedReferences(f.sql)).toBe(true);
        expect(flushReferences(counting(f.sql, counters))).toEqual({ rows: 1200 });
        expect(hasStagedReferences(f.sql)).toBe(false);
        f.deferredCheck();
      });
      // Per staged insert: the edge insert (workspaces) and two target statements.
      expect(counters.statements).toBeLessThan(1200 * 3 + 50);
      expect(f.scalar("SELECT count(*) FROM _gonvex_reference_edges WHERE rule_name='workspace.teams'")).toBe(602);
      expect(f.scalar("SELECT count(*) FROM _gonvex_reference_targets WHERE table_name='teams'")).toBe(1202);
      expect(f.violations()).toBe(1);
      f.rejected("DELETE FROM teams WHERE _id LIKE 't-%'");

      // Re-declaring inventories every row (pages of 256) and keeps the legacy violation.
      f.declare(f.catalog());
      expect(f.scalar("SELECT count(*) FROM _gonvex_reference_edges WHERE rule_name='workspace.teams'")).toBe(602);
      expect(f.scalar("SELECT count(*) FROM _gonvex_reference_edges WHERE enforced_table IS NULL")).toBe(1);
      expect(f.scalar("SELECT sum(owner_count) FROM _gonvex_reference_targets WHERE table_name='teams'")).toBe(1202);
      expect(f.violations()).toBe(1);
      f.rejected("DELETE FROM teams WHERE _id='t-599'");
      f.ok("DELETE FROM workspaces WHERE _id LIKE 'w-%'; DELETE FROM teams WHERE _id LIKE 't-%'");
      expect(f.scalar("SELECT count(*) FROM _gonvex_reference_targets WHERE table_name='teams'")).toBe(2);
    });
  });

  it("assigns inventory versions binding by binding, as Rust does", async () => {
    await tenant((f) => {
      f.ok(`CREATE TABLE settings(_id TEXT PRIMARY KEY,a TEXT,b TEXT);
            INSERT INTO settings VALUES('one','1','x'),('two','2','y'),('three','3','z');`);
      const catalog = f.catalog();
      catalog.tables.push({ name: "settings", key: "_id", live: "true" });
      catalog.bindings = [
        { name: "settingsB", source: "settings", columns: ["b"] },
        { name: "settingsA", source: "settings", columns: ["a"] },
      ];
      f.declare(catalog);
      expect(
        f.storage.sql.exec("SELECT binding_name, row_id, document, version FROM _gonvex_reference_configurations ORDER BY version").toArray(),
      ).toEqual([
        { binding_name: "settingsA", row_id: "one", document: '{"a":"1"}', version: 1 },
        { binding_name: "settingsA", row_id: "two", document: '{"a":"2"}', version: 2 },
        { binding_name: "settingsA", row_id: "three", document: '{"a":"3"}', version: 3 },
        { binding_name: "settingsB", row_id: "one", document: '{"b":"x"}', version: 4 },
        { binding_name: "settingsB", row_id: "two", document: '{"b":"y"}', version: 5 },
        { binding_name: "settingsB", row_id: "three", document: '{"b":"z"}', version: 6 },
      ]);
      expect(f.scalar(`SELECT count(*) FROM _gonvex_reference_targets WHERE table_name='$configuration:settingsA' AND row_id='["one",1]'`)).toBe(2);
      // An unrelated edit keeps the version; a bound edit takes the next one.
      f.ok("UPDATE settings SET b='x2' WHERE _id='one'");
      expect(f.scalar("SELECT version FROM _gonvex_reference_configurations WHERE binding_name='settingsA' AND row_id='one'")).toBe(1);
      expect(f.scalar("SELECT version FROM _gonvex_reference_configurations WHERE binding_name='settingsB' AND row_id='one'")).toBe(7);
      expect(f.scalar(`SELECT count(*) FROM _gonvex_reference_targets WHERE table_name='$configuration:settingsB' AND row_id='["one",4]'`)).toBe(0);
      expect(f.scalar(`SELECT owner_count FROM _gonvex_reference_targets WHERE table_name='$configuration:settingsB' AND row_id='["one",7]' AND state='live'`)).toBe(1);
    });
  });

  it("applies a declaration atomically inside an open transaction", async () => {
    await tenant((f) => {
      const before = f.text("SELECT declaration FROM _gonvex_reference_catalog");
      const rules = f.scalar("SELECT count(*) FROM _gonvex_reference_rules");
      const broken = f.catalog();
      // Fails in validation, after the ledger tables were cleared.
      broken.references.push({ name: "broken", source: "teams", path: "$._id", target: "nowhere", state: "live" });
      f.storage.transactionSync(() => {
        f.storage.sql.exec("INSERT INTO teams VALUES('kept',NULL)");
        expect(() => declareReferences(f.sql, broken, { atomic: f.atomic })).toThrow(
          new ReferenceIntegrityError("reference broken has unknown target nowhere"),
        );
        // The savepoint rolled back the declaration; the transaction goes on.
        expect(f.text("SELECT declaration FROM _gonvex_reference_catalog")).toBe(before);
        expect(f.scalar("SELECT count(*) FROM _gonvex_reference_rules")).toBe(rules);
        expect(f.scalar("SELECT count(*) FROM _gonvex_reference_edges")).toBeGreaterThan(0);
        f.commit();
      });
      expect(f.scalar("SELECT count(*) FROM teams WHERE _id='kept'")).toBe(1);
      expect(f.violations()).toBe(1);
    });
  });

  it("refuses invalid declarations with Rust's texts", async () => {
    await tenant((f) => {
      const declare = (change: (catalog: Record<string, any>) => void) => {
        const catalog = f.catalog();
        change(catalog);
        return f.error(() => f.declare(catalog));
      };
      const refused = (detail: string) => `reference integrity: ${detail}`;
      expect(declare((c) => (c.version = 2))).toBe(refused("invalid reference catalog"));
      expect(declare((c) => delete c.tables)).toBe(refused("invalid reference catalog"));
      expect(declare((c) => c.tables.push({ name: "_gonvex_x", key: "id", live: "true" }))).toBe(refused("invalid application reference table"));
      expect(declare((c) => (c.tables[0].key = "deletedAt"))).toBe(refused("reference table teams must declare its single primary key"));
      expect(declare((c) => delete c.tables[0].live)).toBe(refused("reference table teams needs a lifecycle"));
      expect(declare((c) => (c.tables[0].live = "$.a ? ("))).toBe(refused("invalid JSON path: unexpected None in JSON path"));
      expect(declare((c) => (c.tables[0].targetKeys = {}))).toBe(refused("invalid target key aliases for teams"));
      expect(declare((c) => (c.tables[1].targetKeys = [{ name: "teams", column: "note" }]))).toBe(
        refused("target alias for workspaces needs an existing key column and a separate namespace"),
      );
      expect(declare((c) => (c.tables[1].targetKeys = [{ name: "notes", column: "note" }]))).toBe(
        refused("nonunique target alias for workspaces needs an explicit multiple-owner policy and reason"),
      );
      expect(declare((c) => (c.bindings = {}))).toBe(refused("invalid configuration bindings"));
      expect(declare((c) => (c.bindings = [{ name: "b", source: "teams", columns: [] }]))).toBe(refused("invalid configuration binding"));
      expect(declare((c) => (c.bindings = [{ name: "b", source: "teams", columns: ["absent"] }]))).toBe(refused("configuration binding b needs existing columns"));
      expect(declare((c) => (c.references[0].state = "gone"))).toBe(refused("invalid reference declaration workspace.teams"));
      expect(declare((c) => (c.references[0].paths = []))).toBe(refused("reference workspace.teams needs exactly one path or nonempty paths array"));
      expect(declare((c) => (c.references[0].ignoreValues = ["x"]))).toBe(refused("reference workspace.teams needs declared string sentinels and their meaning"));
      expect(declare((c) => (c.references[0].allowNumbers = true))).toBe(
        refused("reference workspace.teams needs an explicit numeric selector policy and reason"),
      );
      expect(declare((c) => (c.references[0].normalize = "csv"))).toBe(refused("reference workspace.teams needs a supported selector decoder and reason"));
      expect(declare((c) => (c.references[0].encoding = "json-or-value"))).toBe(refused("reference workspace.teams has unsupported storage encoding"));
      expect(declare((c) => (c.references[0].lookup = []))).toBe(refused("reference workspace.teams needs configuration lookup steps"));
      expect(declare((c) => (c.references[0].lookup = { binding: "none", key: "$.a" }))).toBe(
        refused("reference workspace.teams needs a declared configuration binding and key"),
      );
      expect(declare((c) => (c.references[1].targets = {}))).toBe(refused("reference approval.selectors needs discriminator variants"));
      expect(declare((c) => (c.references[0].path = null))).toBe(refused("reference workspace.teams has a null path"));
      expect(declare((c) => (c.exclusions = null))).toBe(refused("reference coverage needs explicit exclusions"));
      expect(declare((c) => (c.exclusions = []))).toBe(refused("reference workspace.teams needs an existing source column"));
      expect(
        declare((c) => {
          c.references = [];
          c.exclusions = [{ source: "teams", column: "_id", kind: "local", reason: "short" }];
        }),
      ).toBe(refused("invalid reference exclusion for teams._id"));
      expect(declare((c) => ((c.references = []), (c.exclusions = [])))).toBe(refused("unclassified reference candidate approvals.selectors"));
      // Nothing above replaced the installed catalog.
      expect(f.catalog().references).toHaveLength(3);
      expect(f.violations()).toBe(1);
    });
  });

  it("is a no-op without a ledger or staged rows", async () => {
    await runInDurableObject(namespace.getByName(`port-${++objects}`), (_instance, state) => {
      const t = new Tenant(state.storage);
      expect(hasStagedReferences(t.sql)).toBe(false);
      expect(flushReferences(t.sql)).toEqual({ rows: 0 });
    });
  });
});
