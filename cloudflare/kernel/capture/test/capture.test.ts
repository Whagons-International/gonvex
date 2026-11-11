// Unit tests for kernel change capture: install, finalize, provenance,
// pruning and the wake-up, in workerd against real Durable Object SQLite.

import { describe, expect, it } from "vitest";
import {
  applyInstallUnit,
  bootstrapDigest,
  CaptureInstallError,
  CaptureState,
  ChangeWake,
  directSql,
  entityCaptureSql,
  finalize,
  INFRASTRUCTURE_SQL,
  installChangeFeed,
  invokeCaptured,
  keepRowidReference,
  planChangeFeedInstall,
  pruneChangeLog,
  pruneDue,
  PRUNE_INTERVAL_MS,
  sha256Hex,
  transactionRow,
  type ChangeWakeup,
} from "../index.ts";
import { numbersByValue } from "./numbers.ts";
import { rows, StandInStorage, withObject } from "./helpers.ts";

/** SHA-256 of `${SYNC_INFRASTRUCTURE_SQL}\n${SCHEMA_OPTIONS_SQL}` in the Rust reference (sha256sum over the Rust sources). */
const RUST_INFRASTRUCTURE_DIGEST = "0f167febfab96cec4f62d5e4cc866f98ee1b87189c1c2eb12af71a09da9213ca";

function setup(state: DurableObjectState, ddl: string) {
  const sql = directSql(state.storage.sql);
  state.storage.transactionSync(() => {
    installChangeFeed(sql);
    for (const s of ddl.split(";").map((x) => x.trim()).filter(Boolean)) sql.run(s);
    installChangeFeed(sql);
  });
  return sql;
}

const commit = <T>(state: DurableObjectState, fn: () => T) => state.storage.transactionSync(fn);

describe("digests and Rust-form scripts", () => {
  it("SHA-256 matches known vectors and the Rust infrastructure receipt", () => {
    expect(sha256Hex("")).toBe("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
    expect(sha256Hex("abc")).toBe("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
    expect(sha256Hex("é😀".repeat(100))).toHaveLength(64);
    expect(bootstrapDigest(INFRASTRUCTURE_SQL)).toBe(RUST_INFRASTRUCTURE_DIGEST);
  });

  it("entity triggers are feed.rs capture_triggers_sql byte for byte", () => {
    // From feed.rs: the format string with table "tasks", key "_id".
    expect(entityCaptureSql("tasks", "_id")).toBe(
      'DROP TRIGGER IF EXISTS "_gonvex_sync_tasks_insert";\nDROP TRIGGER IF EXISTS "_gonvex_sync_tasks_update";\nDROP TRIGGER IF EXISTS "_gonvex_sync_tasks_delete";\n' +
        'CREATE TRIGGER "_gonvex_sync_tasks_insert" AFTER INSERT ON "tasks" BEGIN\n  INSERT INTO _gonvex_sync_changes (table_name, row_id, operation, new_value)\n  VALUES (\'tasks\', CAST(NEW."_id" AS TEXT), \'insert\', gonvex_capture(\'tasks\', NEW.rowid, 0));\nEND;\n' +
        'CREATE TRIGGER "_gonvex_sync_tasks_update" AFTER UPDATE ON "tasks" BEGIN\n  INSERT INTO _gonvex_sync_changes (table_name, row_id, operation, new_value)\n  VALUES (\'tasks\', CAST(NEW."_id" AS TEXT), \'update\', gonvex_capture(\'tasks\', NEW.rowid, 1));\nEND;\n' +
        'CREATE TRIGGER "_gonvex_sync_tasks_delete" AFTER DELETE ON "tasks" BEGIN\n  INSERT INTO _gonvex_sync_changes (table_name, row_id, operation, new_value)\n  VALUES (\'tasks\', CAST(OLD."_id" AS TEXT), \'delete\', gonvex_capture(\'tasks\', OLD.rowid, 2));\nEND;\n',
    );
    expect(keepRowidReference("x gonvex_capture('t', NEW.rowid, 0) y")).toBe("x nullif(gonvex_capture('t', NEW.rowid, 0), NEW.rowid) y");
  });

  it("compares JSON numbers by value and nothing else (note 0026)", () => {
    expect(numbersByValue('{"a":5.0,"b":1.0e+300,"c":"5.0","d":9007199254740993,"e":-0.0,"f":2.5e-05}')).toBe('{"a":5,"b":1e+300,"c":"5.0","d":9007199254740993,"e":0,"f":0.000025}');
    expect(numbersByValue('{"a":1e+300}')).toBe(numbersByValue('{"a":1.0e+300}'));
    expect(numbersByValue('{"a":9007199254740993}')).not.toBe(numbersByValue('{"a":9007199254740992}'));
    expect(numbersByValue('{"s":"say \\"1.0\\""}')).toBe('{"s":"say \\"1.0\\""}');
    expect(numbersByValue(null)).toBeNull();
  });
});

describe("install", () => {
  it("plans infrastructure first, then nothing once installed; drops capture when a table becomes derived", async () => {
    await withObject((state) => {
      const sql = directSql(state.storage.sql);
      const fresh = planChangeFeedInstall(sql);
      // The test object is P01's HelloObject, which creates its own `greetings` table.
      expect(fresh.map((u) => u.receipt)).toEqual(["infrastructure", "table:greetings"]);
      commit(state, () => installChangeFeed(sql));
      expect(planChangeFeedInstall(sql)).toEqual([]);
      commit(state, () => {
        sql.run("CREATE TABLE parents(id TEXT PRIMARY KEY)");
        sql.run("CREATE TABLE edges(id TEXT PRIMARY KEY, owner TEXT REFERENCES parents(id))");
      });
      expect(planChangeFeedInstall(sql).map((u) => u.receipt)).toEqual(["table:edges", "table:parents"]);
      commit(state, () => installChangeFeed(sql));
      expect(rows(state.storage.sql, "SELECT count(*) AS n FROM sqlite_schema WHERE type='trigger' AND tbl_name='edges'")[0]!.n).toBe(3);
      // provision_upgrade_tests.rs derived_table_capture_is_owned_and_removes_old_capture_receipts
      commit(state, () => sql.run("INSERT INTO _gonvex_table_options(name,capture,owner_table,owner_column) VALUES('edges','derived','parents','owner')"));
      const plan = planChangeFeedInstall(sql);
      expect(plan.map((u) => [u.kind, u.receipt, u.digest])).toEqual([["drop", "table:edges", null]]);
      commit(state, () => plan.forEach((u) => applyInstallUnit(sql, u)));
      expect(rows(state.storage.sql, "SELECT count(*) AS n FROM sqlite_schema WHERE type='trigger' AND tbl_name='edges'")[0]!.n).toBe(0);
      expect(rows(state.storage.sql, "SELECT count(*) AS n FROM _gonvex_sync_bootstrap WHERE name='table:edges'")[0]!.n).toBe(0);
      commit(state, () => {
        sql.run("INSERT INTO parents VALUES('p')");
        sql.run("INSERT INTO edges VALUES('e','p')");
        finalize(sql, {});
      });
      expect(rows(state.storage.sql, "SELECT table_name FROM _gonvex_sync_changes").map((r) => r.table_name)).toEqual(["parents"]);
      commit(state, () => sql.run("UPDATE _gonvex_table_options SET owner_column='absent' WHERE name='edges'"));
      expect(() => planChangeFeedInstall(sql)).toThrow(new CaptureInstallError('derived table "edges" must declare its owner foreign key'));
    });
  });

  it("refuses a table without a primary key and a record whose key does not match, like Rust", async () => {
    await withObject((state) => {
      const sql = directSql(state.storage.sql);
      commit(state, () => installChangeFeed(sql));
      commit(state, () => sql.run("CREATE TABLE loose(a TEXT)"));
      expect(() => planChangeFeedInstall(sql)).toThrow('change-feed table "loose" has no primary key');
      commit(state, () => {
        sql.run("DROP TABLE loose");
        sql.run("CREATE TABLE searchDocuments(sourceId TEXT PRIMARY KEY, content TEXT)");
        sql.run(`INSERT INTO _gonvex_record_views(name,storage_table,key_column,schema) VALUES('searchDocuments','searchDocuments','_id','{"key":"sourceId","columns":{"sourceId":{"type":"text"},"content":{"type":"text"}}}')`);
      });
      expect(() => planChangeFeedInstall(sql)).toThrow('logical record "searchDocuments" declares storage key "_id", but "searchDocuments" has primary key "sourceId"');
    });
  });

  it("never captures the platform's _cf_ tables, FTS shadow tables or runtime tables", async () => {
    await withObject(async (state) => {
      await state.storage.put("progress", 1); // creates _cf_KV
      const sql = setup(state, "CREATE VIRTUAL TABLE docs_fts USING fts5(body); CREATE TABLE docs(id TEXT PRIMARY KEY, body TEXT)");
      const captured = rows<{ t: string }>(state.storage.sql, "SELECT DISTINCT tbl_name AS t FROM sqlite_schema WHERE type='trigger' AND name LIKE '\\_gonvex\\_sync\\_%' ESCAPE '\\' ORDER BY 1").map((r) => r.t);
      expect(captured.filter((t) => t !== "greetings")).toEqual(["docs"]);
      expect(captured).not.toContain("_cf_KV");
      expect(planChangeFeedInstall(sql)).toEqual([]);
    });
  });
});

describe("finalize", () => {
  it("one revision per commit, with ordinals, command id and Rust's provenance defaults", async () => {
    await withObject((state) => {
      const sql = setup(state, `CREATE TABLE tasks("_id" TEXT PRIMARY KEY, title TEXT, done BOOLEAN, meta JSON, n BIGINT)`);
      const notice = commit(state, () => {
        sql.run(`INSERT INTO tasks VALUES ('a', 'A', 1, '{"x":[1]}', 5)`);
        sql.run(`UPDATE tasks SET done = 0 WHERE "_id" = 'a'`);
        sql.run(`UPDATE tasks SET done = 0 WHERE "_id" = 'a'`);
        return finalize(sql, { commandId: "cmd-1" });
      })!;
      expect(notice.revision).toBe(1);
      expect(notice.tables).toEqual(["tasks"]);
      expect(notice.changes).toBe(2);
      const changes = rows(state.storage.sql, "SELECT operation, ordinal, new_value, changed_columns, command_id, revision, transaction_id FROM _gonvex_sync_changes ORDER BY event_id");
      expect(changes.length).toBe(2); // the no-op update is not kept
      expect(JSON.parse(String(changes[0]!.new_value))).toEqual({ _id: "a", title: "A", done: true, meta: { x: [1] }, n: 5 });
      expect(changes[1]).toMatchObject({ changed_columns: '["done"]', ordinal: 2, command_id: "cmd-1", revision: 1, transaction_id: 1 });
      expect(rows(state.storage.sql, "SELECT * FROM _gonvex_sync_transactions")[0]).toMatchObject({
        revision: 1, transaction_id: 1, root_command_id: "cmd-1", origin_command_id: "cmd-1", root_invocation_channel: "system", invocation_channel: "system",
        actor_account_id: null, actor_member_id: null, on_behalf_of_member_id: null, agent_execution_id: null,
      });
      // Nothing staged: no revision, nothing written.
      expect(commit(state, () => finalize(sql, { commandId: "cmd-2" }))).toBeUndefined();
      expect(rows(state.storage.sql, "SELECT revision FROM _gonvex_sync_clock")[0]!.revision).toBe(1);
    });
  });

  it("provenance: empty strings are absent; root and root channel default to origin and channel", () => {
    expect(transactionRow({})).toEqual({
      root_command_id: "", origin_command_id: "", root_invocation_channel: "system", invocation_channel: "system",
      actor_account_id: null, actor_member_id: null, on_behalf_of_member_id: null, agent_execution_id: null,
    });
    expect(transactionRow({ commandId: "c", rootCommandId: "", channel: "ui", rootChannel: "", actorAccountId: "", actorMemberId: "m", agentExecutionId: "x" })).toEqual({
      root_command_id: "c", origin_command_id: "c", root_invocation_channel: "ui", invocation_channel: "ui",
      actor_account_id: null, actor_member_id: "m", on_behalf_of_member_id: null, agent_execution_id: "x",
    });
    expect(transactionRow({ commandId: "c", rootCommandId: "r", channel: "scheduler", rootChannel: "ui" })).toMatchObject({ root_command_id: "r", root_invocation_channel: "ui" });
  });

  it("a migration commit (K02): command id migration:<name>, channel system", async () => {
    await withObject((state) => {
      const sql = setup(state, "CREATE TABLE t(id TEXT PRIMARY KEY)");
      commit(state, () => {
        sql.run("INSERT INTO t VALUES ('x')");
        finalize(sql, { commandId: "migration:0002_seed.sql", channel: "system" });
      });
      expect(rows(state.storage.sql, "SELECT origin_command_id, invocation_channel FROM _gonvex_sync_transactions")).toEqual([{ origin_command_id: "migration:0002_seed.sql", invocation_channel: "system" }]);
    });
  });

  it("without a change feed, finalize does nothing", async () => {
    await withObject((state) => {
      const sql = directSql(state.storage.sql);
      expect(commit(state, () => finalize(sql, {}))).toBeUndefined();
    });
  });

  it("a record that is never finalized cannot commit (the deferred guard)", async () => {
    await withObject((state) => {
      const sql = directSql(state.storage.sql);
      commit(state, () => installChangeFeed(sql));
      // The guard is a deferred foreign key: a staged pending row without a flush fails the commit.
      expect(rows(state.storage.sql, "SELECT \"table\", \"from\" FROM pragma_foreign_key_list('_gonvex_record_pending')")).toEqual([{ table: "_gonvex_record_guard", from: "guard" }]);
    });
  });
});

describe("invocations", () => {
  it("a committed Reducer gets a revision and wakes delivery once; a rolled-back or failed one does neither", async () => {
    await withObject(async (state) => {
      setup(state, "CREATE TABLE t(id TEXT PRIMARY KEY, v INTEGER)");
      const storage = new StandInStorage(state.storage);
      const wake = new ChangeWake();
      const wakes: ChangeWakeup[] = [];
      wake.subscribe((w) => void wakes.push(w));
      const context = { state: new CaptureState(), wake };
      const ok = await invokeCaptured(storage, "reducer", { commandId: "r1", channel: "ui" }, async (s) => s.query("INSERT INTO t VALUES ('a', 1)"), context);
      expect(ok.ok && ok.notice?.revision).toBe(1);
      const failed = await invokeCaptured(storage, "reducer", { commandId: "r2" }, async (s) => {
        await s.query("INSERT INTO t VALUES ('b', 1)");
        throw new Error("no");
      }, context);
      expect(failed.ok).toBe(false);
      expect(failed.notice).toBeUndefined();
      const aborted = await invokeCaptured(storage, "reducer", { commandId: "r3" }, async (s) => {
        await s.query("INSERT INTO t VALUES ('c', 1)");
        await s.query("INSERT INTO t VALUES ('a', 1)").catch(() => {});
      }, context);
      expect(aborted.ok).toBe(false);
      expect(String((aborted as { error: Error }).error.message)).toMatch(/^aborted: UNIQUE constraint failed/);
      const query = await invokeCaptured(storage, "query", { commandId: "q" }, async (s) => s.query("SELECT 1"), context);
      expect(query.notice).toBeUndefined();
      await Promise.resolve();
      expect(wakes).toEqual([{ epoch: ok.notice!.epoch, revision: 1, firstRevision: 1, tables: ["t"], changes: 1, commits: 1 }]);
      expect(rows(state.storage.sql, "SELECT revision FROM _gonvex_sync_clock")[0]!.revision).toBe(1);
      expect(rows(state.storage.sql, "SELECT count(*) AS n FROM _gonvex_sync_changes")[0]!.n).toBe(1);
    });
  });

  it("wakes coalesce commits published before listeners run", async () => {
    const wake = new ChangeWake();
    const seen: ChangeWakeup[] = [];
    wake.subscribe((w) => void seen.push(w));
    wake.subscribe(() => {
      throw new Error("a failing listener does not stop the others");
    });
    wake.publish({ epoch: "e", revision: 4, tables: ["b"], changes: 2 });
    wake.publish({ epoch: "e", revision: 5, tables: ["a", "b"], changes: 1 });
    expect(seen).toEqual([]);
    await Promise.resolve();
    expect(seen).toEqual([{ epoch: "e", revision: 5, firstRevision: 4, tables: ["a", "b"], changes: 3, commits: 2 }]);
    expect(wake.revision).toBe(5);
  });
});

describe("pruning", () => {
  it("deletes 30-day-old revisions in pages, raises retained_revision first, keeps newer rows", async () => {
    await withObject(async (state) => {
      const sql = setup(state, "CREATE TABLE t(id TEXT PRIMARY KEY)");
      for (let k = 0; k < 6; k += 1) {
        commit(state, () => {
          sql.run(`INSERT INTO t VALUES ('r${k}a'), ('r${k}b')`);
          finalize(sql, { commandId: `c${k}` });
        });
      }
      // Revisions 1-4 are older than the retention window.
      state.storage.sql.exec("UPDATE _gonvex_sync_changes SET created_at = created_at - 31 * 86400000 WHERE revision <= 4");
      const first = await pruneChangeLog(state.storage, { pageRows: 3, maxPages: 2 });
      expect(first).toMatchObject({ revision: 4, done: false, deletedChanges: 3 });
      expect(rows(state.storage.sql, "SELECT retained_revision FROM _gonvex_sync_clock")[0]!.retained_revision).toBe(4);
      const rest = await pruneChangeLog(state.storage, { pageRows: 3 });
      expect(rest).toMatchObject({ revision: 4, done: true, deletedChanges: 5, deletedTransactions: 4 });
      expect(rows(state.storage.sql, "SELECT DISTINCT revision FROM _gonvex_sync_changes ORDER BY 1").map((r) => r.revision)).toEqual([5, 6]);
      expect(rows(state.storage.sql, "SELECT revision FROM _gonvex_sync_transactions ORDER BY 1").map((r) => r.revision)).toEqual([5, 6]);
      expect(await pruneChangeLog(state.storage)).toMatchObject({ revision: null, done: true });
      expect(pruneDue(undefined, 0)).toBe(true);
      expect(pruneDue(0, PRUNE_INTERVAL_MS - 1)).toBe(false);
    });
  });
});
