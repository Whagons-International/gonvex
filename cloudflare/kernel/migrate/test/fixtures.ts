// A small app and a stand-in for K03's capture hooks, for the workerd tests.
// The stand-in only exercises the install-unit protocol (receipts, batching,
// convergence) and counts finalize calls; K03's real hooks are what the full
// Whagons verification uses (verify/README.md).

import type { CaptureHooks, CommitNotice, InstallUnit } from "../capture-port.ts";
import { directSql } from "../migrator.ts";

export const APP: Record<string, string> = {
  "migrations/0001_init.sql": `-- gonvex:scope tenant
CREATE TABLE boards (id TEXT PRIMARY KEY, name TEXT NOT NULL);
CREATE TABLE tasks (id TEXT PRIMARY KEY, board_id TEXT NOT NULL REFERENCES boards(id), title TEXT NOT NULL, done INTEGER NOT NULL DEFAULT 0);
CREATE INDEX tasks_board ON tasks(board_id);
CREATE TABLE labels (id TEXT PRIMARY KEY, name TEXT NOT NULL UNIQUE);
CREATE TABLE task_labels (task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE, label_id TEXT NOT NULL REFERENCES labels(id), PRIMARY KEY (task_id, label_id));
CREATE VIEW open_tasks AS SELECT id, title FROM tasks WHERE done = 0;
INSERT INTO boards VALUES ('b1', 'Inbox'), ('b2', 'Later');
INSERT INTO tasks(id, board_id, title) VALUES ('t1', 'b1', 'first'), ('t2', 'b1', 'second'), ('t3', 'b2', 'third');
`,
  "migrations/0002_comments.sql": `-- gonvex:scope tenant
CREATE TABLE comments (id TEXT PRIMARY KEY, task_id TEXT NOT NULL REFERENCES tasks(id), body TEXT NOT NULL);
CREATE INDEX comments_task ON comments(task_id);
INSERT INTO comments VALUES ('c1', 't1', 'hello');
ALTER TABLE tasks ADD COLUMN due_at INTEGER;
CREATE TRIGGER tasks_touch AFTER UPDATE OF title ON tasks BEGIN UPDATE tasks SET done = done WHERE id = NEW.id; END;
`,
  // Rebuilds tasks (SQLite's 12-step ALTER): needs foreign keys off, then the check.
  "migrations/0003_rebuild_tasks.sql": `-- gonvex:scope tenant
-- gonvex:foreign-key-rebuild
CREATE TABLE tasks_new (id TEXT PRIMARY KEY, board_id TEXT NOT NULL REFERENCES boards(id), title TEXT NOT NULL CHECK (length(title) > 0), done INTEGER NOT NULL DEFAULT 0, due_at INTEGER);
INSERT INTO tasks_new SELECT id, board_id, title, done, due_at FROM tasks;
DROP VIEW open_tasks;
DROP TABLE tasks;
ALTER TABLE tasks_new RENAME TO tasks;
CREATE INDEX tasks_board ON tasks(board_id);
CREATE VIEW open_tasks AS SELECT id, title FROM tasks WHERE done = 0;
CREATE TRIGGER tasks_touch AFTER UPDATE OF title ON tasks BEGIN UPDATE tasks SET done = done WHERE id = NEW.id; END;
`,
  "migrations/0001_control.sql.txt": "not a migration",
  "migrations/nested/0009_ignored.sql": "-- gonvex:scope tenant\nCREATE TABLE ignored (id INTEGER PRIMARY KEY);",
  "migrations/0004_control_side.sql": "-- gonvex:scope control-plane\nCREATE TABLE control_only (id INTEGER PRIMARY KEY);",
};

const quote = (name: string) => `"${name.replace(/"/g, '""')}"`;

export interface FakeCapture extends CaptureHooks {
  finalizeCalls: Array<{ commandId: string | undefined; pendingRecords: Array<{ table_name: string; row_id: string; old_value: string | null }> }>;
  installed: string[];
}

/** Infrastructure receipt plus one AFTER INSERT marker trigger per application table. */
export function fakeCapture(): FakeCapture {
  const finalizeCalls: FakeCapture["finalizeCalls"] = [];
  const installed: string[] = [];
  return {
    finalizeCalls,
    installed,
    directSql,
    finalize(sql, provenance): CommitNotice | undefined {
      const has = (name: string) => sql.run("SELECT 1 FROM sqlite_schema WHERE type='table' AND name=?", [name]).rows.length > 0;
      const pendingRecords = has("_gonvex_record_pending")
        ? (sql.run("SELECT table_name, row_id, old_value FROM _gonvex_record_pending ORDER BY table_name, row_id").rows.map(([table_name, row_id, old_value]) => ({ table_name, row_id, old_value })) as FakeCapture["finalizeCalls"][number]["pendingRecords"])
        : [];
      if (pendingRecords.length) sql.run("DELETE FROM _gonvex_record_pending");
      finalizeCalls.push({ commandId: provenance.commandId, pendingRecords });
      if (!has("_gonvex_sync_clock")) return undefined;
      const row = sql.run("UPDATE _gonvex_sync_clock SET revision = revision + 1 WHERE singleton = 1 RETURNING revision, epoch").rows[0]!;
      return { epoch: String(row[1]), revision: Number(row[0]), tables: [], changes: 0, rowsWritten: 1 };
    },
    planChangeFeedInstall(sql): InstallUnit[] {
      const units: InstallUnit[] = [];
      const bootstrap = sql.run("SELECT 1 FROM sqlite_schema WHERE type='table' AND name='_gonvex_sync_bootstrap'").rows.length > 0;
      const receipts = new Set(bootstrap ? sql.run("SELECT name FROM _gonvex_sync_bootstrap").rows.map((r) => String(r[0])) : []);
      if (!receipts.has("infrastructure")) {
        units.push({
          receipt: "infrastructure",
          digest: "fake-infrastructure",
          kind: "infrastructure",
          statements: [
            "CREATE TABLE IF NOT EXISTS _gonvex_sync_bootstrap (name TEXT PRIMARY KEY, digest TEXT NOT NULL)",
            "CREATE TABLE IF NOT EXISTS _gonvex_sync_clock (singleton INTEGER PRIMARY KEY CHECK (singleton = 1), epoch TEXT NOT NULL, revision INTEGER NOT NULL DEFAULT 0, retained_revision INTEGER NOT NULL DEFAULT 0)",
            "INSERT INTO _gonvex_sync_clock (singleton, epoch, revision) VALUES (1, lower(hex(randomblob(16))), 0) ON CONFLICT (singleton) DO NOTHING",
          ],
        });
      }
      const tables = sql
        .run("SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE '\\_gonvex%' ESCAPE '\\' AND name NOT LIKE 'gonvex\\_%' ESCAPE '\\' AND name NOT LIKE 'sqlite\\_%' ESCAPE '\\' AND name NOT LIKE '\\_cf\\_%' ESCAPE '\\' AND name <> 'members' ORDER BY name")
        .rows.map((r) => String(r[0]));
      for (const table of tables) {
        if (receipts.has(`table:${table}`)) continue;
        units.push({
          receipt: `table:${table}`,
          digest: `fake-${table}`,
          kind: "table",
          statements: [`DROP TRIGGER IF EXISTS ${quote(`_gonvex_sync_${table}_insert`)}`, `CREATE TRIGGER ${quote(`_gonvex_sync_${table}_insert`)} AFTER INSERT ON ${quote(table)} BEGIN SELECT 1; END`],
        });
      }
      return units;
    },
    applyInstallUnit(sql, unit) {
      for (const s of unit.statements) sql.run(s);
      sql.run("INSERT INTO _gonvex_sync_bootstrap(name,digest) VALUES(?,?) ON CONFLICT(name) DO UPDATE SET digest=excluded.digest", [unit.receipt, unit.digest]);
      installed.push(unit.receipt);
    },
  };
}
