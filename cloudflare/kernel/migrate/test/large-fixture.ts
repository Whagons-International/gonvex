// A synthetic tenant at the scale of the largest P03 import (devcopy-stg1117-12: about 115k rows, a
// schema of thousands of triggers and hundreds of views), for the CPU-budget tests (X10). The shape,
// not the Whagons content, is what matters: DDL on a large schema costs milliseconds per statement,
// and migrations that touch imported rows cost per row.

export interface LargeShape {
  tables: number;
  /** Application triggers per table (the change feed adds its own). */
  triggers: number;
  /** Rows in the one big table, and in each other table. */
  bigRows: number;
  rowsPerTable: number;
}

/** About 115k rows and 6k schema objects: the scale where migration alarms reached 10.6 s of CPU on dev. */
export const LARGE: LargeShape = { tables: 300, triggers: 14, bigRows: 85_000, rowsPerTable: 100 };

const t = (i: number) => `t${String(i).padStart(3, "0")}`;

/** Release 1: the schema the tenant was imported with. */
export function largeBaseMigrations(shape: LargeShape = LARGE): Record<string, string> {
  const lines = ["-- gonvex:scope tenant"];
  for (let i = 0; i < shape.tables; i += 1) {
    const name = t(i);
    const parent = i > 0 ? ` REFERENCES ${t(0)}(id)` : "";
    lines.push(`CREATE TABLE ${name} (id TEXT PRIMARY KEY, parent_id TEXT${parent}, title TEXT NOT NULL, body TEXT, rank INTEGER NOT NULL DEFAULT 0, updated_at INTEGER);`);
    lines.push(`CREATE INDEX ${name}_parent ON ${name}(parent_id);`);
    lines.push(`CREATE INDEX ${name}_rank ON ${name}(rank, updated_at);`);
    lines.push(`CREATE VIEW ${name}_open AS SELECT id, title, rank FROM ${name} WHERE rank >= 0;`);
    for (let k = 0; k < shape.triggers; k += 1) {
      lines.push(`CREATE TRIGGER ${name}_guard_${k} BEFORE UPDATE OF title ON ${name} WHEN NEW.title = '__never_${k}__' BEGIN SELECT RAISE(ABORT, 'guard ${k}'); END;`);
    }
  }
  return { "migrations/0001_base.sql": lines.join("\n") };
}

/**
 * Release 2, applied over the imported rows: per-table DDL (a column, an index, a view and triggers
 * replaced on every table, as the Whagons projection migrations do), and statements that rewrite every
 * row of the big table. Each of those is a single statement and cannot be split.
 */
export function largeReleaseMigrations(shape: LargeShape = LARGE): Record<string, string> {
  const ddl = ["-- gonvex:scope tenant"];
  for (let i = 0; i < shape.tables; i += 1) {
    const name = t(i);
    ddl.push(`ALTER TABLE ${name} ADD COLUMN archived INTEGER NOT NULL DEFAULT 0;`);
    ddl.push(`DROP VIEW ${name}_open;`);
    ddl.push(`CREATE VIEW ${name}_open AS SELECT id, title, rank FROM ${name} WHERE rank >= 0 AND archived = 0;`);
    for (let k = 0; k < Math.min(shape.triggers, 4); k += 1) {
      ddl.push(`DROP TRIGGER ${name}_guard_${k};`);
      ddl.push(`CREATE TRIGGER ${name}_guard_${k} BEFORE UPDATE OF title ON ${name} WHEN NEW.title = '__never_${k}__' AND NEW.archived = 0 BEGIN SELECT RAISE(ABORT, 'guard ${k}'); END;`);
    }
  }
  const data = [
    "-- gonvex:scope tenant",
    `CREATE INDEX ${t(0)}_title ON ${t(0)}(title);`,
    `UPDATE ${t(0)} SET rank = rank + 1 WHERE rank % 7 = 0;`,
    `UPDATE ${t(1)} SET archived = 1 WHERE rank % 2 = 0;`,
  ];
  return { "migrations/0002_archive_columns.sql": ddl.join("\n"), "migrations/0003_backfill.sql": data.join("\n") };
}

/** The P03 import: rows written straight into SQLite, as `insert` pages do (no capture, no finalize). */
export function importLargeRows(sql: SqlStorage, shape: LargeShape = LARGE): number {
  const big = t(0);
  sql.exec(
    `WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < ?)
     INSERT INTO ${big}(id, parent_id, title, body, rank, updated_at)
     SELECT printf('r%06d', i), NULL, printf('row %d', i), printf('%048d', i * 7919), i % 1000, 1700000000000 + i FROM n`,
    shape.bigRows,
  );
  for (let i = 1; i < shape.tables; i += 1) {
    sql.exec(
      `WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < ?)
       INSERT INTO ${t(i)}(id, parent_id, title, body, rank, updated_at)
       SELECT printf('r%06d', i), printf('r%06d', 1 + (i * 37) % ?), printf('row %d', i), printf('%024d', i * 104729), i % 50, 1700000000000 + i FROM n`,
      shape.rowsPerTable,
      shape.bigRows,
    );
  }
  return shape.bigRows + (shape.tables - 1) * shape.rowsPerTable;
}
