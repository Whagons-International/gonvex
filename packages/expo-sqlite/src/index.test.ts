import { describe, expect, it } from "vitest";
import type { ExpoSQLiteDatabase } from "./index.js";
import { ExpoSQLiteLocalReplicaStorage } from "./index.js";

type SQLiteRow = Record<string, string>;

class FakeExpoSQLiteDatabase implements ExpoSQLiteDatabase {
  readonly tables = new Map<string, { columns: string[]; rows: SQLiteRow[] }>();

  seedLegacy() {
    this.tables.set("_gonvex_replica_entities", {
      columns: ["entity", "id", "value"],
      rows: [{ entity: "tasks", id: "task-1", value: JSON.stringify({ id: "task-1", title: "Cached" }) }],
    });
    this.tables.set("_gonvex_replica_queries", {
      columns: ["signature", "value"],
      rows: [{
        signature: "tasks:list",
        value: JSON.stringify({
          signature: "tasks:list", kind: "live", entity: "tasks", key: "id", ids: ["task-1"],
          completeness: "complete", source: "cache",
        }),
      }],
    });
    this.tables.set("_gonvex_replica_meta", {
      columns: ["key", "value"],
      rows: [{ key: "cursor", value: JSON.stringify({ epoch: "tenant-a", revision: 4 }) }],
    });
  }

  async execAsync(sql: string): Promise<void> {
    for (const match of sql.matchAll(/ALTER TABLE\s+(\S+)\s+RENAME TO\s+(\S+)/gi)) {
      const [, oldName, newName] = match;
      const table = this.tables.get(oldName);
      if (!table) throw new Error(`missing table ${oldName}`);
      this.tables.set(newName, table);
      this.tables.delete(oldName);
    }
    for (const match of sql.matchAll(/CREATE TABLE IF NOT EXISTS\s+(\S+)/gi)) {
      const [, name] = match;
      this.tables.set(name, this.tables.get(name) ?? { columns: columnsFor(name), rows: [] });
    }
    const insert = sql.match(/INSERT INTO\s+(\S+)\s+\([^)]*\)\s+SELECT\s+'[^']+',\s+.*?\s+FROM\s+(\S+)/i);
    if (insert) {
      const [, targetName, sourceName] = insert;
      const target = this.tables.get(targetName);
      const source = this.tables.get(sourceName);
      if (!target || !source) throw new Error(`missing migration table ${targetName}/${sourceName}`);
      for (const row of source.rows) target.rows.push({ scope: "default", ...row });
    }
    for (const match of sql.matchAll(/DROP TABLE(?: IF EXISTS)?\s+(\S+)/gi)) {
      this.tables.delete(match[1]);
    }
  }

  async runAsync(): Promise<void> {}

  async getFirstAsync<T>(sql: string, ...params: unknown[]): Promise<T | null> {
    const table = this.tableFrom(sql);
    const row = this.tables.get(table)?.rows.find((candidate) => candidate.scope === params[0]);
    const projected = row && sql.includes("json_quote(signature)") ? { ...row, signature: JSON.stringify(row.signature) } : row;
    return (projected as T | undefined) ?? null;
  }

  async getAllAsync<T>(sql: string, ...params: unknown[]): Promise<T[]> {
    if (sql.includes("PRAGMA table_info")) {
      const table = sql.match(/table_info\(([^)]+)\)/i)?.[1] ?? "";
      return (this.tables.get(table)?.columns ?? []).map((name) => ({ name })) as T[];
    }
    const table = this.tableFrom(sql);
    return (this.tables.get(table)?.rows.filter((row) => row.scope === params[0]) ?? [])
      .map(row => sql.includes("json_quote(signature)") ? { ...row, signature: JSON.stringify(row.signature) } : row) as T[];
  }

  async withTransactionAsync(task: () => Promise<void>): Promise<void> {
    const backup = new Map([...this.tables].map(([name, table]) => [name, {
      columns: [...table.columns],
      rows: table.rows.map((row) => ({ ...row })),
    }]));
    try {
      await task();
    } catch (error) {
      this.tables.clear();
      for (const [name, table] of backup) this.tables.set(name, table);
      throw error;
    }
  }

  private tableFrom(sql: string) {
    return sql.match(/FROM\s+(\S+)/i)?.[1] ?? "";
  }
}

function columnsFor(name: string) {
  if (name.endsWith("_entities")) return ["scope", "entity", "id", "value"];
  if (name.endsWith("_queries")) return ["scope", "signature", "value"];
  return ["scope", "key", "value"];
}

describe("ExpoSQLiteLocalReplicaStorage migrations", () => {
  it("copies legacy rows into normalized scope and drops the legacy tables", async () => {
    const database = new FakeExpoSQLiteDatabase();
    database.seedLegacy();
    const storage = new ExpoSQLiteLocalReplicaStorage(database);

    const snapshot = await storage.load();
    const reread = await storage.load();

    expect(snapshot?.entities.tasks?.["task-1"]).toEqual({ id: "task-1", title: "Cached" });
    expect(snapshot?.liveQueries["tasks:list"]).toMatchObject({ ids: ["task-1"], source: "cache" });
    expect(snapshot?.cursor).toEqual({ epoch: "tenant-a", revision: 4 });
    expect(reread).toEqual(snapshot);
    expect(database.tables.has("_gonvex_replica_entities_legacy_v1")).toBe(false);
    expect(database.tables.has("_gonvex_replica_queries_legacy_v1")).toBe(false);
    expect(database.tables.has("_gonvex_replica_meta_legacy_v1")).toBe(false);
    expect(database.tables.get("_gonvex_replica_entities")?.rows).toHaveLength(1);
    expect(database.tables.get("_gonvex_replica_queries")?.rows).toHaveLength(1);
    expect(database.tables.get("_gonvex_replica_meta")?.rows).toHaveLength(1);
  });
});
