import { describe, expect, it } from "vitest";
import { migrateClientData, migrationChain } from "./client-upgrades.js";
import type { ReducerOutboxEntry } from "./outbox.js";

const entry: ReducerOutboxEntry = { id: 7, scope: "alice/tenant-a", path: "tasks.rename", args: { title: "Monday" },
  idempotencyKey: "original-command", entityKeys: ["task:1"], createdAt: 100, attempts: 1,
  nextAttemptAt: 20, state: "inflight" };
describe("client contract migrations", () => {
  it("crosses several breaking releases without losing intent or receipt identity", () => {
    const chain = migrationChain(1, 3, [
      { from: 1, to: 2, intent: ({ args }) => ({ path: "tasks.setName", args }) },
      { from: 2, to: 3, intent: ({ args }) => ({ path: "tasks.name", args: { name: (args as any).title } }) },
    ]);
    const result = migrateClientData({}, [entry], chain).entries[0]!;
    expect(result).toMatchObject({ ...entry, path: "tasks.name", args: { name: "Monday" },
      receiptPath: "tasks.rename", state: "pending", nextAttemptAt: 0 });
    expect(entry).not.toHaveProperty("receiptPath");
  });
  it("refuses missing paths and downgrades", () => {
    expect(() => migrationChain(1, 3, [{ from: 1, to: 2 }])).toThrow("Missing");
    expect(() => migrationChain(3, 2, [])).toThrow("Unsupported");
  });
  it("preserves original data if a migration requires user resolution", () => {
    expect(() => migrateClientData({}, [entry], [{ from: 1, to: 2, intent() { throw Error("Choose a status"); } }])).toThrow("Choose a status");
    expect(entry.args).toEqual({ title: "Monday" });
  });
  it("invalidates old membership and cursors without clearing confirmed entities", () => {
    const snapshots = { a: { entities: { tasks: { t: { name: "Before" } } }, liveQueries: {}, cursor: { epoch: "old", revision: 9 } } };
    const next = migrateClientData(snapshots, [], [{ from: 1, to: 2 }]).snapshots.a!;
    expect(next.entities).toEqual(snapshots.a.entities);
    expect(next.cursor).toBeUndefined();
  });
});
