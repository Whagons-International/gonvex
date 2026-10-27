import { expect, it, vi } from "vitest";
import { LocalReplica, MemoryLocalReplicaStorage } from "./local-replica.js";
import { mergeReplicaRecord } from "./replica-record.js";

const invalid = [NaN, Infinity, -Infinity, 1.5, Number.MAX_SAFE_INTEGER + 1];

it.each(invalid)("rejects transaction revision %s before changing memory or storage", async revision => {
  const storage = new MemoryLocalReplicaStorage();
  const persist = vi.spyOn(storage, "applyTransaction");
  const replica = new LocalReplica(storage);
  await replica.applyTransaction({cursor: {epoch: "e", revision: 1}, changes: [
    {entity: "tasks", id: "a", operation: "insert", newValue: {id: "a", value: "before"}},
  ]});
  persist.mockClear();
  await expect(replica.applyTransaction({cursor: {epoch: "e", revision}, changes: [
    {entity: "tasks", id: "a", operation: "update", newValue: {value: "invalid"}},
  ]})).rejects.toThrow(/revision/);
  expect(replica.entity("tasks", "a")?.value).toBe("before");
  expect(replica.cursor()).toEqual({epoch: "e", revision: 1});
  expect(persist).not.toHaveBeenCalled();
  expect((await storage.load())?.cursor).toEqual({epoch: "e", revision: 1});
  // A rejected item must not poison the serialized apply queue.
  await replica.applyTransaction({cursor: {epoch: "e", revision: 2}, changes: [
    {entity: "tasks", id: "a", operation: "update", newValue: {value: "after"}},
  ]});
  expect(replica.entity("tasks", "a")?.value).toBe("after");
});

it.each(invalid)("rejects projected row revision %s without changing prior authority", revision => {
  const prior = mergeReplicaRecord(undefined, {id: "a", value: "before"}, {epoch: "e", revision: 1});
  expect(() => mergeReplicaRecord(prior, {value: "invalid"}, {epoch: "e", revision})).toThrow(/revision/);
  expect(prior.row?.value).toBe("before");
  expect(prior.authority.fields.value).toBe(1);
});

it("accepts the maximum safe transaction revision without rounding it", async () => {
  const replica = new LocalReplica(new MemoryLocalReplicaStorage());
  await replica.applyTransaction({cursor: {epoch: "e", revision: Number.MAX_SAFE_INTEGER}, changes: []});
  expect(replica.cursor()?.revision).toBe(Number.MAX_SAFE_INTEGER);
});

it("keeps inherited dictionary names eligible as new projected columns", () => {
  const prior = mergeReplicaRecord(undefined, {id: "a"}, {epoch: "e", revision: 3});
  const next = mergeReplicaRecord(prior, {constructor: "value", toString: "text"}, {epoch: "e", revision: 2});
  expect(next.row?.constructor).toBe("value");
  expect(next.row?.toString).toBe("text");
  expect(next.authority.fields.constructor).toBe(2);
});
