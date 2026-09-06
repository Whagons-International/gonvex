import { expect, it } from "vitest";
import { shareReplicaRows } from "./replica-row-sharing";
it("retains unchanged watch rows across edits and order changes", () => {
  const before = [{ id: "a", value: 1, nested: ["x"] }, { id: "b", value: 2, nested: ["y"] }];
  expect(shareReplicaRows(before, structuredClone(before), "id")).toBe(before);
  const edited = shareReplicaRows(before, [{ id: "b", value: 3, nested: ["y"] }, { id: "a", value: 1, nested: ["x"] }], "id");
  expect(edited[1]).toBe(before[0]);
  expect(edited[0]).not.toBe(before[1]);
  expect(edited[0].value).toBe(3);
  expect(shareReplicaRows(before, [structuredClone(before[1])], "id")).toEqual([before[1]]);
});
it("does not hide a removed field or a changed nested value", () => {
  const before = [{ id: "a", value: 1, nested: { x: true } }];
  expect(shareReplicaRows(before, [{ id: "a", nested: { x: true } }], "id")[0]).not.toBe(before[0]);
  expect(shareReplicaRows(before, [{ id: "a", value: 1, nested: { x: false } }], "id")[0]).not.toBe(before[0]);
});
