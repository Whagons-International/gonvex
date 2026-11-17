// WIRING ONLY. These deterministic error shapes do not prove platform CPU enforcement.
import { expect, it } from "vitest";
import { overrunMarkerKey, tenantDispatchWithDeadline } from "./deadline";
import { env, runInDurableObject } from "cloudflare:test";
it("preserves a reset command's durable marker across unrelated command completion and artifact/path changes", async () => {
  await runInDurableObject(env.HELLO.getByName(`marker-${crypto.randomUUID()}`), (_instance, state) => {
    const failed = overrunMarkerKey("v1", "failed", "errors.spin"), following = overrunMarkerKey("v1", "following", "counter.bump");
    state.storage.kv.put(failed, { commandId: "failed", path: "errors.spin", artifact: "v1" });
    state.storage.kv.put(following, { commandId: "following", path: "counter.bump", artifact: "v1" });
    state.storage.kv.delete(following);
    expect(state.storage.kv.get(failed)).toMatchObject({ commandId: "failed", artifact: "v1" });
    expect(state.storage.kv.get(overrunMarkerKey("v2", "failed", "errors.spin"))).toBeUndefined();
    expect(state.storage.kv.get(overrunMarkerKey("v1", "failed", "counter.bump"))).toBeUndefined();
  });
});
it("maps only CPU reset, records the exact command/path, and never blindly replays unkeyed calls", async () => {
  const frame = { type: "reducer.call" as const, id: "command", path: "errors.spin", args: {} };
  const observed: string[][] = []; let calls = 0;
  const replies = await tenantDispatchWithDeadline("p", frame, async () => { calls++; throw new Error("Durable Object exceeded its CPU time limit and was reset."); }, async (id, path) => { observed.push([id, path]); return "quarantined"; });
  expect(JSON.parse(replies[0]!)).toMatchObject({ type: "reducer.error", id: frame.id, class: "transient", retryable: true });
  expect(observed).toEqual([[frame.id, frame.path]]); expect(calls).toBe(1);
  for (const message of ["Durable Object reset because its code was updated.", "Durable Object is overloaded. Requests queued for too long.", "ordinary reset"]) {
    await expect(tenantDispatchWithDeadline("p", frame, async () => { throw new Error(message); }, async () => { throw new Error("must not quarantine"); })).rejects.toThrow(message);
  }
});
it("re-reads a committed keyed receipt from a fresh dispatch, preserving its real result", async () => {
  let calls = 0;
  const result = '{"type":"reducer.result","id":"command","result":{"durable":true},"committedRevision":9}';
  const dispatch = async () => { if (++calls === 1) throw new Error("Durable Object exceeded its CPU time limit and was reset."); return [result]; };
  expect(await tenantDispatchWithDeadline("p", { type: "reducer.call", id: "command", path: "counter.bump", args: {}, idempotencyKey: "durable-key" }, dispatch, async () => "committed")).toEqual([result]);
  expect(calls).toBe(2);
});
