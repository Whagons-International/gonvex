// X10 (conformance run9, reducer-errors): after a tenant object's CPU reset, the next operator call must
// recover or answer the documented transient error, never HTTP 500.
import { describe, expect, it } from "vitest";
import { HttpError } from "../operator/http";
import { isTenantReset, resetTolerantTenantPort } from "../operator/tenant/reset";
import type { OperatorTenantPort } from "../operator/tenant/port";

/** The texts workerd gives calls queued behind a reset (run9 and 0089). */
const RESETS = [
  "Durable Object storage is no longer accessible.",
  "Durable Object's isolate exceeded its CPU time limit and was reset.",
  "Durable Object reset because its code was updated.",
];

function stubs(behaviour: Array<"reset" | "ok" | Error>, text = RESETS[0]!) {
  const made: number[] = [];
  let calls = 0;
  const factory = () => {
    const n = made.length;
    made.push(n);
    return new Proxy({}, { get: () => async () => {
      const step = behaviour[Math.min(calls++, behaviour.length - 1)]!;
      if (step === "reset") throw new Error(text);
      if (step instanceof Error) throw step;
      return { ok: true, value: { stub: n } };
    } }) as unknown as OperatorTenantPort;
  };
  return { factory, made, calls: () => calls };
}
const noWait = { sleep: async () => undefined };

describe("operator calls on a tenant that was reset", () => {
  it("recognises the reset texts, and not overload or application errors", () => {
    for (const text of RESETS) expect(isTenantReset(new Error(text))).toBe(true);
    expect(isTenantReset(Object.assign(new Error("Durable Object is overloaded. Too many requests queued."), { overloaded: true }))).toBe(false);
    expect(isTenantReset(new Error("no such table: board"))).toBe(false);
    expect(isTenantReset("Durable Object storage is no longer accessible.")).toBe(false);
  });

  it("a read after the reset is answered by the fresh instance (the run9 GET /dev/data/tables/board/rows)", async () => {
    for (const text of RESETS) {
      const s = stubs(["reset", "ok"], text);
      const port = resetTolerantTenantPort("reducer-errors-a", s.factory, noWait);
      await expect(port.operatorReadRows("board", { limit: 1000, offset: 0 } as never)).resolves.toEqual({ ok: true, value: { stub: 1 } });
      expect(s.made).toEqual([0, 1]);
    }
  });

  it("a write the reset interrupted is not replayed and answers 503 transient, retryable", async () => {
    const s = stubs(["reset", "ok"]);
    const port = resetTolerantTenantPort("reducer-errors-a", s.factory, noWait);
    const failure = await port.operatorInsertRow("board", { id: "x" }).catch((e: unknown) => e);
    expect(failure).toBeInstanceOf(HttpError);
    expect(failure).toMatchObject({ status: 503, extra: { class: "transient", retryable: true } });
    expect((failure as Error).message).toMatch(/^tenant "reducer-errors-a" was reset while handling this request \(Durable Object storage is no longer accessible\.\); it was not retried, retry the request$/);
    expect(s.calls()).toBe(1);
    // The next call goes to a fresh stub.
    await expect(port.operatorListTables()).resolves.toEqual({ ok: true, value: { stub: 1 } });
  });

  it("a read that keeps failing on reset stops after its attempts with the same 503, never a 500", async () => {
    const s = stubs(["reset"]);
    const port = resetTolerantTenantPort("t", s.factory, { ...noWait, attempts: 3 });
    await expect(port.operatorActiveMembers()).rejects.toMatchObject({ status: 503, extra: { retryable: true } });
    expect(s.calls()).toBe(3);
  });

  it("other failures pass through unchanged and are not retried", async () => {
    const overloaded = Object.assign(new Error("Durable Object is overloaded."), { overloaded: true });
    const deleted = new Error("tenant deleted");
    for (const error of [overloaded, deleted, new Error("no such table: board")]) {
      const s = stubs([error, "ok"]);
      const port = resetTolerantTenantPort("t", s.factory, noWait);
      await expect(port.operatorReadRows("board", {} as never)).rejects.toBe(error);
      expect(s.calls()).toBe(1);
    }
  });
});
