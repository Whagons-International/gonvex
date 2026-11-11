// The run protocol of direct agent calls (`ActionCalls`) with a controlled
// clock: fencing, the half-life persistence of renewals, the sweep's recovery
// rules, cancel, the attempt cap, receipts, and the rows each step writes.
// `_gonvex_action_outbox` is K11's (0037): nothing here touches it.
import { env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { ACTION_OBJECTS, ActionCalls, RECEIPT_RETENTION_MS, RUNS, type CallFinished, type CallJob } from "../src/calls.js";

const session = { projectId: "p", tenantId: "t", account: { id: "acct-1", email: "a@x" }, member: { id: "m-1" } };

interface Harness {
  calls: ActionCalls;
  jobs: CallJob[];
  finished: CallFinished[];
  clock: { now: number };
  sql: SqlStorage;
  fail: { dispatch: boolean };
  make(): ActionCalls;
}

async function withCalls<T>(run: (h: Harness, state: DurableObjectState) => Promise<T> | T, options: { leaseMs?: number; maxAttempts?: number } = {}): Promise<T> {
  const stub = env.TENANT.getByName(`calls-${crypto.randomUUID()}`);
  return runInDurableObject(stub, async (_instance, state) => {
    const clock = { now: 1_000_000 };
    const jobs: CallJob[] = [];
    const finished: CallFinished[] = [];
    const fail = { dispatch: false };
    let n = 0;
    const make = () => new ActionCalls({
      storage: state.storage, projectId: "p", tenantId: "t",
      dispatcher: { dispatch: async (job) => { if (fail.dispatch) throw new Error("create failed"); jobs.push(job); } },
      wake: () => undefined,
      onFinished: (f) => { finished.push(f); },
      clock: () => clock.now,
      randomUUID: () => `uuid-${++n}`,
      leaseMs: options.leaseMs ?? 300_000,
      maxAttempts: options.maxAttempts ?? 8,
    });
    return run({ calls: make(), jobs, finished, clock, sql: state.storage.sql, fail, make }, state);
  });
}

describe("direct agent calls (ActionCalls)", () => {
  it("begin hands off once; claim fences on attempts; renewals write locked_at only below half a lease", async () => {
    await withCalls(async (h) => {
      const id = await h.calls.begin({ session, path: "agent.turn", args: { q: 1 } });
      expect(id).toBe("call-uuid-1");
      expect(h.jobs).toEqual([{ v: 1, kind: "call", projectId: "p", tenantId: "t", id, n: 1 }]);
      const claim = await h.calls.claim({ v: 1, id, owner: "w1" });
      expect(claim).toMatchObject({ ok: true, attempt: 1, path: "agent.turn", args: { q: 1 }, session, leaseMs: 300_000, renewMs: 30_000 });
      expect(await h.calls.claim({ v: 1, id, owner: "w2" })).toEqual({ v: 1, ok: false, reason: "leased", retryAfterMs: 300_000 });
      let persisted = 0;
      for (let i = 1; i <= 20; i++) {
        h.clock.now = 1_000_000 + i * 30_000;
        const renewal = await h.calls.renew({ v: 1, id, attempt: 1 });
        expect(renewal.ok).toBe(true);
        if (renewal.persisted) persisted++;
      }
      expect(persisted).toBe(3); // 180 s, 360 s, 540 s: about 20 writes per running hour
      expect(await h.calls.renew({ v: 1, id, attempt: 2 })).toEqual({ v: 1, ok: false, persisted: false });
      expect(await h.calls.complete({ v: 1, id, attempt: 2, value: "1" })).toEqual({ v: 1, applied: false, rejected: true });
      expect(await h.calls.complete({ v: 1, id, attempt: 1, value: '{"answer":42}' })).toEqual({ v: 1, applied: true, appliedAttempt: 1 });
      expect(await h.calls.complete({ v: 1, id, attempt: 1, value: '{"answer":42}' })).toEqual({ v: 1, applied: false, duplicate: true, appliedAttempt: 1 });
      expect(h.finished).toEqual([{ id, ok: true, value: '{"answer":42}' }]);
      expect(await h.calls.claim({ v: 1, id, owner: "w3" })).toEqual({ v: 1, ok: false, reason: "done" });
    });
  });

  it("never touches K11's outbox", async () => {
    await withCalls(async (h) => {
      const id = await h.calls.begin({ session, path: "agent.turn", args: {} });
      await h.calls.claim({ v: 1, id, owner: "w" });
      await h.calls.complete({ v: 1, id, attempt: 1, value: "null" });
      await h.calls.sweep();
      // The tenant has K03's outbox table (installed with the runtime tables); a direct call leaves it empty.
      expect(h.sql.exec("SELECT count(*) AS n FROM _gonvex_action_outbox").one()).toEqual({ n: 0 });
      expect(ACTION_OBJECTS.map(([, name]) => name)).toEqual([RUNS, "_gonvex_st_action_runs_live", "_gonvex_st_action_runs_finished"]);
    });
  });

  it("the same owner takes its own lease over at once (a retried Workflow step); an error is the answer, delivered once", async () => {
    await withCalls(async (h) => {
      const id = await h.calls.begin({ session, path: "agent.turn", args: {} });
      await h.calls.claim({ v: 1, id, owner: "w1" });
      expect(await h.calls.claim({ v: 1, id, owner: "w1" })).toMatchObject({ ok: true, attempt: 2 });
      expect(await h.calls.renew({ v: 1, id, attempt: 1 })).toMatchObject({ ok: false });
      expect(await h.calls.complete({ v: 1, id, attempt: 2, error: "boom" })).toMatchObject({ applied: true });
      expect(h.finished).toEqual([{ id, ok: false, error: "boom" }]);
    });
  });

  it("the sweep hands off again an unclaimed hand-off after one lease and an expired lease; a failed create backs off", async () => {
    await withCalls(async (h) => {
      const id = await h.calls.begin({ session, path: "agent.turn", args: {} });
      expect(h.calls.nextAt(h.clock.now)).toBe(1_300_000);
      h.clock.now += 299_999;
      await h.calls.sweep();
      expect(h.jobs.map((job) => job.n)).toEqual([1]);
      h.clock.now += 1;
      await h.calls.sweep();
      expect(h.jobs.map((job) => job.n)).toEqual([1, 2]);
      // Claimed, then the runner goes silent: due one lease after the persisted locked_at.
      await h.calls.claim({ v: 1, id, owner: "zombie" });
      const claimedAt = h.clock.now;
      h.clock.now = claimedAt + 200_000;
      expect(await h.calls.renew({ v: 1, id, attempt: 1 })).toMatchObject({ persisted: true });
      h.clock.now = claimedAt + 200_000 + 299_999;
      await h.calls.sweep();
      expect(h.jobs).toHaveLength(2);
      h.clock.now += 1;
      await h.calls.sweep();
      expect(h.jobs.map((job) => job.n)).toEqual([1, 2, 3]);
      expect(await h.calls.claim({ v: 1, id, owner: "fresh" })).toMatchObject({ ok: true, attempt: 2 });
      expect(await h.calls.complete({ v: 1, id, attempt: 1, value: "1" })).toMatchObject({ applied: false, rejected: true });
      // A create that fails is tried again after one renewal interval, not in a tight alarm loop.
      h.fail.dispatch = true;
      const other = await h.calls.begin({ session, path: "agent.turn", args: {} });
      expect(h.calls.nextAt(h.clock.now)).toBe(h.clock.now + 30_000);
      h.fail.dispatch = false;
      h.clock.now += 30_000;
      expect(await h.calls.sweep()).toBe(1);
      expect(h.jobs.at(-1)).toMatchObject({ id: other, n: 1 });
    });
  });

  it("a lease extended only in memory never outlives an object reset: recovery uses locked_at", async () => {
    await withCalls(async (h) => {
      const id = await h.calls.begin({ session, path: "agent.turn", args: {} });
      await h.calls.claim({ v: 1, id, owner: "w" });
      h.clock.now += 100_000;
      expect(await h.calls.renew({ v: 1, id, attempt: 1 })).toMatchObject({ ok: true, persisted: false });
      const fresh = h.make();
      expect(fresh.inspect(id)).toMatchObject({ leaseExpiry: 1_300_000 });
      h.clock.now = 1_300_000;
      await fresh.sweep();
      expect(h.jobs.at(-1)).toMatchObject({ id, n: 2 });
    });
  });

  it("cancel ends a live call dead and answers it; a runner that dies too often fails the call", async () => {
    await withCalls(async (h) => {
      const id = await h.calls.begin({ session, path: "agent.turn", args: {} });
      await h.calls.claim({ v: 1, id, owner: "w" });
      expect(await h.calls.cancel(id)).toBe(true);
      expect(await h.calls.renew({ v: 1, id, attempt: 1 })).toMatchObject({ ok: false });
      expect(await h.calls.cancel(id)).toBe(false);
      const dying = await h.calls.begin({ session, path: "agent.turn", args: {} });
      for (let owner = 1; owner <= 2; owner++) expect(await h.calls.claim({ v: 1, id: dying, owner: `w${owner}` })).toMatchObject({ ok: owner === 1 ? true : false });
      h.clock.now += 300_001;
      expect(await h.calls.claim({ v: 1, id: dying, owner: "w2" })).toMatchObject({ ok: true, attempt: 2 });
      h.clock.now += 300_001;
      expect(await h.calls.claim({ v: 1, id: dying, owner: "w3" })).toMatchObject({ ok: false, reason: "dead" });
      expect(h.finished).toEqual([
        { id, ok: false, error: "cancelled" },
        { id: dying, ok: false, error: "Action runner stopped 2 times without finishing" },
      ]);
    }, { maxAttempts: 2 });
  });

  it("a result is delivered whole and kept for a duplicate only up to 1 MiB; receipts are pruned after the retention", async () => {
    await withCalls(async (h) => {
      const id = await h.calls.begin({ session, path: "agent.turn", args: {} });
      await h.calls.claim({ v: 1, id, owner: "w" });
      await h.calls.complete({ v: 1, id, attempt: 1, value: JSON.stringify("x".repeat(2 << 20)) });
      expect(h.finished[0]!.value!.length).toBe((2 << 20) + 2);
      expect(h.sql.exec(`SELECT result IS NULL AS dropped, result_bytes FROM ${RUNS} WHERE id = ?`, id).one()).toEqual({ dropped: 1, result_bytes: (2 << 20) + 2 });
      expect(h.calls.nextAt(h.clock.now)).toBe(h.clock.now + RECEIPT_RETENTION_MS);
      h.clock.now += RECEIPT_RETENTION_MS + 1;
      expect(await h.calls.sweep()).toBe(1);
      expect(h.calls.nextAt(h.clock.now)).toBeNull();
    });
  });

  it("rows written per direct call (the PR's numbers)", async () => {
    await withCalls(async (h) => {
      let base = 0;
      const delta = () => { const d = h.calls.stats.rowsWritten - base; base = h.calls.stats.rowsWritten; return d; };
      const id = await h.calls.begin({ session, path: "agent.turn", args: { q: 1 } });
      const begin = delta();
      await h.calls.claim({ v: 1, id, owner: "w" });
      const claim = delta();
      for (let i = 1; i <= 120; i++) { h.clock.now += 30_000; await h.calls.renew({ v: 1, id, attempt: 1 }); }
      const renewalsPerHour = delta();
      await h.calls.complete({ v: 1, id, attempt: 1, value: "{}" });
      const complete = delta();
      // SQLite rows written, index entries included. Recorded in README.md; asserted so a change shows up here.
      expect({ begin, claim, renewalsPerHour, complete }).toEqual({ begin: 4, claim: 1, renewalsPerHour: 20, complete: 2 });
    });
  });
});
