// The tenant-side lease protocol with a controlled clock: fencing, the
// half-life persistence of renewals, backoff, the dead state, the sweep's
// recovery rules, receipts, and the rows each step writes.
import { env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { ActionOutbox, OUTBOX, RECEIPT_RETENTION_MS, retryDelayMs, RUNS, type ActionJob } from "../src/outbox.js";

interface Harness {
  outbox: ActionOutbox;
  jobs: ActionJob[];
  wakes: number[];
  clock: { now: number };
  sql: SqlStorage;
  insert(id: string, path?: string, extra?: Record<string, unknown>): void;
}

async function withOutbox<T>(run: (h: Harness, state: DurableObjectState) => Promise<T> | T, options: { leaseMs?: number; maxAttempts?: number } = {}): Promise<T> {
  const stub = env.TENANT.getByName(`outbox-${crypto.randomUUID()}`);
  return runInDurableObject(stub, async (_instance, state) => {
    const clock = { now: 1_000_000 };
    const jobs: ActionJob[] = [];
    const wakes: number[] = [];
    const make = () => new ActionOutbox({
      storage: state.storage, projectId: "p", tenantId: "t",
      dispatcher: { dispatch: async (job) => { jobs.push(job); } },
      wake: (at) => { wakes.push(at); },
      clock: () => clock.now,
      leaseMs: options.leaseMs ?? 300_000,
      maxAttempts: options.maxAttempts ?? 8,
    });
    const h: Harness = {
      outbox: make(), jobs, wakes, clock, sql: state.storage.sql,
      insert(id, path = "mail.send", extra = {}) {
        state.storage.sql.exec(`INSERT INTO ${OUTBOX} (id, action_path, args, actor_user_id, provenance, available_at) VALUES (?, ?, ?, ?, ?, ?)`,
          id, path, JSON.stringify(extra.args ?? { n: 1 }), (extra.actor as string | undefined) ?? "acct-1", "{}", (extra.availableAt as number | undefined) ?? clock.now);
      },
    };
    return run(h, state);
  });
}

describe("the outbox lease protocol", () => {
  it("claim fences on attempts; renewals write locked_at only below half a lease", async () => {
    await withOutbox(async (h) => {
      h.insert("a1");
      const claim = await h.outbox.claim({ v: 1, id: "a1", owner: "w1" });
      expect(claim).toMatchObject({ ok: true, attempt: 1, path: "mail.send", args: { n: 1 }, actorAccountId: "acct-1", leaseMs: 300_000, renewMs: 30_000 });
      expect(h.sql.exec(`SELECT status, attempts, locked_at FROM ${OUTBOX}`).one()).toEqual({ status: "processing", attempts: 1, locked_at: 1_000_000 });
      // Another owner: refused while leased.
      expect(await h.outbox.claim({ v: 1, id: "a1", owner: "w2" })).toEqual({ v: 1, ok: false, reason: "leased", retryAfterMs: 300_000 });
      // Every 30 s for 10 minutes: 20 renewals, persisted only when less than half the lease (150 s) is left.
      let persisted = 0;
      for (let i = 1; i <= 20; i++) {
        h.clock.now = 1_000_000 + i * 30_000;
        const renewal = await h.outbox.renew({ v: 1, id: "a1", attempt: 1 });
        expect(renewal.ok).toBe(true);
        if (renewal.persisted) persisted++;
      }
      expect(persisted).toBe(3); // at 180 s, 360 s and 540 s: about 24 writes per running hour
      expect(await h.outbox.renew({ v: 1, id: "a1", attempt: 2 })).toEqual({ v: 1, ok: false, persisted: false });
      // Stale and replayed completions.
      expect(await h.outbox.complete({ v: 1, id: "a1", attempt: 2 })).toEqual({ v: 1, applied: false, rejected: true });
      expect(await h.outbox.complete({ v: 1, id: "a1", attempt: 1, value: '{"ok":true}' })).toEqual({ v: 1, applied: true, appliedAttempt: 1 });
      expect(await h.outbox.complete({ v: 1, id: "a1", attempt: 1 })).toEqual({ v: 1, applied: false, duplicate: true, appliedAttempt: 1 });
      expect(h.sql.exec(`SELECT count(*) AS n FROM ${OUTBOX}`).one().n).toBe(0);
      expect(h.sql.exec(`SELECT state, attempts, result_bytes FROM ${RUNS}`).one()).toEqual({ state: "done", attempts: 1, result_bytes: 11 });
      expect(await h.outbox.claim({ v: 1, id: "a1", owner: "w3" })).toEqual({ v: 1, ok: false, reason: "done" });
    });
  });

  it("the same owner takes its own lease over at once (a retried Workflow step)", async () => {
    await withOutbox(async (h) => {
      h.insert("a2");
      await h.outbox.claim({ v: 1, id: "a2", owner: "w1" });
      expect(await h.outbox.claim({ v: 1, id: "a2", owner: "w1" })).toMatchObject({ ok: true, attempt: 2 });
      expect(await h.outbox.renew({ v: 1, id: "a2", attempt: 1 })).toMatchObject({ ok: false });
    });
  });

  it("failures back off as Rust does, and the 8th ends dead", async () => {
    expect([1, 2, 3, 10, 11, 30].map(retryDelayMs)).toEqual([1_000, 2_000, 4_000, 512_000, 900_000, 900_000]);
    await withOutbox(async (h) => {
      h.insert("a3");
      for (let attempt = 1; attempt <= 8; attempt++) {
        const claim = await h.outbox.claim({ v: 1, id: "a3", owner: `w${attempt}` });
        expect(claim).toMatchObject({ ok: true, attempt });
        const result = await h.outbox.fail({ v: 1, id: "a3", attempt, error: `boom ${attempt}` });
        expect(result.state).toBe(attempt < 8 ? "retry" : "dead");
        if (attempt < 8) {
          const row = h.sql.exec<{ status: string; available_at: number; last_error: string }>(`SELECT status, available_at, last_error FROM ${OUTBOX}`).one();
          expect(row).toEqual({ status: "pending", available_at: h.clock.now + retryDelayMs(attempt), last_error: `boom ${attempt}` });
          expect(await h.outbox.claim({ v: 1, id: "a3", owner: "early" })).toMatchObject({ ok: false, reason: "notDue" });
          h.clock.now = row.available_at;
        }
      }
      expect(h.sql.exec(`SELECT count(*) AS n FROM ${OUTBOX}`).one().n).toBe(0);
      expect(h.outbox.listDead()).toEqual({ items: [{ id: "a3", function: "mail.send", args: { n: 1 }, attempts: 8, lastError: "boom 8", deadAt: h.clock.now }], cursor: null });
      expect(await h.outbox.claim({ v: 1, id: "a3", owner: "late" })).toMatchObject({ ok: false, reason: "dead" });
      // A stale failure report changes nothing.
      expect(await h.outbox.fail({ v: 1, id: "a3", attempt: 8, error: "again" })).toEqual({ v: 1, state: "stale" });
      expect(await h.outbox.requeue("a3")).toBe(true);
      expect(h.sql.exec(`SELECT status, attempts, actor_user_id FROM ${OUTBOX}`).one()).toEqual({ status: "pending", attempts: 0, actor_user_id: "acct-1" });
      expect(h.jobs.at(-1)).toMatchObject({ id: "a3", n: 1 });
    });
  });

  it("a terminal failure (the hop limit) is dead at once; a row already at MAX_ATTEMPTS is dead at claim", async () => {
    await withOutbox(async (h) => {
      h.insert("a4");
      await h.outbox.claim({ v: 1, id: "a4", owner: "w" });
      expect(await h.outbox.fail({ v: 1, id: "a4", attempt: 1, error: "durable work chain exceeded the maximum of 32 scheduled or outbox hops", terminal: true })).toEqual({ v: 1, state: "dead" });
      h.insert("a5");
      h.sql.exec(`UPDATE ${OUTBOX} SET attempts = 2, last_error = 'old' WHERE id = 'a5'`);
      expect(await h.outbox.claim({ v: 1, id: "a5", owner: "w" })).toMatchObject({ ok: false, reason: "dead" });
      expect(h.outbox.listDead().items.map((item) => [item.id, item.lastError])).toEqual([["a4", "durable work chain exceeded the maximum of 32 scheduled or outbox hops"], ["a5", "old"]]);
    }, { maxAttempts: 2 });
  });

  it("the sweep: hands off new rows, re-hands an unclaimed hand-off after one lease, an expired lease at once, a due retry; never scheduler rows", async () => {
    await withOutbox(async (h) => {
      h.insert("new");
      h.insert("sched", "_gonvex.scheduler.enqueue");
      await h.outbox.afterCommit();
      expect(h.jobs.map((job) => [job.id, job.n])).toEqual([["new", 1]]);
      // Not claimed within one lease: handed off again.
      h.clock.now += 299_999;
      expect(await h.outbox.sweep()).toBe(1_000_000 + 300_000);
      h.clock.now += 1;
      await h.outbox.sweep();
      expect(h.jobs.map((job) => [job.id, job.n])).toEqual([["new", 1], ["new", 2]]);
      // Claimed, then the runner goes silent: due again one lease after the persisted locked_at.
      await h.outbox.claim({ v: 1, id: "new", owner: "zombie" });
      const claimedAt = h.clock.now;
      h.clock.now = claimedAt + 200_000;
      await h.outbox.renew({ v: 1, id: "new", attempt: 1 }); // persisted (less than half left)
      h.clock.now = claimedAt + 200_000 + 299_999;
      await h.outbox.sweep();
      expect(h.jobs).toHaveLength(2);
      h.clock.now += 1;
      await h.outbox.sweep();
      expect(h.jobs.map((job) => job.n)).toEqual([1, 2, 3]);
      expect(await h.outbox.claim({ v: 1, id: "new", owner: "fresh" })).toMatchObject({ ok: true, attempt: 2 });
      expect(await h.outbox.complete({ v: 1, id: "new", attempt: 1 })).toMatchObject({ applied: false, rejected: true });
      expect(h.sql.exec(`SELECT count(*) AS n FROM ${OUTBOX} WHERE action_path = '_gonvex.scheduler.enqueue'`).one().n).toBe(1);
    });
  });

  it("a lease extended only in memory never outlives an object reset: recovery uses locked_at", async () => {
    await withOutbox(async (h, state) => {
      h.insert("r1");
      await h.outbox.claim({ v: 1, id: "r1", owner: "w" });
      h.clock.now += 100_000;
      expect(await h.outbox.renew({ v: 1, id: "r1", attempt: 1 })).toMatchObject({ ok: true, persisted: false });
      // A fresh instance (the alarm woke a reset object): no in-memory lease.
      const fresh = new ActionOutbox({ storage: state.storage, projectId: "p", tenantId: "t", dispatcher: { dispatch: async (job) => { h.jobs.push(job); } }, wake: () => undefined, clock: () => h.clock.now });
      expect(fresh.inspect("r1")).toMatchObject({ leaseExpiry: 1_300_000 });
      h.clock.now = 1_300_000;
      await fresh.sweep();
      expect(h.jobs.at(-1)).toMatchObject({ id: "r1" });
    });
  });

  it("cancel ends a live row dead; its next renewal is refused", async () => {
    await withOutbox(async (h) => {
      h.insert("c1");
      await h.outbox.claim({ v: 1, id: "c1", owner: "w" });
      expect(await h.outbox.cancel("c1")).toBe(true);
      expect(await h.outbox.renew({ v: 1, id: "c1", attempt: 1 })).toMatchObject({ ok: false });
      expect(await h.outbox.cancel("c1")).toBe(false);
      expect(h.outbox.listDead().items[0]).toMatchObject({ id: "c1", lastError: "cancelled" });
    });
  });

  it("receipts are pruned after the retention", async () => {
    await withOutbox(async (h) => {
      h.insert("p1");
      await h.outbox.claim({ v: 1, id: "p1", owner: "w" });
      await h.outbox.complete({ v: 1, id: "p1", attempt: 1 });
      expect(await h.outbox.sweep()).toBe(h.clock.now + RECEIPT_RETENTION_MS);
      h.clock.now += RECEIPT_RETENTION_MS + 1;
      expect(await h.outbox.sweep()).toBeNull();
      expect(h.sql.exec(`SELECT count(*) AS n FROM ${RUNS}`).one().n).toBe(0);
    });
  });

  it("direct calls: a run of origin `call`, delivered once through onFinished", async () => {
    const stub = env.TENANT.getByName(`calls-${crypto.randomUUID()}`);
    await runInDurableObject(stub, async (_instance, state) => {
      const finished: unknown[] = [];
      const outbox = new ActionOutbox({ storage: state.storage, projectId: "p", tenantId: "t", dispatcher: { dispatch: async () => undefined }, wake: () => undefined, onFinished: (f) => { finished.push(f); } });
      const session = { projectId: "p", tenantId: "t", account: { id: "acct-1", email: "a@x" }, member: { id: "m-1" } };
      const id = await outbox.beginCall({ session, path: "agent.turn", args: { q: 1 } });
      const claim = await outbox.claim({ v: 1, id, owner: "w" });
      expect(claim).toMatchObject({ ok: true, origin: "call", attempt: 1, path: "agent.turn", args: { q: 1 }, session });
      expect(await outbox.complete({ v: 1, id, attempt: 1, value: '{"answer":42}' })).toMatchObject({ applied: true });
      expect(await outbox.complete({ v: 1, id, attempt: 1, value: '{"answer":42}' })).toMatchObject({ duplicate: true });
      expect(finished).toEqual([{ id, ok: true, value: '{"answer":42}' }]);
      const big = await outbox.beginCall({ session, path: "agent.turn", args: {} });
      await outbox.claim({ v: 1, id: big, owner: "w" });
      await outbox.complete({ v: 1, id: big, attempt: 1, value: JSON.stringify("x".repeat(2 << 20)) });
      // Delivered whole, kept only up to 1 MiB (a DO row holds 2 MB).
      expect((finished[1] as { value: string }).value.length).toBe((2 << 20) + 2);
      expect(state.storage.sql.exec(`SELECT result IS NULL AS dropped, result_bytes FROM ${RUNS} WHERE id = ?`, big).one()).toEqual({ dropped: 1, result_bytes: (2 << 20) + 2 });
    });
  });

  it("rows written per Action (the PR's numbers)", async () => {
    await withOutbox(async (h) => {
      h.insert("n1");
      const base = { ...h.outbox.stats };
      const delta = () => { const now = { ...h.outbox.stats }; const d = now.rowsWritten - base.rowsWritten; base.rowsWritten = now.rowsWritten; return d; };
      await h.outbox.afterCommit();
      const handOff = delta();
      await h.outbox.claim({ v: 1, id: "n1", owner: "w" });
      const claim = delta();
      let renewals = 0;
      for (let i = 1; i <= 120; i++) { h.clock.now += 30_000; await h.outbox.renew({ v: 1, id: "n1", attempt: 1 }); }
      renewals = delta();
      await h.outbox.complete({ v: 1, id: "n1", attempt: 1 });
      const complete = delta();
      // SQLite rows written, index entries included. Recorded in README.md; asserted so a change shows up here.
      expect({ handOff, claim, renewalsPerHour: renewals, complete }).toEqual({ handOff: 3, claim: 4, renewalsPerHour: 40, complete: 3 });
    });
  });
});
