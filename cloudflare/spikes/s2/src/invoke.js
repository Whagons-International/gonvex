// One Reducer (or Query) invocation in one atomic transaction.
//
// `transaction(run)` is engine-specific: in the Durable Object it is
// ctx.storage.transaction(async txn => run(() => txn.rollback())); in the
// node:sqlite reference it is BEGIN ... COMMIT/ROLLBACK. The module's awaits
// all happen inside it. A handler error, or a SQL failure the module caught
// without ROLLBACK TO (Rust marks such a transaction failed and refuses to
// commit it), rolls the whole invocation back.
import { dispatch } from "./bootstrap.js";
import { SqlHost } from "./host.js";

/**
 * Settles `promise` while pumping microtasks. If it settles before `limit`
 * hops, no await inside it waited on real I/O or a timer: the whole
 * invocation ran inside one macrotask (JS turn).
 */
async function settleWithinMicrotasks(promise, limit = 200_000) {
  let settled = false;
  promise.then(() => { settled = true; }, () => { settled = true; });
  let hops = 0;
  while (!settled && hops < limit) { await null; hops++; }
  return { withinOneTurn: settled, hops };
}

export async function invoke(options) {
  const { engine, transaction, binding, path, kind = "reducer", args, argsSchema, identity, invocation, now, intentEntropy } = options;
  const request = {
    kind, function: path, now, intentEntropy, identity, invocation,
    capabilities: kind === "reducer" ? { dbRead: true, dbWrite: true, actionOutbox: true, scheduler: true, storage: true } : { dbRead: true },
    maxResultBytes: 8 * 1024 * 1024,
  };
  return transaction(async (rollback) => {
    const host = new SqlHost(engine, {
      capability: kind, identity, invocation,
      permissions: options.permissions, schedulable: options.schedulable,
      wallNow: options.wallNow ?? (() => now), decltypeFallback: options.decltypeFallback,
    });
    const running = dispatch(binding, request, args, (payload) => host.handle(payload), argsSchema);
    const turn = options.measureTurns ? await settleWithinMicrotasks(running) : undefined;
    let envelope = await running;
    if (envelope.status === "ok" && host.aborted !== undefined) {
      envelope = { status: "error", kind: "commit", message: `transaction failed and was not committed: ${host.aborted}` };
    }
    let commit = envelope.status === "ok";
    await engine.settle(commit);
    if (commit) {
      const guard = engine.deferredViolation();
      host.stats.fkGuardRowsRead = guard.rowsRead;
      host.stats.fkGuardMs = guard.ms;
      if (guard.violation) { envelope = { status: "error", kind: "commit", message: guard.violation }; commit = false; }
    }
    if (!commit) rollback();
    return { envelope, committed: commit, stats: host.stats, turn };
  });
}
