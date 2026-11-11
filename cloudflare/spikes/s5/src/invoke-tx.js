// One invocation in one storage transaction, with S2's rules
// (cloudflare/spikes/s2/src/invoke.js), but with the dispatch step pluggable:
//
//   inline: dispatch runs in this isolate and host calls are local sync SQL (S2, "baked").
//   rpc:    dispatch runs in a Dynamic Worker; each host call comes back over
//           Workers RPC into this object, where SqlHost runs the SQL.
//   facet:  the whole of this function runs inside a Durable Object Facet
//           loaded from the Dynamic Worker, against the facet's own SQLite.
//
// `run(host)` must return the dispatch envelope promise.
import { SqlHost } from "./host.js";

export const reducerCapabilities = { dbRead: true, dbWrite: true, actionOutbox: true, scheduler: true, storage: true };

export function requestOf(call) {
  const kind = call.kind ?? "reducer";
  return {
    kind, function: call.path, now: call.now, intentEntropy: call.intentEntropy,
    identity: call.identity, invocation: call.invocation,
    capabilities: kind === "reducer" ? reducerCapabilities : { dbRead: true },
    maxResultBytes: 8 * 1024 * 1024,
  };
}

/**
 * options: { engine, transaction(run) , call, permissions, schedulable, wallNow, clock }
 * `clock()` is the only clock that moves on the platform: Date.now() advances
 * only across I/O there, so durations measured with it count waits, not CPU.
 */
export async function invokeTx(options, run) {
  const { engine, transaction, call } = options;
  const kind = call.kind ?? "reducer";
  return transaction(async (rollback) => {
    const host = new SqlHost(engine, {
      capability: kind, identity: call.identity, invocation: call.invocation,
      permissions: options.permissions, schedulable: options.schedulable,
      wallNow: options.wallNow ?? (() => call.now),
    });
    let envelope = await run(host);
    if (envelope.status === "ok" && host.aborted !== undefined) {
      envelope = { status: "error", kind: "commit", message: `transaction failed and was not committed: ${host.aborted}` };
    }
    let commit = envelope.status === "ok";
    await engine.settle(commit);
    if (commit) {
      const guard = engine.deferredViolation();
      if (guard.violation) { envelope = { status: "error", kind: "commit", message: guard.violation }; commit = false; }
    }
    if (!commit) rollback();
    const { statementsLog, ...stats } = host.stats;
    return { envelope, committed: commit, stats };
  });
}

/** The storage-transaction wrapper S2 recommends for K04. */
export const storageTransaction = (storage) => (run) => storage.transaction((txn) => run(() => txn.rollback()));

/** DDL one statement at a time, then seeded rows (S2's /provision). */
export function provisionSql(sql, { statements, rows, extra = [] }) {
  const failures = [];
  let executed = 0;
  for (const statement of [...statements, ...extra.map((text) => ({ type: "extra", name: "extra", sql: text }))]) {
    try { sql.exec(statement.sql); executed++; } catch (error) { failures.push({ type: statement.type, name: statement.name, error: String(error.message).slice(0, 200) }); }
  }
  let inserted = 0;
  for (const { table, rows: data } of rows ?? []) {
    for (const row of data) {
      const keys = Object.keys(row);
      try {
        sql.exec(`INSERT INTO "${table}" (${keys.map((k) => `"${k}"`).join(",")}) VALUES (${keys.map((_, i) => `?${i + 1}`).join(",")})`, ...keys.map((k) => row[k]));
        inserted++;
      } catch (error) { failures.push({ type: "row", name: table, error: String(error.message).slice(0, 200) }); }
    }
  }
  return { executed, inserted, failures: failures.length, refused: failures.filter((f) => f.type !== "row").map((f) => f.name), databaseSize: sql.databaseSize };
}

/** Row count per non-empty table, for cross-mode comparison. */
export function tableCounts(sql) {
  const out = {};
  for (const { name } of sql.exec("SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '_cf_%' AND name NOT LIKE '__miniflare%' ORDER BY name").toArray()) {
    const n = sql.exec(`SELECT count(*) AS n FROM "${name.replaceAll('"', '""')}"`).one().n;
    if (n) out[name] = n;
  }
  return out;
}
