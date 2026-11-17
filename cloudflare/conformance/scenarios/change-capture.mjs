/**
 * Change capture (K03): the change log a backend writes for a fixed set of
 * Reducer transactions. Each transaction is one `capture.apply` call
 * (fixtures/app/gonvex/capture.ts) with a chosen command id, so command ids
 * and provenance are comparable. After the writes the scenario reads the
 * tenant's `_gonvex_sync_changes` and `_gonvex_sync_transactions` through the
 * adapter (`readChangeLog`, capability `changeLog`) and records them with
 * only what the contract leaves free removed: event ids become ranks,
 * revisions are relative to the first one, and created_at is dropped.
 *
 * The same TRANSACTIONS are replayed through the kernel's capture in workerd
 * against the Rust output recorded by `src/capture-golden.mjs`
 * (cloudflare/kernel/capture/test/capture-parity.test.ts).
 */

import { numbersByValue } from '../src/number-text.mjs';

const CTE_ROWS = `WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 40)
  INSERT INTO capture_values (id, label, n, amount, flag) SELECT printf('bulk-%02d', i), CASE WHEN i % 2 THEN 'odd' ELSE 'even' END, i, i / 4.0, i % 3 = 0 FROM n`;

/** One Reducer each. `fail` rolls the Reducer back after its statements. */
export const TRANSACTIONS = [
  { id: 'k03-01-insert', note: 'insert with every column kind; a trigger writes capture_totals', steps: [
    { sql: `INSERT INTO capture_values VALUES ('v1', 'red', 3, 1.5, 1, '{"a":[1,2],"b":"x"}', 'hello')` },
  ] },
  { id: 'k03-02-values', note: 'large integer, computed REAL, false, malformed JSON, escapes and non-ASCII text', steps: [
    { sql: `INSERT INTO capture_values VALUES ('v2', 'red', 9007199254740993, 0.1 + 0.2, 0, 'not json', char(1, 9, 10, 31, 34, 92, 127) || 'é😀/' || char(8232))` },
  ] },
  { id: 'k03-03-reals', note: 'REAL values whose JSON text has an exponent or a sign (0026)', steps: [
    { sql: `INSERT INTO capture_values (id, amount) VALUES ('r1', 1e300), ('r2', 1e-7), ('r3', 2.5e-5), ('r4', 1e16), ('r5', -0.0), ('r6', 9e999), ('r7', 5), ('r8', 123456789.125), ('r9', 1e15)` },
  ] },
  { id: 'k03-04-noop', note: 'an update that changes nothing: no revision', steps: [
    { sql: `UPDATE capture_values SET label = label, n = '3' WHERE id = 'v1'` },
  ] },
  { id: 'k03-05-update', note: 'several columns, a storage-class change (TEXT to BLOB), JSON rewritten', steps: [
    { sql: `UPDATE capture_values SET note = x'616263', meta = '{"b":"x","a":[1,2]}', flag = NULL WHERE id = 'v1'` },
    { sql: `UPDATE capture_values SET n = n + 1 WHERE id IN ('v1', 'v2')` },
  ] },
  { id: 'k03-06-caught', note: 'a multi-row insert fails on its second row inside a savepoint; the rest commits', steps: [
    { sql: `SAVEPOINT k03_caught` },
    { sql: `INSERT INTO capture_values (id) VALUES ('v9'), ('v1')`, catch: true },
    { sql: `ROLLBACK TO k03_caught` },
    { sql: `RELEASE k03_caught` },
    { sql: `INSERT INTO capture_values (id, label) VALUES ('v10', 'blue')` },
  ] },
  { id: 'k03-07-savepoint', note: 'writes rolled back to a savepoint leave no change rows', steps: [
    { sql: `SAVEPOINT k03_outer` },
    { sql: `INSERT INTO capture_values (id) VALUES ('gone')` },
    { sql: `UPDATE capture_values SET note = 'gone' WHERE id = 'v10'` },
    { sql: `ROLLBACK TO k03_outer` },
    { sql: `RELEASE k03_outer` },
    { sql: `INSERT INTO capture_values (id, note) VALUES ('v11', 'kept')` },
  ] },
  { id: 'k03-08-counter', note: 'integer key from the rowid; an app trigger updates the row it fired for', steps: [
    { sql: `INSERT INTO capture_counters (total) VALUES (1)` },
    { sql: `UPDATE capture_counters SET total = 5 WHERE id = 1` },
  ] },
  { id: 'k03-09-pairs', note: 'a trigger writes a captured WITHOUT ROWID table: Rust fails the write (no such column: NEW.rowid, note 0026)', steps: [
    { sql: `INSERT INTO capture_pair_sources VALUES ('p1', 2)` },
  ] },
  { id: 'k03-10-derived', note: 'a derived table is not captured: no revision', steps: [
    { sql: `INSERT INTO capture_edges VALUES ('e1', 'v1'), ('e2', 'v2')` },
  ] },
  { id: 'k03-11-delete', note: 'deletes: a cascade into the derived table, and a trigger updating capture_totals', steps: [
    { sql: `DELETE FROM capture_values WHERE id = 'v2'` },
  ] },
  { id: 'k03-12-record', note: 'a logical record: storage, owned tags, a dependency label', steps: [
    { sql: `INSERT INTO capture_labels VALUES ('l1', 'Urgent', 'red')` },
    { sql: `INSERT INTO capture_doc_storage VALUES ('d1', 'Doc', 0, 'l1'), ('d2', 'Other', 1, NULL)` },
    { sql: `INSERT INTO capture_doc_tags VALUES ('t1', 'd1', 'b'), ('t2', 'd1', 'a')` },
  ] },
  { id: 'k03-13-dependency', note: 'a watched dependency column changes the record; an unwatched one does not', steps: [
    { sql: `UPDATE capture_labels SET text = 'Later' WHERE id = 'l1'` },
    { sql: `UPDATE capture_labels SET color = 'blue' WHERE id = 'l1'` },
  ] },
  { id: 'k03-14-record-noop', note: 'record storage rewritten with the same values: no record change, no revision', steps: [
    { sql: `UPDATE capture_doc_storage SET title = title` },
  ] },
  { id: 'k03-15-record-delete', note: 'an owned row and a record deleted (with its cascade)', steps: [
    { sql: `DELETE FROM capture_doc_tags WHERE id = 't2'` },
    { sql: `UPDATE capture_doc_storage SET done = 1 WHERE _id = 'd1'` },
    { sql: `DELETE FROM capture_doc_storage WHERE _id = 'd2'` },
  ] },
  { id: 'k03-16-rejected', note: 'a Reducer that throws after writing: nothing commits', fail: true, steps: [
    { sql: `INSERT INTO capture_values (id) VALUES ('rejected')` },
  ] },
  { id: 'k03-17-params', note: 'bound parameters of every JSON type', steps: [
    { sql: `INSERT INTO capture_values (id, label, n, amount, flag, meta) VALUES ($1, $2, $3, $4, $5, $6)`, params: ['v12', 'params', 7, 2.25, true, { k: [1, 'two', null] }] },
  ] },
  { id: 'k03-18-bulk', note: 'one statement writing 40 rows: ordinals follow event order', steps: [{ sql: CTE_ROWS }] },
];

/** The change log after `base`, without what the contract leaves free. */
export function comparableLog(log, base) {
  const changes = log.changes.filter(c => c.revision !== null && c.revision > base);
  const order = new Map([...changes].sort((a, b) => a.event_id - b.event_id).map((c, i) => [c.event_id, i + 1]));
  return {
    pending: log.changes.filter(c => c.revision === null).length,
    changes: changes
      .sort((a, b) => a.revision - b.revision || a.ordinal - b.ordinal)
      .map(c => ({
        rev: c.revision - base, sameTransaction: c.transaction_id === c.revision, ordinal: c.ordinal, event: order.get(c.event_id),
        command: c.command_id, table: c.table_name, row: c.row_id, op: c.operation,
        old: numbersByValue(c.old_value), new: numbersByValue(c.new_value), changed: c.changed_columns,
        createdAtIsInteger: Number.isSafeInteger(c.created_at),
      })),
    transactions: log.transactions
      .filter(t => t.revision > base)
      .map(t => ({
        rev: t.revision - base, sameTransaction: t.transaction_id === t.revision, root: t.root_command_id, origin: t.origin_command_id,
        rootChannel: t.root_invocation_channel, channel: t.invocation_channel, actorAccount: t.actor_account_id, actorMember: t.actor_member_id,
        onBehalfOf: t.on_behalf_of_member_id, agentExecution: t.agent_execution_id,
      })),
    clock: { revisionsAfterBase: log.clock.revision - base, retainedIsBase: log.clock.retained_revision <= base },
  };
}

export default {
  name: 'change-capture',
  description: 'Change rows, revisions and provenance for fixed Reducer writes (K03)',
  tenants: { a: ['alice'] },
  requires: ['changeLog'],
  tables: ['board'],
  async run(ctx) {
    const tenant = ctx.tenants.a;
    let socket, base;
    await ctx.step('connect and read the clock', async () => {
      socket = await ctx.raw('writer');
      const session = await ctx.rawLogin(socket, 'alice');
      await socket.auth({ project: ctx.project, tenant, token: session.accessToken });
      base = (await ctx.backend.readChangeLog(ctx.project, tenant)).clock.revision;
    });
    await ctx.step('apply the transactions', async () => {
      for (const t of TRANSACTIONS) {
        socket.send({ type: 'reducer.call', id: t.id, path: 'capture.apply', args: { steps: t.steps, fail: t.fail ?? false } });
        const reply = await socket.next(m => (m.type === 'reducer.result' || m.type === 'reducer.error') && m.id === t.id, { label: `${t.id} reply` });
        ctx.observe(t.id, reply.type === 'reducer.result'
          ? { result: reply.result, committed: reply.committedRevision === undefined ? 'none' : 'revision' }
          : { error: reply.error, class: reply.class ?? null });
      }
    });
    await ctx.step('read the change log', async () => {
      const log = comparableLog(await ctx.backend.readChangeLog(ctx.project, tenant), base);
      ctx.observe('changeLog', log);
      ctx.expect(log.pending === 0, 'no unrevisioned change rows after commit');
      ctx.expect(log.changes.every(c => c.sameTransaction && c.createdAtIsInteger), 'transaction_id = revision and integer created_at on every change');
    });
  },
};
