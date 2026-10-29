import { DatabaseSync } from 'node:sqlite';
import { setImmediate } from 'node:timers/promises';
import { afterEach, expect, it } from 'vitest';
import { ExpoSQLiteLocalReplicaStorage, type ExpoSQLiteDatabase } from './index.js';
import { ExpoSQLiteOutboxStore } from './outbox.js';

const handles: Handle[] = [];
afterEach(() => { for (const handle of handles.splice(0)) handle.sql.close(); });
function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>(done => { resolve = done; });
  return { promise, resolve };
}
class Handle implements ExpoSQLiteDatabase {
  readonly sql = new DatabaseSync(':memory:');
  readonly statements: string[] = [];
  private pause?: { table: string; entered: ReturnType<typeof deferred>; release: ReturnType<typeof deferred>; fail: boolean };
  constructor() { handles.push(this); }
  holdNextWrite(table: string, fail = false) {
    const pause = { table, entered: deferred(), release: deferred(), fail };
    this.pause = pause;
    return pause;
  }
  async execAsync(sql: string) { this.statements.push(sql); this.sql.exec(sql); }
  async runAsync(sql: string, ...params: unknown[]) {
    const result = this.sql.prepare(sql).run(...params as any[]);
    const pause = this.pause;
    if (pause && sql.includes(`INTO ${pause.table} `)) {
      this.pause = undefined;
      pause.entered.resolve();
      await pause.release.promise;
      if (pause.fail) throw Error('injected failure after a successful SQLite write');
    }
    return result;
  }
  async getAllAsync<T>(sql: string, ...params: unknown[]) { return this.sql.prepare(sql).all(...params as any[]) as T[]; }
  async getFirstAsync<T>(sql: string, ...params: unknown[]) { return (this.sql.prepare(sql).get(...params as any[]) as T) ?? null; }
  // The native Expo JS transaction method catches BEGIN failures too. Omitting
  // that rollback would hide one operation rolling back another's transaction.
  async withTransactionAsync(task: () => Promise<void>) {
    try { await this.execAsync('BEGIN'); await task(); await this.execAsync('COMMIT'); }
    catch (error) { await this.execAsync('ROLLBACK'); throw error; }
  }
}
const empty = { entities: {}, liveQueries: {} };
const base = { cursor: { epoch: 'e', revision: 7 }, entities: { tasks: { a: { id: 'a', name: 'Original' } } }, liveQueries: {} };
const change = { cursor: { epoch: 'e', revision: 8 }, changes: [
  { entity: 'tasks', id: 'a', operation: 'update' as const, newValue: { id: 'a', name: 'Changed' } },
  { entity: 'tasks', id: 'c', operation: 'insert' as const, newValue: { id: 'c', name: 'New' } },
] };
const draft = (key: string) => ({ scope: 'account', path: 'tasks.write', args: { key },
  idempotencyKey: key, entityKeys: [], createdAt: 1, attempts: 0, nextAttemptAt: 0, state: 'pending' as const });

it('one replica and outbox sharing a handle cannot roll back each other or certify partial data', async () => {
  const handle = new Handle(), replica = new ExpoSQLiteLocalReplicaStorage(handle), outbox = new ExpoSQLiteOutboxStore(handle);
  await replica.replaceSnapshot(base, 'account'); await outbox.load();
  const hold = handle.holdNextWrite('_gonvex_replica_entities');
  const write = replica.applyTransaction(change, empty, 'account');
  const settledWrite = write.then(() => 'fulfilled', () => 'rejected');
  await hold.entered.promise;
  const before = handle.statements.length;
  let settled = false;
  const append = outbox.append(draft('intent')).finally(() => { settled = true; });
  const settledAppend = append.then(() => 'fulfilled', () => 'rejected');
  try {
    // All operations in this real-SQLite fixture yield through promises only.
    // One event-loop turn drains ready work until the explicit write gate.
    await setImmediate();
    const during = { settled, statements: handle.statements.slice(before) };
    hold.release.resolve();
    const outcomes = await Promise.all([settledWrite, settledAppend]);
    const saved = await replica.load('account');
    expect({ during, outcomes, saved: { cursor: saved?.cursor, rows: saved?.entities.tasks },
      keys: (await outbox.load()).map(entry => entry.idempotencyKey) }).toEqual({
      during: { settled: false, statements: [] }, outcomes: ['fulfilled', 'fulfilled'],
      saved: { cursor: { epoch: 'e', revision: 8 }, rows: { a: { id: 'a', name: 'Changed' }, c: { id: 'c', name: 'New' } } }, keys: ['intent'],
    });
  } finally { hold.release.resolve(); await Promise.all([settledWrite, settledAppend]); }
});

it('separate outbox instances sharing a handle reserve distinct committed identities', async () => {
  const handle = new Handle(), first = new ExpoSQLiteOutboxStore(handle), second = new ExpoSQLiteOutboxStore(handle);
  await first.load(); await second.load();
  const hold = handle.holdNextWrite('_gonvex_outbox');
  const a = first.append(draft('first')), settledA = a.then(() => 'fulfilled', () => 'rejected');
  await hold.entered.promise;
  const b = second.append(draft('second')), settledB = b.then(() => 'fulfilled', () => 'rejected');
  try {
    await setImmediate(); hold.release.resolve();
    expect(await Promise.all([settledA, settledB])).toEqual(['fulfilled', 'fulfilled']);
    const entries = await first.load();
    expect(entries.map(entry => [entry.id, entry.idempotencyKey])).toEqual([[1, 'first'], [2, 'second']]);
  } finally { hold.release.resolve(); await Promise.all([settledA, settledB]); }
});

it('a second replica cannot read or publish another operation\'s uncommitted rows', async () => {
  const handle = new Handle(), writer = new ExpoSQLiteLocalReplicaStorage(handle), reader = new ExpoSQLiteLocalReplicaStorage(handle);
  await writer.replaceSnapshot(base, 'account'); await reader.load('account');
  const hold = handle.holdNextWrite('_gonvex_replica_entities');
  const writing = writer.applyTransaction(change, empty, 'account');
  await hold.entered.promise;
  let published = false;
  const reading = reader.load('account').then(value => { published = true; return value; });
  try {
    await setImmediate(); const publishedBeforeCommit = published; hold.release.resolve();
    await writing; const saved = await reading;
    expect({ publishedBeforeCommit, cursor: saved?.cursor, name: saved?.entities.tasks.a.name }).toEqual({
      publishedBeforeCommit: false, cursor: { epoch: 'e', revision: 8 }, name: 'Changed',
    });
  } finally { hold.release.resolve(); await Promise.allSettled([writing, reading]); }
});

it('independent database handles make progress while another handle is held', async () => {
  const first = new Handle(), other = new Handle();
  const replica = new ExpoSQLiteLocalReplicaStorage(first), outbox = new ExpoSQLiteOutboxStore(other);
  await replica.replaceSnapshot(base, 'account'); await outbox.load();
  const hold = first.holdNextWrite('_gonvex_replica_entities');
  const writing = replica.applyTransaction(change, empty, 'account');
  await hold.entered.promise;
  try { expect((await outbox.append(draft('independent'))).idempotencyKey).toBe('independent'); }
  finally { hold.release.resolve(); await writing; }
});

it('rollback finishes before the next admission and rejection does not poison later work', async () => {
  const handle = new Handle(), replica = new ExpoSQLiteLocalReplicaStorage(handle), outbox = new ExpoSQLiteOutboxStore(handle);
  await replica.replaceSnapshot(base, 'account'); await outbox.load();
  const hold = handle.holdNextWrite('_gonvex_replica_entities', true);
  const write = replica.applyTransaction(change, empty, 'account');
  const failure = write.catch(error => String(error));
  await hold.entered.promise;
  const append = outbox.append(draft('after-rollback'));
  const settledAppend = append.then(() => 'fulfilled', () => 'rejected');
  try {
    hold.release.resolve();
    expect(await failure).toContain('injected failure');
    expect(await settledAppend).toBe('fulfilled');
    const rolledBack = await replica.load('account');
    expect(rolledBack?.cursor).toEqual(base.cursor);
    expect(rolledBack?.entities.tasks).toEqual(base.entities.tasks);
    await replica.applyTransaction(change, empty, 'account');
    expect((await replica.load('account'))?.cursor).toEqual(change.cursor);
    expect((await outbox.load()).map(entry => entry.idempotencyKey)).toEqual(['after-rollback']);
  } finally { hold.release.resolve(); await Promise.allSettled([write, append]); }
});

it('fresh replica and outbox instances initialize one shared handle without overlapping migrations', async () => {
  const handle = new Handle();
  const first = new ExpoSQLiteLocalReplicaStorage(handle), second = new ExpoSQLiteLocalReplicaStorage(handle);
  const a = new ExpoSQLiteOutboxStore(handle), b = new ExpoSQLiteOutboxStore(handle);
  const results = await Promise.allSettled([first.load('account'), second.load('account'), a.load(), b.load()]);
  expect(results.map(result => result.status)).toEqual(['fulfilled', 'fulfilled', 'fulfilled', 'fulfilled']);
  await first.replaceSnapshot(base, 'account');
  await Promise.all([a.append(draft('a')), b.append(draft('b'))]);
  expect((await b.load()).map(entry => entry.id)).toEqual([1, 2]);
  expect((await second.load('account'))?.cursor).toEqual(base.cursor);
});
