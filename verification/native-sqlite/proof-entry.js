import { StoreReducerOutbox } from '@proof/queue';
import * as SQLite from 'expo-sqlite';
import { Platform } from 'react-native';
import { ExpoSQLiteLocalReplicaStorage as BaselineReplica, ExpoSQLiteOutboxStore as BaselineOutbox } from './.work/sdk/baseline/index.ts';
import { ExpoSQLiteLocalReplicaStorage as FixedReplica, ExpoSQLiteOutboxStore as FixedOutbox } from './.work/sdk/fixed/index.ts';
const base = {
  cursor: {
    epoch: 'e',
    revision: 7
  },
  entities: {
    tasks: {
      a: {
        id: 'a',
        name: 'Original'
      }
    }
  },
  liveQueries: {}
};
const empty = {
  entities: {},
  liveQueries: {}
};
const change = {
  cursor: {
    epoch: 'e',
    revision: 8
  },
  changes: [{
    entity: 'tasks',
    id: 'a',
    operation: 'update',
    newValue: {
      id: 'a',
      name: 'Changed'
    }
  }, {
    entity: 'tasks',
    id: 'c',
    operation: 'insert',
    newValue: {
      id: 'c',
      name: 'New'
    }
  }]
};
const draft = key => ({
  scope: 'account',
  path: 'tasks.write',
  args: {
    key
  },
  idempotencyKey: key,
  entityKeys: [],
  createdAt: 1,
  attempts: 0,
  nextAttemptAt: 0,
  state: 'pending'
});
const deferred = () => {
  let resolve;
  const promise = new Promise(done => {
    resolve = done;
  });
  return {
    promise,
    resolve
  };
};
const turn = () => new Promise(resolve => setTimeout(resolve, 0));
const outcome = promise => promise.then(value => ({
  status: 'fulfilled',
  value
}), error => ({
  status: 'rejected',
  error: String(error)
}));
const check = (condition, message, data) => {
  if (!condition) throw Error(message + ': ' + JSON.stringify(data));
};
function handle(database) {
  const state = {
    statements: [],
    reads: 0,
    pause: null
  };
  const nativeExec = database.execAsync.bind(database);
  database.execAsync = async sql => {
    state.statements.push(sql);
    return nativeExec(sql);
  };
  const proxy = {
    async execAsync(sql) {
      return database.execAsync(sql);
    },
    async runAsync(sql, ...params) {
      const result = await database.runAsync(sql, ...params);
      const p = state.pause;
      if (p && sql.includes('INTO ' + p.table + ' ')) {
        state.pause = null;
        p.entered.resolve();
        await p.release.promise;
        if (p.fail) throw Error('injected successful-write failure');
      }
      return result;
    },
    async getAllAsync(sql, ...params) {
      state.reads++;
      return database.getAllAsync(sql, ...params);
    },
    async getFirstAsync(sql, ...params) {
      state.reads++;
      return database.getFirstAsync(sql, ...params);
    },
    // This is the actual installed Expo SQLite transaction method.
    withTransactionAsync(task) {
      return database.withTransactionAsync(task);
    }
  };
  return {
    proxy,
    state,
    hold(table, fail = false) {
      return state.pause = {
        table,
        fail,
        entered: deferred(),
        release: deferred()
      };
    }
  };
}
async function fixture(config, variant, id, run) {
  const name = `gonvex-proof-${config.runId}-${variant}-${id}.db`;
  await SQLite.deleteDatabaseAsync(name).catch(() => {});
  const database = await SQLite.openDatabaseAsync(name);
  const h = handle(database),
    Replica = variant === 'baseline' ? BaselineReplica : FixedReplica,
    Outbox = variant === 'baseline' ? BaselineOutbox : FixedOutbox;
  try {
    return await run(h, Replica, Outbox, database);
  } finally {
    await database.closeAsync();
    await SQLite.deleteDatabaseAsync(name);
  }
}
async function replicaOutbox(config, variant) {
  return fixture(config, variant, 'replica-outbox', async (h, Replica, Outbox) => {
    const replica = new Replica(h.proxy),
      outbox = new Outbox(h.proxy);
    await replica.replaceSnapshot(base, 'account');
    await outbox.load();
    const hold = h.hold('_gonvex_replica_entities'),
      writing = outcome(replica.applyTransaction(change, empty, 'account'));
    await hold.entered.promise;
    const before = h.state.statements.length;
    let settled = false;
    const appending = outcome(outbox.append(draft('intent'))).then(v => {
      settled = true;
      return v;
    });
    try {
      await turn();
      if (variant === 'baseline') await appending;
      const during = {
        settled,
        statements: h.state.statements.slice(before)
      };
      hold.release.resolve();
      const results = await Promise.all([writing, appending]);
      const saved = await replica.load('account');
      const value = {
        during,
        outcomes: results.map(v => v.status),
        saved,
        entries: await outbox.load()
      };
      if (variant === 'baseline') check(value.during.statements.includes('ROLLBACK') && value.outcomes.every(v => v === 'rejected') && saved.cursor.revision === 8 && saved.entities.tasks.a.name === 'Original' && !saved.entities.tasks.c, 'baseline did not reproduce the partial-data certificate', value);else check(!during.settled && during.statements.length === 0 && value.outcomes.every(v => v === 'fulfilled') && saved.cursor.revision === 8 && saved.entities.tasks.a.name === 'Changed' && saved.entities.tasks.c.name === 'New' && value.entries[0].idempotencyKey === 'intent', 'fixed shared-handle guarantee failed', value);
      return value;
    } finally {
      hold.release.resolve();
      await Promise.all([writing, appending]);
    }
  });
}
async function twoOutboxes(config, variant) {
  return fixture(config, variant, 'two-outboxes', async (h, Replica, Outbox) => {
    const first = new Outbox(h.proxy),
      second = new Outbox(h.proxy);
    await first.load();
    await second.load();
    const hold = h.hold('_gonvex_outbox'),
      a = outcome(first.append(draft('first')));
    await hold.entered.promise;
    const b = outcome(second.append(draft('second')));
    try {
      await turn();
      if (variant === 'baseline') await b;
      hold.release.resolve();
      const results = await Promise.all([a, b]);
      const rows = await first.load();
      const value = {
        outcomes: results.map(v => v.status),
        identities: rows.map(v => [v.id, v.idempotencyKey])
      };
      check(variant === 'baseline' ? value.outcomes.every(v => v === 'rejected') && /transaction/i.test(results[1].error) && rows.length === 0 : value.outcomes.every(v => v === 'fulfilled') && JSON.stringify(value.identities) === JSON.stringify([[1, 'first'], [2, 'second']]), 'outbox ownership observation mismatch', value);
      return value;
    } finally {
      hold.release.resolve();
      await Promise.all([a, b]);
    }
  });
}
async function reader(config, variant) {
  return fixture(config, variant, 'reader', async (h, Replica) => {
    const writer = new Replica(h.proxy),
      reader = new Replica(h.proxy);
    await writer.replaceSnapshot(base, 'account');
    await reader.load('account');
    const hold = h.hold('_gonvex_replica_entities'),
      writing = outcome(writer.applyTransaction(change, empty, 'account'));
    await hold.entered.promise;
    const reads = h.state.reads;
    let published = false;
    const reading = reader.load('account').then(v => {
      published = true;
      return v;
    });
    try {
      await turn();
      if (variant === 'baseline') await reading;
      const before = {
        published,
        reads: h.state.reads - reads
      };
      hold.release.resolve();
      const write = await writing,
        saved = await reading;
      const value = {
        before,
        write: write.status,
        saved
      };
      check(variant === 'baseline' ? before.published && saved.cursor.revision === 7 && saved.entities.tasks.a.name === 'Changed' : !before.published && before.reads === 0 && write.status === 'fulfilled' && saved.cursor.revision === 8 && saved.entities.tasks.a.name === 'Changed', 'read isolation mismatch', value);
      return value;
    } finally {
      hold.release.resolve();
      await Promise.allSettled([writing, reading]);
    }
  });
}
async function rejectedOwner(config) {
  return fixture(config, 'fixed', 'rejection', async (h, Replica, Outbox) => {
    const replica = new Replica(h.proxy),
      outbox = new Outbox(h.proxy);
    await replica.replaceSnapshot(base, 'account');
    await outbox.load();
    const hold = h.hold('_gonvex_replica_entities', true),
      writing = outcome(replica.applyTransaction(change, empty, 'account'));
    await hold.entered.promise;
    const append = outcome(outbox.append(draft('after-rollback')));
    hold.release.resolve();
    const [write, next] = await Promise.all([writing, append]);
    const rolledBack = await replica.load('account');
    check(write.status === 'rejected' && next.status === 'fulfilled' && rolledBack.cursor.revision === 7 && rolledBack.entities.tasks.a.name === 'Original' && !rolledBack.entities.tasks.c, 'rollback ownership mismatch', {
      write,
      next,
      rolledBack
    });
    await replica.applyTransaction(change, empty, 'account');
    return {
      write: write.status,
      next: next.status,
      revision: (await replica.load('account')).cursor.revision
    };
  });
}
async function migrations(config) {
  return fixture(config, 'fixed', 'migrations', async (h, Replica, Outbox) => {
    const a = new Replica(h.proxy),
      b = new Replica(h.proxy),
      c = new Outbox(h.proxy),
      d = new Outbox(h.proxy);
    const values = await Promise.all([a.load('account'), b.load('account'), c.load(), d.load()]);
    await a.replaceSnapshot(base, 'account');
    await Promise.all([c.append(draft('a')), d.append(draft('b'))]);
    const ids = (await c.load()).map(v => v.id);
    check(JSON.stringify(ids) === '[1,2]', 'migration or identity failure', ids);
    return {
      initialized: values.length,
      ids,
      revision: (await b.load('account')).cursor.revision
    };
  });
}
async function independent(config) {
  const held = deferred(),
    release = deferred();
  const writer = fixture(config, 'fixed', 'independent-a', async (h, Replica) => {
    const r = new Replica(h.proxy);
    await r.replaceSnapshot(base, 'account');
    const hold = h.hold('_gonvex_replica_entities');
    const writing = r.applyTransaction(change, empty, 'account');
    await hold.entered.promise;
    held.resolve();
    await release.promise;
    hold.release.resolve();
    await writing;
    return true;
  });
  await held.promise;
  try {
    return await fixture(config, 'fixed', 'independent-b', async (h, Replica, Outbox) => {
      const q = new Outbox(h.proxy);
      const row = await q.append(draft('independent'));
      check(row.id === 1, 'independent handle did not commit', row);
      return {
        independentCommitted: true
      };
    });
  } finally {
    release.resolve();
    await writer;
  }
}
async function crashPhase(config, emit) {
  const name = `gonvex-proof-${config.runId}-process-crash.db`;
  if (config.phase === 'crash-seed') await SQLite.deleteDatabaseAsync(name).catch(() => {});
  const db = await SQLite.openDatabaseAsync(name),
    h = handle(db),
    replica = new FixedReplica(h.proxy),
    outbox = new FixedOutbox(h.proxy);
  const journal = await db.getFirstAsync('PRAGMA journal_mode'),
    sync = await db.getFirstAsync('PRAGMA synchronous');
  if (config.phase === 'crash-seed') {
    await replica.replaceSnapshot(base, 'account');
    const entry = await outbox.append({
      ...draft('native-intent'),
      state: 'inflight',
      sentAt: 1
    });
    await emit({
      type: 'committed-before-crash',
      database: name,
      id: entry.id,
      key: entry.idempotencyKey,
      journal,
      sync
    });
    const hold = h.hold('_gonvex_replica_entities');
    void replica.applyTransaction(change, empty, 'account');
    await hold.entered.promise;
    await emit({
      type: 'write-held-for-process-crash',
      database: name,
      successfulEntityStatement: true,
      id: entry.id,
      key: entry.idempotencyKey
    });
    await new Promise(() => {});
  } else if (config.phase === 'recover') {
    const saved = await replica.load('account'),
      entries = await outbox.load();
    check(saved.cursor.revision === 7 && saved.entities.tasks.a.name === 'Original' && !saved.entities.tasks.c && entries.length === 1 && entries[0].id === 1 && entries[0].idempotencyKey === 'native-intent' && entries[0].state === 'inflight', 'process crash did not preserve atomic rows and intent', {
      saved,
      entries
    });
    const queue = new StoreReducerOutbox(outbox);
    await queue.loadAll('account');
    const recovered = await outbox.load();
    check(recovered[0].state === 'pending' && recovered[0].id === 1 && recovered[0].idempotencyKey === 'native-intent', 'native queue recovery changed identity', recovered);
    const next = await outbox.append(draft('after-crash'));
    check(next.id === 2, 'native sequence reset', next);
    await replica.applyTransaction(change, empty, 'account');
    await emit({
      type: 'recovered-and-recommitted',
      saved,
      recovered,
      nextId: next.id,
      committedRevision: (await replica.load('account')).cursor.revision,
      journal,
      sync
    });
    await db.closeAsync();
  } else if (config.phase === 'reopen-committed') {
    const saved = await replica.load('account'),
      entries = await outbox.load();
    check(saved.cursor.revision === 8 && saved.entities.tasks.a.name === 'Changed' && saved.entities.tasks.c.name === 'New' && entries.length === 2 && entries[0].id === 1 && entries[0].idempotencyKey === 'native-intent' && entries[1].id === 2, 'acknowledged state did not survive app restart', {
      saved,
      entries
    });
    await emit({
      type: 'process-restart-complete',
      passed: true,
      saved,
      entries,
      journal,
      sync
    });
    await db.closeAsync();
    await SQLite.deleteDatabaseAsync(name);
  } else throw Error('unknown crash phase');
}
export async function execute(url, observe) {
  const config = await (await fetch(url + '/config')).json();
  check(/^[a-zA-Z0-9_-]{1,80}$/.test(config.runId), 'invalid fixture namespace', {});
  const emit = async event => {
    const payload = { ...event, runId: config.runId, phase: config.phase, bundleSha256: config.bundleSha256 };
    observe(payload);
    await fetch(url + '/event', {
      method: 'POST',
      headers: {
        'content-type': 'application/json'
      },
      body: JSON.stringify(payload)
    });
  };
  const probe = await SQLite.openDatabaseAsync('gonvex-native-version.db');
  const version = await probe.getFirstAsync('SELECT sqlite_version() AS version');
  await probe.closeAsync();
  await SQLite.deleteDatabaseAsync('gonvex-native-version.db');
  check(Boolean(globalThis.HermesInternal), 'Hermes is required', {});
  await emit({
    type: 'environment',
    platform: Platform.OS,
    android: Platform.Version,
    hermes: true,
    sqlite: version.version,
    phase: config.phase,
    runId: config.runId
  });
  if (config.phase !== 'all') return crashPhase(config, emit);
  const cases = [];
  for (const [variant, functions] of [['baseline', [replicaOutbox, twoOutboxes, reader]], ['fixed', [replicaOutbox, twoOutboxes, reader, rejectedOwner, migrations, independent]]]) for (const run of functions) {
    await emit({
      type: 'running',
      variant,
      case: run.name
    });
    const result = await run(config, variant);
    const event = {
      type: 'case',
      variant,
      case: run.name,
      result
    };
    cases.push(event);
    await emit(event);
  }
  await emit({
    type: 'complete',
    passed: true,
    baselineCounterexamples: 3,
    fixedCases: 6,
    cases
  });
}
