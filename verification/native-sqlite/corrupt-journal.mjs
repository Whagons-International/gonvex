// Shared by actual Expo/Hermes execution and the Node SQLite process runner.
export async function corruptJournalPhase(config, emit, { SQLite, Outbox, Queue }) {
  const name = `gonvex-historical-${config.runId}-corrupt.db`;
  if (config.phase === 'journal-corrupt-seed') await SQLite.deleteDatabaseAsync(name).catch(() => {});
  const database = await SQLite.openDatabaseAsync(name);
  await database.execAsync('PRAGMA synchronous=FULL; PRAGMA journal_mode=DELETE;');
  const store = new Outbox(database);
  const draft = key => ({ scope: 'account', path: 'tasks.write', args: { key }, idempotencyKey: key,
    entityKeys: [], createdAt: 1, attempts: 0, nextAttemptAt: 0, state: 'committed' });
  if (config.phase === 'journal-corrupt-seed') {
    await store.append(draft('native-corrupt'));
    await store.append(draft('native-good'));
    await database.runAsync('UPDATE _gonvex_outbox SET value=? WHERE id=1', '{invalid accepted bytes');
    await emit({ type: 'corrupt-journal-seeded',
      rows: await database.getAllAsync('SELECT id, scope, idempotency_key, state, value FROM _gonvex_outbox ORDER BY id'),
      sync: await database.getFirstAsync('PRAGMA synchronous'), journal: await database.getFirstAsync('PRAGMA journal_mode') });
  } else if (config.phase === 'journal-corrupt-recover') {
    const errors = {};
    for (const [label, run] of [['load', () => store.load()], ['update', () => store.update(1, entry => ({ ...entry, state: 'pending' }))]]) {
      try { await run(); errors[label] = null; } catch (error) { errors[label] = { name: error.name, message: String(error) }; }
    }
    const outbox = new Queue(store);
    try {
      for (const [label, run] of [['restore', () => outbox.loadAll('account')], ['enqueue', () => outbox.enqueue({ scope: 'account', path: 'tasks.write', args: {} })]]) {
        try { await run(); errors[label] = null; } catch (error) { errors[label] = { name: error.name, message: String(error) }; }
      }
      await emit({ type: 'corrupt-journal-refused', errors,
        rows: await database.getAllAsync('SELECT id, scope, idempotency_key, state, value FROM _gonvex_outbox ORDER BY id'),
        sync: await database.getFirstAsync('PRAGMA synchronous'), journal: await database.getFirstAsync('PRAGMA journal_mode') });
    } finally { outbox.dispose?.(); }
  } else throw Error('unknown corrupt-journal phase');
  await database.closeAsync();
  if (config.phase === 'journal-corrupt-recover') await SQLite.deleteDatabaseAsync(name);
}
