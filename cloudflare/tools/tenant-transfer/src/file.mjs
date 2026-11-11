// node:sqlite adapter with the Durable Object SqlStorage shape, so the
// catalog readers in sql.mjs and the codec run unchanged against a file.

import { DatabaseSync } from 'node:sqlite';

export function openFile(path, { readOnly = false } = {}) {
  const db = new DatabaseSync(path, { readOnly });
  const cache = new Map();
  const prepare = (sql) => {
    let statement = cache.get(sql);
    if (!statement) {
      statement = db.prepare(sql);
      cache.set(sql, statement);
      if (cache.size > 256) cache.delete(cache.keys().next().value);
    }
    return statement;
  };
  const sql = {
    exec(text, ...params) {
      return {
        toArray: () => {
          const s = prepare(text);
          s.setReturnArrays(false);
          return s.all(...params);
        },
        one: () => {
          const s = prepare(text);
          s.setReturnArrays(false);
          return s.get(...params);
        },
        raw: () => {
          const s = prepare(text);
          s.setReturnArrays(true);
          return s.iterate(...params);
        },
      };
    },
    run(text, ...params) {
      return prepare(text).run(...params);
    },
  };
  return { db, sql, close: () => db.close() };
}
