/** Preserve synchronous execution for memory/OPFS sources. Only actual disk
 * waits introduce promises; short-circuiting and iterator cleanup stay exact. */
export type MaybePromise<T> = T | PromiseLike<T>;
export type RowStream<T> = Iterable<T> | AsyncIterable<T>;
export const isPending = <T>(value: MaybePromise<T>): value is PromiseLike<T> =>
  value !== null && (typeof value === 'object' || typeof value === 'function') && typeof (value as PromiseLike<T>).then === 'function';
export function mapResult<T, U>(value: MaybePromise<T>, read: (value: T) => MaybePromise<U>): MaybePromise<U> {
  return isPending(value) ? Promise.resolve(value).then(read) : read(value);
}
export function readValues<T, U>(values: readonly T[], read: (value: T) => MaybePromise<U>): MaybePromise<U[]> {
  const results: U[] = []; let index = 0;
  const next = (): MaybePromise<U[]> => {
    while (index < values.length) {
      const value = read(values[index++]!);
      if (isPending(value)) return Promise.resolve(value).then(value => { results.push(value); return next(); });
      results.push(value);
    }
    return results;
  };
  return next();
}
export function firstResult<T, U>(values: readonly T[], read: (value: T) => MaybePromise<U>,
  accepts: (value: U) => boolean, fallback: U): MaybePromise<U> {
  let index = 0;
  const next = (): MaybePromise<U> => {
    while (index < values.length) {
      const value = read(values[index++]!);
      if (isPending(value)) return Promise.resolve(value).then(value => accepts(value) ? value : next());
      if (accepts(value)) return value;
    }
    return fallback;
  };
  return next();
}
export function visitRows<T>(rows: RowStream<T>, visit: (row: T) => MaybePromise<void>): MaybePromise<void> {
  if (Symbol.asyncIterator in rows) return (async () => { for await (const row of rows) await visit(row); })();
  const iterator = rows[Symbol.iterator]();
  const fail = (error: unknown): MaybePromise<never> => {
    let closed: IteratorResult<T> | undefined;
    try { closed = iterator.return?.(); } catch { throw error; }
    if (closed && isPending(closed)) return Promise.resolve(closed).then(() => { throw error; }, () => { throw error; });
    throw error;
  };
  const next = (): MaybePromise<void> => {
    try {
      for (;;) {
        const entry = iterator.next(); if (entry.done) return;
        const result = visit(entry.value);
        if (isPending(result)) return Promise.resolve(result).then(next, fail);
      }
    } catch (error) { return fail(error); }
  };
  return next();
}
/** Visit until the callback returns false, closing its iterator exactly once.
 * Sync sources stay synchronous; async cleanup completes before returning. */
export function visitRowsWhile<T>(rows: RowStream<T>, visit: (row: T) => MaybePromise<boolean>): MaybePromise<void> {
  if (Symbol.asyncIterator in rows) return (async () => {
    const iterator = rows[Symbol.asyncIterator](); let exhausted = false, failed = false;
    try {
      for (;;) { const entry = await iterator.next(); if (entry.done) { exhausted = true; return; } if (!await visit(entry.value)) return; }
    } catch (error) { failed = true; throw error; }
    finally {
      if (!exhausted) try { await iterator.return?.(); } catch (error) { if (!failed) throw error; }
    }
  })();
  const iterator = rows[Symbol.iterator](); let closed = false;
  const close = (): MaybePromise<void> => {
    if (closed) return; closed = true;
    const result = iterator.return?.();
    return isPending(result) ? Promise.resolve(result).then(() => undefined) : undefined;
  };
  const fail = (error: unknown): MaybePromise<never> => {
    try { const result = close(); if (isPending(result)) return Promise.resolve(result).then(() => { throw error; }, () => { throw error; }); }
    catch { throw error; } throw error;
  };
  const next = (): MaybePromise<void> => {
    try {
      for (;;) {
        const entry = iterator.next(); if (entry.done) return;
        const result = visit(entry.value);
        if (isPending(result)) return Promise.resolve(result).then(keepGoing => keepGoing ? next() : close(), fail);
        if (!result) return close();
      }
    } catch (error) { return fail(error); }
  };
  return next();
}
export function immediate<T>(value: MaybePromise<T>): T {
  if (isPending(value)) throw Error('Asynchronous local-view source requires readLocalViewAsync');
  return value;
}
