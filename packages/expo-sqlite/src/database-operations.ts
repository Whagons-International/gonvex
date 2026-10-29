// Expo's transaction callback does not own its connection exclusively. Every
// SDK operation on the same handle must finish, including rollback, before
// another operation can read or write that connection.
const tails = new WeakMap<object, Promise<unknown>>();

export function withSQLiteOperation<T>(database: object, run: () => Promise<T>): Promise<T> {
  const prior = tails.get(database) ?? Promise.resolve();
  const operation = prior.then(run);
  // Rejection belongs to this caller; it must not poison later admissions.
  tails.set(database, operation.then(() => undefined, () => undefined));
  return operation;
}
