import type { IndexableType } from 'dexie';

type SequenceTable = {
  where(index: string): {
    between(lower: IndexableType, upper: IndexableType, includeLower?: boolean, includeUpper?: boolean): {
      reverse(): { limit(count: number): { keys(): PromiseLike<IndexableType[]> } };
    };
  };
};
type SequenceDatabase = {
  meta: { get(key: [string, string]): PromiseLike<{ value: unknown } | undefined> };
  entities: SequenceTable;
  windows: SequenceTable;
};

export function replicaSequenceEncoding(record: { value: unknown } | undefined, sequence: number): boolean {
  return record === undefined ? sequence === 0
    : typeof record.value === 'string' && record.value === String(sequence);
}

export function replicaSequenceCovers(sequence: number, entity: number, window: number, reset: number): boolean {
  return Number.isSafeInteger(sequence) && sequence >= 0
    && Number.isSafeInteger(entity) && entity >= 0 && entity <= sequence
    && Number.isSafeInteger(window) && window >= 0 && window <= sequence
    && Number.isSafeInteger(reset) && reset >= 0 && reset <= sequence;
}

/** Call within the same transaction that reads or publishes replica state. */
export async function assertReplicaSequence(
  database: SequenceDatabase, scope: string, prior: { value: unknown } | undefined, sequence: number,
): Promise<void> {
  if (!replicaSequenceEncoding(prior, sequence)) throw new Error('Invalid replica storage sequence.');
  const [entity, window, reset] = await Promise.all([
    database.entities.where('[scope+sequence]').between([scope, -Infinity], [scope, []], true, false).reverse().limit(1).keys(),
    database.windows.where('[scope+sequence]').between([scope, -Infinity], [scope, []], true, false).reverse().limit(1).keys(),
    database.meta.get([scope, 'resetSequence']),
  ]);
  const resetSequence = Number(reset?.value ?? 0);
  // Read compound index keys only, including tombstones. Integrity checks must
  // not deserialize an arbitrarily large entity or membership payload.
  const entitySequence = (entity[0] as [string, number] | undefined)?.[1] ?? 0;
  const windowSequence = (window[0] as [string, number] | undefined)?.[1] ?? 0;
  if (!replicaSequenceEncoding(reset, resetSequence)
    || !replicaSequenceCovers(sequence, entitySequence, windowSequence, resetSequence)) {
    throw new Error('Invalid replica storage sequence.');
  }
}

export async function readReplicaSequence(database: SequenceDatabase, scope: string): Promise<number> {
  const prior = await database.meta.get([scope, 'sequence']);
  const sequence = Number(prior?.value ?? 0);
  await assertReplicaSequence(database, scope, prior, sequence);
  return sequence;
}
