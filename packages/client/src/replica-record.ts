import type {ReplicaCursor} from '@gonvex/protocol';
import type {ReplicaRow} from './local-replica.js';

/** Per-field authority is required because collections can project different
 * columns at different revisions, including when two tabs resume together. */
export type ReplicaRecordVersion = {
  epoch:string;
  fields:Record<string,number>;
  /** Last valid deletion also fences projections from an earlier row lifetime. */
  deleted?:number;
};
export type VersionedReplicaRecord = {
  row:ReplicaRow | null;
  authority:ReplicaRecordVersion;
};

export function mergeReplicaRecord(
  previous:VersionedReplicaRecord | undefined,
  incoming:ReplicaRow | null,
  cursor:ReplicaCursor,
): VersionedReplicaRecord {
  const prior = previous?.authority.epoch === cursor.epoch ? previous : undefined;
  const priorAuthority = prior?.authority;
  if (incoming === null) {
    const newest = Math.max(priorAuthority?.deleted ?? -1, ...Object.values(priorAuthority?.fields ?? {}), -1);
    if (cursor.revision < newest || (prior?.row === null && priorAuthority?.deleted === cursor.revision)) return prior!;
    return {row:null,authority:{epoch:cursor.epoch,fields:{},deleted:cursor.revision}};
  }
  // A delayed snapshot must never resurrect a row deleted at a newer revision.
  if (priorAuthority?.deleted !== undefined && cursor.revision <= priorAuthority.deleted) return prior!;
  let authority = priorAuthority ?? {epoch:cursor.epoch,fields:{}};
  let row: ReplicaRow = prior?.row ?? {};
  let copiedRow = !prior?.row;
  let copiedAuthority = !priorAuthority;
  const mutableAuthority = () => {
    if (!copiedAuthority) {
      authority = {...authority,fields:{...authority.fields}};
      copiedAuthority = true;
    }
    return authority;
  };
  // Keep a valid deletion fence even after a narrow projection recreates the
  // row. Otherwise a delayed old projection can fill its missing old fields.
  // Legacy invalid authority must retain the previous recovery behavior.
  if (authority.deleted !== undefined && (!Number.isSafeInteger(authority.deleted) || authority.deleted < 0)) delete mutableAuthority().deleted;
  for (const column of Object.keys(incoming)) {
    if (cursor.revision < (authority.fields[column] ?? -1)) continue;
    const value = incoming[column];
    const before = row[column];
    const equal = Object.is(before,value) || (before !== null && value !== null && typeof before === 'object' && typeof value === 'object' && JSON.stringify(before) === JSON.stringify(value));
    if (!equal || !Object.prototype.hasOwnProperty.call(row,column)) {
      if (!copiedRow) { row = {...row}; copiedRow = true; }
      setOwnField(row, column, value);
    }
    if (authority.fields[column] !== cursor.revision) setOwnField(mutableAuthority().fields, column, cursor.revision);
  }
  if (prior && !copiedRow && !copiedAuthority) return prior;
  return {row,authority};
}

/** Assignment to __proto__ on a plain object invokes its inherited setter.
 * Rows and authority dictionaries must preserve that JSON key as ordinary data. */
function setOwnField<T>(target: Record<string, T>, key: string, value: T): void {
  if (key === '__proto__') Object.defineProperty(target, key, {value, enumerable: true, writable: true, configurable: true});
  else target[key] = value;
}
