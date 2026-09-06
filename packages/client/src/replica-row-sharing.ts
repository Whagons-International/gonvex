import type { ReplicaRow } from "./local-replica.js";

function equalValue(left: unknown, right: unknown): boolean {
  if (left === right) return true;
  if (!left || !right || typeof left !== "object" || typeof right !== "object") return false;
  if (Array.isArray(left)) return Array.isArray(right) && left.length === right.length && left.every((value, index) => equalValue(value, right[index]));
  if (Array.isArray(right)) return false;
  const a = left as Record<string, unknown>, b = right as Record<string, unknown>;
  const keys = Object.keys(a);
  return keys.length === Object.keys(b).length && keys.every(key => Object.prototype.hasOwnProperty.call(b, key) && equalValue(a[key], b[key]));
}

/** Preserve immutable watch snapshots for unchanged normalized rows. */
export function shareReplicaRows<T extends ReplicaRow>(previous: T[] | undefined, next: T[], key: string): T[] {
  if (!previous) return next;
  const byId = new Map(previous.map(row => [row[key], row]));
  let same = previous.length === next.length;
  const shared = next.map((row, index) => {
    const old = byId.get(row[key]);
    const value = old && equalValue(old, row) ? old : row;
    if (value !== previous[index]) same = false;
    return value;
  });
  return same ? previous : shared;
}
