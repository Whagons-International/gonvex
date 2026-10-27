/** One comparator for resident and disk-backed Replica Collection ordering. */
export function replicaMembershipOrderValue(value: unknown): string | number | null {
  return typeof value === 'string' || typeof value === 'number' ? value : null;
}
export function compareReplicaMembershipValues(left: unknown, right: unknown, leftID: string, rightID: string, direction: 'asc' | 'desc' | undefined): number {
  const a = replicaMembershipOrderValue(left), b = replicaMembershipOrderValue(right);
  if (a === null && b !== null) return 1;
  if (a !== null && b === null) return -1;
  if (a !== null && b !== null && a !== b) {
    const sign = direction === 'asc' ? 1 : -1;
    return a < b ? -sign : sign;
  }
  return leftID < rightID ? -1 : leftID > rightID ? 1 : 0;
}
