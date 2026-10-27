/** Pure revision decisions shared by sync and storage callers.
 * Keep I/O and object mutation outside this module. See
 * docs/verification/sdk-revisions.md for the source-linked proof boundary. */
export function isReplicaRevision(revision: number): boolean {
  return Number.isSafeInteger(revision) && revision >= 0;
}

export function isTransactionRevision(revision: number): boolean {
  return Number.isSafeInteger(revision) && revision > 0;
}

/** Callers validate incoming revisions first. Preserve the original rejection
 * comparisons so a valid update can repair legacy NaN authority. Equal-revision
 * projections may fill missing fields. */
export function canReplaceReplicaField(incoming: number, current: number): boolean {
  return !(incoming < current);
}

/** A deletion wins a tie against a delayed row projection. */
export function canResurrectReplicaRow(incoming: number, deleted: number): boolean {
  return !(incoming <= deleted);
}

export function canDeleteReplicaRow(incoming: number, newest: number): boolean {
  return !(incoming < newest);
}
