// Task-update expectations for run.ts: which edits a user must eventually see
// in its tasks.recentWorkspaceTasks replica before teardown.
//
// Rules (docs/performance/delivery-loss.md):
// - Match on the value the save wrote, never on a server timestamp compared
//   with the generator's clock: the generator (MacBook) and the runtime
//   (arch3) disagree by tens of milliseconds and the sign flips between runs.
// - A save that failed (e.g. budget_exceeded before the Reducer ran) changed
//   nothing, so it expects nothing.
// - A save still pending at teardown is not a delivery loss (counted apart).
// - A later edit of the same row excuses an earlier expectation; the row's
//   updatedAt is server time, so allow clockToleranceMs of skew.

/** recordTaskViewByTaskId sets firstViewedAt once, to the server's clock. */
export const firstViewed = (row) => Number(row?.firstViewedAt) > 0;

/** Whether a first view by `member` changes the row at all (the app skips the creator). */
export const viewSetsFirstViewedAt = (row, member) => !firstViewed(row) && row?.createdBy !== member;

/**
 * True when `op` still expects a change this row does not show.
 * op: {at, state: 'pending'|'ok'|'failed', matches(row, at)}
 */
export function unmetTaskUpdate(op, row, { subscribedAt = 0, clockToleranceMs = 250 } = {}) {
  if (!row || op.at < subscribedAt || op.state === 'failed') return false;
  if (op.matches(row, op.at)) return false;
  return !(Number(row.updatedAt) >= op.at - clockToleranceMs);
}
