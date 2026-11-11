// Server frames of the Reducer path, in serde's key order
// (rust/crates/protocol/src/lib.rs ServerMessage), and the watermark rule
// that completes a durable call (rust/crates/runtime/src/lib.rs:4203-4265).

import type { MessageTrace } from "../session/decode";
import { frames as sessionFrames } from "../session/protocol";
import { retryable, type ReducerFailure } from "./errors.ts";

const TRACE_ORDER = [
  "clientSentAtMs", "serverReceivedAtMs", "serverReducerStartedAtMs", "serverReducerCommittedAtMs",
  "serverCompletedAtMs", "serverBroadcastScheduledAtMs", "serverChangeCommittedAtMs",
  "serverSubscriptionStartedAtMs", "serverSubscriptionSentAtMs", "serverDurationMs", "queryPerf",
] as const;

/** `trace` is echoed verbatim, re-serialized in struct order (the server fills no timing field). */
function traceText(trace: MessageTrace): string {
  const out: Record<string, unknown> = {};
  for (const key of TRACE_ORDER) if (trace[key] !== undefined) out[key] = trace[key];
  return JSON.stringify(out);
}

/**
 * `reducer.result`: `id`, `path`, `result`, `originCommandId` (= `id`),
 * `committedRevision?`, `trace?`. `resultJson` is spliced as is: it is the
 * module's JSON text, or a stored receipt's, never re-parsed.
 */
export function reducerResult(id: string, path: string, resultJson: string, committedRevision: number | undefined, trace?: MessageTrace): string {
  return `{"type":"reducer.result","id":${JSON.stringify(id)},"path":${JSON.stringify(path)},"result":${resultJson},"originCommandId":${JSON.stringify(id)}`
    + (committedRevision === undefined ? "" : `,"committedRevision":${committedRevision}`)
    + (trace ? `,"trace":${traceText(trace)}` : "")
    + "}";
}

/** `reducer.error` with the class and `retryable` of `failure`. */
export function reducerError(id: string, path: string | undefined, failure: ReducerFailure, trace?: MessageTrace): string {
  return sessionFrames.reducerError(id, path, failure.message, failure.class, trace);
}

/** `query_result(id, path, result)`: fixed fields, then the payload map in key order (`path`, `reason`, `result`). */
export function queryResult(id: string, path: string, resultJson: string): string {
  return `{"type":"query.result","id":${JSON.stringify(id)},"path":${JSON.stringify(path)},"reason":"initial","result":${resultJson}}`;
}

export function queryError(id: string, path: string, failure: ReducerFailure): string {
  return sessionFrames.queryError(id, path, failure.message);
}

export const replicaWatermark = (revision: number): string => sessionFrames.replicaWatermark(revision);

/**
 * `call_watermark_after_sent_replica_work`: an authenticated tenant call
 * that committed through `committed` completes at once with a watermark when
 * every subscription of the socket was already sent through it, or when the
 * socket has none (`floor` undefined). Otherwise the feed delivers the
 * transaction and its watermark (Rust makes the batch urgent).
 *
 * `floor` is `min(subscription cursors).max(sent revision)` of the socket,
 * which only the delivery owner (K10) knows.
 */
export function completesNow(committed: number | undefined, floor: number | undefined): boolean {
  if (committed === undefined) return false;
  return floor === undefined || floor >= committed;
}

export { retryable };
