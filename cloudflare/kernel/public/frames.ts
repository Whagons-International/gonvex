/**
 * Frame rules of the public transport: what a frame costs against the call
 * budgets, which frames run tenant code (a fresh admission check), and the
 * correlated errors for a refused frame. Rust: lib.rs:2661-2690 (cost),
 * 3906-3934 (`public_frame_runs_tenant_code`, `public_admission_errors`),
 * 4360-4430 (`public_limit_errors`).
 */
import type { ClientFrame } from "../session/decode";
import { frames } from "../session/protocol";

export const RATE_LIMIT_ERROR = "public call rate limit exceeded";
export const ADMISSION_ERROR = "public tenant session is unavailable";
/** The unsolicited `auth.error` id for a refused frame that has no id of its own to answer. */
export const RATE_LIMIT_ID = "public-rate-limit";

/** A batch costs one per item (at least one); a snapshot ack costs nothing once native rows were negotiated. */
export function frameCost(frame: ClientFrame, nativeRows: boolean): number {
  switch (frame.type) {
    case "reducer.callMany": return Math.max(1, frame.calls.length);
    case "query.subscribeMany": return Math.max(1, frame.subscribes.length);
    case "replica.openMany": return Math.max(1, frame.opens.length);
    case "replica.snapshotAck": return nativeRows ? 0 : 1;
    default: return 1;
  }
}

/** Frames whose handling runs tenant functions or reads tenant data: admission is re-read before them. `reducer.callMany` is checked per call. */
export function runsTenantCode(frame: ClientFrame): boolean {
  switch (frame.type) {
    case "query.call":
    case "query.subscribe":
    case "query.subscribeMany":
    case "replica.open":
    case "replica.openMany":
    case "reducer.call":
    case "action.call":
      return true;
    default:
      return false;
  }
}

/** `public_limit_errors` with `error` as the text: one correlated error per refused call. */
export function refusedFrameErrors(frame: ClientFrame, error: string): string[] {
  switch (frame.type) {
    case "auth": return [frames.authError(frame.id, error, "transient")];
    case "query.call":
    case "query.subscribe": return [frames.queryError(frame.id, frame.path, error)];
    case "reducer.call": return [frames.reducerError(frame.id, frame.path, error, "rejected")];
    case "reducer.callMany": return frame.calls.map(c => frames.reducerError(c.id, c.path, error, "rejected"));
    case "query.subscribeMany": return frame.subscribes.map(s => frames.queryError(s.id, s.path, error));
    case "replica.open": return [frames.replicaError(frame.id, frame.path, error)];
    case "replica.openMany": return frame.opens.map(o => frames.replicaError(o.id, o.path, error));
    case "action.call": return [frames.actionError(frame.id, frame.path, error)];
    // The catch-all keeps the id `public-rate-limit` in `public_admission_errors` too: that rewrite
    // replaces only the text of every `AuthError` (lib.rs:3919-3934).
    default: return [frames.authError(RATE_LIMIT_ID, error, "transient")];
  }
}

export const publicLimitErrors = (frame: ClientFrame) => refusedFrameErrors(frame, RATE_LIMIT_ERROR);
export const publicAdmissionErrors = (frame: ClientFrame) => refusedFrameErrors(frame, ADMISSION_ERROR);
