/**
 * Wire constants and server frames of the session layer. Every builder writes
 * keys in the order serde serializes the Rust `ServerMessage` variant
 * (`rust/crates/protocol/src/lib.rs`), so the bytes match, not only the JSON.
 */
import type { Json, MessageTrace } from "./decode";

/** `PROTOCOL_VERSION` (proto:91). */
export const PROTOCOL_VERSION = 2;

export const LIMITS = {
  /** `GONVEX_PUBLIC_MAX_PAYLOAD_BYTES` (public_functions.rs:26), in UTF-8 bytes. */
  publicPayloadBytes: 64 * 1024,
  /** `GONVEX_AUTH_TIMEOUT_SECONDS`: pending deadline and each auth attempt. */
  authTimeoutMs: 10_000,
  /** `auth_revalidation::INTERVAL` (C.3.8 item 10). */
  revalidateMs: 30_000,
  /** 0016 G0-6: a socket with no outbound frame for this long gets a watermark. */
  livenessMs: 20_000,
  /** 0016 G0-7 / 0018: the kernel never sends a frame of this many bytes or more. */
  maxOutboundBytes: 32 * 1024 * 1024 - 1024,
  /** Batch limits (A.1.7). */
  subscribeMany: 256,
  callMany: 256,
  openMany: 256,
} as const;

/**
 * Close codes the session layer sends (A.1.9). `ended` and `failed` stand in
 * for Rust's "no close frame" exits (the task returns and the TCP connection
 * closes): the platform always sends a close frame, so the kernel picks one
 * that carries no protocol meaning, and the SDK reconnects on any close.
 */
export const CLOSE = {
  invalidFrame: { code: 1003, reason: "invalid Gonvex protocol frame" },
  authDeadline: { code: 1008, reason: "authentication deadline exceeded" },
  publicScopeFixed: { code: 1008, reason: "public connection scope is fixed" },
  publicPayloadTooLarge: { code: 1009, reason: "public payload too large" },
  /**
   * An oversize frame on the public transport. Rust's compressed transport
   * fails the read and sends 1002 with no reason; the 1009 "public transport
   * limit exceeded" its handler writes afterwards never reaches the client
   * (measured, contract-changes 0020).
   */
  publicTransportLimit: { code: 1002, reason: "" },
  ended: { code: 1000, reason: "" },
  failed: { code: 1011, reason: "" },
} as const;
export type CloseFrame = { readonly code: number; readonly reason: string };

export const AUTH_ERRORS = {
  storeUnavailable: "auth session store is unavailable",
  publicUnavailable: "public tenant session is unavailable",
  projectRequired: "project is required for public Control Plane calls",
  sessionRequired: "a Gonvex app session is required",
  developerNeedsTenant: "developer grants require an active tenant",
  publicTransport: "public authentication requires the public WebSocket transport",
  publicScopeFixed: "public connection scope is fixed",
  expired: "authentication expired or was revoked",
  membershipChanged: "tenant membership changed; authenticate again",
  serviceInvalid: "service credential is invalid or not permitted for this project",
} as const;

export const UNAUTHENTICATED = {
  query: "authenticate with an active tenant before calling a Query",
  subscribe: "authenticate with an active tenant before subscribing to a Live Query",
  reducer: "authenticate with an active tenant before calling a Reducer",
  action: "authenticate with an active tenant before calling an Action",
  replica: "authenticate with an active tenant before opening a Replica Collection",
  presence: "presence requires an authenticated tenant member session",
} as const;

export type ReducerErrorClass = "rejected" | "transient" | "update_required" | "unauthenticated";

/** Server frames as JSON text. */
export const frames = {
  authResult: (id: string, result: Json) => JSON.stringify({ type: "auth.result", id, result }),
  authError: (id: string, error: string, code?: string) =>
    JSON.stringify(code === undefined ? { type: "auth.error", id, error } : { type: "auth.error", id, error, code }),
  queryError: (id: string, path: string | undefined, error: string) =>
    JSON.stringify(path === undefined ? { type: "query.error", id, error } : { type: "query.error", id, path, error }),
  reducerError: (id: string, path: string | undefined, error: string, errorClass: ReducerErrorClass, trace?: MessageTrace) =>
    JSON.stringify({
      type: "reducer.error", id, ...(path === undefined ? {} : { path }), error, class: errorClass,
      retryable: errorClass === "transient" || errorClass === "unauthenticated", ...(trace ? { trace: echoTrace(trace) } : {}),
    }),
  actionError: (id: string, path: string | undefined, error: string, trace?: MessageTrace) =>
    JSON.stringify({ type: "action.error", id, ...(path === undefined ? {} : { path }), error, ...(trace ? { trace: echoTrace(trace) } : {}) }),
  replicaError: (id: string, path: string | undefined, error: string) =>
    JSON.stringify(path === undefined ? { type: "replica.error", id, error } : { type: "replica.error", id, path, error }),
  presenceError: (id: string, error: string) => JSON.stringify({ type: "presence.error", id, error }),
  clientUpdateRequired: (contract: number) => JSON.stringify({ type: "client.updateRequired", reason: "CLIENT_UPDATE_REQUIRED", contract }),
  systemReload: (artifactHash: string | undefined) =>
    JSON.stringify(artifactHash === undefined ? { type: "system.reload", reason: "module generation changed" } : { type: "system.reload", reason: "module generation changed", artifactHash }),
  replicaWatermark: (revision: number) => JSON.stringify({ type: "replica.watermark", revision }),
};

const TRACE_ORDER = [
  "clientSentAtMs", "serverReceivedAtMs", "serverReducerStartedAtMs", "serverReducerCommittedAtMs",
  "serverCompletedAtMs", "serverBroadcastScheduledAtMs", "serverChangeCommittedAtMs",
  "serverSubscriptionStartedAtMs", "serverSubscriptionSentAtMs", "serverDurationMs", "queryPerf",
] as const;
/** A decoded `MessageTrace` re-serialized as serde does: struct order, absent fields skipped. */
export function echoTrace(trace: MessageTrace): Record<string, Json> {
  const out: Record<string, Json> = {};
  for (const key of TRACE_ORDER) if (trace[key] !== undefined) out[key] = trace[key] as Json;
  return out;
}

/** UTF-8 length of a string, computed only when it can matter for `limit`. */
export function exceedsUtf8(text: string, limit: number): boolean {
  if (text.length > limit) return true; // every UTF-16 unit is at least one byte
  if (text.length * 3 <= limit) return false; // every unit is at most three bytes
  let bytes = 0;
  for (let i = 0; i < text.length; i++) {
    const c = text.charCodeAt(i);
    if (c < 0x80) bytes += 1;
    else if (c < 0x800) bytes += 2;
    else if (c >= 0xd800 && c <= 0xdbff) { bytes += 4; i++; }
    else bytes += 3;
    if (bytes > limit) return true;
  }
  return false;
}
