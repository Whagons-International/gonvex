/**
 * `session.ready` (CONTRACT.md A.1.3) and the client capabilities a session
 * negotiates on each `auth` (A.1.4: every capability is re-read on every
 * `auth` frame and only the value 1 counts).
 */
import type { ClientCapabilities } from "./decode";
import { PROTOCOL_VERSION } from "./protocol";

export interface ReadyOptions {
  /** A.1.3 `runtimeVersion`, adapt: the deploy's Git SHA or Worker version id. */
  runtimeVersion: string;
  /** Presence v2 is advertised as Rust does by default (`GONVEX_PRESENCE_V2`). */
  presence: boolean;
  /** Serverless DOD Phase 2b: advertise WLP1 binary PackedPage support (opt-in). */
  packedPages?: boolean;
  /** Serverless DOD Phase 2b: advertise stateless Ed25519 edge session tokens (opt-in). */
  edgeSessionToken?: boolean;
}

/**
 * The first frame on every socket, before any client frame. Keys in serde's
 * `ServerCapabilities` order. `replicaFieldPatches`, `replicaSetDigest` and
 * `replicaNativeRows` are not advertised (0012 R20, A.1.3 "replace").
 */
export function sessionReadyFrame({ runtimeVersion, presence, packedPages, edgeSessionToken }: ReadyOptions): string {
  return JSON.stringify({
    type: "session.ready",
    capabilities: {
      replicaNoOldValues: 1,
      replicaSnapshotPages: 1,
      replicaResume: 1,
      queryResume: 1,
      preserveSubscriptions: 1,
      telemetryBatch: 1,
      protocolVersion: PROTOCOL_VERSION,
      runtimeVersion,
      replicaBatch: 1,
      replicaIntegrity: 1,
      queryBatch: 1,
      queryResultBatch: 1,
      reducerBatch: 1,
      replicaWatermark: 1,
      reducerReceiptLookup: 1,
      controlWatermark: 1,
      replicaBatchFrames: 1,
      ...(presence ? { presence: 1 } : {}),
      ...(packedPages ? { replicaPackedPages: 1 } : {}),
      ...(edgeSessionToken ? { edgeSessionToken: 1 } : {}),
    },
  });
}

/** What one `auth` negotiated. Stored in the socket attachment. */
export interface Negotiated {
  snapshotPages: boolean;
  noOldValues: boolean;
  fieldPatches: boolean;
  batchFrames: boolean;
  nativeRows: boolean;
  setDigest: boolean;
  telemetryBatch: boolean;
  presence: boolean;
  /** Present and true only when both server and client negotiated `replicaPackedPages: 1`. */
  packedPages?: boolean;
  /** Present and true only when both server and client negotiated `edgeSessionToken: 1`. */
  edgeSessionToken?: boolean;
}

export const NOTHING_NEGOTIATED: Negotiated = {
  snapshotPages: false, noOldValues: false, fieldPatches: false, batchFrames: false,
  nativeRows: false, setDigest: false, telemetryBatch: false, presence: false,
};

export interface ServerDodCapabilities {
  readonly packedPages?: boolean | undefined;
  readonly edgeSessionToken?: boolean | undefined;
}

/** lib.rs:2899-2920. `presence`, `packedPages`, and `edgeSessionToken` also need the server flag on. */
export function negotiate(
  capabilities: ClientCapabilities | undefined,
  serverPresence: boolean,
  serverDod?: ServerDodCapabilities,
): Negotiated {
  const one = (value: number | undefined) => value === 1;
  const c = capabilities ?? {};
  const snapshotPages = one(c.replicaSnapshotPages);
  const setDigest = one(c.replicaSetDigest);
  return {
    snapshotPages,
    noOldValues: one(c.replicaNoOldValues),
    fieldPatches: one(c.replicaFieldPatches),
    batchFrames: one(c.replicaBatchFrames),
    // snapshot_delivery.rs:13-19: all three at version 1.
    nativeRows: one(c.replicaNativeRows) && setDigest && snapshotPages,
    setDigest,
    telemetryBatch: one(c.telemetryBatch),
    presence: serverPresence && one(c.presence),
    ...(serverDod?.packedPages && one(c.replicaPackedPages) ? { packedPages: true } : {}),
    ...(serverDod?.edgeSessionToken && one(c.edgeSessionToken) ? { edgeSessionToken: true } : {}),
  };
}
