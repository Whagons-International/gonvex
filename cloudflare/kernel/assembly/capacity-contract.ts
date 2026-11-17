/** Dev-fixture HTTP contract. Type-only imports keep this consumable by drivers. */
import type { CapacitySample, DrainResult } from "../gateway/placement";

export const CAPACITY_PATH = "/__assembly/capacity/v1";
export const CAPACITY_LIMITS = Object.freeze({ slots: 4, events: 16, responseBytes: 32 * 1024,
  catalogReplyBytes: 512 * 1024, controllerReplyBytes: 96 * 1024, holderReplyBytes: 4096, concurrency: 2,
  catalogMs: 750, controllerMs: 1500, holderMs: 750, requestMs: 6000, cachedStaleMs: 30_000 });
export type ReadFailure = "NOT_BOUND" | "TIMEOUT" | "ABORTED" | "RPC_FAILED" | "MALFORMED"
  | "OVERSIZED" | "EPOCH_MISMATCH" | "NOT_REQUESTED" | "NOT_SELECTED" | "RETIRED";
export type CapacityRead<T> = { status: "OK"; value: T } | { status: ReadFailure };
export interface CapacityQuery {
  cell: number; cursor: number; limit: number; eventCursor: number; eventLimit: number;
  fresh: boolean; runId: string | null;
}
export interface CapacityObservation {
  /** Original source at/window timestamps, never rewritten to the HTTP capture time. */
  sample: CapacitySample | null;
  state: "FRESH" | "STALE" | "UNKNOWN" | "CLOCK_SKEW";
  ageMs: number | null;
  windowState: "FRESH" | "STALE" | "UNKNOWN" | "CLOCK_SKEW";
}
export interface CapacitySlot {
  name: string; epoch: number; mode: "active" | "draining" | "retired";
  changedAt: number; pressuredSince: number | null; replacement: string | null; nextCheck: number | null;
  /** Exact persisted controller evidence; no maturity/continuous occupancy claim. */
  pressureEvidence: { epoch: number; window: readonly number[]; since: number } | null;
  cached: CapacityObservation;
  fresh: { capacity: CapacityRead<CapacityObservation>; drain: CapacityRead<DrainResult> };
}
export interface CapacityEvent { at: number; action: "allocate" | "drain" | "retire"; gateway: string; epoch: number }
export interface CapacityIdentity {
  originService: "kernel-http";
  /** HTTP Worker only. This does not attribute code execution inside a DO. */
  worker: { id: string; tag: string; timestamp: string } | null;
  runtimeWorker: { id: string; tag: string; timestamp: string } | null;
  runtimeLabel: string | null;
  catalog: CapacityRead<{ originService: "runtime-catalog"; sourceInterface: "RuntimeCatalog.identity";
    version: string; projects: number; releasesSha256: string; generation: "UNAVAILABLE_IDENTITY_PORT_HAS_HASHES_ONLY" }>;
  config: { controllerBindingPresent: boolean; runtimeCatalogBindingPresent: boolean; staticGatewayShards: number; factsSha256: string };
  objectCodeVersions: "UNAVAILABLE_NO_OBJECT_VERSION_PORT";
}
export interface CapacityCoverage { cursor: number; limit: number; returned: number; total: number; nextCursor: number | null; hasMore: boolean }
export interface CapacityPage {
  v: 1; at: number; runId: string | null; mode: "cached" | "fresh";
  pageStatus: "COMPLETE" | "PARTIAL";
  identity: CapacityIdentity;
  controller: CapacityRead<{ cell: number; epoch: number; pending: number; readAt: number; snapshotSha256: string }>;
  coverage: null | { cell: number; totalCells: 32; fleet: "ONE_CELL_PAGE_ONLY";
    slots: CapacityCoverage; events: CapacityCoverage & { sourceLimit: 64; loss: "UNKNOWN"; oldestAt: number | null; newestAt: number | null };
    pagination: "COMPARE_CONTROLLER_SNAPSHOT_SHA256" };
  slots: CapacitySlot[]; events: CapacityEvent[];
  provenance: {
    clock: "UNIX_MS_SERVICE_CLOCKS_NOT_SYNCHRONIZED";
    catalogIdentityCalls: number; controllerStatusCalls: number; capacityCalls: number; drainStatusCalls: number;
    controllerReadMayWake: true; freshMayWakeHolders: boolean;
    metricsSnapshotMayAdvance: boolean; socketSampleMayBeRecorded: boolean;
    drainResult: "LAST_TRACKED_RESULT_NO_RESULT_TIMESTAMP";
    cancellation: "STOPS_FURTHER_CALLS_INFLIGHT_RPC_NOT_CANCELLED";
    runId: "CORRELATION_ONLY_NOT_COST_ATTRIBUTION";
    history: "LAST_64_EVENTS_NO_COMPLETE_TIMELINE_OR_LOSS_COUNTER";
    windowHistory: "LATEST_COMPLETED_BUCKET_ONLY_GAPS_REQUIRE_CLIENT_COMPARISON";
    sockets: "CURRENT_GAUGE_AND_BUCKET_SAMPLED_MIN_NOT_CONTINUOUS_OCCUPANCY";
  };
  unavailable: readonly { metric: string; status: "UNAVAILABLE"; sourceInterface: string; reason: string }[];
}
export interface CapacityError { v: 1; error: "BAD_QUERY" | "BAD_CURSOR" | "METHOD" | "ABORTED" | "RESPONSE_BUDGET" }
