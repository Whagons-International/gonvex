/**
 * What the socket side of telemetry needs from the control object (K06), and
 * the adapter from a control object stub. The control object hosts
 * `TelemetryStore` behind three RPC methods (`control/src/control-object.ts`).
 *
 * The session layer still talks to K05's control stand-in, which has no
 * store; until the gateway is wired to the real control object, `Ports.telemetry`
 * stays unset and the socket side answers as Rust does without a Control
 * Plane: `error telemetry is unavailable`, performance events dropped.
 */
import type { PerformanceRow, PreparedErrorEvent } from "./envelope";
import type { ErrorSessionInput } from "./store";

export interface TelemetryPort {
  /** The `gonvex_support_sessions` upsert of `register_error_session`; false when the store failed. */
  registerErrorSession(input: ErrorSessionInput): Promise<boolean>;
  /** `persist_event` for each event, in order: whether each was new; null when the store failed. */
  recordErrors(projectId: string, events: PreparedErrorEvent[]): Promise<boolean[] | null>;
  /** `capture_performance_event` inserts; failures are dropped, as Rust drops them. */
  recordPerformance(projectId: string, rows: PerformanceRow[]): Promise<void>;
}

/** The control object's telemetry RPC (`ControlObject.telemetry*`). */
export interface TelemetryControlRpc {
  telemetryRegisterSession(input: ErrorSessionInput): Promise<boolean>;
  telemetryRecordErrors(projectId: string, events: PreparedErrorEvent[]): Promise<boolean[] | null>;
  telemetryRecordPerformance(projectId: string, rows: PerformanceRow[]): Promise<number>;
}

/** A `TelemetryPort` over the control object's RPC. A failed call is a store failure, never a socket failure. */
export function controlTelemetryPort(control: () => TelemetryControlRpc): TelemetryPort {
  return {
    async registerErrorSession(input) {
      try {
        return await control().telemetryRegisterSession(input);
      } catch (error) {
        console.error("telemetry: error session not registered", error);
        return false;
      }
    },
    async recordErrors(projectId, events) {
      try {
        return await control().telemetryRecordErrors(projectId, events);
      } catch (error) {
        console.error("telemetry: error events not stored", error);
        return null;
      }
    },
    async recordPerformance(projectId, rows) {
      try {
        await control().telemetryRecordPerformance(projectId, rows);
      } catch (error) {
        // `tracing::debug!("performance telemetry was dropped")`
        console.debug("telemetry: performance events dropped", error);
      }
    },
  };
}
