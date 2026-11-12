/**
 * Workers Analytics Engine as the sink of record for performance events
 * (0041 §1). Every accepted event is written as one data point from the
 * object that owns the socket: no RPC, no SQLite row, no disk of ours, and the
 * platform's own retention (three months). The control object keeps only the
 * bounded recent window the Rust-shaped dashboard routes read.
 *
 * Data point layout (M05 reads it with the Analytics Engine SQL API):
 *
 * | Field | Value |
 * | --- | --- |
 * | `index1` | project id (the sampling key) |
 * | `blob1` … `blob8` | kind, path, outcome, reason, tenant id, account id, event id, error (≤ 1,000 chars) |
 * | `double1` | client duration in ms, -1 when absent |
 * | `double2` | `clientReceivedAtMs` |
 * | `double3` | `clientSentAtMs`, -1 when absent |
 * | `double4` | 1 for `outcome = "error"`, else 0 |
 */
import type { PerformanceRow } from "./envelope";

export interface PerformanceSink {
  write(projectId: string, rows: readonly PerformanceRow[]): void;
}

/** Analytics Engine caps an index at 96 bytes. */
const index = (projectId: string) => {
  const bytes = new TextEncoder().encode(projectId);
  return bytes.length <= 96 ? projectId : new TextDecoder().decode(bytes.subarray(0, 96)).replace(/�+$/, "");
};

export function analyticsEngineSink(dataset: AnalyticsEngineDataset | undefined): PerformanceSink | undefined {
  if (!dataset) return undefined;
  return {
    write(projectId, rows) {
      for (const row of rows) {
        try {
          dataset.writeDataPoint({
            indexes: [index(projectId)],
            blobs: [row.kind, row.path, row.outcome, row.reason, row.tenantId, row.accountId, row.eventId, row.error],
            doubles: [row.clientDurationMs ?? -1, row.clientReceivedAtMs, row.clientSentAtMs ?? -1, row.outcome === "error" ? 1 : 0],
          });
        } catch (error) {
          // Best effort, like every performance event.
          console.debug("telemetry: analytics data point dropped", error);
          return;
        }
      }
    },
  };
}
