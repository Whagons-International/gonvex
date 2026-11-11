/**
 * K15 probe Worker: `POST /write?n=&run=` writes `n` performance rows through
 * the kernel's Analytics Engine sink and reports the time it took.
 */
import { analyticsEngineSink } from "../../sink";
import type { PerformanceRow } from "../../envelope";

interface Env {
  TELEMETRY_EVENTS: AnalyticsEngineDataset;
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    if (url.pathname !== "/write" || request.method !== "POST") return Response.json({ ok: true, probe: "k15-ae" });
    const n = Math.min(Number(url.searchParams.get("n") ?? "1"), 250);
    const run = url.searchParams.get("run") ?? "run";
    const rows: PerformanceRow[] = Array.from({ length: n }, (_, i) => ({
      eventId: `${run}-${i}`, tenantId: "probe-tenant", accountId: "probe-account", kind: i % 2 ? "query" : "reducer", path: "k15.probe",
      reason: "", outcome: i % 10 === 0 ? "error" : "ok", error: i % 10 === 0 ? "probe error" : "", clientSentAtMs: null,
      clientReceivedAtMs: Date.now(), clientDurationMs: i, trace: null, device: null,
    }));
    const sink = analyticsEngineSink(env.TELEMETRY_EVENTS)!;
    const started = performance.now();
    sink.write(`k15-probe-${run}`, rows);
    return Response.json({ ok: true, written: n, ms: performance.now() - started });
  },
};
