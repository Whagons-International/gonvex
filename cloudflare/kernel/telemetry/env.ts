/**
 * Telemetry configuration of an object that owns sockets (a gateway).
 *
 * - `TELEMETRY_EVENTS`: an Analytics Engine dataset binding. When bound, every
 *   accepted performance event is written there (0040 §1). Deployments name the
 *   dataset `gonvex_cf_dev_<name>`; nothing is created by the kernel.
 * - `GONVEX_TELEMETRY_STORE_PER_MINUTE`: performance rows per project per
 *   minute this object forwards to the control object's SQLite window
 *   (default 600).
 */
import { analyticsEngineSink } from "./sink";
import { STORE_PER_MINUTE, type TelemetryHostState, telemetryHostState } from "./socket";

export interface TelemetryEnv {
  TELEMETRY_EVENTS?: AnalyticsEngineDataset;
  GONVEX_TELEMETRY_STORE_PER_MINUTE?: string;
}

export function telemetryHostFromEnv(env: TelemetryEnv, presenceEnabled: boolean): TelemetryHostState {
  const configured = env.GONVEX_TELEMETRY_STORE_PER_MINUTE ? Number(env.GONVEX_TELEMETRY_STORE_PER_MINUTE) : NaN;
  return telemetryHostState({
    presenceEnabled,
    sink: analyticsEngineSink(env.TELEMETRY_EVENTS),
    storePerMinute: Number.isInteger(configured) && configured >= 0 ? configured : STORE_PER_MINUTE,
  });
}
