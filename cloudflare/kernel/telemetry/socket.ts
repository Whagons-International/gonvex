/**
 * The telemetry frames of one socket, in the object that owns it (a gateway):
 * `error.register`, `error.heartbeat`, `error.envelope`, `telemetry.event`
 * and `telemetry.batch` (`lib.rs:3703-3850` → `telemetry.rs`).
 *
 * Everything that needs no storage runs here, next to the socket, so the
 * control object only stores: attribution from the socket, the rate limit,
 * envelope limits, scrubbing, fingerprints and event ids, performance-event
 * validation. Every frame is handled whether or not the socket has a tenant;
 * only the project decides (`connection.project_id`), as in Rust.
 *
 * Error support sessions (`register_error_session`) are answered from the
 * socket's record when they are already registered with the same attribution,
 * as Rust answers them from its presence hub; only a new session or changed
 * attribution reaches the control object.
 */
import type { Json } from "../session/decode";
import type { Negotiated } from "../session/capabilities";
import type { Connection } from "../session/ports";
import {
  ACK_ERRORS, LIMITS, type Attribution, type PerformanceFrame, type PerformanceRow,
  boundedText, checkEnvelope, errorAck, performanceRow, prepareEvents, validPerformanceEvent,
} from "./envelope";
import { TelemetryLimiter, telemetrySubject } from "./limiter";
import type { TelemetryPort } from "./port";
import type { PerformanceSink } from "./sink";

/** The error support session this connection registered (Rust's `SupportLive` for `session_id = connection id`). */
export interface ErrorSessionRecord {
  project: string;
  tenant: string;
  account: string;
  release: string;
  environment: string;
}

/** The fields of the socket's record telemetry reads and writes (K05 `SessionRecord`). */
export interface TelemetryRecordHost {
  connectionId: string;
  connection: Connection;
  negotiated: Negotiated;
  /** K15: the registered error support session (repeats with the same attribution need no write). */
  errorSession?: ErrorSessionRecord | null;
  /** K15: `telemetry_device`, the `device` of the last `auth` frame (fallback for performance events). */
  telemetryDevice?: Json | null;
}

export interface TelemetrySessionHost {
  readonly record: TelemetryRecordHost;
  persist(): void;
}

/** Shared by the sockets of one owning object. */
export interface TelemetryHostState {
  /** `telemetry_allowed`'s windows (0012 R11: per owning object). */
  limiter: TelemetryLimiter;
  /** The kernel's per-object budget of performance rows forwarded to the control object's SQLite window. */
  storeBudget: TelemetryLimiter;
  /** Rows per project per minute this object forwards to the control object (0041 §1). */
  storePerMinute: number;
  /** Analytics Engine, when bound: every accepted performance event. */
  sink?: PerformanceSink | undefined;
  /** Rust's `support_is_current` answers only while presence v2 is enabled. */
  presenceEnabled: boolean;
  now?: () => number;
  /** X17: performance rows wait here and reach the control object in one call per project (see PerformanceBuffer). */
  performance?: PerformanceBuffer | undefined;
}

/**
 * X17: performance rows (`telemetry.*`, answered with nothing) leave the socket's frame queue at once and reach the
 * control object a few seconds later, one call per project for every socket of this object. Before, each
 * `telemetry.batch` held its socket's queue for a control round trip, and the client sends one right before its sign-in
 * `auth` frames; its insert also held every other reply of the (single) control object behind the write's replication,
 * because a Durable Object's output gate covers the whole object. The rows, their order within a project, the rate
 * limits and the store budget are unchanged; only when they are written moves (Rust writes them in the background too).
 */
export const PERFORMANCE_FLUSH_MS = 5_000;
export const PERFORMANCE_FLUSH_ROWS = 200;
export class PerformanceBuffer {
  private readonly pending = new Map<string, { port: TelemetryPort; rows: PerformanceRow[] }>();
  private timer: ReturnType<typeof setTimeout> | undefined;
  private rows = 0;
  constructor(private readonly delayMs = PERFORMANCE_FLUSH_MS, private readonly maxRows = PERFORMANCE_FLUSH_ROWS,
    private readonly timers: { set(fn: () => void, ms: number): ReturnType<typeof setTimeout>; clear(t: ReturnType<typeof setTimeout>): void } = { set: (fn, ms) => setTimeout(fn, ms), clear: (t) => clearTimeout(t) }) {}
  add(port: TelemetryPort, projectId: string, rows: readonly PerformanceRow[]): void {
    const entry = this.pending.get(projectId) ?? { port, rows: [] };
    entry.port = port;
    entry.rows.push(...rows);
    this.pending.set(projectId, entry);
    this.rows += rows.length;
    if (this.rows >= this.maxRows) void this.flush();
    else this.timer ??= this.timers.set(() => { this.timer = undefined; void this.flush(); }, this.delayMs);
  }
  /** Writes everything buffered now (also the timer's and a full buffer's path). */
  async flush(): Promise<void> {
    if (this.timer !== undefined) { this.timers.clear(this.timer); this.timer = undefined; }
    const batches = [...this.pending]; this.pending.clear(); this.rows = 0;
    await Promise.all(batches.map(async ([projectId, { port, rows }]) => {
      for (let i = 0; i < rows.length; i += this.maxRows) {
        try { await port.recordPerformance(projectId, rows.slice(i, i + this.maxRows)); }
        catch (error) { console.error("telemetry: performance rows not stored", error); }
      }
    }));
  }
  get size(): number { return this.rows; }
}

/** Default forwarding budget: 600 rows a minute per gateway and project (8 gateways: at most 80 rows/s). */
export const STORE_PER_MINUTE = 600;

export function telemetryHostState(options: { presenceEnabled: boolean; sink?: PerformanceSink | undefined; storePerMinute?: number; now?: () => number; performance?: PerformanceBuffer }): TelemetryHostState {
  return {
    ...(options.performance ? { performance: options.performance } : {}),
    limiter: new TelemetryLimiter(),
    storeBudget: new TelemetryLimiter(),
    storePerMinute: options.storePerMinute ?? STORE_PER_MINUTE,
    sink: options.sink,
    presenceEnabled: options.presenceEnabled,
    ...(options.now ? { now: options.now } : {}),
  };
}

export type TelemetryFrame =
  | { type: "error.register"; id: string; release?: string; environment?: string }
  | { type: "error.heartbeat"; id: string }
  | { type: "error.envelope"; id: string; events: Json[] }
  | ({ type: "telemetry.event" } & PerformanceFrame)
  | { type: "telemetry.batch"; events: PerformanceFrame[] };

export const TELEMETRY_FRAMES: ReadonlySet<string> = new Set(["error.register", "error.heartbeat", "error.envelope", "telemetry.event", "telemetry.batch"]);

export class SocketTelemetry {
  constructor(
    private readonly session: TelemetrySessionHost,
    private readonly port: TelemetryPort | undefined,
    private readonly host: TelemetryHostState,
  ) {}

  private get now(): number {
    return (this.host.now ?? Date.now)();
  }

  private attribution(): Attribution {
    const { connection, connectionId } = this.session.record;
    return {
      projectId: connection.projectId,
      tenantId: connection.tenant?.route.tenantId ?? "",
      connectionId,
      accountId: connection.identity ? connection.identity.account.id : null,
    };
  }

  private allowed(attribution: Attribution): boolean {
    const { key, limit } = telemetrySubject(attribution.projectId, attribution.accountId, attribution.connectionId);
    return this.host.limiter.allow(key, limit, this.now);
  }

  /** Frames to send in reply, in order (`error.ack` for `error.*`, nothing for `telemetry.*`). */
  async handle(frame: TelemetryFrame): Promise<string[]> {
    switch (frame.type) {
      case "error.register":
        return [await this.register(frame.id, frame.release, frame.environment)];
      case "error.heartbeat":
        return [await this.register(frame.id, undefined, undefined)];
      case "error.envelope":
        return [await this.envelope(frame.id, frame.events)];
      case "telemetry.event": {
        const { type: _, ...event } = frame;
        await this.performance([event]);
        return [];
      }
      case "telemetry.batch":
        if (this.session.record.negotiated.telemetryBatch && frame.events.length <= LIMITS.maxBatchEvents) await this.performance(frame.events);
        return [];
    }
  }

  /** `register_error_session` (`error.register`, and `error.heartbeat` with neither field). */
  private async register(id: string, rawRelease: string | undefined, rawEnvironment: string | undefined): Promise<string> {
    const attribution = this.attribution();
    if (!this.port || attribution.projectId === "") return errorAck(id, { error: ACK_ERRORS.unavailable });
    if (!this.allowed(attribution)) return errorAck(id, { error: ACK_ERRORS.rateLimited });
    if (attribution.accountId === null) return errorAck(id);
    const release = rawRelease === undefined ? undefined : boundedText(rawRelease, 200);
    const environment = rawEnvironment === undefined ? undefined : boundedText(rawEnvironment, 100);
    const record = this.session.record;
    const live = record.errorSession;
    // `support_is_current`: `None` release/environment (a heartbeat) match any registered value.
    if (this.host.presenceEnabled && live && live.project === attribution.projectId && live.tenant === attribution.tenantId && live.account === attribution.accountId
      && (release === undefined || live.release === release) && (environment === undefined || live.environment === environment)) {
      return errorAck(id);
    }
    const stored = await this.port.registerErrorSession({
      connectionId: attribution.connectionId, projectId: attribution.projectId, tenantId: attribution.tenantId, accountId: attribution.accountId,
      release: release ?? "", environment: environment ?? "", live: this.host.presenceEnabled,
    });
    if (!stored) return errorAck(id, { error: ACK_ERRORS.store });
    if (this.host.presenceEnabled) {
      // `support_register`: an empty value keeps the one registered before.
      record.errorSession = {
        project: attribution.projectId, tenant: attribution.tenantId, account: attribution.accountId,
        release: release || live?.release || "", environment: environment || live?.environment || "",
      };
      this.session.persist();
    }
    return errorAck(id);
  }

  /** `capture_error_envelope` */
  private async envelope(id: string, events: Json[]): Promise<string> {
    const attribution = this.attribution();
    if (!this.port || attribution.projectId === "") return errorAck(id, { error: ACK_ERRORS.unavailable });
    if (!this.allowed(attribution)) return errorAck(id, { error: ACK_ERRORS.rateLimited });
    const refused = checkEnvelope(events);
    if (refused) return errorAck(id, { error: refused });
    const prepared = await prepareEvents(attribution, events, () => this.now);
    if (prepared.length === 0) return errorAck(id, { accepted: 0, fingerprints: [] });
    const inserted = await this.port.recordErrors(attribution.projectId, prepared);
    if (!inserted) return errorAck(id, { error: ACK_ERRORS.store });
    const fingerprints = prepared.filter((_, i) => inserted[i]).map((event) => event.fingerprint);
    return errorAck(id, { accepted: fingerprints.length, fingerprints });
  }

  /** `capture_performance_event` for each event, in order; nothing is answered. */
  private async performance(events: readonly PerformanceFrame[]): Promise<void> {
    const attribution = this.attribution();
    const record = this.session.record;
    const rows: PerformanceRow[] = [];
    for (const event of events) {
      // Validation first; the rate limit is the last condition, so an invalid event costs no budget.
      if (!validPerformanceEvent(attribution.projectId, event) || !this.allowed(attribution)) continue;
      rows.push(performanceRow(attribution, event, record.telemetryDevice ?? undefined));
    }
    // Without a Control Plane, Rust drops the event after the checks.
    if (rows.length === 0 || !this.port) return;
    this.host.sink?.write(attribution.projectId, rows);
    const budget = `store:${attribution.projectId}`;
    const stored = rows.filter(() => this.host.storeBudget.allow(budget, this.host.storePerMinute, this.now));
    if (stored.length === 0) return;
    if (this.host.performance) this.host.performance.add(this.port, attribution.projectId, stored);
    else await this.port.recordPerformance(attribution.projectId, stored);
  }
}
