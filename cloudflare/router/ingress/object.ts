/**
 * One ingress object per (project, tenant) (K18).
 *
 * It is the single place where deliveries to a tenant meet, so it can:
 * - limit the rate per tenant (the gateway's `tenant_limiter`, 50/s with a
 *   burst of 100, as a per-object counter: 0012 R11);
 * - recognize a replayed delivery: a delivery that already completed answers
 *   200 without running the Action again, and a duplicate that arrives while
 *   the first is running waits for the first one's answer;
 * - run the Action through the service-call port.
 *
 * Everything here is in memory. Nothing ephemeral is written to SQLite (DOD),
 * so the ledger does not survive an eviction; the Actions are idempotent per
 * provider event (the gateway README: "both Actions are safe to repeat";
 * `relay.recordWebhookEvent` dedupes on the provider event id), which bounds
 * what a replay after an eviction can do. 0040 §5.
 */
import { DurableObject } from "cloudflare:workers";
import type { IngressGrant } from "./directory.ts";
import { bindingPort, commandId, type ServiceActionPort, type ServiceActionReport } from "./invoke.ts";

export interface RateLimit {
  perSecond: number;
  burst: number;
}

/** whagons-api-gateway `GATEWAY_TENANT_RATE_LIMIT` default. */
export const WEBHOOK_RATE: RateLimit = { perSecond: 50, burst: 100 };
/** Share pages and images: link-preview fetchers ask a few times per link. */
export const SHARE_RATE: RateLimit = { perSecond: 20, burst: 40 };
/** Ledger entries kept per tenant before the oldest are dropped. */
export const MAX_LEDGER = 10_000;

/** `<per second>[/<burst>]`, as the gateway parses its limits. `0` disables. */
export function parseRate(value: string | undefined, fallback: RateLimit): RateLimit {
  if (!value || !value.trim()) return fallback;
  const [rate, burst] = value.split("/", 2).map((part) => Number(part.trim()));
  const perSecond = rate ?? Number.NaN;
  const size = burst === undefined ? Math.max(1, perSecond) : burst;
  if (!Number.isFinite(perSecond) || !Number.isFinite(size) || perSecond < 0 || size < 1) return fallback;
  return { perSecond, burst: size };
}

export interface IngressObjectEnv {
  INGRESS_ACTIONS?: unknown;
  GONVEX_INGRESS_TENANT_RATE?: string;
  GONVEX_INGRESS_SHARE_RATE?: string;
}

export interface DeliveryRequest {
  v: 1;
  grant: IngressGrant;
  path: string;
  args: Record<string, unknown>;
  /** Which rate bucket the request draws from. */
  bucket: "webhook" | "share";
  /** Identity of this delivery for replay detection; absent for share pages. */
  replay?: { key: string; untilMs: number } | undefined;
}

export type DeliveryOutcome =
  | { kind: "done"; report: ServiceActionReport; duplicate?: true }
  | { kind: "limited" }
  | { kind: "unavailable"; reason: string };

interface Bucket {
  tokens: number;
  at: number;
}

type LedgerEntry = { state: "done"; untilMs: number } | { state: "running"; outcome: Promise<DeliveryOutcome> };

/** Token bucket per object and bucket name. */
export class Limiter {
  private readonly buckets = new Map<string, Bucket>();

  constructor(private readonly limits: Record<string, RateLimit>) {}

  take(name: string, nowMs: number): boolean {
    const limit = this.limits[name];
    if (!limit || limit.perSecond === 0) return true;
    const bucket = this.buckets.get(name) ?? { tokens: limit.burst, at: nowMs };
    bucket.tokens = Math.min(limit.burst, bucket.tokens + ((nowMs - bucket.at) / 1000) * limit.perSecond);
    bucket.at = nowMs;
    this.buckets.set(name, bucket);
    if (bucket.tokens < 1) return false;
    bucket.tokens -= 1;
    return true;
  }
}

/** The replay ledger and the invocation, without the Durable Object around them (tested directly). */
export class DeliveryDesk {
  readonly ledger = new Map<string, LedgerEntry>();

  constructor(
    private readonly port: () => ServiceActionPort | null,
    private readonly limiter: Limiter,
    private readonly clock: () => number = () => Date.now(),
  ) {}

  async deliver(request: DeliveryRequest): Promise<DeliveryOutcome> {
    const now = this.clock();
    const replay = request.replay;
    if (replay) {
      const entry = this.ledger.get(replay.key);
      if (entry?.state === "done" && entry.untilMs > now) {
        return { kind: "done", report: { ok: true, value: null }, duplicate: true };
      }
      if (entry?.state === "running") {
        const outcome = await entry.outcome;
        return outcome.kind === "done" ? { ...outcome, duplicate: true } : outcome;
      }
    }
    if (!this.limiter.take(request.bucket, now)) return { kind: "limited" };
    const outcome = this.invoke(request);
    if (!replay) return outcome;
    this.ledger.set(replay.key, { state: "running", outcome });
    let settled: DeliveryOutcome | undefined;
    try {
      settled = await outcome;
      return settled;
    } finally {
      // Only a delivery the Action accepted is remembered; a failed one must
      // run again when the provider retries it.
      if (settled?.kind === "done" && settled.report.ok) this.remember(replay.key, replay.untilMs);
      else this.ledger.delete(replay.key);
    }
  }

  private async invoke(request: DeliveryRequest): Promise<DeliveryOutcome> {
    const { grant, path } = request;
    if (!grant.functions.includes(path)) {
      return { kind: "unavailable", reason: `function ${JSON.stringify(path)} is not allowed for the ingress service principal` };
    }
    const port = this.port();
    if (!port) return { kind: "unavailable", reason: "no Action runtime is bound (INGRESS_ACTIONS)" };
    try {
      const report = await port.serviceAction({
        v: 1,
        principal: { id: grant.principalId, projectId: grant.projectId, tenantId: grant.tenantId, functions: grant.functions },
        path,
        args: request.args,
        channel: "system",
        commandId: commandId(),
      });
      return { kind: "done", report };
    } catch (error) {
      return { kind: "done", report: { ok: false, error: String((error as Error)?.message ?? error), transient: true } };
    }
  }

  private remember(key: string, untilMs: number): void {
    const now = this.clock();
    this.ledger.delete(key);
    if (this.ledger.size >= MAX_LEDGER) {
      for (const [entry, value] of this.ledger) if (value.state === "done" && value.untilMs <= now) this.ledger.delete(entry);
      // Still full: drop the oldest completed entries (a Map iterates in insertion order).
      for (const [entry, value] of this.ledger) {
        if (this.ledger.size < MAX_LEDGER) break;
        if (value.state === "done") this.ledger.delete(entry);
      }
    }
    this.ledger.set(key, { state: "done", untilMs });
  }
}

export class IngressObject extends DurableObject<IngressObjectEnv> {
  private readonly desk: DeliveryDesk;

  constructor(ctx: DurableObjectState, env: IngressObjectEnv) {
    super(ctx, env);
    const limiter = new Limiter({
      webhook: parseRate(env.GONVEX_INGRESS_TENANT_RATE, WEBHOOK_RATE),
      share: parseRate(env.GONVEX_INGRESS_SHARE_RATE, SHARE_RATE),
    });
    this.desk = new DeliveryDesk(() => bindingPort(env.INGRESS_ACTIONS), limiter);
  }

  async deliver(request: DeliveryRequest): Promise<DeliveryOutcome> {
    return this.desk.deliver(request);
  }
}

/** The object's name: one per canonical (project, tenant). */
export const ingressObjectName = (grant: IngressGrant): string => `${grant.projectId}\u0000${grant.tenantId}`;
