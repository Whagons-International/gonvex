// Provenance of durable work (MR/lib.rs InvocationProvenance; CONTRACT E.b.3
// "Provenance", "Hop limit", E.c.1 "Provenance"). Stored provenance is the
// camelCase JSON Rust writes with the queued work; a durable hop resets the
// synchronous depth and counts hops so self-rescheduling chains end.

export type Channel = "ui" | "agent" | "api" | "scheduler" | "system";

const CHANNELS = new Set<string>(["ui", "agent", "api", "scheduler", "system"]);

/** InvocationProvenance as serde writes it (camelCase; `durableHops` omitted at 0, `delegation` when absent). */
export interface Provenance {
  channel: Channel;
  rootChannel: Channel;
  actorAccountId: string | null;
  actorMemberId: string | null;
  onBehalfOfMemberId: string | null;
  rootCommandId: string;
  commandId: string;
  parentCommandId: string | null;
  agentExecutionId: string | null;
  threadId: string | null;
  turnId: string | null;
  toolCallId: string | null;
  artifactHash: string;
  depth: number;
  durableHops?: number;
  actionStack: string[];
  delegation?: unknown;
}

/** RT/execution.rs SYSTEM_ACCOUNT_ID; outbox actors starting with `_gonvex_` run as the system session. */
export const SYSTEM_ACCOUNT_ID = "_gonvex_system";
export const SYSTEM_MEMBER_ID = "_gonvex_system";

/** MR/lib.rs MAX_DURABLE_HOPS (`GONVEX_MAX_DURABLE_HOPS` overrides, > 0). */
export const MAX_DURABLE_HOPS = 32;

export function maxDurableHopsFromEnv(value: string | undefined): number {
  const parsed = value?.trim() && /^\d+$/.test(value.trim()) ? Number(value.trim()) : Number.NaN;
  return Number.isInteger(parsed) && parsed > 0 && parsed <= 255 ? parsed : MAX_DURABLE_HOPS;
}

/** MR/lib.rs DurableHopsExceeded. */
export function durableHopsExceeded(hops: number): string {
  return `durable work chain exceeded the maximum of ${hops} scheduled or outbox hops`;
}

export function defaultProvenance(): Provenance {
  return {
    channel: "ui", rootChannel: "ui", actorAccountId: null, actorMemberId: null, onBehalfOfMemberId: null,
    rootCommandId: "", commandId: "", parentCommandId: null, agentExecutionId: null, threadId: null, turnId: null,
    toolCallId: null, artifactHash: "", depth: 0, actionStack: [],
  };
}

const optionalText = (value: unknown): string | null | false =>
  value === undefined || value === null ? null : typeof value === "string" ? value : false;
const u8 = (value: unknown): number | false =>
  value === undefined ? 0 : typeof value === "number" && Number.isInteger(value) && value >= 0 && value <= 255 ? value : false;

/**
 * `serde_json::from_value::<InvocationProvenance>`: `undefined` where serde
 * would fail (missing `channel`, `rootChannel`, `rootCommandId`, `commandId`
 * or `artifactHash`, or a value of the wrong type). Unknown fields are ignored.
 */
export function parseProvenance(value: unknown): Provenance | undefined {
  if (typeof value === "string") {
    try {
      value = JSON.parse(value);
    } catch {
      return undefined;
    }
  }
  if (typeof value !== "object" || value === null || Array.isArray(value)) return undefined;
  const v = value as Record<string, unknown>;
  const channel = v["channel"];
  const rootChannel = v["rootChannel"];
  if (typeof channel !== "string" || !CHANNELS.has(channel) || typeof rootChannel !== "string" || !CHANNELS.has(rootChannel)) return undefined;
  for (const key of ["rootCommandId", "commandId", "artifactHash"]) if (typeof v[key] !== "string") return undefined;
  const texts: Record<string, string | null> = {};
  for (const key of ["actorAccountId", "actorMemberId", "onBehalfOfMemberId", "parentCommandId", "agentExecutionId", "threadId", "turnId", "toolCallId"]) {
    const text = optionalText(v[key]);
    if (text === false) return undefined;
    texts[key] = text;
  }
  const depth = u8(v["depth"]);
  const hops = u8(v["durableHops"]);
  if (depth === false || hops === false) return undefined;
  const stack = v["actionStack"] ?? [];
  if (!Array.isArray(stack) || stack.some((entry) => typeof entry !== "string")) return undefined;
  const delegation = v["delegation"];
  if (delegation !== undefined && delegation !== null && (typeof delegation !== "object" || Array.isArray(delegation))) return undefined;
  const parsed: Provenance = {
    channel: channel as Channel, rootChannel: rootChannel as Channel,
    actorAccountId: texts["actorAccountId"]!, actorMemberId: texts["actorMemberId"]!, onBehalfOfMemberId: texts["onBehalfOfMemberId"]!,
    rootCommandId: v["rootCommandId"] as string, commandId: v["commandId"] as string, parentCommandId: texts["parentCommandId"]!,
    agentExecutionId: texts["agentExecutionId"]!, threadId: texts["threadId"]!, turnId: texts["turnId"]!, toolCallId: texts["toolCallId"]!,
    artifactHash: v["artifactHash"] as string, depth, actionStack: stack as string[],
  };
  if (hops > 0) parsed.durableHops = hops;
  if (delegation !== undefined && delegation !== null) parsed.delegation = delegation;
  return parsed;
}

/** `durable_hop(max)`: one more hop, depth back to zero; the error text when the chain is used up. */
export function durableHop(provenance: Provenance, maxHops: number): string | undefined {
  const hops = provenance.durableHops ?? 0;
  if (hops >= maxHops) return durableHopsExceeded(hops);
  provenance.durableHops = hops + 1;
  provenance.depth = 0;
  return undefined;
}

/** The session facts `direct_provenance` reads. */
export interface SessionActor {
  accountId: string;
  memberId: string;
  /** The service principal of a delegated session (`ctx.invocation.delegation`). */
  delegation?: unknown;
}

export const SYSTEM_SESSION: SessionActor = Object.freeze({ accountId: SYSTEM_ACCOUNT_ID, memberId: SYSTEM_MEMBER_ID });

const nonEmpty = (value: string): string | null => (value.trim() ? value.trim() : null);

/** RT/execution.rs direct_provenance for a durable run (channel `system`, so delegation never turns it into `api`). */
export function directProvenance(session: SessionActor, commandId: string, artifactHash: string): Provenance {
  const provenance: Provenance = {
    ...defaultProvenance(),
    channel: "system", rootChannel: "system",
    actorAccountId: nonEmpty(session.accountId), actorMemberId: nonEmpty(session.memberId),
    rootCommandId: commandId, commandId, artifactHash,
  };
  if (session.delegation !== undefined && session.delegation !== null) provenance.delegation = session.delegation;
  return provenance;
}

/**
 * A run's provenance before the runner resolves its session. `direct` means
 * the stored provenance had no root: the runner (which resolves the session)
 * calls `completeDirect` with it, as Rust builds `direct_provenance` from the
 * resolved session. For the system actor the scheduler completes it itself.
 */
export interface RunProvenance {
  provenance: Provenance;
  direct: boolean;
}

/** Fills the session's actor into a `direct` provenance (RT/execution.rs direct_provenance fields). */
export function completeDirect(run: RunProvenance, session: SessionActor): Provenance {
  if (!run.direct) return run.provenance;
  const provenance = { ...run.provenance, actorAccountId: nonEmpty(session.accountId), actorMemberId: nonEmpty(session.memberId) };
  if (session.delegation !== undefined && session.delegation !== null) provenance.delegation = session.delegation;
  return provenance;
}
