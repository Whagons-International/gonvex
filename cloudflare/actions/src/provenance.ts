// Invocation provenance of Actions (rust/crates/module-runtime/src/lib.rs
// InvocationProvenance; rust/crates/runtime/src/execution.rs
// direct_provenance, initial_action_provenance, delegated_provenance;
// action_calls.rs invoke_tool). Serialized camelCase, as Rust persists it in
// outbox rows and scheduled jobs.
import type { ActionSession } from "./session.js";
import { isAnonymous } from "./session.js";

export type Channel = "ui" | "agent" | "api" | "scheduler" | "system";

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
  /** Synchronous nesting depth (tools, `ctx.functions`); bounded by 8. */
  depth: number;
  /** Durable hops (outbox, scheduler) since the root; bounded by 32. */
  durableHops: number;
  actionStack: string[];
  /** Root execution deadline. Never exposed to modules and never persisted. */
  deadlineUnixMs: number | null;
  delegation: unknown;
}

export const MAX_DURABLE_HOPS = 32;

const nonempty = (value: string | null | undefined): string | null => {
  const trimmed = (value ?? "").trim();
  return trimmed ? trimmed : null;
};

const CHANNELS = new Set<Channel>(["ui", "agent", "api", "scheduler", "system"]);
const channelOf = (value: unknown, fallback: Channel): Channel => (CHANNELS.has(value as Channel) ? (value as Channel) : fallback);

/** `direct_provenance`: a delegated session's UI calls start on `api`. */
export function directProvenance(session: ActionSession, channel: Channel, commandId: string, artifactHash: string): Provenance {
  const delegation = session.delegation ?? null;
  const effective: Channel = channel === "ui" && delegation !== null ? "api" : channel;
  return {
    channel: effective,
    rootChannel: effective,
    actorAccountId: nonempty(session.account.id),
    actorMemberId: nonempty(session.member.id),
    onBehalfOfMemberId: null,
    rootCommandId: commandId,
    commandId,
    parentCommandId: null,
    agentExecutionId: null,
    threadId: null,
    turnId: null,
    toolCallId: null,
    artifactHash,
    depth: 0,
    durableHops: 0,
    actionStack: [],
    deadlineUnixMs: null,
    delegation,
  };
}

const agentExecutionId = (randomUUID: () => string) => `agent_${randomUUID().replaceAll("-", "")}`;

/** `initial_action_provenance`. */
export function initialActionProvenance(
  session: ActionSession,
  inherited: Provenance | undefined,
  commandId: string,
  artifactHash: string,
  agentProfile: boolean,
  randomUUID: () => string,
): Provenance {
  const provenance = inherited ? clone(inherited) : directProvenance(session, "ui", commandId, artifactHash);
  // A durable Action may resume after a deploy: it sees the artifact running now.
  provenance.artifactHash = artifactHash;
  if (agentProfile && provenance.agentExecutionId === null) {
    if (provenance.onBehalfOfMemberId === null) provenance.onBehalfOfMemberId = nonempty(session.member.id);
    provenance.agentExecutionId = agentExecutionId(randomUUID);
  }
  return provenance;
}

/** `delegated_provenance`: the child of a `ctx.functions.invoke` call. */
export function delegatedProvenance(session: ActionSession, parent: Provenance, commandId: string, artifactHash: string, randomUUID: () => string): Provenance {
  return {
    channel: "agent",
    rootChannel: parent.rootChannel,
    actorAccountId: nonempty(session.account.id),
    actorMemberId: nonempty(session.member.id),
    onBehalfOfMemberId: nonempty(session.member.id),
    rootCommandId: parent.rootCommandId.trim() ? parent.rootCommandId : parent.commandId,
    commandId,
    parentCommandId: parent.commandId,
    agentExecutionId: parent.agentExecutionId ?? agentExecutionId(randomUUID),
    threadId: parent.threadId,
    turnId: parent.turnId,
    toolCallId: parent.toolCallId,
    artifactHash,
    depth: parent.depth + 1,
    durableHops: parent.durableHops,
    actionStack: [...parent.actionStack],
    deadlineUnixMs: parent.deadlineUnixMs,
    delegation: parent.delegation,
  };
}

/** `invoke_tool`: the tool call's provenance. */
export function toolProvenance(session: ActionSession, parent: Provenance, commandId: string): Provenance {
  const provenance = clone(parent);
  provenance.parentCommandId = provenance.commandId;
  provenance.commandId = commandId;
  provenance.channel = "agent";
  provenance.depth = parent.depth + 1;
  provenance.onBehalfOfMemberId = isAnonymous(session) ? null : session.member.id;
  return provenance;
}

/** `durable_hop`: `false` when the chain already reached `max` hops. */
export function durableHop(provenance: Provenance, max = MAX_DURABLE_HOPS): boolean {
  if (provenance.durableHops >= max) return false;
  provenance.durableHops += 1;
  provenance.depth = 0;
  return true;
}

export const hopsExceeded = (hops: number) => `durable work chain exceeded the maximum of ${hops} scheduled or outbox hops`;

/** `public_info`: what `ctx.invocation` shows (every key, `null` when absent). */
export function publicInfo(provenance: Provenance): Record<string, unknown> {
  return {
    channel: provenance.channel,
    rootChannel: provenance.rootChannel,
    actorAccountId: provenance.actorAccountId,
    actorMemberId: provenance.actorMemberId,
    onBehalfOfMemberId: provenance.onBehalfOfMemberId,
    rootCommandId: provenance.rootCommandId,
    commandId: provenance.commandId,
    parentCommandId: provenance.parentCommandId,
    agentExecutionId: provenance.agentExecutionId,
    threadId: provenance.threadId,
    turnId: provenance.turnId,
    toolCallId: provenance.toolCallId,
    artifactHash: provenance.artifactHash,
    delegation: provenance.delegation ?? null,
  };
}

/** The persisted form (outbox rows, scheduled jobs): no deadline, hops only when non-zero. */
export function persisted(provenance: Provenance): Record<string, unknown> {
  const { deadlineUnixMs: _deadline, durableHops, delegation, ...rest } = provenance;
  return { ...rest, ...(durableHops ? { durableHops } : {}), ...(delegation !== null && delegation !== undefined ? { delegation } : {}) };
}

/**
 * Decodes a persisted provenance. Rust's serde rejects a row without the
 * required fields, and the outbox then starts a fresh provenance; `undefined`
 * here means the same.
 */
export function parseProvenance(value: unknown): Provenance | undefined {
  let raw = value;
  if (typeof raw === "string") {
    try { raw = JSON.parse(raw); } catch { return undefined; }
  }
  if (raw === null || typeof raw !== "object" || Array.isArray(raw)) return undefined;
  const r = raw as Record<string, unknown>;
  if (typeof r.commandId !== "string" || typeof r.rootCommandId !== "string" || !CHANNELS.has(r.channel as Channel)) return undefined;
  const optional = (name: string) => (typeof r[name] === "string" ? (r[name] as string) : null);
  return {
    channel: r.channel as Channel,
    rootChannel: channelOf(r.rootChannel, r.channel as Channel),
    actorAccountId: optional("actorAccountId"),
    actorMemberId: optional("actorMemberId"),
    onBehalfOfMemberId: optional("onBehalfOfMemberId"),
    rootCommandId: r.rootCommandId,
    commandId: r.commandId,
    parentCommandId: optional("parentCommandId"),
    agentExecutionId: optional("agentExecutionId"),
    threadId: optional("threadId"),
    turnId: optional("turnId"),
    toolCallId: optional("toolCallId"),
    artifactHash: typeof r.artifactHash === "string" ? r.artifactHash : "",
    depth: Number.isSafeInteger(r.depth) ? (r.depth as number) : 0,
    durableHops: Number.isSafeInteger(r.durableHops) ? (r.durableHops as number) : 0,
    actionStack: Array.isArray(r.actionStack) ? r.actionStack.filter((item): item is string => typeof item === "string") : [],
    deadlineUnixMs: null,
    delegation: r.delegation ?? null,
  };
}

export function clone(provenance: Provenance): Provenance {
  return { ...provenance, actionStack: [...provenance.actionStack] };
}
