/**
 * Presence v2 state for the scopes one tenant object owns: a port of
 * `PresenceHub` (`rust/crates/runtime/src/presence.rs` at `0bf3ad13`), with
 * the same aggregation, grace, leading edge and trailing window. It is pure:
 * no I/O, no timers, time is passed in (`now` monotonic-ish ms, `wall` Unix ms).
 *
 * Differences from Rust, all placement-driven (contract E.e.2):
 *  - A connection's subscriber entry names the gateway holding its socket,
 *    because a publication is delivered once per gateway, not into one slot
 *    per connection (`TenantPresence` keeps the per-gateway queue).
 *  - `publish` returns the publications instead of pushing slots.
 */

export type PresenceState = "active" | "idle" | "offline";

export interface PresenceMember {
  memberId: string;
  state: PresenceState;
  since: number;
}

export interface PresenceConfig {
  /** `GONVEX_PRESENCE_V2` (default on). */
  enabled: boolean;
  /** `GONVEX_PRESENCE_TICK_MS` (default 250): coalescing window. */
  tickMs: number;
  /** `GONVEX_PRESENCE_LEADING_EDGE` (default on). Off: every scope publishes on a fixed tick. */
  leadingEdge: boolean;
  /** `GONVEX_PRESENCE_OFFLINE_GRACE_MS` (default 10000, 0 allowed). */
  offlineGraceMs: number;
  /** `GONVEX_PRESENCE_LEGACY_BEAT_PATHS`. */
  legacyBeatPaths: readonly string[];
  /** `GONVEX_PRESENCE_LEGACY_TRANSITION_PATHS`. */
  legacyTransitionPaths: readonly string[];
  /** Pending publications per gateway before its subscribers are resynchronized with a snapshot (Rust: per connection). */
  maxPendingBatches: number;
}

export const DEFAULT_PRESENCE_CONFIG: PresenceConfig = Object.freeze({
  enabled: true,
  tickMs: 250,
  leadingEdge: true,
  offlineGraceMs: 10_000,
  legacyBeatPaths: [],
  legacyTransitionPaths: [],
  maxPendingBatches: 32,
});

export interface PresenceEnv {
  GONVEX_PRESENCE?: string;
  GONVEX_PRESENCE_V2?: string;
  GONVEX_PRESENCE_TICK_MS?: string;
  GONVEX_PRESENCE_LEADING_EDGE?: string;
  GONVEX_PRESENCE_OFFLINE_GRACE_MS?: string;
  GONVEX_PRESENCE_LEGACY_BEAT_PATHS?: string;
  GONVEX_PRESENCE_LEGACY_TRANSITION_PATHS?: string;
}

/** `config.rs` `presence_config`: booleans accept Rust's spellings; bad numbers keep the default. */
export function presenceConfig(env: PresenceEnv): PresenceConfig {
  const flag = (value: string | undefined, fallback: boolean) => {
    const v = value?.trim().toLowerCase();
    if (v === undefined || v === "") return fallback;
    if (["1", "true", "yes", "on"].includes(v)) return true;
    if (["0", "false", "no", "off"].includes(v)) return false;
    return fallback;
  };
  const int = (value: string | undefined, fallback: number, min: number) => {
    const n = Number(value?.trim());
    return value !== undefined && value.trim() !== "" && Number.isInteger(n) && n >= min ? n : fallback;
  };
  const list = (value: string | undefined) => (value ?? "").split(",").map(s => s.trim()).filter(Boolean);
  const d = DEFAULT_PRESENCE_CONFIG;
  // K05 advertises with GONVEX_PRESENCE ("0" turns it off); GONVEX_PRESENCE_V2 is Rust's name.
  const enabled = flag(env.GONVEX_PRESENCE_V2, d.enabled) && env.GONVEX_PRESENCE !== "0";
  return {
    enabled,
    tickMs: int(env.GONVEX_PRESENCE_TICK_MS, d.tickMs, 1),
    leadingEdge: flag(env.GONVEX_PRESENCE_LEADING_EDGE, d.leadingEdge),
    offlineGraceMs: int(env.GONVEX_PRESENCE_OFFLINE_GRACE_MS, d.offlineGraceMs, 0),
    legacyBeatPaths: list(env.GONVEX_PRESENCE_LEGACY_BEAT_PATHS),
    legacyTransitionPaths: list(env.GONVEX_PRESENCE_LEGACY_TRANSITION_PATHS),
    maxPendingBatches: d.maxPendingBatches,
  };
}
const RUST_WHITESPACE = new RegExp("^[\\t-\\r \\u0085\\u00a0\\u1680\\u2000-\\u200a\\u2028\\u2029\\u202f\\u205f\\u3000]+|[\\t-\\r \\u0085\\u00a0\\u1680\\u2000-\\u200a\\u2028\\u2029\\u202f\\u205f\\u3000]+$", "g");


/** Rust's `str::trim` (Unicode White_Space; unlike `String.prototype.trim`, U+FEFF is kept and U+0085 trimmed). */
export function rustTrim(value: string): string {
  return value.replace(RUST_WHITESPACE, "");
}

/** `parse_state`: trimmed, ASCII-lowercased; `gone` is the old spelling of `offline`. */
export function parseState(value: string): PresenceState | null {
  switch (rustTrim(value).replace(/[A-Z]/g, c => c.toLowerCase())) {
    case "active": return "active";
    case "idle": return "idle";
    case "offline":
    case "gone": return "offline";
    default: return null;
  }
}

/** Rust `String` ordering (UTF-8 bytes = code points), not UTF-16 units. */
export function compareCodePoints(a: string, b: string): number {
  if (a === b) return 0;
  const n = Math.min(a.length, b.length);
  for (let i = 0; i < n; i++) {
    const x = a.codePointAt(i)!, y = b.codePointAt(i)!;
    if (x !== y) return x < y ? -1 : 1;
    if (x > 0xffff) i++;
  }
  return a.length < b.length ? -1 : a.length > b.length ? 1 : 0;
}

/** `{"type":"presence.snapshot","id","serverTime","members"}` in serde order. */
export function encodeSnapshot(id: string, serverTime: number, members: readonly PresenceMember[]): string {
  return JSON.stringify({ type: "presence.snapshot", id, serverTime, members: members.map(memberJson) });
}

/** `{"type":"presence.patch","serverTime","changes"}` in serde order. */
export function encodePatch(serverTime: number, changes: readonly PresenceMember[]): string {
  return JSON.stringify({ type: "presence.patch", serverTime, changes: changes.map(memberJson) });
}

const memberJson = (m: PresenceMember) => ({ memberId: m.memberId, state: m.state, since: m.since });

/** Several publications merged for a connection that fell behind (`drain`): last state per Member wins, first-seen order. */
export function mergePublications(batches: readonly Publication[]): string | null {
  if (batches.length === 0) return null;
  if (batches.length === 1) return batches[0]!.encoded;
  const merged: PresenceMember[] = [];
  const index = new Map<string, number>();
  for (const batch of batches) {
    for (const change of batch.changes) {
      const at = index.get(change.memberId);
      if (at === undefined) { index.set(change.memberId, merged.length); merged.push(change); } else merged[at] = change;
    }
  }
  return encodePatch(batches.at(-1)!.serverTime, merged);
}

/** One coalesced publication of one scope, encoded once. */
export interface Publication {
  scope: string;
  serverTime: number;
  changes: PresenceMember[];
  encoded: string;
  /** Gateways with at least one subscriber of the scope at publication time. */
  gateways: string[];
}

interface Member {
  /** Connection id -> that connection's own state. */
  connections: Map<string, "active" | "idle">;
  published: { state: PresenceState; since: number } | null;
  graceUntil: number | null;
}

interface Scope {
  members: Map<string, Member>;
  /** Connection id -> gateway holding its socket. */
  subscribers: Map<string, string>;
  /** Subscribers per gateway (publication fan-out is one delivery per gateway). */
  gateways: Map<string, number>;
  dirty: Set<string>;
  lastPublished: number | null;
  due: number | null;
}

interface Registration {
  scope: string;
  gateway: string;
  member: string | null;
  subscriptionId: string | null;
  legacy: boolean;
  /** When the hub learned of the connection (a sweep only removes connections it asked about). */
  since: number;
}

/** A connection as the gateway reports it (rebuild and sweep). */
export interface ConnectionReport {
  connectionId: string;
  projectId: string;
  memberId: string;
  /** Own state; null when the connection reported none (or reported `offline`). */
  state: "active" | "idle" | null;
  /** Subscription id; null when not subscribed. */
  subscriptionId: string | null;
  /** When the socket last changed its own state (Unix ms): the restored `since` (absent: the restore time). */
  since?: number;
}

export const scopeKey = (projectId: string, tenantId: string) => `${projectId}\u0000${tenantId}`;

export class PresenceHub {
  private readonly scopes = new Map<string, Scope>();
  private readonly connections = new Map<string, Registration>();

  constructor(readonly config: PresenceConfig = DEFAULT_PRESENCE_CONFIG) {}

  isLegacyBeat(path: string): boolean {
    return this.config.enabled && this.config.legacyBeatPaths.includes(path);
  }

  isLegacyTransition(path: string): boolean {
    return this.config.enabled && this.config.legacyTransitionPaths.includes(path);
  }

  private scope(key: string): Scope {
    let scope = this.scopes.get(key);
    if (!scope) this.scopes.set(key, scope = { members: new Map(), subscribers: new Map(), gateways: new Map(), dirty: new Set(), lastPublished: null, due: null });
    return scope;
  }

  /** `Scope::schedule`: due when the Member can be published, never inside the window after the last publication. */
  private mark(key: string, member: string, now: number): void {
    const scope = this.scopes.get(key);
    if (!scope) return;
    const m = scope.members.get(member);
    const ready = m && m.connections.size === 0 && m.graceUntil !== null ? Math.max(m.graceUntil, now) : now;
    const at = this.align(scope, ready);
    if (scope.due === null || at < scope.due) scope.due = at;
    scope.dirty.add(member);
  }

  /** The window rule (leading edge), or the fixed process tick when the leading edge is off. */
  private align(scope: Scope, at: number): number {
    const window = this.config.tickMs;
    if (!this.config.leadingEdge) return Math.ceil(at / window) * window;
    return scope.lastPublished === null ? at : Math.max(at, scope.lastPublished + window);
  }

  private addSubscriber(scope: Scope, connection: string, gateway: string): void {
    const previous = scope.subscribers.get(connection);
    if (previous === gateway) return;
    if (previous !== undefined) this.dropSubscriber(scope, connection);
    scope.subscribers.set(connection, gateway);
    scope.gateways.set(gateway, (scope.gateways.get(gateway) ?? 0) + 1);
  }

  private dropSubscriber(scope: Scope, connection: string): void {
    const gateway = scope.subscribers.get(connection);
    if (gateway === undefined) return;
    scope.subscribers.delete(connection);
    const n = (scope.gateways.get(gateway) ?? 1) - 1;
    if (n <= 0) scope.gateways.delete(gateway); else scope.gateways.set(gateway, n);
  }

  /** `detach`: removes the connection's member state and its subscription. Returns the dirty (scope, member). */
  private detach(connection: string, now: number, grace: boolean): [string, string] | null {
    const registration = this.connections.get(connection);
    if (!registration) return null;
    this.connections.delete(connection);
    const scope = this.scopes.get(registration.scope);
    if (!scope) return null;
    this.dropSubscriber(scope, connection);
    if (registration.member === null) return null;
    const member = scope.members.get(registration.member);
    if (!member) return null;
    if (member.connections.delete(connection) && member.connections.size === 0) {
      member.graceUntil = grace ? now + this.config.offlineGraceMs : null;
    }
    return [registration.scope, registration.member];
  }

  private detachAndMark(connection: string, now: number, grace: boolean): void {
    const dirty = this.detach(connection, now, grace);
    if (dirty) this.mark(dirty[0], dirty[1], now);
  }

  private register(connection: string, key: string, gateway: string, legacy: boolean, now: number): Registration {
    let registration = this.connections.get(connection);
    if (!registration) this.connections.set(connection, registration = { scope: key, gateway, member: null, subscriptionId: null, legacy, since: now });
    registration.gateway = gateway;
    return registration;
  }

  /** `set_state`: this connection's own state. `offline` removes the connection at once (no grace). */
  setState(connection: string, gateway: string, key: string, member: string, state: PresenceState, legacy: boolean, now: number): void {
    const existing = this.connections.get(connection);
    if (existing && (existing.scope !== key || (existing.member !== null && existing.member !== member))) {
      this.detachAndMark(connection, now, false);
    }
    const registration = this.register(connection, key, gateway, legacy, now);
    registration.legacy = legacy;
    const memberId = registration.member ?? member;
    registration.member = memberId;
    const scope = this.scope(key);
    let entry = scope.members.get(memberId);
    if (!entry) scope.members.set(memberId, entry = { connections: new Map(), published: null, graceUntil: null });
    if (state === "offline") {
      entry.connections.delete(connection);
      if (entry.connections.size === 0) entry.graceUntil = null;
      registration.member = null;
    } else {
      entry.connections.set(connection, state);
      entry.graceUntil = null;
    }
    this.mark(key, memberId, now);
  }

  /** `legacy_beat`: registers the connection as active if it is not yet present; otherwise changes nothing. */
  legacyBeat(connection: string, gateway: string, key: string, member: string, now: number): void {
    const registration = this.connections.get(connection);
    if (registration && registration.scope === key && registration.member === member) return;
    this.setState(connection, gateway, key, member, "active", true, now);
  }

  /** `subscribe`: registers the subscriber and returns the snapshot to send first. */
  subscribe(connection: string, gateway: string, key: string, id: string, now: number, wall: number): string {
    const existing = this.connections.get(connection);
    if (existing && existing.scope !== key) this.detachAndMark(connection, now, false);
    const registration = this.register(connection, key, gateway, false, now);
    registration.subscriptionId = id;
    const scope = this.scope(key);
    this.addSubscriber(scope, connection, gateway);
    return encodeSnapshot(id, wall, this.publishedMembers(scope));
  }

  unsubscribe(connection: string): void {
    const registration = this.connections.get(connection);
    if (!registration) return;
    registration.subscriptionId = null;
    const scope = this.scopes.get(registration.scope);
    if (scope) this.dropSubscriber(scope, connection);
    if (registration.member === null) this.connections.delete(connection);
  }

  /** The connection closed or its session changed (`disconnect` / `reset`): removed with grace. */
  disconnect(connection: string, now: number): void {
    this.detachAndMark(connection, now, true);
  }

  /** Rebuild after the object lost its memory: the connection as its gateway reports it, published as is (no patch). */
  restore(report: ConnectionReport, gateway: string, key: string, now: number, wall: number): void {
    const registration = this.register(report.connectionId, key, gateway, false, now);
    const scope = this.scope(key);
    if (report.state !== null && report.memberId) {
      registration.member = report.memberId;
      let entry = scope.members.get(report.memberId);
      if (!entry) scope.members.set(report.memberId, entry = { connections: new Map(), published: null, graceUntil: null });
      entry.connections.set(report.connectionId, report.state);
      // `since` of the aggregate: the earliest state change among the connections that hold it (Rust's
      // publication time is unknown after the memory loss; this is the closest the sockets can tell).
      const aggregate = aggregateOf(entry)!;
      const at = report.state === aggregate ? Math.min(report.since ?? wall, wall) : wall;
      if (entry.published?.state !== aggregate) entry.published = { state: aggregate, since: at };
      else if (report.state === aggregate && at < entry.published.since) entry.published.since = at;
    }
    if (report.subscriptionId !== null) {
      registration.subscriptionId = report.subscriptionId;
      this.addSubscriber(scope, report.connectionId, gateway);
    }
    if (registration.member === null && registration.subscriptionId === null) this.connections.delete(report.connectionId);
  }

  /** Connections the hub believes are on `gateway`, registered before `before`. */
  connectionsOn(gateway: string, before: number): string[] {
    const out: string[] = [];
    for (const [id, r] of this.connections) if (r.gateway === gateway && r.since < before) out.push(id);
    return out;
  }

  /** Every gateway holding a registered connection. */
  gateways(): Set<string> {
    const out = new Set<string>();
    for (const r of this.connections.values()) out.add(r.gateway);
    return out;
  }

  /**
   * `run_due`: publishes every scope that is due and returns the publications
   * plus the next due time (null: nothing pending, no timer needed).
   */
  runDue(now: number, wall: number): { publications: Publication[]; next: number | null } {
    const publications: Publication[] = [];
    let next: number | null = null;
    for (const [key, scope] of this.scopes) {
      if (scope.due !== null && scope.due <= now) {
        const publication = this.publishScope(key, scope, now, wall);
        if (publication) publications.push(publication);
      }
      if (scope.due !== null && (next === null || scope.due < next)) next = scope.due;
      if (scope.members.size === 0 && scope.subscribers.size === 0) this.scopes.delete(key);
    }
    return { publications, next };
  }

  /** `tick`: publishes every pending transition now, ignoring the window (tests). */
  tick(now: number, wall: number): Publication[] {
    const out: Publication[] = [];
    for (const [key, scope] of this.scopes) {
      const publication = this.publishScope(key, scope, now, wall);
      if (publication) out.push(publication);
      if (scope.members.size === 0 && scope.subscribers.size === 0) this.scopes.delete(key);
    }
    return out;
  }

  /** `publish_scope`. */
  private publishScope(key: string, scope: Scope, now: number, wall: number): Publication | null {
    if (scope.dirty.size === 0) { scope.due = null; return null; }
    const changes: PresenceMember[] = [];
    for (const memberId of [...scope.dirty]) {
      const member = scope.members.get(memberId);
      if (!member) { scope.dirty.delete(memberId); continue; }
      const aggregate = aggregateOf(member);
      if (aggregate === null && member.graceUntil !== null && member.graceUntil > now) continue;
      const next: PresenceState = aggregate ?? "offline";
      scope.dirty.delete(memberId);
      const previous = member.published?.state ?? "offline";
      if (next !== previous) {
        changes.push({ memberId, state: next, since: wall });
        member.published = { state: next, since: wall };
      }
      if (next === "offline") scope.members.delete(memberId);
    }
    let publication: Publication | null = null;
    if (changes.length) {
      scope.lastPublished = now;
      changes.sort((a, b) => compareCodePoints(a.memberId, b.memberId));
      publication = { scope: key, serverTime: wall, changes, encoded: encodePatch(wall, changes), gateways: [...scope.gateways.keys()] };
    }
    this.reschedule(scope, now);
    return publication;
  }

  /** `Scope::reschedule`: the earliest time a still-dirty (graced) Member becomes publishable. */
  private reschedule(scope: Scope, now: number): void {
    let earliest: number | null = null;
    for (const id of scope.dirty) {
      const member = scope.members.get(id);
      if (!member) continue;
      const at = member.connections.size === 0 ? (member.graceUntil ?? now) : now;
      if (earliest === null || at < earliest) earliest = at;
    }
    scope.due = earliest === null ? null : this.align(scope, earliest);
  }

  /** Online Members of a scope, sorted by id (`published_members`). */
  published(key: string): PresenceMember[] {
    const scope = this.scopes.get(key);
    return scope ? this.publishedMembers(scope) : [];
  }

  private publishedMembers(scope: Scope): PresenceMember[] {
    const out: PresenceMember[] = [];
    for (const [memberId, m] of scope.members) {
      if (m.published && m.published.state !== "offline") out.push({ memberId, state: m.published.state, since: m.published.since });
    }
    return out.sort((a, b) => compareCodePoints(a.memberId, b.memberId));
  }

  /** Gateways with subscribers of `key`. */
  subscriberGateways(key: string): string[] {
    return [...(this.scopes.get(key)?.gateways.keys() ?? [])];
  }

  /** Scope keys with at least one subscriber on `gateway`. */
  scopesOn(gateway: string): string[] {
    const out: string[] = [];
    for (const [key, scope] of this.scopes) if (scope.gateways.has(gateway)) out.push(key);
    return out;
  }

  trackedMembers(): number {
    let n = 0;
    for (const scope of this.scopes.values()) n += scope.members.size;
    return n;
  }

  registered(connection: string): { member: string | null; subscriptionId: string | null; gateway: string } | null {
    const r = this.connections.get(connection);
    return r ? { member: r.member, subscriptionId: r.subscriptionId, gateway: r.gateway } : null;
  }
}

function aggregateOf(member: Member): "active" | "idle" | null {
  if (member.connections.size === 0) return null;
  for (const state of member.connections.values()) if (state === "active") return "active";
  return "idle";
}
