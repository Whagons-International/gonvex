import type { ClientMessage, PresenceMember, PresenceReportedState, ServerMessage } from "@gonvex/protocol";

/**
 * Presence v2 client state.
 *
 * The runtime keeps presence in memory and publishes only Member transitions.
 * This feed:
 * - reports the connection's own state with `presence.set` when it changes
 *   and again after every `auth.result` (never periodically: the socket is
 *   the liveness signal);
 * - while at least one listener exists, sends `presence.subscribe` after every
 *   `auth.result` and keeps a map of online Members from `presence.snapshot`
 *   (replace) and `presence.patch` (merge; `offline` removes).
 *
 * It does nothing unless the runtime advertised `capabilities.presence === 1`.
 */
export type PresenceFeedSnapshot = {
  /** The connected runtime implements presence v2. */
  readonly supported: boolean;
  /** A snapshot for the current session has been applied. */
  readonly ready: boolean;
  /** Online Members (active or idle) keyed by Member id. */
  readonly members: ReadonlyMap<string, PresenceMember>;
};

export type PresenceFeedTransport = {
  supported(): boolean;
  /** True when the socket is open and the current session is authenticated. */
  canSend(): boolean;
  send(message: ClientMessage): void;
};

const EMPTY: ReadonlyMap<string, PresenceMember> = new Map();

export class PresenceFeed {
  private reported: PresenceReportedState | undefined;
  private listeners = new Set<() => void>();
  private members: ReadonlyMap<string, PresenceMember> = EMPTY;
  private subscriptionId: string | undefined;
  private subscriptionCounter = 0;
  private ready = false;
  private snapshotValue: PresenceFeedSnapshot = { supported: false, ready: false, members: EMPTY };

  constructor(private readonly transport: PresenceFeedTransport) {}

  /** Records and (when possible) sends this connection's own state. */
  set(state: PresenceReportedState): void {
    if (this.reported === state) return;
    this.reported = state;
    if (this.transport.supported() && this.transport.canSend()) {
      this.transport.send({ type: "presence.set", state });
    }
  }

  reportedState(): PresenceReportedState | undefined {
    return this.reported;
  }

  subscribe(listener: () => void): () => void {
    this.listeners.add(listener);
    if (this.listeners.size === 1) this.sendSubscribe();
    return () => {
      if (!this.listeners.delete(listener) || this.listeners.size > 0) return;
      if (this.subscriptionId && this.transport.supported() && this.transport.canSend()) {
        this.transport.send({ type: "presence.unsubscribe" });
      }
      this.subscriptionId = undefined;
      this.ready = false;
      this.members = EMPTY;
      this.publish(false);
    };
  }

  snapshot(): PresenceFeedSnapshot {
    const supported = this.transport.supported();
    if (this.snapshotValue.supported !== supported) this.publish(false);
    return this.snapshotValue;
  }

  /** The session authenticated (or re-authenticated) on an open socket. */
  authenticated(): void {
    this.ready = false;
    this.subscriptionId = undefined;
    if (!this.transport.supported()) {
      this.publish();
      return;
    }
    if (this.reported) this.transport.send({ type: "presence.set", state: this.reported });
    if (this.listeners.size > 0) this.sendSubscribe();
    this.publish();
  }

  /** The socket closed or the session ended; the server drops our state. */
  disconnected(): void {
    this.prepareDisconnect()?.();
  }

  /** Invalidate before other disconnect observers can replace the session. */
  prepareDisconnect(): (() => void) | undefined {
    this.subscriptionId = undefined;
    if (!this.ready) return;
    // Keep the last known Members until the next snapshot replaces them,
    // but report that the view is no longer live.
    this.ready = false;
    this.publish(false);
    return () => this.notify();
  }

  /** Returns true when the frame was a presence frame. */
  handle(message: ServerMessage): boolean {
    switch (message.type) {
      case "presence.snapshot": {
        if (message.id !== this.subscriptionId) return true;
        const members = new Map<string, PresenceMember>();
        for (const member of message.members) {
          if (member.state !== "offline") members.set(member.memberId, member);
        }
        this.members = members;
        this.ready = true;
        this.publish();
        return true;
      }
      case "presence.patch": {
        if (!this.subscriptionId || !this.ready || message.changes.length === 0) return true;
        const members = new Map(this.members);
        for (const change of message.changes) {
          if (change.state === "offline") members.delete(change.memberId);
          else members.set(change.memberId, change);
        }
        this.members = members;
        this.publish();
        return true;
      }
      case "presence.error":
        if (message.id === this.subscriptionId) {
          this.subscriptionId = undefined;
          this.ready = false;
          this.publish();
        }
        return true;
      default:
        return false;
    }
  }

  private sendSubscribe() {
    if (!this.transport.supported() || !this.transport.canSend()) return;
    this.subscriptionCounter += 1;
    this.subscriptionId = `presence-${this.subscriptionCounter}`;
    this.transport.send({ type: "presence.subscribe", id: this.subscriptionId });
  }

  private publish(notify = true) {
    this.snapshotValue = { supported: this.transport.supported(), ready: this.ready, members: this.members };
    if (notify) this.notify();
  }

  private notify(): void {
    for (const listener of Array.from(this.listeners)) listener();
  }
}
