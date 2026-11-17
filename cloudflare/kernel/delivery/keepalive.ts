// BUG-2: a tenant object that holds subscriptions must not hibernate between a
// session's ordinary events.
//
// Subscriptions live in the tenant object's memory (README "Tenant restarts"), and
// the delivery `instance` is new on every construction. The platform hibernates a
// Durable Object after 10 s with no incoming request when nothing is pending (no
// timer, no I/O, no outbound connection), and hibernation discards that memory.
// A tenant has no sockets of its own (they live in gateways), so during a
// two-client session it hibernated in ordinary pauses of 6-18 s. Its next event
// rebuilt the engine with a new instance and no subscriptions, and each socket's
// next delivery came back as `replica.reset change-feed-reset` for every open
// replica instead of a batch (dev walkthrough 2026-11-17 21:42-21:56: 13 bursts per
// client, each after an idle gap that ended in a 100-470 ms CPU cold start).
//
// While the engine holds subscriptions, one timer stays pending; activity pushes
// its deadline out. A pending timer makes the object non-hibernatable, so a session
// with an event at least every `graceMs` keeps its subscriptions. After `graceMs`
// with no activity the timer lapses and the object can hibernate (and then resets
// as before). The cost is duration billing for at most `graceMs` past the last
// event of a tenant that still has subscribers; a tenant without subscribers never
// arms it.

/** Idle time after the last event before a subscribed tenant may hibernate. */
export const KEEPALIVE_GRACE_MS = 120_000;

export interface KeepAliveClock {
  now(): number;
  set(fn: () => void, ms: number): unknown;
  clear(handle: unknown): void;
}

const platformClock: KeepAliveClock = {
  now: () => Date.now(),
  set: (fn, ms) => setTimeout(fn, ms),
  clear: (handle) => clearTimeout(handle as ReturnType<typeof setTimeout>),
};

export class SubscriberKeepAlive {
  private timer: unknown;
  private until = 0;

  constructor(
    private readonly active: () => boolean,
    private readonly graceMs = KEEPALIVE_GRACE_MS,
    private readonly clock: KeepAliveClock = platformClock,
  ) {}

  /** An event reached the tenant: hold it for `graceMs` more while it has subscriptions. */
  touch(): void {
    if (this.graceMs <= 0 || !this.active()) return this.release();
    this.until = this.clock.now() + this.graceMs;
    if (this.timer === undefined) this.arm(this.graceMs);
  }

  /** True while a timer keeps the object from hibernating. */
  get holding(): boolean { return this.timer !== undefined; }

  release(): void {
    if (this.timer !== undefined) this.clock.clear(this.timer);
    this.timer = undefined;
  }

  private arm(ms: number): void {
    this.timer = this.clock.set(() => {
      this.timer = undefined;
      const rest = this.until - this.clock.now();
      if (rest > 0 && this.active()) this.arm(rest);
    }, ms);
  }
}

/** `GONVEX_TENANT_KEEPALIVE_MS`: grace in ms; 0 disables; unset or invalid: the default. */
export function keepAliveGrace(value: string | undefined): number {
  if (value === undefined || value === "") return KEEPALIVE_GRACE_MS;
  const n = Number(value);
  return Number.isInteger(n) && n >= 0 && n <= 15 * 60_000 ? n : KEEPALIVE_GRACE_MS;
}
