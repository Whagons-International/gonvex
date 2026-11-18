// BUG-2 / BUG-9: a tenant object that holds subscriptions must not hibernate while any socket holds one.
//
// Subscriptions live in the tenant object's memory (README "Tenant restarts"), and the delivery
// `instance` is new on every construction. The platform hibernates a Durable Object after 10 s with no
// incoming request when nothing is pending (no timer, no I/O, no outbound connection), and hibernation
// discards that memory. A tenant has no sockets of its own (they live in gateways), so it hibernated in
// a session's pauses and its next event reached every socket as `replica.reset change-feed-reset` for
// every open replica (dev 2026-11-17 21:42-21:56, BUG-2).
//
// The first fix (#449) held the tenant for 120 s after its last event. That still let an idle session
// lose its subscriptions: on -04 (2026-11-18 03:58:59Z) both clients sat idle past the grace, the tenant
// hibernated, and the next event reset both. Rust's tenant never drops subscriptions while clients are
// connected, so the lead's decision (BUG-9) is: no idle expiry. While the engine holds a subscription,
// pending open, initial or live query, one timer stays pending (re-armed every `checkMs`), which keeps
// the object from hibernating. It is released only when the last subscription goes. The cost, DO
// duration while users are connected, is accepted.

/** How often the held timer re-checks that a subscription still exists. Not an expiry. */
export const KEEPALIVE_CHECK_MS = 60_000;

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

  /** `checkMs` <= 0 disables the keep-alive (an operator escape hatch only). */
  constructor(
    private readonly active: () => boolean,
    private readonly checkMs = KEEPALIVE_CHECK_MS,
    private readonly clock: KeepAliveClock = platformClock,
  ) {}

  /** Subscription state may have changed: hold the tenant while any subscription exists, else let it go. */
  touch(): void {
    if (this.checkMs <= 0 || !this.active()) return this.release();
    if (this.timer === undefined) this.arm();
  }

  /** True while a timer keeps the object from hibernating. */
  get holding(): boolean { return this.timer !== undefined; }

  release(): void {
    if (this.timer !== undefined) this.clock.clear(this.timer);
    this.timer = undefined;
  }

  private arm(): void {
    this.timer = this.clock.set(() => {
      this.timer = undefined;
      if (this.active()) this.arm();
    }, this.checkMs);
  }
}

/** `GONVEX_TENANT_KEEPALIVE_MS`: the re-check period in ms; 0 disables; unset or invalid: the default. */
export function keepAliveCheck(value: string | undefined): number {
  if (value === undefined || value === "") return KEEPALIVE_CHECK_MS;
  const n = Number(value);
  return Number.isInteger(n) && n >= 0 && n <= 10 * 60_000 ? n : KEEPALIVE_CHECK_MS;
}
