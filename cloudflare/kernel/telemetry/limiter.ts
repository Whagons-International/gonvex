/**
 * `TelemetryLimiter` (telemetry.rs:24-56): fixed 60 s windows per key; a key
 * whose window started 120 s ago or more is forgotten.
 *
 * Rust keeps one limiter per process. The kernel keeps one per object that
 * owns sockets (0012 R11: per-object counters in the object that owns the
 * socket), so an account with sockets on several gateways gets the budget on
 * each of them. It is memory only, like Rust's: a hibernated or restarted
 * gateway starts with fresh windows, as a restarted Rust process does.
 */
import { LIMITS } from "./envelope";

interface Window {
  started: number;
  count: number;
}

export class TelemetryLimiter {
  private readonly windows = new Map<string, Window>();
  private sweptAt = 0;

  constructor(private readonly windowMs: number = LIMITS.windowMs) {}

  /** One unit of `key`'s budget, if `limit` allows it. */
  allow(key: string, limit: number, now: number = Date.now()): boolean {
    // `retain(age < 120 s)`. Rust sweeps on every call; once a second is the same set of live windows.
    if (now - this.sweptAt >= 1_000 || now < this.sweptAt) {
      this.sweptAt = now;
      for (const [name, window] of this.windows) if (now - window.started >= 2 * this.windowMs) this.windows.delete(name);
    }
    let window = this.windows.get(key);
    if (!window || now - window.started >= 2 * this.windowMs) {
      window = { started: now, count: 0 };
      this.windows.set(key, window);
    }
    if (now - window.started >= this.windowMs) {
      window.started = now;
      window.count = 0;
    }
    if (window.count >= limit) return false;
    window.count++;
    return true;
  }

  /** Keys currently tracked (tests). */
  get size(): number {
    return this.windows.size;
  }
}

/** `telemetry_allowed`'s key and limit: 120 per account, else 20 per connection, per project. */
export function telemetrySubject(projectId: string, accountId: string | null, connectionId: string): { key: string; limit: number } {
  return accountId !== null
    ? { key: `${projectId}:account:${accountId}`, limit: LIMITS.accountPerWindow }
    : { key: `${projectId}:connection:${connectionId}`, limit: LIMITS.connectionPerWindow };
}
