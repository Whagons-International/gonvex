// One alarm per tenant object (CONTRACT E.timers, 0016 G0-9). Every periodic
// duty of the tenant object is an `AlarmSource`: it says when it next needs
// the object and does its work when the alarm fires. The alarm is set to the
// minimum over the sources, so an idle tenant with no due work is never woken.
//
// The scheduler registers two sources (jobs and the action outbox; retention
// is a third). Other packages register theirs on the same `TenantAlarm`:
// change-log pruning (K03 `pruneDue`), receipt expiry (K07, 0009), the
// membership projection (K06 tenant port), presence grace (K14), support
// last-seen flushes. Durable Object alarms are at-least-once, so every source
// must be idempotent (claims, revision guards and conditional deletes are).

/** Where the alarm lives: `ctx.storage` in a tenant object, a fake in a simulation. */
export interface AlarmPort {
  getAlarm(): Promise<number | null>;
  setAlarm(scheduledTime: number): Promise<void>;
  deleteAlarm(): Promise<void>;
}

/** What a source may spend in one alarm event. */
export interface AlarmBudget {
  /** The alarm's start time (the clock when the event began). */
  readonly startedAt: number;
  /** Stop starting new work after this many units (runs, deliveries, pages); finish what is running. */
  readonly maxUnits: number;
}

export interface AlarmSource {
  readonly name: string;
  /** When this source next needs the object (Unix ms), or null. Synchronous: one indexed probe or memory. */
  nextAt(now: number): number | null;
  /** Does the due work. Called only when `nextAt(now) <= now`. Returns how many units it did. */
  run(now: number, budget: AlarmBudget): Promise<number> | number;
}

export interface SourceReport {
  name: string;
  units: number;
  error?: string;
}

export interface AlarmReport {
  firedAt: number;
  sources: SourceReport[];
  /** The alarm set afterwards, or null when nothing is pending. */
  next: number | null;
}

export interface TenantAlarmOptions {
  clock?: () => number;
  /** Units per source per event (default 16, Rust's CLAIM_BATCH). */
  maxUnits?: number;
  /** Delay before a source that threw is tried again; doubles per failure up to `maxRetryMs`. */
  retryMs?: number;
  maxRetryMs?: number;
  onError?: (source: string, error: unknown) => void;
}

export class TenantAlarm {
  private readonly sources: AlarmSource[] = [];
  /** The alarm this instance last set (cached; `undefined` until read from the port). */
  private armed: number | null | undefined;
  private readonly failures = new Map<string, { count: number; until: number }>();
  private firing = false;
  private readonly clock: () => number;
  private readonly maxUnits: number;
  private readonly retryMs: number;
  private readonly maxRetryMs: number;
  private readonly onError: (source: string, error: unknown) => void;
  /** Alarm events and settings since this instance started (for the numbers in the README). */
  readonly stats = { fired: 0, set: 0, deleted: 0 };

  constructor(private readonly port: AlarmPort, options: TenantAlarmOptions = {}) {
    this.clock = options.clock ?? (() => Date.now());
    this.maxUnits = options.maxUnits ?? 16;
    this.retryMs = options.retryMs ?? 1_000;
    this.maxRetryMs = options.maxRetryMs ?? 60_000;
    this.onError = options.onError ?? ((source, error) => console.error(`tenant alarm source ${source} failed`, error));
  }

  register(source: AlarmSource): void {
    if (this.sources.some((existing) => existing.name === source.name)) throw new Error(`alarm source ${source.name} is already registered`);
    this.sources.push(source);
  }

  /** The earliest time any source needs the object. */
  nextDue(now = this.clock()): number | null {
    let next: number | null = null;
    for (const source of this.sources) {
      let at = source.nextAt(now);
      const failure = this.failures.get(source.name);
      if (at !== null && failure && at < failure.until) at = failure.until;
      if (at !== null && (next === null || at < next)) next = at;
    }
    return next;
  }

  /**
   * Moves the alarm earlier when a source has earlier work (call after a
   * commit that queued work). Never moves it later: a later alarm fires,
   * finds nothing due and re-arms. Cheap when nothing changed.
   */
  async arm(): Promise<number | null> {
    if (this.firing) return this.armed ?? null;
    const next = this.nextDue();
    if (next === null) return this.armed ?? null;
    if (this.armed === undefined) this.armed = await this.port.getAlarm();
    if (this.armed === null || next < this.armed) await this.set(next);
    return this.armed;
  }

  /** The Durable Object `alarm()` handler: runs every due source, then sets the alarm to the new minimum. */
  async fire(): Promise<AlarmReport> {
    if (this.firing) throw new Error("the tenant alarm is already firing");
    this.firing = true;
    this.stats.fired++;
    // The platform cleared the alarm that fired.
    this.armed = null;
    const firedAt = this.clock();
    const reports: SourceReport[] = [];
    try {
      for (const source of this.sources) {
        const now = this.clock();
        const failure = this.failures.get(source.name);
        if (failure && failure.until > now) continue;
        const at = source.nextAt(now);
        if (at === null || at > now) continue;
        try {
          const units = await source.run(now, { startedAt: firedAt, maxUnits: this.maxUnits });
          this.failures.delete(source.name);
          reports.push({ name: source.name, units });
        } catch (error) {
          const count = (failure?.count ?? 0) + 1;
          const delay = Math.min(this.retryMs * 2 ** (count - 1), this.maxRetryMs);
          this.failures.set(source.name, { count, until: this.clock() + delay });
          reports.push({ name: source.name, units: 0, error: error instanceof Error ? error.message : String(error) });
          this.onError(source.name, error);
        }
      }
    } finally {
      this.firing = false;
    }
    let next = this.nextDue();
    // A source that says it is due but did nothing must not spin the object: try again a little later.
    const now = this.clock();
    if (next !== null && next <= now && reports.every((report) => report.units === 0)) next = now + this.retryMs;
    if (next === null) {
      await this.port.deleteAlarm();
      this.stats.deleted++;
      this.armed = null;
    } else {
      await this.set(next);
    }
    return { firedAt, sources: reports, next };
  }

  private async set(at: number): Promise<void> {
    await this.port.setAlarm(at);
    this.stats.set++;
    this.armed = at;
  }
}

/** `ctx.storage` as an AlarmPort. */
export function storageAlarm(storage: DurableObjectStorage): AlarmPort {
  return {
    getAlarm: () => storage.getAlarm(),
    setAlarm: (at) => storage.setAlarm(at),
    deleteAlarm: () => storage.deleteAlarm(),
  };
}
