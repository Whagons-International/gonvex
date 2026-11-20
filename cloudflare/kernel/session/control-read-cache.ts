/**
 * X17l: the gateway's short cache of the Control reads a cold load repeats (lead decision 2026-11-20, the #539 model).
 *
 * `control.tenants.mine`, `control.developer.status` and `control.auth.publicSettings` each cost gateway -> Control ->
 * gateway per call or subscribe, and a cold load asks for them several times across its two sockets. A successful
 * `query.result` is kept per (project, caller, frame type, path, args) for at most `CONTROL_READ_MS`:
 *   - `controlChanged(project)` (Control's notify; coverage below) drops the project first,
 *     before the subscription refresh reads live;
 *   - every Control write that passes through this gateway drops the project before and after it, so a client always
 *     reads its own writes;
 *   - the TTL is the backstop if a notify is lost.
 * A reply is stored only if the project was not invalidated while it was in flight (per-project generation, #542
 * review): a pre-change reply landing after a notify or another socket's write is never cached after it.
 * Memory: bounded by serialized size (`CONTROL_READ_MAX_BYTES` per gateway object, an entry at most an eighth of it).
 *
 * Notify coverage (what can change the three reads): socket Control writes (`write()`), membership projections,
 * operator project members, auth mode, Google provider enable/disable and redirect removal, tenants with owner,
 * memberships, the DEV e2e member seed, and (#539) operator account disable/delete, operator tenant delete, project
 * delete, HTTP logout and refresh-token reuse. The same notice drops this gateway's identity leases (X17j), and a
 * write forwarded through this gateway or a Control restart drops both stores at once.
 * A gateway object that is not a registered watcher of the project gets no notices: one that has only served
 * `query.call`s (no Control subscribe) and had no sessions when its constructor or alarm ran. Its hits are bounded by
 * the TTL only. A watcher registration that lapsed (Control drops it after `NOTICE_ATTEMPTS` failed notices; it is
 * persisted otherwise) is redone by the next subscribe once `watchedRecently` expires (at most `CONTROL_READ_MS`)
 * or by the gateway alarm's `refreshControlWatches`.
 * Another gateway sees a change one Control alarm later (the notice is sent from Control's alarm, X17e).
 * Residual difference from Rust (which reads live): another client's change can be returned up to one notify delay
 * late, at most `CONTROL_READ_MS` on a non-watcher gateway or if a notify is lost. Impersonated sessions and
 * errors are never cached.
 */
export const CONTROL_READ_MS = 10_000;
export const CACHED_CONTROL_READS: ReadonlySet<string> = new Set(["control.tenants.mine", "control.developer.status", "control.auth.publicSettings"]);
const MAX_ENTRIES = 1024;
/** #542 review: the bound is the serialized size (a `tenants.mine` result carries every tenant's profile JSON). */
export const CONTROL_READ_MAX_BYTES = 4 * 1024 * 1024;

interface Entry { readonly project: string; readonly at: number; readonly result: unknown; readonly bytes: number }

export class ControlReadCache {
  private readonly entries = new Map<string, Entry>();
  /** #542 review: per-project generation, bumped by `invalidate`; a read started before a bump is never stored. */
  private readonly generations = new Map<string, number>();
  private bytes = 0;
  constructor(private readonly now: () => number = () => Date.now(), private readonly ttlMs = CONTROL_READ_MS, private readonly maxBytes = CONTROL_READ_MAX_BYTES) {}

  /** The project's generation; capture it before the Control read and pass it to `put`. */
  generation(project: string): number { return this.generations.get(project) ?? 0; }

  get(key: string): { result: unknown } | undefined {
    const entry = this.entries.get(key);
    if (!entry) return undefined;
    if (this.now() - entry.at >= this.ttlMs) { this.remove(key); return undefined; }
    return { result: entry.result };
  }
  /**
   * Stores `result` read under `generation` (from `generation(project)` before the read), unless the project was
   * invalidated since: a reply in flight across a notify or a write is not cached after it (#542 review).
   */
  put(key: string, project: string, result: unknown, generation: number): boolean {
    if (generation !== this.generation(project)) return false;
    let bytes: number;
    try { bytes = 2 * (key.length + JSON.stringify(result).length) + 64; } catch { return false; }
    if (bytes > this.maxBytes / 8) return false; // one entry never takes more than an eighth of the budget
    this.remove(key);
    this.entries.set(key, { project, at: this.now(), result, bytes });
    this.bytes += bytes;
    while (this.entries.size > MAX_ENTRIES || this.bytes > this.maxBytes) this.remove(this.entries.keys().next().value!);
    return true;
  }
  invalidate(project: string): void {
    this.generations.set(project, this.generation(project) + 1);
    for (const [key, entry] of this.entries) if (entry.project === project) this.remove(key);
  }
  private remove(key: string): void {
    const entry = this.entries.get(key);
    if (!entry) return;
    this.entries.delete(key);
    this.bytes -= entry.bytes;
  }
  /** Projects this gateway object registered as a Control watcher in the last `CONTROL_READ_MS` (a cached subscribe skips the repeat). */
  private readonly watched = new Map<string, number>();
  watchedRecently(project: string): boolean { const at = this.watched.get(project); return at !== undefined && this.now() - at < this.ttlMs; }
  noteWatched(project: string): void {
    this.watched.delete(project); this.watched.set(project, this.now());
    while (this.watched.size > MAX_ENTRIES) this.watched.delete(this.watched.keys().next().value!);
  }
  /** Test seam: as if the registration lapsed. */
  forgetWatched(project: string): void { this.watched.delete(project); }
  get size(): number { return this.entries.size; }
  get totalBytes(): number { return this.bytes; }
}

/**
 * One cache per gateway object (#539 review lesson): `gatedEnv` gives each gateway instance its own env object, seen by
 * both its ports and the object, so the env identifies the instance; the cache goes with it and is the one its
 * notices reach.
 */
const caches = new WeakMap<object, ControlReadCache>();
export function controlReadsFor(owner: object): ControlReadCache {
  let cache = caches.get(owner);
  if (!cache) caches.set(owner, cache = new ControlReadCache());
  return cache;
}

interface CallerView { connection: { projectId: string; identity?: { account?: { id?: string } } | null; tenant?: { route?: { tenantId?: string } } | null; impersonationId?: string } }
interface ReadFrame { type: string; id?: unknown; path?: unknown; args?: unknown }

/** The cache key of a cacheable Control read, or undefined (not cached). */
export function controlReadKey(socket: CallerView, frame: ReadFrame): string | undefined {
  if ((frame.type !== "query.call" && frame.type !== "query.subscribe") || typeof frame.path !== "string" || !CACHED_CONTROL_READS.has(frame.path)) return undefined;
  const connection = socket.connection;
  if (!connection?.projectId || connection.impersonationId) return undefined;
  let args: string;
  try { args = JSON.stringify(frame.args ?? {}); } catch { return undefined; }
  return [connection.projectId, connection.identity?.account?.id ?? "", connection.tenant?.route?.tenantId ?? "", frame.type, frame.path, args].join("\u0000");
}

/** Whether `frame` is a Control write (it drops the project's cached reads, read-your-writes). */
export function controlWrite(frame: ReadFrame): boolean { return frame.type === "action.call" || frame.type === "reducer.call"; }
