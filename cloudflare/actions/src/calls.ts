// Direct agent `action.call`s in the tenant object: each becomes a run in
// `_gonvex_st_action_runs`, run by a Workflow (ActionWorkflow) with the lease
// protocol S4 proved, and its result is delivered once through `onFinished`
// for K05/K07 to answer the socket.
//
// One claimer per table (0037, as 0022 §1): K11's scheduler owns
// `_gonvex_action_outbox`; outbox and scheduled Actions reach K12 only through
// K11's hand-off (handoff.ts). This file never reads or writes that table.
//
// The protocol (S4, cloudflare/spikes/s4/src/tenant.ts):
// - a claim sets `status='processing'`, `locked_at=now`, `attempts+1`; the
//   attempt fences every later renewal and completion; the owner that already
//   holds the run (a retried Workflow step) takes it over at once, any other
//   owner waits until the lease has expired;
// - a renewal extends the lease in memory and writes `locked_at` only when
//   less than half of the persisted lease is left. The persisted `locked_at`
//   alone bounds recovery, so an alarm that wakes a fresh instance still sees
//   the lease expire on time;
// - completion is fenced on the attempt and applied in one transaction; a
//   replay is `duplicate`, another attempt's completion is `rejected`;
// - a direct call is answered once and not retried, as Rust answers the
//   socket once; only a runner that died (its lease expired, or the step
//   crashed) is run again, at most `maxAttempts` times, then the call fails;
// - the sweep (an alarm source on K11's `TenantAlarm`) hands a run off again
//   when its hand-off was not claimed within one lease or its lease expired,
//   and prunes old receipts.
import { sha256Hex } from "../../kernel/capture/digest";
import type { ActionSession } from "./session.js";
import type { Claim, ClaimRequest, CompleteRequest, Completion, Renewal, RenewRequest } from "./ports.js";

export const LEASE_MS = 300_000;
export const RENEW_MS = 30_000;
/** IB2's cap (0001), applied to runners that died: a call whose runner died this often fails. */
export const MAX_ATTEMPTS = 8;
/** Receipts of applied completions outlive every engine retry of the step that applied them. */
export const RECEIPT_RETENTION_MS = 7 * 24 * 3600_000;
/** A result is kept for a duplicate completion only up to this size (a DO row holds 2 MB). */
export const MAX_STORED_RESULT_BYTES = 1 << 20;
const INPUTS = "_gonvex_st_action_inputs";
const INPUT_CHUNKS = "_gonvex_st_action_input_chunks";
const INLINE_INPUT_BYTES = 1 << 20;
const REPLIES = "_gonvex_st_action_replies";
const REPLY_CHUNKS = "_gonvex_st_action_reply_chunks";
const REPLY_CHUNK_BYTES = 64 * 1024;
const PAGE = 100;
const DISPATCH_CONCURRENCY = 16;
const MAX_REPLY_FAILURES = 8;

export const RUNS = "_gonvex_st_action_runs";

export const RUNS_SQL = `CREATE TABLE IF NOT EXISTS _gonvex_st_action_runs (
  id TEXT PRIMARY KEY,
  state TEXT NOT NULL DEFAULT 'live' CHECK (state IN ('live', 'done', 'dead')),
  status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'processing')),
  attempts INTEGER NOT NULL DEFAULT 0,
  owner TEXT,
  locked_at INTEGER,
  dispatched_at INTEGER,
  dispatch_count INTEGER NOT NULL DEFAULT 0,
  action_path TEXT NOT NULL,
  args TEXT NOT NULL,
  session TEXT,
  provenance TEXT,
  created_at INTEGER NOT NULL,
  finished_at INTEGER,
  last_error TEXT,
  result TEXT,
  result_bytes INTEGER
);
CREATE INDEX IF NOT EXISTS _gonvex_st_action_runs_live ON _gonvex_st_action_runs (id) WHERE state = 'live';
CREATE INDEX IF NOT EXISTS _gonvex_st_action_runs_finished ON _gonvex_st_action_runs (finished_at) WHERE state <> 'live';
CREATE TABLE IF NOT EXISTS _gonvex_st_action_inputs (id TEXT PRIMARY KEY, generation TEXT NOT NULL, bytes INTEGER NOT NULL, chunks INTEGER NOT NULL, hash TEXT NOT NULL, blocked TEXT);
CREATE TABLE IF NOT EXISTS _gonvex_st_action_input_chunks (id TEXT NOT NULL, generation TEXT NOT NULL, part INTEGER NOT NULL, body BLOB NOT NULL, PRIMARY KEY (id,generation,part));
CREATE TABLE IF NOT EXISTS _gonvex_st_action_replies (id TEXT PRIMARY KEY, next_at INTEGER NOT NULL, generation TEXT NOT NULL, bytes INTEGER NOT NULL, chunks INTEGER NOT NULL, hash TEXT NOT NULL, failures INTEGER NOT NULL DEFAULT 0, blocked TEXT);
CREATE INDEX IF NOT EXISTS _gonvex_st_action_replies_due ON _gonvex_st_action_replies (next_at, id);
CREATE TABLE IF NOT EXISTS _gonvex_st_action_reply_chunks (id TEXT NOT NULL, part INTEGER NOT NULL, body BLOB NOT NULL, PRIMARY KEY (id, part));`;

/**
 * Kernel objects K12 adds to a tenant, for A01's `addedObjects()` (0037): P03
 * export and M02 leave them out, as for K11's `SCHEDULER_OBJECTS`.
 */
export const ACTION_OBJECTS: readonly (readonly ["table" | "index", string])[] = [
  ["table", RUNS],
  ["table", INPUTS],
  ["table", INPUT_CHUNKS],
  ["table", REPLIES],
  ["table", REPLY_CHUNKS],
  ["index", "_gonvex_st_action_replies_due"],
  ["index", "_gonvex_st_action_runs_live"],
  ["index", "_gonvex_st_action_runs_finished"],
];

/** What the tenant hands to a runner: one Workflow instance per (run, hand-off number). */
export interface CallJob { v: 1; kind: "call"; projectId: string; tenantId: string; id: string; n: number }

export interface Dispatcher {
  /** Starts a runner for `job`. Idempotent per (id, n): a second create of the same instance is not an error. */
  dispatch(job: CallJob): Promise<void>;
}

/** A direct call's outcome, delivered once (K05/K07 turn it into `action.result` / `action.error`). */
export interface CallFinished { reply?: { host: string; connectionId: string; epoch: number; id: string; path: string; generation?: number; tenantId: string; traceJson?: string }; committedRevision?: number | undefined; id: string; ok: boolean; value?: string | undefined; error?: string | undefined }

export interface ActionCallsOptions {
  storage: DurableObjectStorage;
  projectId: string;
  tenantId: string;
  dispatcher: Dispatcher;
  /** Re-arms the object's alarm after work was queued (K11's `TenantAlarm.arm`); the alarm runs `alarmSource()`. */
  wake: () => void | Promise<void>;
  onFinished?: ((finished: CallFinished) => void | Promise<void>) | undefined;
  clock?: (() => number) | undefined;
  randomUUID?: (() => string) | undefined;
  leaseMs?: number | undefined;
  renewMs?: number | undefined;
  maxAttempts?: number | undefined;
}

interface Run {
  id: string;
  state: "live" | "done" | "dead";
  status: "pending" | "processing";
  attempts: number;
  owner: string | null;
  locked_at: number | null;
  dispatched_at: number | null;
  dispatch_count: number;
  action_path: string;
  args: string;
  session: string | null;
  provenance: string | null;
  created_at: number;
  last_error: string | null;
}

/** Counters for the PR's numbers and the tests (rows as SQLite counts them, index entries included). */
export interface CallStats { statements: number; rowsRead: number; rowsWritten: number; dispatches: number }

/** K11's `AlarmSource` shape (kernel/scheduler/alarm.ts), structurally. */
export interface CallsAlarmSource {
  readonly name: string;
  nextAt(now: number): number | null;
  run(now: number, budget: { readonly startedAt: number; readonly maxUnits: number }): Promise<number>;
}

export class ActionCalls {
  private readonly sql: SqlStorage;
  private readonly clock: () => number;
  private readonly randomUUID: () => string;
  readonly leaseMs: number;
  readonly renewMs: number;
  readonly maxAttempts: number;
  /** Leases extended by renewals since `locked_at` was last written. Never needed for recovery. */
  private readonly leases = new Map<string, number>();
  /** A hand-off that failed is tried again after one renewal interval, not in a tight alarm loop. */
  private readonly retryAt = new Map<string, number>();
  /** Coalesce concurrent Workflow/sweep redelivery, never re-execute the Action. */
  private readonly replying = new Map<string, Promise<void>>();
  readonly stats: CallStats = { statements: 0, rowsRead: 0, rowsWritten: 0, dispatches: 0 };

  constructor(private readonly options: ActionCallsOptions) {
    this.sql = options.storage.sql;
    this.clock = options.clock ?? (() => Date.now());
    this.randomUUID = options.randomUUID ?? (() => crypto.randomUUID());
    this.leaseMs = options.leaseMs ?? LEASE_MS;
    this.renewMs = options.renewMs ?? RENEW_MS;
    this.maxAttempts = options.maxAttempts ?? MAX_ATTEMPTS;
    this.sql.exec(RUNS_SQL);
  }

  private exec<T extends Record<string, SqlStorageValue>>(statement: string, ...bindings: unknown[]): T[] {
    this.stats.statements++;
    const cursor = this.sql.exec<T>(statement, ...bindings);
    const rows = cursor.toArray();
    this.stats.rowsRead += cursor.rowsRead;
    this.stats.rowsWritten += cursor.rowsWritten;
    return rows;
  }

  private load(id: string): Run | undefined {
    return this.exec(`SELECT * FROM ${RUNS} WHERE id = ?`, id)[0] as unknown as Run | undefined;
  }

  /** The later of the persisted lease (`locked_at` + one lease) and one extended in memory since. */
  leaseExpiry(run: Pick<Run, "id" | "locked_at">): number {
    return Math.max(this.leases.get(run.id) ?? 0, (run.locked_at ?? 0) + this.leaseMs);
  }

  /**
   * Hands runs to runners: creates run concurrently (16 at a time; S4
   * measured 740 ms p50 per Workflow `create()`), then one statement records
   * every hand-off that succeeded. A failed create is left for the sweep.
   */
  private async dispatchMany(runs: ReadonlyArray<Pick<Run, "id" | "dispatch_count">>): Promise<Set<string>> {
    const done: Array<{ id: string; n: number }> = [];
    for (let start = 0; start < runs.length; start += DISPATCH_CONCURRENCY) {
      await Promise.all(runs.slice(start, start + DISPATCH_CONCURRENCY).map(async (run) => {
        const n = run.dispatch_count + 1;
        try {
          await this.options.dispatcher.dispatch({ v: 1, kind: "call", projectId: this.options.projectId, tenantId: this.options.tenantId, id: run.id, n });
          done.push({ id: run.id, n });
          this.retryAt.delete(run.id);
        } catch {
          // The sweep tries again after one renewal interval.
          this.retryAt.set(run.id, this.clock() + this.renewMs);
        }
      }));
    }
    if (done.length) {
      this.stats.dispatches += done.length;
      this.exec(
        `UPDATE ${RUNS} SET dispatched_at = ?, dispatch_count = json_extract(j.value, '$.n')
         FROM json_each(?) AS j WHERE ${RUNS}.id = json_extract(j.value, '$.id')`,
        this.clock(), JSON.stringify(done),
      );
    }
    return new Set(done.map((run) => run.id));
  }

  /** Starts a direct agent call. Large exact inputs are private binary chunks,
   * atomically published with the run; the inline representation stays readable. */
  async begin(input: { session: ActionSession; path: string; args: unknown; provenance?: unknown }): Promise<string> {
    const id = `call-${this.randomUUID()}`;
    const value = { path:input.path, args:input.args??null, session:input.session, provenance:input.provenance??null };
    const text=JSON.stringify(value), bytes=new TextEncoder().encode(text);
    const chunked=bytes.byteLength>INLINE_INPUT_BYTES;
    this.options.storage.transactionSync(()=>{
      this.exec(`INSERT INTO ${RUNS} (id, action_path, args, session, provenance, created_at) VALUES (?, ?, ?, ?, ?, ?)`,
        id,input.path,chunked?"null":JSON.stringify(value.args),chunked?null:JSON.stringify(value.session),chunked||value.provenance===null?null:JSON.stringify(value.provenance),this.clock());
      if(chunked){
        const generation=crypto.randomUUID(),chunks=Math.ceil(bytes.byteLength/REPLY_CHUNK_BYTES);
        this.exec(`INSERT INTO ${INPUTS} (id,generation,bytes,chunks,hash) VALUES (?,?,?,?,?)`,id,generation,bytes.byteLength,chunks,sha256Hex(id+"\0"+generation+"\0"+text));
        for(let part=0;part<chunks;part++)this.exec(`INSERT INTO ${INPUT_CHUNKS} (id,generation,part,body) VALUES (?,?,?,?)`,id,generation,part,bytes.slice(part*REPLY_CHUNK_BYTES,(part+1)*REPLY_CHUNK_BYTES).buffer);
      }
    });
    await this.dispatchMany([{ id, dispatch_count: 0 }]);
    await this.options.wake();
    return id;
  }

  private inputs(run:Run):{path:string;args:unknown;session:ActionSession;provenance:unknown}{
    const manifest=this.exec<{generation:string;bytes:number;chunks:number;hash:string;blocked:string|null}>(`SELECT generation,bytes,chunks,hash,blocked FROM ${INPUTS} WHERE id=?`,run.id)[0];
    let value:{path:string;args:unknown;session:ActionSession;provenance:unknown};
    try{
      if(manifest){
        if(manifest.blocked)throw new Error("Action input integrity requires operator recovery");
        if(!Number.isSafeInteger(manifest.bytes)||manifest.bytes<=0||!Number.isSafeInteger(manifest.chunks)||manifest.chunks!==Math.ceil(manifest.bytes/REPLY_CHUNK_BYTES)||!/^[a-f0-9]{64}$/.test(manifest.hash)||!manifest.generation)throw new Error("Action input manifest is corrupt");
        const decoder=new TextDecoder("utf-8",{fatal:true,ignoreBOM:false});let text="",after=-1,count=0,bytes=0;
        for(;;){
          const page=this.exec<{part:number;body:ArrayBuffer}>(`SELECT part,body FROM ${INPUT_CHUNKS} WHERE id=? AND generation=? AND part>? ORDER BY part LIMIT 16`,run.id,manifest.generation,after);
          for(const row of page){
            if(row.part!==count++||!(row.body instanceof ArrayBuffer)||row.body.byteLength>REPLY_CHUNK_BYTES||row.body.byteLength===0)throw new Error("Action input chunks are incomplete");
            bytes+=row.body.byteLength;if(bytes>manifest.bytes||count>manifest.chunks)throw new Error("Action input chunks exceed their manifest");
            text+=decoder.decode(row.body,{stream:true});after=row.part;
          }
          if(page.length<16)break;
        }
        text+=decoder.decode();
        if(bytes!==manifest.bytes||count!==manifest.chunks||sha256Hex(run.id+"\0"+manifest.generation+"\0"+text)!==manifest.hash)throw new Error("Action input integrity check failed");
        value=JSON.parse(text);
      }else{
        value={path:run.action_path,args:JSON.parse(run.args),session:JSON.parse(run.session??"null"),provenance:run.provenance===null?null:JSON.parse(run.provenance)};
      }
      if(!value||value.path!==run.action_path||!value.session||value.session.projectId!==this.options.projectId||value.session.tenantId!==this.options.tenantId||typeof value.session.account?.id!=="string"||typeof value.session.member?.id!=="string")throw new Error("Action input ownership/context is invalid");
      return value;
    }catch(error){
      // Retain uncertain credential-sensitive input authority. Indexed blocked
      // rows are excluded from automatic hand-off; an explicit repair is needed.
      this.exec(`INSERT INTO ${INPUTS} (id,generation,bytes,chunks,hash,blocked) VALUES (?,'unavailable',0,0,'',?)
        ON CONFLICT(id) DO UPDATE SET blocked=excluded.blocked`,run.id,"input integrity requires operator recovery");
      throw error;
    }
  }

  private deleteInputs(id:string):void{
    this.exec(`DELETE FROM ${INPUT_CHUNKS} WHERE id=?`,id);
    this.exec(`DELETE FROM ${INPUTS} WHERE id=?`,id);
  }

  // ---- the run protocol (tenant RPCs, one object argument each) -----------------

  async claim(request: ClaimRequest): Promise<Claim> {
    const now = this.clock();
    const run = this.load(request.id);
    if (!run) return { v: 1, ok: false, reason: "missing" };
    if (run.state !== "live") return { v: 1, ok: false, reason: run.state };
    if (run.status === "processing" && run.owner !== request.owner) {
      const expiry = this.leaseExpiry(run);
      if (expiry > now) return { v: 1, ok: false, reason: "leased", retryAfterMs: expiry - now };
    }
    if (run.attempts >= this.maxAttempts) {
      await this.kill(run, run.last_error ?? `Action runner stopped ${run.attempts} times without finishing`);
      return { v: 1, ok: false, reason: "dead" };
    }
    const input=this.inputs(run);
    const attempt = run.attempts + 1;
    this.exec(`UPDATE ${RUNS} SET status = 'processing', locked_at = ?, attempts = ?, owner = ? WHERE id = ?`, now, attempt, request.owner, run.id);
    this.leases.set(run.id, now + this.leaseMs);
    await this.options.wake();
    return {
      v: 1, ok: true, id: run.id, attempt, path: input.path, args:input.args, provenance: input.provenance===null?null:JSON.stringify(input.provenance),
      session: input.session, leaseMs: this.leaseMs, renewMs: this.renewMs,
    };
  }

  /** Only the current attempt may extend its lease; `locked_at` is written only below half a lease. */
  async renew(request: RenewRequest): Promise<Renewal> {
    const run = this.load(request.id);
    if (!run || run.state !== "live" || run.status !== "processing" || run.attempts !== request.attempt) return { v: 1, ok: false, persisted: false };
    const now = this.clock();
    this.leases.set(run.id, now + this.leaseMs);
    if ((run.locked_at ?? 0) + this.leaseMs >= now + this.leaseMs / 2) return { v: 1, ok: true, persisted: false };
    this.exec(`UPDATE ${RUNS} SET locked_at = ? WHERE id = ? AND attempts = ?`, now, run.id, request.attempt);
    await this.options.wake();
    return { v: 1, ok: true, persisted: true };
  }

  private replyContext(run: Run): Pick<CallFinished, "reply"> {
    const session = this.inputs(run).session;
    if (!session?.reply) return {};
    const { host, connectionId, epoch, id, path } = session.reply;
    return { reply: { host, connectionId, epoch, id, path, tenantId: session.tenantId, ...(session.reply.generation === undefined ? {} : { generation: session.reply.generation }), ...(session.reply.traceJson ? { traceJson: session.reply.traceJson } : {}) } };
  }

  /** Durable terminal execution and reply settlement are distinct. A transport
   * failure retries this same stored answer, never a new Action invocation. */
  async complete(request: CompleteRequest): Promise<Completion> {
    const run = this.load(request.id);
    if (run?.state === "done") {
      // Preserve the original terminal duplicate receipt for every attempt.
      // Only the attempt that actually completed may retry its stored reply.
      if (run.attempts === request.attempt) await this.deliverReply(run.id);
      return { v: 1, applied: false, duplicate: true, appliedAttempt: run.attempts };
    }
    if (!run || run.state !== "live" || run.status !== "processing" || run.attempts !== request.attempt) return { v: 1, applied: false, rejected: true };
    const bytes = request.value === undefined ? null : new TextEncoder().encode(request.value).byteLength;
    const stored = request.value !== undefined && (bytes ?? 0) <= MAX_STORED_RESULT_BYTES ? request.value : null;
    const outcome: CallFinished = request.error !== undefined
      ? { id: run.id, ok: false, error: request.error, ...this.replyContext(run) }
      : { id: run.id, ok: true, value: request.value ?? "null", ...(request.committedRevision === undefined ? {} : { committedRevision: request.committedRevision }), ...this.replyContext(run) };
    const applied = this.options.storage.transactionSync(() => {
      const changed = this.exec(
        `UPDATE ${RUNS} SET state = 'done', finished_at = ?, result = ?, result_bytes = ?, last_error = ?, owner = NULL, session = NULL, args='null', provenance=NULL
         WHERE id = ? AND state = 'live' AND status = 'processing' AND attempts = ? RETURNING id`,
        this.clock(), stored, bytes, request.error === undefined ? null : request.error.slice(0, MAX_STORED_RESULT_BYTES/4), run.id, request.attempt,
      ).length === 1;
      if (changed) { this.storeReply(outcome); this.deleteInputs(run.id); }
      return changed;
    });
    if (!applied) return { v: 1, applied: false, rejected: true };
    this.leases.delete(run.id);
    await this.deliverReply(run.id);
    return { v: 1, applied: true, appliedAttempt: request.attempt };
  }

  private storeReply(outcome: CallFinished): void {
    const text=JSON.stringify(outcome),bytes = new TextEncoder().encode(text),generation=crypto.randomUUID();
    const count = Math.ceil(bytes.byteLength / REPLY_CHUNK_BYTES);
    this.exec(`INSERT INTO ${REPLIES} (id,next_at,generation,bytes,chunks,hash) VALUES (?,?,?,?,?,?)`, outcome.id,this.clock(),generation,bytes.byteLength,count,sha256Hex(outcome.id+"\0"+generation+"\0"+text));
    // Binary chunks preserve UTF-8 even where a surrogate/codepoint crosses a
    // boundary. No row/argument silently truncates a legal large result.
    for (let part = 0; part < count; part++) this.exec(`INSERT INTO ${REPLY_CHUNKS} (id, part, body) VALUES (?, ?, ?)`,
      outcome.id, part, bytes.slice(part * REPLY_CHUNK_BYTES, (part + 1) * REPLY_CHUNK_BYTES).buffer);
  }

  private deliverReply(id: string): Promise<void> {
    const pending = this.replying.get(id);
    if (pending) return pending;
    const work = this.applyReply(id).finally(() => { if (this.replying.get(id) === work) this.replying.delete(id); });
    this.replying.set(id, work);
    return work;
  }

  private async applyReply(id: string): Promise<void> {
    const receipt = this.exec<{chunks:number;generation:string;bytes:number;hash:string;blocked:string|null}>(`SELECT chunks,generation,bytes,hash,blocked FROM ${REPLIES} WHERE id=?`, id)[0];
    if (!receipt) return;
    if(receipt.blocked)throw new Error("Action reply settlement requires operator recovery");
    let outcome:CallFinished;
    try{
    if(!Number.isSafeInteger(receipt.bytes)||receipt.bytes<=0||!Number.isSafeInteger(receipt.chunks)||receipt.chunks!==Math.ceil(receipt.bytes/REPLY_CHUNK_BYTES)||!/^[a-f0-9]{64}$/.test(receipt.hash)||!receipt.generation)throw new Error("Action reply manifest is corrupt");
    const decoder = new TextDecoder("utf-8", { fatal: true, ignoreBOM: false });
    let text = "", after = -1, count = 0, bytes=0;
    for (;;) {
      const page = this.exec<{ part: number; body: ArrayBuffer }>(`SELECT part, body FROM ${REPLY_CHUNKS} WHERE id=? AND part>? ORDER BY part LIMIT 16`, id, after);
      for (const row of page) {
        if (row.part !== count++ || !(row.body instanceof ArrayBuffer) || row.body.byteLength===0 || row.body.byteLength>REPLY_CHUNK_BYTES) throw new Error("Action reply chunks are incomplete");
        bytes+=row.body.byteLength;if(bytes>receipt.bytes||count>receipt.chunks)throw new Error("Action reply chunks exceed their manifest");
        text += decoder.decode(row.body, { stream: true }); after = row.part;
      }
      if (page.length < 16) break;
    }
    if (count !== receipt.chunks) throw new Error("Action reply chunks are incomplete");
    text += decoder.decode();
    if(bytes!==receipt.bytes||sha256Hex(id+"\0"+receipt.generation+"\0"+text)!==receipt.hash)throw new Error("Action reply integrity check failed");
    outcome = JSON.parse(text) as CallFinished;
    if (outcome.id !== id) throw new Error("Action reply identity mismatch");
    }catch(error){this.exec(`UPDATE ${REPLIES} SET blocked=? WHERE id=?`,"reply integrity requires operator recovery",id);throw error;}
    try {
      await this.options.onFinished?.(outcome);
      this.options.storage.transactionSync(() => {
        this.exec(`DELETE FROM ${REPLY_CHUNKS} WHERE id=?`, id);
        this.exec(`DELETE FROM ${REPLIES} WHERE id=?`, id);
      });
    } catch (error) {
      this.exec(`UPDATE ${REPLIES} SET next_at=?,failures=failures+1,blocked=CASE WHEN failures+1>=? THEN 'delivery requires operator recovery' ELSE NULL END WHERE id=?`, this.clock() + this.renewMs,MAX_REPLY_FAILURES,id);
      await this.options.wake();
      throw error;
    }
  }

  /** Cancels a live call (the client went away): the run ends `dead`, its next renewal is refused. */
  async cancel(id: string): Promise<boolean> {
    const run = this.load(id);
    if (!run || run.state !== "live") return false;
    await this.kill(run, "cancelled");
    return true;
  }

  private async kill(run: Run, error: string): Promise<void> {
    const outcome={ id: run.id, ok: false, error, ...this.replyContext(run) };
    this.options.storage.transactionSync(() => {
      this.exec(`UPDATE ${RUNS} SET state = 'dead', finished_at = ?, last_error = ?, owner = NULL, session = NULL,args='null',provenance=NULL WHERE id = ?`, this.clock(),error.slice(0,MAX_STORED_RESULT_BYTES/4),run.id);
      this.storeReply(outcome);
      this.deleteInputs(run.id);
    });
    this.leases.delete(run.id);
    await this.deliverReply(run.id);
  }

  // ---- recovery: an alarm source on K11's TenantAlarm --------------------------------

  /** When a live run next needs the sweep; `now` means hand it off now. */
  private dueAt(run: Pick<Run, "id" | "status" | "locked_at" | "dispatched_at">, now: number): number {
    const handedOff = run.dispatched_at !== null && run.dispatched_at + this.leaseMs > now
      ? run.dispatched_at + this.leaseMs
      : Math.max(now, this.retryAt.get(run.id) ?? 0);
    if (run.status === "processing") {
      const expiry = this.leaseExpiry(run);
      return expiry > now ? expiry : handedOff;
    }
    return handedOff;
  }

  /** Earliest time the sweep has work: live runs are the open direct calls of one tenant, a handful. */
  nextAt(now: number): number | null {
    let next = this.exec<{ at: number | null }>(`SELECT MIN(next_at) AS at FROM ${REPLIES} WHERE blocked IS NULL`)[0]?.at ?? Infinity;
    for (const run of this.exec<{ id: string; status: string; locked_at: number | null; dispatched_at: number | null }>(
      `SELECT id, status, locked_at, dispatched_at FROM ${RUNS} WHERE state = 'live' AND NOT EXISTS (SELECT 1 FROM ${INPUTS} i WHERE i.id=${RUNS}.id AND i.blocked IS NOT NULL)`)) {
      next = Math.min(next, this.dueAt(run as Pick<Run, "id" | "status" | "locked_at" | "dispatched_at">, now));
    }
    const oldest = this.exec<{ at: number | null }>(`SELECT MIN(finished_at) AS at FROM ${RUNS} WHERE state <> 'live' AND id NOT IN (SELECT id FROM ${REPLIES}) AND id NOT IN (SELECT id FROM ${INPUTS})`)[0]?.at ?? null;
    if (oldest !== null) next = Math.min(next, oldest + RECEIPT_RETENTION_MS);
    return Number.isFinite(next) ? next : null;
  }

  /** Hands off every live run that needs it, in pages, and prunes receipts in one bounded statement. */
  async sweep(now = this.clock()): Promise<number> {
    let units = 0;
    const replies = this.exec<{ id: string }>(`SELECT id FROM ${REPLIES} WHERE next_at<=? AND blocked IS NULL ORDER BY next_at,id LIMIT ${PAGE}`, now);
    for (const reply of replies) {
      try { await this.deliverReply(reply.id); units++; } catch { /* retained with bounded retry interval */ }
    }
    let after = "";
    for (;;) {
      const page = this.exec(`SELECT id, status, locked_at, dispatched_at, dispatch_count FROM ${RUNS} WHERE state = 'live' AND id > ? AND NOT EXISTS (SELECT 1 FROM ${INPUTS} i WHERE i.id=${RUNS}.id AND i.blocked IS NOT NULL) ORDER BY id LIMIT ${PAGE}`, after) as unknown as Run[];
      const due = page.filter((run) => this.dueAt(run, now) <= now);
      units += (await this.dispatchMany(due)).size;
      if (page.length < PAGE) break;
      after = page[page.length - 1]!.id;
    }
    units += this.exec(`DELETE FROM ${RUNS} WHERE id IN (SELECT id FROM ${RUNS} WHERE state <> 'live' AND finished_at < ? AND id NOT IN (SELECT id FROM ${REPLIES}) AND id NOT IN (SELECT id FROM ${INPUTS}) LIMIT 500) RETURNING id`, now - RECEIPT_RETENTION_MS).length;
    return units;
  }

  /** Registers with K11's `TenantAlarm` (`alarm.register(calls.alarmSource())`). */
  alarmSource(): CallsAlarmSource {
    return { name: "actions.calls", nextAt: (now) => this.nextAt(now), run: (now) => this.sweep(now) };
  }

  /** One run's state, for tests and an operator view. */
  inspect(id: string): Record<string, unknown> | null {
    const run = this.load(id);
    return run ? { ...run, leaseExpiry: run.state === "live" && run.status === "processing" ? this.leaseExpiry(run) : null } : null;
  }

  /** Forgets in-memory leases, as an object reset does (tests). */
  forgetMemory(): void { this.leases.clear(); }
}
