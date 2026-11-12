/// <reference path="../host/src/shims.d.ts" />
// The Reducer path in the tenant object: `reducer.call` / `reducer.callMany`
// and the receipt query `__gonvex.reducerReceipt`.
//
// Reference: rust/crates/runtime/src/execution.rs
// `execute_tenant_reducer_with_access_counted` (405-790), `replay_receipt`,
// `reducer_receipt_status` (143-245); lib.rs `call_reducer` (5007-5080) and the
// socket loop's watermark rule (3271-3360, 4195-4345).
//
// Order of checks, exactly as Rust: module → function (missing, internal,
// kind) → public access → idempotency key (non-anonymous, trimmed) and the
// receipt table → client contract → artifact hash → argument schema →
// provenance → entropy → [kernel: migration gate] → receipt claim (replay) →
// module → stored result → reference flush → finalize → FK check → commit.
//
// Rust claims the receipt when the transaction opens, at the module's first
// database call, and a conflicting claim replays the stored result. The
// kernel serializes the Reducer path per object, so reading the receipt
// before the module runs gives the same outcome; the claim and the result are
// written together just before commit.

import type { CommitNotice } from "../capture/finalize.ts";
import { directSql, type CaptureSql } from "../capture/sql.ts";
import { validateValue, SchemaError, type TenantModuleHost, type Identity, type InvocationInfo } from "../host/src/index.js";
import type { ReducerCallRequest } from "../session/decode";
import type { SocketView, TenantSession } from "../session/ports";
import { StorageError, type StorageSession } from "../storage/index";
import { errors, receiptQueryErrors, type ReducerFailure } from "./errors.ts";
import { completesNow, queryError, queryResult, reducerError, reducerResult, replicaWatermark } from "./frames.ts";
import { acceptsClientArtifact, type ProjectModule } from "./module.ts";
import type { PermissionBits } from "./permissions.ts";
import { AmbiguousReceiptsError, ensureReceipts, readReceipt, receiptTables, RECEIPT_QUERY, storeReceipt } from "./receipts.ts";
import type { ReducerStorage } from "./storage.ts";

/** Rust's Q/R execution deadline (GONVEX_MODULE_HOST_EXECUTION_TIMEOUT_MS), installed into the provenance. */
export const EXECUTION_TIMEOUT_MS = 10_000;
const ENTROPY = /^[0-9a-f]{64}$/;

/** The object storage the path reads outside invocations (receipts, clock, members). */
export interface DirectStorage {
  readonly sql: SqlStorage;
  transactionSync<T>(closure: () => T): T;
}

/** What one committed Reducer call did, for the object's post-commit work. */
export interface CommittedCall {
  readonly notice: CommitNotice | undefined;
  /** `actions.enqueue` or a Reducer `scheduler.*` wrote the outbox (Rust `outbox_written`). */
  readonly outboxWritten: boolean;
  /** The revision changed `members` (Rust `members_changed`: the membership projector has work). */
  readonly membersChanged: boolean;
}

export interface ReducerPathOptions {
  readonly storage: ReducerStorage;
  readonly direct: DirectStorage;
  readonly host: TenantModuleHost;
  /** The active module definition (manifest), or undefined when the project has none. */
  readonly module: () => ProjectModule | undefined;
  readonly permissions: PermissionBits;
  /** Rust's schema lock (0028 §3): K02's `gateClosed()`. */
  readonly gateClosed?: () => boolean;
  /** Declared reference integrity, run before finalize (Rust `reference_integrity::flush`). */
  readonly flushReferences?: (sql: CaptureSql) => void;
  /** Object clock (ms). */
  readonly clock?: () => number;
  /** After a committed call (outbox wake, membership projection, delivery). */
  readonly onCommit?: (call: CommittedCall) => void;
  /**
   * The socket's lowest subscription revision (`min(cursors).max(sent)`), or
   * undefined when it has no subscription. K10 owns it; without it every
   * socket counts as having none, as Rust treats a socket that subscribed to
   * nothing.
   */
  readonly subscriptionFloor?: (socket: SocketView) => number | undefined;
  /**
   * The session's own Member changed in the call's commit: its socket must
   * discard the tenant session, then get the watermark (lib.rs
   * `own_membership_changed`, `membership_changed_frames`). The object
   * delivers it to the socket's gateway after this reply.
   */
  readonly ownMembershipChanged?: (socket: SocketView, revision: number) => void;
}

/**
 * `ExecutionAccess` fields a caller other than a member socket sets (K17:
 * a service principal's allowlisted call, service_principal.rs:509-525).
 */
export interface ExecutionAccess {
  /** `allow_internal`: the call may target an `internal` function. */
  readonly allowInternal?: boolean;
  /** The provenance channel (`direct_provenance(session, Api, ..)`); default `ui`, or `api` for a delegated session. */
  readonly channel?: "api" | "system";
}

/** Outcome of one call before it becomes frames. */
export type CallOutcome =
  | { readonly ok: true; readonly result: string; readonly committedRevision: number | undefined }
  | { readonly ok: false; readonly failure: ReducerFailure };

interface MemberRow {
  account_id: string;
  status: string;
  role: string;
  permissions: string;
  membership_revision: number;
}

/** Account and member identity as the module sees it (execution.rs `invocation`). */
export function identityOf(session: TenantSession): Identity {
  const nonempty = (value: string) => (value.trim() ? value.trim() : null);
  const tenant = { id: session.route.tenantId, projectId: session.route.projectId, name: null };
  if (session.anonymous) return { tenant, account: null, member: null, developerSession: session.developerSession as Record<string, unknown> | null };
  const account = session.identity.account;
  const member = session.member;
  return {
    tenant,
    account: { id: account.id, email: nonempty(account.email), name: nonempty(account.name), avatarUrl: nonempty(account.avatarUrl) },
    member: {
      id: member.id, accountId: member.accountId, status: nonempty(member.status), role: nonempty(member.role),
      displayName: nonempty(member.displayName), permissions: member.permissions,
    },
    developerSession: session.developerSession as Record<string, unknown> | null,
  };
}

/**
 * `direct_provenance(session, channel, command_id, artifact_hash).public_info()`.
 * The default channel is `ui`, which a delegated session turns into `api`;
 * a service socket's own calls pass `api` (K17), or `system` for a principal
 * configured so (0045).
 */
export function invocationInfoOf(session: TenantSession, commandId: string, artifactHash: string, requested?: "api" | "system"): InvocationInfo {
  const nonempty = (value: string) => (value.trim() ? value.trim() : null);
  const delegation = session.delegation ?? null;
  const channel = requested ?? (delegation ? "api" : "ui");
  return {
    channel,
    rootChannel: channel,
    actorAccountId: nonempty(session.identity.account.id),
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
    delegation,
  };
}

/** lib.rs `member_access_changed`. */
function memberAccessChanged(session: TenantSession, current: MemberRow | undefined): boolean {
  if (!current) return true;
  const admitted = session.member;
  return admitted.accountId !== current.account_id || admitted.status !== current.status || admitted.role !== current.role
    || JSON.stringify(admitted.permissions) !== JSON.stringify(JSON.parse(current.permissions || "null"))
    || admitted.membershipRevision !== Number(current.membership_revision);
}

/** Rows read and written by the last executed call (budgets, PR numbers). */
export interface CallNumbers {
  readonly path: string;
  readonly statements: number;
  readonly rowsRead: number;
  readonly rowsWritten: number;
  /** Of which K03 finalize (revision, change rows, provenance). */
  readonly captureRowsWritten: number;
  readonly catalogRowsRead: number;
  readonly schemaRowsRead: number;
  readonly hostCalls: number;
}

export class ReducerPath {
  private queue: Promise<unknown> = Promise.resolve();
  /** The last call that ran the module (replays and refusals do not count). */
  lastNumbers: CallNumbers | undefined;
  private receiptsReady: number | undefined;
  private readonly clock: () => number;
  private readonly sql: CaptureSql;

  constructor(private readonly options: ReducerPathOptions) {
    this.clock = options.clock ?? (() => Date.now());
    this.sql = directSql(options.direct.sql);
  }

  /** Runs `fn` after every call already queued on this object (one invocation at a time, as K01 requires). */
  serial<T>(fn: () => Promise<T>): Promise<T> {
    const next = this.queue.then(fn, fn);
    this.queue = next.catch(() => undefined);
    return next;
  }

  // ---- frames ------------------------------------------------------------------------

  /** `reducer.call` from a socket with a tenant session: the frames to send, in order. */
  async reducerCall(socket: SocketView, request: ReducerCallRequest): Promise<string[]> {
    const session = socket.tenant;
    if (!session) return [reducerError(request.id, request.path, errors.unauthenticated(), request.trace)];
    const outcome = await this.serial(() => this.execute(session, request));
    if (!outcome.ok) return [reducerError(request.id, request.path, outcome.failure, request.trace)];
    const out = [reducerResult(request.id, request.path, outcome.result, outcome.committedRevision, request.trace)];
    this.complete(socket, session, outcome.committedRevision, out);
    return out;
  }

  /** `reducer.callMany`: each call in order, as `reducer.call` (lib.rs:3362-3466). */
  async reducerCallMany(socket: SocketView, calls: readonly ReducerCallRequest[]): Promise<string[]> {
    const out: string[] = [];
    for (const call of calls) out.push(...await this.reducerCall(socket, call));
    return out;
  }

  /** `query.call` of `__gonvex.reducerReceipt`. */
  async receiptQuery(socket: SocketView, id: string, args: unknown): Promise<string[]> {
    const session = socket.tenant!;
    const outcome = await this.serial(async () => this.receiptStatus(session, args));
    if (!outcome.ok) return [queryError(id, RECEIPT_QUERY, outcome.failure)];
    const out = [queryResult(id, RECEIPT_QUERY, outcome.result)];
    this.complete(socket, session, outcome.committedRevision, out);
    return out;
  }

  /** The terminal watermark, or the own-membership hand-off (lib.rs:3296-3346). */
  private complete(socket: SocketView, session: TenantSession, committed: number | undefined, out: string[]): void {
    if (!completesNow(committed, this.options.subscriptionFloor?.(socket))) return;
    if (this.ownMembershipChanged(session)) {
      // auth.error membership-changed, then the watermark, after this reply (the gateway sends both).
      this.options.ownMembershipChanged?.(socket, committed!);
      return;
    }
    out.push(replicaWatermark(committed!));
  }

  private ownMembershipChanged(session: TenantSession): boolean {
    if (session.anonymous || session.member.id.startsWith("_gonvex_service:") || !this.options.ownMembershipChanged) return false;
    try {
      const row = this.options.direct.sql.exec<MemberRow & Record<string, SqlStorageValue>>(
        "SELECT account_id, status, role, permissions, membership_revision FROM members WHERE id = ?", session.member.id,
      ).toArray()[0];
      return memberAccessChanged(session, row);
    } catch (error) {
      console.warn("reducer: could not re-read the session Member after a commit", error);
      return false;
    }
  }

  // ---- execution -------------------------------------------------------------------------

  /** One Reducer call (execute_tenant_reducer_with_access_counted). Run under `serial`. */
  async execute(session: TenantSession, request: ReducerCallRequest, access: ExecutionAccess = {}): Promise<CallOutcome> {
    const fail = (failure: ReducerFailure): CallOutcome => ({ ok: false, failure });
    const path = request.path;
    const module = this.options.module();
    if (!module) return fail(errors.moduleMissing(session.identity.projectId));
    const definition = module.functions.get(path);
    if (!definition) return fail(errors.functionMissing(path));
    if (definition.internal && !access.allowInternal) return fail(errors.internalFunction(path));
    if (definition.kind !== "reducer") return fail(errors.wrongKind(path, "reducer", definition.kind));
    // require_public_access: `allow_internal` also admits an internal function on an anonymous session.
    const publicOrAllowed = definition.public || (access.allowInternal === true && definition.internal);
    if (session.anonymous && (!publicOrAllowed || definition.replica || definition.delivery === "live" || definition.delivery === "replica")) {
      return fail(errors.hostCall(`authenticate with an active tenant before calling a non-public ${definition.kind}`));
    }
    const key = session.anonymous ? undefined : request.idempotencyKey?.trim() || undefined;
    if (key !== undefined) {
      try {
        this.ensureReceipts();
      } catch (error) {
        if (error instanceof AmbiguousReceiptsError) return fail(errors.ambiguousReceipts());
        throw error;
      }
    }
    const receiptPath = request.receiptPath ?? path;
    if (request.clientContract !== undefined && module.clientContract !== null && request.clientContract !== BigInt(module.clientContract)) {
      return fail(errors.clientUpdateRequired(request.clientContract, module.clientContract));
    }
    if (request.artifactHash !== undefined && !acceptsClientArtifact(module, request.artifactHash, request.clientContract)) {
      return fail(errors.staleArtifact(request.artifactHash, module.artifactHash));
    }
    try {
      validateValue(definition.argsSchema, request.args);
    } catch (error) {
      if (error instanceof SchemaError) return fail(errors.invalidArguments(path, error.message));
      throw error;
    }
    const commandId = request.id;
    const invocation = invocationInfoOf(session, commandId, module.artifactHash, access.channel);
    const deadlineUnixMs = this.clock() + EXECUTION_TIMEOUT_MS;
    if (request.intentEntropy !== undefined && !ENTROPY.test(request.intentEntropy)) return fail(errors.invalidArguments(path, "Invalid reducer entropy"));
    if (this.options.gateClosed?.()) return fail(errors.migrationInProgress());

    // Receipt claim: a committed key replays its stored result instead of running again.
    if (key !== undefined) {
      const replay = this.replay(session, key, receiptPath);
      if (replay) return replay;
    }
    // Committed permission bits, before the transaction opens (a Reducer never sees its own grant rows).
    if (this.options.permissions.declared) this.options.permissions.ensure(this.sql);

    const subject = session.identity.account.id;
    const flush = this.options.flushReferences;
    this.options.storage.arm({
      provenance: {
        commandId,
        rootCommandId: invocation.rootCommandId as string,
        channel: invocation.channel as string,
        rootChannel: invocation.rootChannel as string,
        actorAccountId: invocation.actorAccountId as string | null,
        actorMemberId: invocation.actorMemberId as string | null,
        onBehalfOfMemberId: null,
        agentExecutionId: null,
      },
      afterSuccess: (sessionSql: StorageSession, value: string) => {
        // store_reducer_result, then the commit's reference flush (finalize follows in K03).
        if (key !== undefined) storeReceipt(sessionSql, subject, key, receiptPath, value);
        if (flush) {
          try {
            flush(sessionSql);
          } catch (error) {
            if (error instanceof StorageError) throw error;
            throw new StorageError((error as Error).message, "refused");
          }
        }
      },
    });
    // K04 can answer without opening storage (quarantined command, no generation): never leave the hooks armed.
    const report = await this.runDisarming({
      function: path,
      kind: "reducer",
      args: request.args,
      identity: identityOf(session),
      invocation,
      ...(request.intentEntropy !== undefined ? { intentEntropy: request.intentEntropy } : {}),
      deadlineUnixMs,
      anonymous: session.anonymous,
    });
    const commit = this.options.storage.takeReport();
    const stats = report.stats;
    this.lastNumbers = {
      path, statements: stats.statements, rowsRead: stats.rowsRead, rowsWritten: stats.rowsWritten,
      captureRowsWritten: commit?.capture.rowsWritten ?? 0, catalogRowsRead: stats.catalogRowsRead, schemaRowsRead: stats.schemaRowsRead, hostCalls: stats.hostCalls,
    };
    if (!report.ok) return fail({ message: report.error!.message, class: report.error!.class });
    const notice = commit?.notice;
    const byKind = report.stats.byKind;
    const outboxWritten = (byKind["actionEnqueue"] ?? 0) + (byKind["scheduleAfter"] ?? 0) + (byKind["scheduleAt"] ?? 0) > 0;
    if (commit?.committed) {
      this.options.onCommit?.({ notice, outboxWritten, membersChanged: notice?.tables.includes("members") ?? false });
    }
    return { ok: true, result: report.value ?? "null", committedRevision: notice?.revision };
  }

  private async runDisarming(call: Parameters<TenantModuleHost["run"]>[0]): ReturnType<TenantModuleHost["run"]> {
    try {
      return await this.options.host.run(call);
    } finally {
      this.options.storage.disarm();
    }
  }

  /** `ensure_reducer_idempotency`, once per schema generation of the object. */
  private ensureReceipts(): void {
    const generation = this.options.storage.catalog.generation;
    if (this.receiptsReady === generation) return;
    this.options.direct.transactionSync(() => ensureReceipts(this.sql));
    this.receiptsReady = generation;
  }

  /** `replay_receipt`: the stored result and the clock's revision, or undefined when the key is new. */
  private replay(session: TenantSession, key: string, receiptPath: string): CallOutcome | undefined {
    const receipt = readReceipt(this.sql, session.identity.account.id, key);
    if (receipt.state === "none") return undefined;
    if (receipt.path !== receiptPath) return { ok: false, failure: errors.pathMismatch(key, receipt.path, receiptPath) };
    if (receipt.state === "expired") return { ok: false, failure: errors.receiptExpired() };
    return { ok: true, result: receipt.result, committedRevision: this.clockRevision() };
  }

  private clockRevision(): number {
    const row = this.sql.run("SELECT revision FROM _gonvex_sync_clock WHERE singleton = 1").rows[0];
    return row ? Number(row[0]) : 0;
  }

  /** `reducer_receipt_status` (execution.rs:143-245): result as JSON text. Run under `serial`. */
  receiptStatus(session: TenantSession, args: unknown): CallOutcome {
    const fail = (failure: ReducerFailure): CallOutcome => ({ ok: false, failure });
    if (session.anonymous) return fail(receiptQueryErrors.anonymous());
    const object = args !== null && typeof args === "object" && !Array.isArray(args) ? (args as Record<string, unknown>) : {};
    const key = typeof object.idempotencyKey === "string" ? object.idempotencyKey.trim() : "";
    const path = typeof object.receiptPath === "string" ? object.receiptPath : "";
    if (!key || !path) return fail(receiptQueryErrors.missingArgs());
    const active = Number(this.sql.run(
      "SELECT EXISTS(SELECT 1 FROM members WHERE id=?1 AND account_id=?2 AND status='active')", [session.member.id, session.identity.account.id],
    ).rows[0]![0]) === 1;
    if (!active) return fail(errors.memberNotFound(session.identity.account.id));
    const tables = receiptTables(this.sql);
    if (tables.current && tables.legacy) return fail(errors.hostCall("ambiguous current and legacy receipt history requires recovery"));
    if (!tables.current && !tables.legacy) return { ok: true, result: '{"found":false}', committedRevision: undefined };
    const table = tables.current ? "_gonvex_reducer_idempotency" : "_gonvex_mutation_idempotency";
    const row = this.sql.run(`SELECT path, result IS NOT NULL AS complete FROM ${table} WHERE subject=?1 AND idempotency_key=?2`, [session.identity.account.id, key]).rows[0];
    if (!row) return { ok: true, result: '{"found":false}', committedRevision: undefined };
    const storedPath = String(row[0]);
    if (storedPath !== path) return fail(errors.pathMismatch(key, storedPath, path));
    // A committed receipt with no result is a tombstone (0009 rule 3); Rust has none, so it never answers this.
    if (Number(row[1]) !== 1) return fail(errors.receiptExpired());
    const clock = this.sql.run("SELECT epoch, revision FROM _gonvex_sync_clock WHERE singleton=1").rows[0];
    const epoch = String(clock?.[0] ?? "");
    const revision = Number(clock?.[1]);
    if (!epoch || !Number.isSafeInteger(revision) || revision < 0) return fail(receiptQueryErrors.clock());
    return { ok: true, result: `{"found":true,"cursor":{"epoch":${JSON.stringify(epoch)},"revision":${revision}}}`, committedRevision: revision };
  }
}
