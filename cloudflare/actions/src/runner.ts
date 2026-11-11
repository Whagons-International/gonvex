// Runs one handed-off Action on the Action side (a Workflow step): claim,
// run with the lease renewed every 30 s, then report. The Rust counterpart
// is `deliver_outbox_action` (rust/crates/runtime/src/execution.rs).
//
// - Actor: a `_gonvex_*` or empty actor runs as the system session; any other
//   actor is re-resolved as a Member first (durable work keeps attribution,
//   not stale authority). A resolution failure is a delivery failure.
// - Provenance: one durable hop (32 at most, else `dead`), no inherited
//   deadline, parent = the queuing command, command `outbox-<id>`, channel
//   `system`. A row without a readable provenance starts a fresh one.
// - A refused renewal means another attempt owns the row (or it was
//   cancelled): this attempt stops and reports nothing. Its result, if it
//   ever finishes, is never applied.
import type { ActionJob } from "./outbox.js";
import type { Claim, ControlPort, TenantActionPort } from "./ports.js";
import { directProvenance, durableHop, hopsExceeded, MAX_DURABLE_HOPS, parseProvenance, type Provenance } from "./provenance.js";
import type { ActionRuntime, ActionStats } from "./runtime.js";
import { SYSTEM_ACCOUNT_ID, systemSession, type ActionSession } from "./session.js";
import { tenantRpc } from "./rpc.js";

export type RunOutcome =
  | { kind: "done"; origin: "outbox" | "call"; attempt: number; ok: boolean; value?: string | undefined; error?: string | undefined; bytes: number; renewals: RenewalStats; stats: ActionStats }
  | { kind: "failed"; attempt: number; error: string; state: "retry" | "dead" | "stale"; renewals: RenewalStats }
  | { kind: "skipped"; reason: string; retryAfterMs?: number | undefined }
  | { kind: "lost"; attempt: number; renewals: RenewalStats };

export interface RenewalStats { renewed: number; persisted: number; errors: number; gapsMs: number[] }

export interface RunnerDeps {
  runtime: ActionRuntime;
  tenant: TenantActionPort;
  control?: ControlPort | undefined;
  clock?: (() => number) | undefined;
  /** Overrides the tenant's renewal interval (tests). */
  renewMs?: number | undefined;
  /** Test hook: stop renewing (a zombie) once this returns true. */
  stopRenewing?: ((attempt: number, elapsedMs: number) => boolean) | undefined;
}

async function sessionFor(deps: RunnerDeps, job: ActionJob, claim: Extract<Claim, { ok: true }>): Promise<ActionSession> {
  if (claim.origin === "call" && claim.session) return claim.session;
  const actor = claim.actorAccountId;
  if (actor === "" || actor.startsWith("_gonvex_")) return systemSession(job.projectId, job.tenantId);
  if (!deps.control) throw new Error("Control Plane is unavailable");
  return deps.control.tenantSessionForAccount(job.projectId, job.tenantId, actor);
}

/** Claims `job` as `owner` and runs it. The caller applies a `done` outcome with `complete`. */
export async function runClaimed(deps: RunnerDeps, job: ActionJob, owner: string): Promise<RunOutcome> {
  const clock = deps.clock ?? (() => Date.now());
  const claim = await tenantRpc("claim", () => deps.tenant.claim({ v: 1, id: job.id, owner }));
  if (!claim.ok) return { kind: "skipped", reason: claim.reason, retryAfterMs: claim.retryAfterMs };
  const attempt = claim.attempt;
  const renewals: RenewalStats = { renewed: 0, persisted: 0, errors: 0, gapsMs: [] };
  const fail = async (error: string, terminal = false): Promise<RunOutcome> => {
    const result = await tenantRpc("fail", () => deps.tenant.fail({ v: 1, id: job.id, attempt, error, terminal }));
    return { kind: "failed", attempt, error, state: result.state, renewals };
  };

  let session: ActionSession;
  try {
    session = await sessionFor(deps, job, claim);
  } catch (error) {
    return fail(String((error as Error)?.message ?? error));
  }

  let provenance: Provenance | undefined;
  if (claim.origin === "outbox") {
    const stored = parseProvenance(claim.provenance);
    if (stored) {
      // A loop guard, not a transient failure: retrying would only repeat it.
      if (!durableHop(stored, MAX_DURABLE_HOPS)) return fail(hopsExceeded(stored.durableHops), true);
      provenance = stored;
    } else {
      provenance = directProvenance(session, "system", job.id, "");
      provenance.durableHops = 1;
    }
    // Durable work starts after the queuing transaction, maybe after a restart: no old deadline.
    provenance.deadlineUnixMs = null;
    provenance.parentCommandId = provenance.commandId;
    provenance.commandId = `outbox-${job.id}`;
    provenance.channel = "system";
  } else {
    provenance = parseProvenance(claim.provenance);
  }

  // Renew every `renewMs` while the Action runs; a refusal ends this attempt.
  const started = clock();
  let lastRenewal = started;
  let lostLease: () => void = () => undefined;
  const lost = new Promise<"lost">((resolve) => { lostLease = () => resolve("lost"); });
  let stopped = false;
  const renewMs = deps.renewMs ?? claim.renewMs;
  const timer = setInterval(() => {
    if (stopped || deps.stopRenewing?.(attempt, clock() - started)) return;
    const sent = clock();
    tenantRpc("renew", () => deps.tenant.renew({ v: 1, id: job.id, attempt })).then((renewal) => {
      if (stopped) return;
      if (!renewal.ok) { lostLease(); return; }
      renewals.renewed++;
      if (renewal.persisted) renewals.persisted++;
      renewals.gapsMs.push(sent - lastRenewal);
      lastRenewal = sent;
    }, () => { renewals.errors++; });
  }, renewMs);

  try {
    const report = await Promise.race([
      deps.runtime.execute({ session, path: claim.path, args: claim.args, provenance, allowInternal: false }),
      lost,
    ]);
    if (report === "lost") return { kind: "lost", attempt, renewals };
    if (report.ok) return { kind: "done", origin: claim.origin, attempt, ok: true, value: report.value, bytes: new TextEncoder().encode(report.value).byteLength, renewals, stats: report.stats };
    // A direct call is answered once, as Rust answers the socket; an outbox Action is retried.
    if (claim.origin === "call") return { kind: "done", origin: "call", attempt, ok: false, error: report.error, bytes: 0, renewals, stats: report.stats };
    return fail(report.error);
  } finally {
    stopped = true;
    clearInterval(timer);
  }
}

export { SYSTEM_ACCOUNT_ID };
