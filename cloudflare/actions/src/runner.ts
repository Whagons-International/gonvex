// Runs one direct agent call on the Action side (a Workflow step): claim the
// run in the tenant object, run the Action as the caller's session with the
// lease renewed every 30 s, then report. Outbox and scheduled Actions do not
// come through here: K11 claims them and hands them over (handoff.ts).
//
// A refused renewal means another attempt owns the run, or it was cancelled:
// this attempt stops and reports nothing; its result, if it ever finishes, is
// never applied.
import type { CallJob } from "./calls.js";
import type { TenantActionPort } from "./ports.js";
import { parseProvenance } from "./provenance.js";
import type { ActionRuntime, ActionStats } from "./runtime.js";
import { tenantRpc } from "./rpc.js";

export type RunOutcome =
  | { kind: "done"; attempt: number; ok: boolean; value?: string | undefined; error?: string | undefined; bytes: number; committedRevision?: number | undefined; renewals: RenewalStats; stats: ActionStats }
  | { kind: "skipped"; reason: string; retryAfterMs?: number | undefined }
  | { kind: "lost"; attempt: number; renewals: RenewalStats };

export interface RenewalStats { renewed: number; persisted: number; errors: number; gapsMs: number[] }

export interface RunnerDeps {
  runtime: ActionRuntime;
  tenant: TenantActionPort;
  clock?: (() => number) | undefined;
  /** Overrides the tenant's renewal interval (tests). */
  renewMs?: number | undefined;
  /** Test hook: stop renewing (a zombie) once this returns true. */
  stopRenewing?: ((attempt: number, elapsedMs: number) => boolean) | undefined;
}

/** Claims `job`'s run as `owner` and runs it. The caller applies a `done` outcome with `completeCall`. */
export async function runCall(deps: RunnerDeps, job: CallJob, owner: string): Promise<RunOutcome> {
  const clock = deps.clock ?? (() => Date.now());
  const claim = await tenantRpc("claimCall", () => deps.tenant.claimCall({ v: 1, id: job.id, owner }));
  if (!claim.ok) return { kind: "skipped", reason: claim.reason, retryAfterMs: claim.retryAfterMs };
  const attempt = claim.attempt;
  const renewals: RenewalStats = { renewed: 0, persisted: 0, errors: 0, gapsMs: [] };

  const started = clock();
  let lastRenewal = started;
  let lostLease: () => void = () => undefined;
  const lost = new Promise<"lost">((resolve) => { lostLease = () => resolve("lost"); });
  let stopped = false;
  const timer = setInterval(() => {
    if (stopped || deps.stopRenewing?.(attempt, clock() - started)) return;
    const sent = clock();
    tenantRpc("renewCall", () => deps.tenant.renewCall({ v: 1, id: job.id, attempt })).then((renewal) => {
      if (stopped) return;
      if (!renewal.ok) { lostLease(); return; }
      renewals.renewed++;
      if (renewal.persisted) renewals.persisted++;
      renewals.gapsMs.push(sent - lastRenewal);
      lastRenewal = sent;
    }, () => { renewals.errors++; });
  }, deps.renewMs ?? claim.renewMs);

  try {
    const report = await Promise.race([
      deps.runtime.execute({ session: claim.session, path: claim.path, args: claim.args, provenance: parseProvenance(claim.provenance), allowInternal: false }),
      lost,
    ]);
    if (report === "lost") return { kind: "lost", attempt, renewals };
    // A direct call is answered once, as Rust answers the socket once: its failure is its answer.
    return report.ok
      ? { kind: "done", attempt, ok: true, value: report.value, committedRevision: report.committedRevision, bytes: new TextEncoder().encode(report.value).byteLength, renewals, stats: report.stats }
      : { kind: "done", attempt, ok: false, error: report.error, bytes: 0, renewals, stats: report.stats };
  } finally {
    stopped = true;
    clearInterval(timer);
  }
}
