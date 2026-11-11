// Runner C: a Durable Object per Action, driven by its alarm.
//
// `heartbeat` mode starts the Action as a detached promise and re-arms the
// alarm every 30 s while it runs, so the object keeps receiving events and no
// single invocation holds the Action. `await` mode runs the whole Action
// inside one alarm invocation (bounded by the 15-minute alarm wall limit).
import { DurableObject } from "cloudflare:workers";
import { runAction, type RunOutcome } from "./action";
import { tenantStub, type Env, type Job } from "./env";

type Mode = "heartbeat" | "await";
interface Stored extends Job {
  mode: Mode;
}

const HEARTBEAT_MS = 30_000;

export class RunnerObject extends DurableObject<Env> {
  private running: Promise<void> | null = null;
  private readonly wokeAt = Date.now();

  async start(tenant: string, id: string, mode: Mode) {
    await this.ctx.storage.put("job", { tenant, id, mode } satisfies Stored);
    await this.ctx.storage.setAlarm(Date.now());
  }

  override async alarm(info?: AlarmInvocationInfo) {
    const job = await this.ctx.storage.get<Stored>("job");
    if (!job) return;
    const log = ((await this.ctx.storage.get<unknown[]>("alarms")) ?? []).slice(-50);
    log.push({ at: Date.now(), retry: info?.retryCount ?? 0, running: Boolean(this.running), wokeAt: this.wokeAt });
    await this.ctx.storage.put("alarms", log);
    const owner = `alarm:${job.tenant}/${job.id}`;
    if (job.mode === "await") {
      await this.execute(job, owner);
      return;
    }
    if (!this.running) {
      this.running = this.execute(job, owner).finally(() => {
        this.running = null;
      });
    }
    await this.ctx.storage.setAlarm(Date.now() + HEARTBEAT_MS);
  }

  private async execute(job: Stored, owner: string) {
    const outcome: RunOutcome = await runAction(this.env, job, owner);
    let applied: unknown = null;
    if (outcome.kind === "done") {
      applied = await tenantStub(this.env, job.tenant).complete(job.id, outcome.attempt, outcome.result);
    }
    await this.ctx.storage.put("last", { at: Date.now(), outcome: { ...outcome, result: undefined }, applied });
    if (outcome.kind === "skipped" && outcome.reason === "leased") {
      await this.ctx.storage.setAlarm(Date.now() + (outcome.retryAfterMs ?? HEARTBEAT_MS) + 1000);
    } else if (outcome.kind === "failed") {
      await this.ctx.storage.setAlarm(Date.now() + Math.min(2 ** outcome.attempt, 60) * 1000);
    } else {
      await this.ctx.storage.delete("job");
      await this.ctx.storage.deleteAlarm();
    }
  }

  /** Test hook: reset the object mid-run, as an eviction or crash would. */
  kill() {
    this.ctx.abort("s4 runner kill");
  }

  async state() {
    return {
      job: await this.ctx.storage.get("job"),
      last: await this.ctx.storage.get("last"),
      alarms: await this.ctx.storage.get("alarms"),
      alarm: await this.ctx.storage.getAlarm(),
      running: Boolean(this.running),
      wokeAt: this.wokeAt,
    };
  }
}
