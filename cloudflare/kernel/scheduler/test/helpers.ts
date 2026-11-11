// Test helpers: a Durable Object per test with the tenant runtime tables, a
// simulated clock and alarm, and recording stand-ins for the Reducer path
// (K07) and the Action runtime (K12).
import { env, runInDurableObject } from "cloudflare:test";
import { SYNC_INFRASTRUCTURE_SQL } from "../../capture/index.ts";
import { TenantAlarm, type AlarmPort } from "../alarm.ts";
import type { ActionHandoff, ActiveModule, DurableRun, FunctionKind, HandoffOutcome, ReducerRunner, RunOutcome, SchedulerEvent } from "../ports.ts";
import { Scheduler, type SchedulerOptions } from "../scheduler.ts";
import type { CronSpec } from "../specs.ts";

let objects = 0;

export async function withObject<T>(fn: (state: DurableObjectState) => T | Promise<T>): Promise<T> {
  objects += 1;
  const stub = env.HELLO.getByName(`k11-test-${objects}-${Math.random()}`);
  return runInDurableObject(stub, (_instance, state) => fn(state));
}

/** The runtime tables Rust creates in every tenant database (K03 installs them), the action outbox included. */
export function installTenantRuntime(state: DurableObjectState): void {
  state.storage.transactionSync(() => {
    state.storage.sql.exec(SYNC_INFRASTRUCTURE_SQL);
  });
}

export const rows = <T = Record<string, SqlStorageValue>>(state: DurableObjectState, query: string, ...binds: SqlStorageValue[]): T[] =>
  [...state.storage.sql.exec(query, ...binds)] as T[];

export class Clock {
  constructor(public now: number) {}
  readonly read = () => this.now;
}

/** An alarm port that only records; the test advances the clock to it and fires. */
export class SimulatedAlarm implements AlarmPort {
  at: number | null = null;
  sets = 0;
  async getAlarm() { return this.at; }
  async setAlarm(at: number) { this.at = at; this.sets++; }
  async deleteAlarm() { this.at = null; }
}

export interface RecordedRun {
  run: DurableRun;
  at: number;
  kind: "reducer" | "action";
}

export class Module implements ActiveModule {
  constructor(public kinds: Record<string, FunctionKind>, public cronSpecs: CronSpec[] = [], public hash = "artifact-1") {}
  kindOf(path: string) { return this.kinds[path]; }
  crons() { return this.cronSpecs; }
  artifactHash() { return this.hash; }
}

/** Records every run; `reply` decides the outcome (ok by default). */
export class Runners implements ReducerRunner, ActionHandoff {
  readonly runs: RecordedRun[] = [];
  reply: (run: DurableRun, kind: "reducer" | "action") => HandoffOutcome | Promise<HandoffOutcome> = () => ({ ok: true });
  constructor(private readonly clock: Clock) {}
  async runReducer(run: DurableRun): Promise<RunOutcome> {
    this.runs.push({ run, at: this.clock.now, kind: "reducer" });
    const outcome = await this.reply(run, "reducer");
    if ("accepted" in outcome) throw new Error("a Reducer cannot be accepted");
    return outcome;
  }
  async startAction(run: DurableRun): Promise<HandoffOutcome> {
    this.runs.push({ run, at: this.clock.now, kind: "action" });
    return this.reply(run, "action");
  }
}

export interface Harness {
  state: DurableObjectState;
  clock: Clock;
  port: SimulatedAlarm;
  alarm: TenantAlarm;
  scheduler: Scheduler;
  module: Module;
  runners: Runners;
  events: SchedulerEvent[];
  /** Fires the alarm at its time (advancing the clock) until it is past `until` or unset. Returns the number of events. */
  runUntil(until: number): Promise<number>;
}

export function harness(state: DurableObjectState, options: { start: number; module?: Module; project?: string; tenant?: string } & Partial<Omit<SchedulerOptions, "storage" | "module" | "reducers" | "actions" | "clock">>): Harness {
  installTenantRuntime(state);
  const clock = new Clock(options.start);
  const port = new SimulatedAlarm();
  const alarm = new TenantAlarm(port, { clock: clock.read, onError: () => {} });
  const module = options.module ?? new Module({});
  const runners = new Runners(clock);
  const events: SchedulerEvent[] = [];
  const { start: _start, module: _module, ...rest } = options;
  const scheduler = new Scheduler({
    ...rest,
    storage: state.storage,
    project: options.project ?? "project-x",
    tenant: options.tenant ?? "tenant-a",
    module,
    reducers: runners,
    actions: runners,
    clock: clock.read,
    log: (event) => events.push(event),
  });
  scheduler.attach(alarm);
  return {
    state, clock, port, alarm, scheduler, module, runners, events,
    async runUntil(until) {
      let fired = 0;
      while (port.at !== null && port.at <= until) {
        if (fired > 50_000) throw new Error(`the alarm keeps firing at ${port.at}`);
        clock.now = Math.max(clock.now, port.at);
        await alarm.fire();
        fired++;
      }
      return fired;
    },
  };
}
