import type { GonvexClient, CallOptions } from "../index.js";

export type AdmissionLane = <T>(run: () => Promise<T>) => Promise<T>;
const lanes = new WeakMap<CallOptions, AdmissionLane>();
export function engineAdmissionLane(options: CallOptions) { return lanes.get(options); }

/** Reuse existing fences for a bounded stream; acknowledgements stay per intent. */
export class EngineAdmissionCohort {
  private readonly entries: Array<{ options: CallOptions & { engineFresh?: boolean }; begin(): Promise<{ operation: Promise<unknown> }>; resolve(value: { operation: Promise<unknown> }): void; reject(error: unknown): void }> = [];
  private readonly scope: string;
  private accepting = true;
  private startedAt?: number;
  private wake?: () => void;
  constructor(private readonly client: GonvexClient) { this.scope = client["outboxScope"]; }
  canAccept(count = 1) {
    if (this.startedAt !== undefined && performance.now() - this.startedAt >= 50) this.seal();
    return this.accepting && this.scope === this.client["outboxScope"] && this.entries.length + count <= 1024;
  }
  seal() { this.accepting = false; this.wake?.(); }
  enqueue(options: CallOptions & { engineFresh?: boolean }, begin: () => Promise<{ operation: Promise<unknown> }>) {
    const result = new Promise<{ operation: Promise<unknown> }>((resolve, reject) => this.entries.push({ options, begin, resolve, reject }));
    this.wake?.(); return result;
  }
  flush() {
    if (!this.entries.length) { this.seal(); return Promise.resolve(); }
    const client = this.client, store = client["sharedOutboxStore"]!;
    let lane: Promise<unknown> = Promise.resolve();
    const admissionLane: AdmissionLane = run => {
      const operation = lane.then(run); lane = operation.catch(() => undefined); return operation;
    };
    const execute = async () => {
      this.startedAt = performance.now();
      const deadline = setTimeout(() => this.seal(), 50);
      try {
        let starts: Promise<unknown> = Promise.resolve(), pendingStarts = 0, offset = 0;
        while (offset < this.entries.length) {
          const entries = this.entries.slice(offset); offset = this.entries.length;
          for (const entry of entries) lanes.set(entry.options, admissionLane);
          await Promise.allSettled(entries.map(async entry => {
            try {
              const ordered = !entry.options.engineFresh || pendingStarts > 0;
              const starting = ordered ? starts.then(entry.begin) : entry.begin();
              if (ordered) {
                pendingStarts++;
                starts = starting.then(() => { pendingStarts--; }, () => { pendingStarts--; });
              }
              const started = await starting;
              entry.resolve(started); await started.operation;
            } catch (error) { entry.reject(error); }
          }));
          if (offset < this.entries.length) continue;
          if (!this.canAccept()) break;
          // The next awaited UI call can reuse this lease without delaying
          // the durable reply. Idle and elapsed bounds let delivery proceed.
          await new Promise<void>(resolve => {
            const idle = setTimeout(resolve, 5);
            this.wake = () => { clearTimeout(idle); resolve(); };
          });
          this.wake = undefined;
        }
      } finally {
        clearTimeout(deadline); this.seal();
        for (const entry of this.entries) lanes.delete(entry.options);
      }
    };
    return Promise.all([client["outboxReady"], client["replicaReady"]]).then(() =>
      store.coordinate!(this.scope, "delivery", () => client["inLocalLane"](() => store.coordinate!(this.scope, "intent", execute))),
    ).catch(error => { this.seal(); for (const entry of this.entries) entry.reject(error); });
  }
}
