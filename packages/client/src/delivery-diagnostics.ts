import type { BrowserTelemetryInfo } from '@gonvex/protocol';

type Diagnostics = NonNullable<BrowserTelemetryInfo['deliveryDiagnostics']>;

/** Bounded context for slow deliveries; all browser durations use performance.now(). */
export class DeliveryDiagnostics {
  private observer?: PerformanceObserver;
  private attemptedObserver = false;
  private tasks: Array<{ start: number; duration: number }> = [];
  private started = 0;
  private decoded = 0;
  private handlerDelay?: number;

  begin(eventTimestamp: number | undefined) {
    this.started = performance.now();
    this.decoded = this.started;
    // Older engines may expose epoch timestamps. Do not mix those with this clock.
    this.handlerDelay = typeof eventTimestamp === 'number' && eventTimestamp > 0
      && eventTimestamp <= this.started && this.started - eventTimestamp <= 60_000
      ? this.started - eventTimestamp : undefined;
    if (!this.attemptedObserver) {
      this.attemptedObserver = true;
      try {
        if (typeof PerformanceObserver !== 'undefined'
          && PerformanceObserver.supportedEntryTypes?.includes('longtask')) {
          this.observer = new PerformanceObserver(list => this.addTasks(list.getEntries()));
          this.observer.observe({ type: 'longtask', buffered: true });
        }
      } catch { this.observer = undefined; }
    }
  }

  decodedMessage() { this.decoded = performance.now(); }

  private addTasks(entries: PerformanceEntry[]) {
    for (const entry of entries) {
      if (Number.isFinite(entry.duration) && entry.duration >= 0) {
        this.tasks.push({ start: entry.startTime, duration: entry.duration });
      }
    }
    this.tasks = this.tasks.slice(-32);
  }

  snapshot(bufferedAmount: number): Diagnostics {
    if (this.observer) this.addTasks(this.observer.takeRecords());
    const now = performance.now();
    this.tasks = this.tasks.filter(task => task.start + task.duration >= now - 30_000);
    return {
      messageHandlerDelayMs: this.handlerDelay,
      messageDecodeMs: Math.max(0, this.decoded - this.started),
      messageProcessingMs: Math.max(0, now - this.decoded),
      longTaskSupported: Boolean(this.observer),
      longTaskCount: this.tasks.length,
      longTaskMaxMs: Math.max(0, ...this.tasks.map(task => task.duration)),
      longTaskTotalMs: this.tasks.reduce((sum, task) => sum + task.duration, 0),
      visibilityState: typeof document === 'undefined' ? undefined : document.visibilityState,
      online: typeof navigator === 'undefined' ? undefined : navigator.onLine,
      bufferedAmount: Number.isFinite(bufferedAmount) ? bufferedAmount : 0,
    };
  }

  close() {
    this.observer?.disconnect();
    this.observer = undefined;
    this.attemptedObserver = false;
    this.tasks = [];
  }
}
