import { SAMPLE_MS, pressured, type CapacitySample, type CapacityWindow } from "./placement";

/** Constant-space per-holder telemetry. No tenant labels or per-commit storage writes. */
export class GatewayMetrics {
  private started = Date.now();
  private frames = 0;
  private bytes = 0;
  private deliveryMs = 0;
  private queueAgeMs = 0;
  private rpcMs = 0;
  private reported = 0;
  private lastText = "";
  private lastBytes = 0;
  private socketMinimum = 0;
  private socketSamples = 0;
  private instance = this.newInstance();
  private sequence = 0;
  private last: (CapacitySample & CapacityWindow) | undefined;
  private newInstance(): number {
    const words = crypto.getRandomValues(new Uint32Array(2));
    return (words[0]! & 0xfffff) * 0x100000000 + words[1]! || 1;
  }
  private clear(): void {
    this.frames = this.bytes = this.deliveryMs = this.queueAgeMs = this.rpcMs = this.socketMinimum = this.socketSamples = 0;
    this.lastText = ""; this.lastBytes = 0;
  }
  private advance(now: number): void {
    if (now < this.started) {
      this.instance = this.newInstance(); this.sequence = 0; this.started = now; this.last = undefined; this.clear();
      return;
    }
    const steps = Math.floor((now - this.started) / SAMPLE_MS);
    if (!steps) return;
    const end = this.started + SAMPLE_MS;
    const closed = this.current(0, this.socketMinimum, end);
    this.last = Object.freeze({ ...closed, windowInstance: this.instance, windowSequence: this.sequence,
      windowStart: this.started, windowEnd: end, windowSockets: closed.sockets, windowSocketSamples: this.socketSamples,
      windowFramesPerSecond: closed.framesPerSecond, windowBytesPerSecond: closed.bytesPerSecond,
      windowDeliveryMs: closed.deliveryMs, windowQueueAgeMs: closed.queueAgeMs, windowRpcMs: closed.rpcMs });
    this.sequence += steps; this.started += steps * SAMPLE_MS; this.clear();
  }
  sent(text: string): void {
    this.advance(Date.now());
    this.frames++;
    // Common fan-out repeats encoded text: count UTF8 once, retain one bounded reference.
    if (text !== this.lastText) {
      this.lastBytes = utf8Bytes(text);
      this.lastText = text.length <= 256 * 1024 ? text : "";
    }
    this.bytes += this.lastBytes;
  }
  delivery(ms: number, queueAgeMs = 0, rpcMs = 0): void {
    this.advance(Date.now());
    this.deliveryMs = Math.max(this.deliveryMs, ms);
    this.queueAgeMs = Math.max(this.queueAgeMs, queueAgeMs);
    this.rpcMs = Math.max(this.rpcMs, rpcMs);
  }
  due(): boolean { return Date.now() - this.reported >= SAMPLE_MS; }
  private current(epoch: number, sockets: number, now = Date.now()): CapacitySample {
    const seconds = Math.max(1, (now - this.started) / 1000);
    return { v: 1, epoch, at: now, sockets, framesPerSecond: this.frames / seconds, bytesPerSecond: this.bytes / seconds,
      deliveryMs: this.deliveryMs, queueAgeMs: this.queueAgeMs, rpcMs: this.rpcMs };
  }
  snapshot(epoch: number, sockets: number): CapacitySample {
    const now = Date.now(); this.advance(now);
    // No historical count is inferred at a boundary: this value belongs only to the current bucket.
    this.socketMinimum = this.socketSamples ? Math.min(this.socketMinimum, sockets) : sockets;
    this.socketSamples = Math.min(Number.MAX_SAFE_INTEGER, this.socketSamples + 1);
    const sample = this.current(epoch, sockets, now), last = this.last;
    // A rate needs a reporting horizon: just after a bucket boundary the new
    // counters alone look idle. Keep only the latest completed bucket's work
    // rates for one interval; never carry its latency maxima or socket gauge.
    const recentThroughput = last && now - last.windowEnd <= SAMPLE_MS
      && pressured({ ...last, sockets: 0, deliveryMs: 0, queueAgeMs: 0, rpcMs: 0 });
    sample.currentPressure = pressured(sample) || recentThroughput ? 1 : 0;
    // Admission retains recent maxima; rebalance receives the EXACT immutable completed bucket separately.
    if (!last || now - last.windowEnd > SAMPLE_MS * 2) return sample;
    return { ...sample, framesPerSecond: Math.max(sample.framesPerSecond, last.framesPerSecond),
      bytesPerSecond: Math.max(sample.bytesPerSecond, last.bytesPerSecond), deliveryMs: Math.max(sample.deliveryMs, last.deliveryMs),
      queueAgeMs: Math.max(sample.queueAgeMs, last.queueAgeMs), rpcMs: Math.max(sample.rpcMs, last.rpcMs),
      windowInstance: last.windowInstance, windowSequence: last.windowSequence, windowStart: last.windowStart, windowEnd: last.windowEnd,
      windowSockets: last.windowSockets, windowSocketSamples: last.windowSocketSamples, windowFramesPerSecond: last.windowFramesPerSecond, windowBytesPerSecond: last.windowBytesPerSecond,
      windowDeliveryMs: last.windowDeliveryMs, windowQueueAgeMs: last.windowQueueAgeMs, windowRpcMs: last.windowRpcMs };
  }
  rotate(): void {
    const now = Date.now(); this.advance(now); this.reported = now;
    // A forced report/probe never opens a new measurement bucket or re-ages old activity.
  }

}

/** Count without allocating a second frame-sized buffer; matches TextEncoder's lone-surrogate replacement. */
function utf8Bytes(text: string): number {
  let bytes = 0;
  for (let i = 0; i < text.length; i++) {
    const c = text.charCodeAt(i);
    if (c < 0x80) bytes++;
    else if (c < 0x800) bytes += 2;
    else if (c >= 0xd800 && c <= 0xdbff && text.charCodeAt(i + 1) >= 0xdc00 && text.charCodeAt(i + 1) <= 0xdfff) { bytes += 4; i++; }
    else bytes += 3;
  }
  return bytes;
}
