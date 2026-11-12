import { SAMPLE_MS, type CapacitySample } from "./placement";

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
  private last: CapacitySample | undefined;
  sent(text: string): void {
    this.frames++;
    // Common fan-out repeats encoded text: count UTF8 once, retain one bounded reference.
    if (text !== this.lastText) {
      this.lastBytes = utf8Bytes(text);
      this.lastText = text.length <= 256 * 1024 ? text : "";
    }
    this.bytes += this.lastBytes;
  }
  delivery(ms: number, queueAgeMs = 0, rpcMs = 0): void {
    this.deliveryMs = Math.max(this.deliveryMs, ms);
    this.queueAgeMs = Math.max(this.queueAgeMs, queueAgeMs);
    this.rpcMs = Math.max(this.rpcMs, rpcMs);
  }
  due(): boolean { return Date.now() - this.reported >= SAMPLE_MS; }
  private current(epoch: number, sockets: number): CapacitySample {
    const now = Date.now(), seconds = Math.max(1, (now - this.started) / 1000);
    return { v: 1, epoch, at: now, sockets, framesPerSecond: this.frames / seconds, bytesPerSecond: this.bytes / seconds,
      deliveryMs: this.deliveryMs, queueAgeMs: this.queueAgeMs, rpcMs: this.rpcMs };
  }
  snapshot(epoch: number, sockets: number): CapacitySample {
    const sample = this.current(epoch, sockets), last = this.last;
    // An allocation probe just after report/rotation must not erase sustained pressure with a 0ms window.
    if (!last || Date.now() - last.at > SAMPLE_MS * 2) return sample;
    return { ...sample, framesPerSecond: Math.max(sample.framesPerSecond, last.framesPerSecond),
      bytesPerSecond: Math.max(sample.bytesPerSecond, last.bytesPerSecond), deliveryMs: Math.max(sample.deliveryMs, last.deliveryMs),
      queueAgeMs: Math.max(sample.queueAgeMs, last.queueAgeMs), rpcMs: Math.max(sample.rpcMs, last.rpcMs) };
  }
  rotate(): void {
    this.last = this.current(0, 0);
    this.reported = this.started = Date.now();
    this.lastText = ""; this.lastBytes = 0;
    this.frames = this.bytes = this.deliveryMs = this.queueAgeMs = this.rpcMs = 0;
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
