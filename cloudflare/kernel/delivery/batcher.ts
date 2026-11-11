// Delivery v2 on one socket (delivery.rs `ReplicaBatcher`): feed items of a
// socket that negotiated `replicaBatchFrames` go out as one `replica.batch`
// frame per tick, with the socket's stream bindings and the watermark in the
// tail. Placement-neutral: it runs in the object that holds the socket.
//
// Timing (delivery.rs 41-71, 617-657): a batch holding changed rows is sent
// 50 ms after it opened; a ready- or watermark-only batch waits 1000 ms; it is
// sent at once when a call result waits for its watermark, when it reaches
// 512 KiB, or before any other outbound frame.

import { BATCH_IDLE_MS, BATCH_MAX_BYTES, BATCH_TICK_MS } from "./protocol.ts";

export interface BatchSocket {
  /** Sends a frame without flushing the batch (the batch itself). */
  sendRaw(text: string): void;
  /** `tenant` and `class` of the envelope, or undefined without a tenant session. */
  envelope(): { tenant: string; class: string } | undefined;
  now(): number;
  /** Runs `fn` after `ms` (the holder is awake while a batch is open); returns a cancel function. */
  later(ms: number, fn: () => void): () => void;
}

interface OpenBatch {
  readonly prefix: string;
  readonly parts: string[];
  bytes: number;
  items: number;
  data: number;
  watermark: number | undefined;
  readonly openedAt: number;
}

export class ReplicaBatcher {
  enabled = false;
  private readonly bindings = new Map<string, string>();
  private open: OpenBatch | undefined;
  /** The last watermark sent in a batch (the envelope's `from`). */
  private lastWatermark = 0;
  /** Revisions call results wait for (their watermark). */
  private readonly waiting = new Set<number>();
  private cancel: (() => void) | undefined;
  private dueAt: number | undefined;

  constructor(private readonly socket: BatchSocket) {}

  /** Forget stream bindings (every `auth`; the client rebinds by overwriting). */
  clearBindings(): void {
    this.bindings.clear();
    this.waiting.clear();
  }

  get hasOpen(): boolean {
    return this.open !== undefined;
  }

  /** A call result waits for `revision`: the batch whose watermark covers it goes at once. */
  urgent(revision: number): void {
    if (revision > 0) this.waiting.add(revision);
    this.schedule();
  }

  private ensureOpen(): OpenBatch | undefined {
    if (this.open) return this.open;
    const envelope = this.socket.envelope();
    if (!envelope) return undefined;
    const prefix = `{"type":"replica.batch","tenant":${JSON.stringify(envelope.tenant)},"class":"${envelope.class}","from":${this.lastWatermark},"items":[`;
    this.open = { prefix, parts: [], bytes: prefix.length + 2, items: 0, data: 0, watermark: undefined, openedAt: this.socket.now() };
    return this.open;
  }

  private push(batch: OpenBatch, text: string): void {
    if (batch.items > 0) batch.parts.push(",");
    batch.parts.push(text);
    batch.items += 1;
    batch.bytes += text.length + 1;
  }

  private bind(batch: OpenBatch, id: string, token: string): void {
    if (this.bindings.get(id) === token) return;
    for (const [other, bound] of this.bindings) if (bound === token && other !== id) this.bindings.delete(other);
    this.bindings.set(id, token);
    this.push(batch, `{"type":"replica.stream","id":${JSON.stringify(id)},"stream":${JSON.stringify(token)}}`);
  }

  /** A transaction or other data item. */
  item(text: string): void {
    const batch = this.ensureOpen();
    if (!batch) return;
    this.push(batch, text);
    batch.data += 1;
    this.schedule();
  }

  /** A delta item: its shared body, then this socket's tail. */
  delta(id: string, token: string, body: string, tail: string): void {
    const batch = this.ensureOpen();
    if (!batch) return;
    this.bind(batch, id, token);
    this.push(batch, body + tail);
    batch.data += 1;
    this.schedule();
  }

  /** A ready item (per socket, inline). */
  ready(id: string, token: string, text: string): void {
    const batch = this.ensureOpen();
    if (!batch) return;
    this.bind(batch, id, token);
    this.push(batch, text);
    this.schedule();
  }

  /** A presence frame (delivery v3): sent at once unless the batch already holds row changes. */
  presence(text: string): void {
    const hadData = (this.open?.data ?? 0) > 0;
    const batch = this.ensureOpen();
    if (!batch) return;
    this.push(batch, text);
    batch.data += 1;
    if (!hadData) this.flush();
    else this.schedule();
  }

  /** The connection's watermark for the open batch (opening a watermark-only batch when needed). */
  watermark(revision: number): void {
    const batch = this.ensureOpen();
    if (!batch) return;
    batch.watermark = Math.max(batch.watermark ?? revision, revision);
    this.schedule();
  }

  /** Sends the open batch now. */
  flush(): void {
    this.cancel?.();
    this.cancel = undefined;
    this.dueAt = undefined;
    const batch = this.open;
    if (!batch) return;
    this.open = undefined;
    const tail = batch.watermark !== undefined ? `],"watermark":${batch.watermark}}` : "]}";
    this.socket.sendRaw(batch.prefix + batch.parts.join("") + tail);
    if (batch.watermark !== undefined) {
      this.lastWatermark = Math.max(this.lastWatermark, batch.watermark);
      for (const revision of this.waiting) if (revision <= batch.watermark) this.waiting.delete(revision);
    }
  }

  private callWaiting(): boolean {
    const watermark = this.open?.watermark;
    if (watermark === undefined) return false;
    for (const revision of this.waiting) if (revision <= watermark) return true;
    return false;
  }

  /** Arms the batch's deadline (or sends it now). */
  private schedule(): void {
    const batch = this.open;
    if (!batch) return;
    if (this.callWaiting() || batch.bytes >= BATCH_MAX_BYTES) return this.flush();
    const due = batch.openedAt + (batch.data > 0 ? BATCH_TICK_MS : BATCH_IDLE_MS);
    if (this.dueAt === due) return;
    this.cancel?.();
    this.dueAt = due;
    this.cancel = this.socket.later(Math.max(0, due - this.socket.now()), () => {
      this.cancel = undefined;
      this.dueAt = undefined;
      this.flush();
    });
  }
}
