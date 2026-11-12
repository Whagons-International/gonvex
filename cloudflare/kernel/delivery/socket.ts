// The holder side of replica delivery for one socket: applies the tenant's
// `SocketWork` in order, batches feed items for `replicaBatchFrames` sockets
// (`ReplicaBatcher`), keeps watermarks monotonic, and resets subscriptions a
// restarted tenant object no longer has. Placement-neutral: `Session` embeds
// it in a gateway object or in the tenant object.

import { ReplicaBatcher, type BatchSocket } from "./batcher.ts";
import { resetFrame, watermarkFrame } from "./frames.ts";
import type { ReplicaDelivery, SocketWork } from "./protocol.ts";

/** Per subscription id: its path. Kept in the socket record with the tenant instance that serves them. */
export type TrackedReplicas = Record<string, string>;

export interface SocketDeliveryHost extends BatchSocket {
  /** Sends a frame, flushing an open batch first. */
  send(text: string): void;
  /** Ends the connection where Rust's socket task returns (1011, A.1.9). */
  end(): void;
  batchFrames(): boolean;
  /** Last replica watermark sent on this socket since auth. */
  watermark(): number;
  setWatermark(revision: number): void;
  tracked(): TrackedReplicas;
  /** The tenant instance that served this socket's subscriptions ("" before the first delivery). */
  instance(): string;
  setInstance(instance: string): void;
  /** The tracked subscriptions changed (persist the record). */
  trackedChanged(): void;
}

export class SocketDelivery {
  readonly batcher: ReplicaBatcher;

  constructor(private readonly host: SocketDeliveryHost) {
    this.batcher = new ReplicaBatcher(host);
  }

  /** Every `auth`: batch frames as negotiated, bindings forgotten (delivery.rs `set_enabled`, `clear_bindings`). */
  authenticated(batchFrames: boolean): void {
    this.batcher.flush();
    this.batcher.enabled = batchFrames;
    this.batcher.clearBindings();
  }

  /** The client opened a subscription (it is tracked from the request on, so a lost open is reset too). */
  opening(id: string, path: string): void {
    this.host.tracked()[id] = path;
    this.host.trackedChanged();
  }

  /** The client closed a subscription, or the session dropped them all. */
  forget(id?: string): void {
    const tracked = this.host.tracked();
    if (id === undefined) {
      if (!Object.keys(tracked).length) return;
      for (const key of Object.keys(tracked)) delete tracked[key];
    } else {
      if (!(id in tracked)) return;
      delete tracked[id];
    }
    this.host.trackedChanged();
  }

  /** A watermark (monotonic: one at or below the last sent certifies nothing new). */
  watermark(revision: number): void {
    if (revision <= this.host.watermark()) return;
    this.host.setWatermark(revision);
    if (this.host.batchFrames()) this.batcher.watermark(revision);
    else this.host.send(watermarkFrame(revision));
  }

  /** Whole-holder recovery includes opens tracked by the holder but not yet installed in the tenant. */
  recover(instance: string, reason: string): void {
    this.batcher.flush();
    for (const [id, path] of Object.entries(this.host.tracked())) this.host.send(resetFrame(id, path, reason));
    this.forget();
    this.batcher.clearBindings();
    this.host.setInstance(instance);
    this.host.trackedChanged();
  }

  /** One delivery for this socket (`work` is its own part, if any). */
  apply(payload: ReplicaDelivery, work: SocketWork | undefined): void {
    const tracked = this.host.tracked();
    let changed = false;
    const previous = this.host.instance();
    if (previous !== payload.instance) {
      // A restarted tenant object lost its subscriptions and pending opens: the client resumes each one.
      if (previous !== "") {
        for (const [id, path] of Object.entries(tracked)) {
          if (work?.opened?.[id] !== undefined) continue;
          delete tracked[id];
          changed = true;
          this.host.send(resetFrame(id, path, "change-feed-reset"));
        }
      }
      this.host.setInstance(payload.instance);
      changed = true;
    }
    if (work) {
      for (const op of work.ops) {
        switch (op[0]) {
          case "f": this.host.send(op[1]); break;
          case "i": this.batcher.item(payload.shared[op[1]]!); break;
          case "d": this.batcher.delta(op[1], op[2], payload.shared[op[3]]!, op[4]); break;
          case "r": this.batcher.ready(op[1], op[2], op[3]); break;
          case "x": this.host.end(); return;
        }
      }
      for (const [id, path] of Object.entries(work.opened ?? {})) {
        if (tracked[id] === path) continue;
        tracked[id] = path;
        changed = true;
      }
      for (const id of work.removed ?? []) {
        if (id in tracked) {
          delete tracked[id];
          changed = true;
        }
      }
    }
    if (changed) this.host.trackedChanged();
    const revision = work?.watermark ?? payload.watermark;
    if (revision !== undefined) this.watermark(revision);
  }
}
