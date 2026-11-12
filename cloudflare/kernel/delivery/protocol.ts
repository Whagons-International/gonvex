// The delivery interface between the tenant object (subscriptions, feed,
// snapshots) and the object that holds a socket (a gateway by default, or the
// tenant object itself). One payload per commit or open per holder; texts
// shared by several sockets appear once (`shared`) and are referenced by
// index, so a delta is encoded once per visibility class, not per socket.
//
// Versioned and additive (the deploy skew of S3 §7): unknown fields are
// ignored, and a holder that does not know `replica` drops it.

export const REPLICA_DELIVERY_VERSION = 1;

/** One operation on one socket, in order. */
export type SocketOp =
  /** A plain frame (sent after any open batch is flushed). */
  | readonly ["f", string]
  /** A batch data item (a transaction), `shared[index]`. Non-batch sockets never get it. */
  | readonly ["i", number]
  /** A delta item: subscription id, stream token, `shared[index]` (body without its closing brace), the per-socket tail. */
  | readonly ["d", string, string, number, string]
  /** A ready item: subscription id, stream token, the frame addressed to the token. */
  | readonly ["r", string, string, string]
  /** End the connection (Rust returns from the socket loop: subscriptions of two database epochs). */
  | readonly ["x"];

export interface SocketWork {
  readonly ops: readonly SocketOp[];
  /** The commit's watermark for this socket. */
  readonly watermark?: number;
  /** Subscriptions installed by this delivery: id → path (the holder resets them if the tenant instance changes). */
  readonly opened?: Readonly<Record<string, string>>;
  /** Subscriptions the tenant removed (needHashes, error, reset). */
  readonly removed?: readonly string[];
}

export interface ReplicaDelivery {
  readonly v: number;
  /** The tenant object's instance: a new one means its in-memory subscriptions are gone. */
  readonly instance: string;
  readonly shared: readonly string[];
  /** Work per connection id. */
  readonly sockets: Readonly<Record<string, SocketWork>>;
  /** A commit's watermark for every other socket of the tenant on the holder. */
  readonly watermark?: number;
  /** A commit changed these Members' rows (lib.rs `membership_affects`): their sessions are discarded. */
  readonly membership?: { readonly revision: number; readonly members: readonly string[]; readonly accounts: readonly string[] };
}

/** The holder's answer: connections it no longer has (the tenant drops their state). */
export interface ReplicaReceipt {
  readonly v: number;
  readonly sockets: number;
  readonly missing?: readonly string[];
}

/** `replica.batch` timing (delivery.rs `BatchConfig` defaults). */
export const BATCH_TICK_MS = 50;
export const BATCH_IDLE_MS = 1000;
export const BATCH_MAX_BYTES = 512 << 10;
