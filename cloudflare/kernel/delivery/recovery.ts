// Structural, additive recovery types during K22/X01 deploy skew. The protocol
// package's matching fields land with X01; no runtime outlet/engine cycle.
import type { ReplicaDelivery, ReplicaReceipt } from "./protocol";
import type { SocketView } from "../session/ports";
import type { SocketWork } from "./protocol";
/** Created by Session only; never taken from a decoded client frame. */
export type RecoverySocketView = SocketView & { readonly queryRecovery: true };
export type ScopedSocketWork = SocketWork & { readonly subscriptionEpoch?: number };
export type RecoveryDelivery = ReplicaDelivery & { readonly recovery?: { readonly reason: "change-feed-lagged" } };
export type RecoveryReceipt = ReplicaReceipt & { readonly recoveryAck?: 1 };
export const recoveryOf = (payload: ReplicaDelivery) => (payload as RecoveryDelivery).recovery;
