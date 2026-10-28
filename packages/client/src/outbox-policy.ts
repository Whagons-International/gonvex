/** Closed queue states. See docs/verification/outbox-delivery.md. */
export type ReducerOutboxState = "pending" | "inflight" | "committed" | "failed" | "rejected";
export type DeliveryAction = "start" | "recover";

/** Delivery/recovery never revives accepted or parked work. Explicit retry,
 * rejection, commit and deletion are separate operations outside this policy. */
export function deliveryState(state: ReducerOutboxState, action: DeliveryAction): ReducerOutboxState {
  return action === "start"
    ? state === "pending" ? "inflight" : state
    : state === "inflight" ? "pending" : state;
}
