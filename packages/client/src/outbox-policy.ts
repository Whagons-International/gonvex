/** Closed queue states. See docs/verification/outbox-delivery.md. */
export type ReducerOutboxState = "pending" | "inflight" | "committed" | "failed" | "rejected";
export type DeliveryAction = "start" | "recover" | "commit" | "reject" | "retry" | "fail-retry" | "fail-park";

/** Positive acceptance is conclusive. Parked work requires explicit retry;
 * stale delivery/recovery/failure bookkeeping cannot re-arm it. Deletion is
 * separate from this transition on an existing record. */
export function deliveryState(state: ReducerOutboxState, action: DeliveryAction): ReducerOutboxState {
  return action === "start"
    ? state === "pending" ? "inflight" : state
    : action === "recover"
      ? state === "inflight" ? "pending" : state
      : action === "commit"
        ? "committed"
        : action === "reject"
          ? state === "committed" ? state : "rejected"
          : action === "retry"
            ? state === "failed" ? "pending" : state === "rejected" ? "pending" : state
            : action === "fail-retry"
              ? state === "inflight" ? "pending" : state
              : state === "pending" ? "failed" : state === "inflight" ? "failed" : state;
}
