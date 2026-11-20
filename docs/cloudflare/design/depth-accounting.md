# Depth accounting: launching gateway calls without waiting for a fresh event

Status: design only (2026-11-20). Nothing here is implemented. This is option (c) from the X17 create analysis, written
so Gabriel can weigh it next to (d) "reply before durable" and options A/D/E. Base: `cloudflare/integration` at
`e77a48f6`.

## Summary

| Question | Answer |
|---|---|
| What it removes | The X02 CallOrigin wait. Today, when the newest request a gateway holds is a peer RPC (usually a tenant delivery), every outgoing call waits for the next fresh event. Each wait is one storage alarm. |
| Measured cost today (dev, b29, x18m, SJC) | Every realtime create waits, after the rewrite Action's result: the queued `tasks.create` dispatch 20–27 ms, and the capacity `observe` calls 18–39 ms. The same mechanism also delays `watch` (22–56 ms), `runtimeInstance` (up to 50 ms), `sessionRevalidate` (27 ms) and telemetry (17–39 ms) during loads. |
| Expected saving | **≈20–40 ms per create**, on the critical path of both A's result and B's row. It is not enough on its own to close -04 or -12 create (over Rust_RTT + 300 ms by +174/+307 and +381/+135 ms in b29). It removes a fixed tax from most other gateway calls during busy sessions. |
| What changes | Every gateway↔tenant RPC carries the caller's depth. Both object kinds track the depth of their own newest live request and launch an outgoing call at once when that depth plus one is within a fixed budget. Otherwise the call waits as today. |
| Risk | Medium. The platform's counter is not observable, so the bookkeeping must be conservative. A mistake costs a "Subrequest depth limit exceeded" 1011 close (#529's failure), not data. Rollout is safe in both directions: a missing depth counts as unknown, which keeps today's wait. |
| Effort | ≈1–1.5 engineer-weeks with tests and two review rounds. |

## 1. The rule today (X02)

The platform counts a Durable Object's outgoing call from its **newest in-flight incoming request**, not from the code
that made the call. A finished request stays the newest until another one arrives (`kernel/session/README.md`,
"Subrequest depth"). On 2026-11-17 a gateway that called its tenant while a tenant delivery was its newest request
extended that delivery's chain. The tenant answered with another delivery, and the two objects ratcheted the depth
until the platform refused the call: three 1011 closes and a failed presence leave.

`CallOrigin` (`kernel/session/placement/origin.ts`) fixes this conservatively:
- A call launches at once only while every request that can be the platform's front is a fresh event (a client
  WebSocket event, the router's upgrade, the alarm) or a bounded-depth notice (X17e: Control's alarm sends).
- Otherwise the call waits, in order, for the next fresh event, and the host arms an alarm now. On dev an alarm
  arrives 20–60 ms later.

The rule is correct, but it treats every peer request as unbounded, even though nearly all of them are shallow: the
tenant's delivery RPC usually sits at depth 2 (client event 0 → gateway call 1 → tenant delivery 2).

## 2. Design

### 2.1 Depth on the wire

- **Gateway → tenant.** Every RPC the gateway sends (`dispatch`, `observe`, `connectionReset`, `presenceLeave`,
  replica opens, recovery) carries `depth = front(gateway) + 1`.
- **Tenant → gateway.** Every RPC the tenant sends (`deliverReplicaBatch` and the other delivery/recovery entry
  points, `settleActionReply`, `actionFinished`, `presenceChanged`) carries `depth = front(tenant) + 1`.
- **Other callers.** Control → gateway notices already arrive at a known depth (X17e: sent from Control's alarm,
  depth 1). ActionHost, ActionWorker and controller calls carry the field the same way.
- **Unknown depth.** A missing field means unknown. An unknown-depth request is treated exactly as a peer is today:
  calls launched under it wait.

### 2.2 Front tracking on both object kinds

- `CallOrigin` keeps its entry list (newest alive request first) and adds `depth` per entry:
  - fresh events: 0;
  - notices: 1;
  - RPCs: the carried value, or `Infinity` when unknown.
- `front()` is the depth of the newest entry that can still be the platform's front, by the same liveness rule as
  today's `safe()`. If several entries could be the front, it is the maximum over them.
- A call launches at once if `front() + 1 ≤ DEPTH_BUDGET`. Otherwise it waits, as today.
- `DEPTH_BUDGET` is a conservative constant, proposed 4, well under the platform's limit. That limit is not
  documented as a stable number, so the design must not depend on its exact value.
- The tenant gets the same tracker. Today the tenant has no X02 gate (its deliveries are the peer side). With depth
  accounting it has to know its own front, to stamp `depth` on its deliveries.

### 2.3 What the create path looks like

| Step | Depth today | With accounting |
|---|---|---|
| Client frame (Action) | gateway event 0 | 0 |
| Gateway → tenant `action.call` | 1 | 1 |
| Tenant tool commit → delivery to gateway | 2 (peer, unknown) | 2 (known) |
| Gateway → tenant queued `tasks.create` | waits for the alarm (20–40 ms), then 1 | launches at once: 3 |
| Tenant commit → delivery to gateway | 2 | 4 |
| Next queued call under that delivery | waits | 5 > budget 4: waits, as today |

The ratchet in #529 is bounded by construction: a chain stops one step short of the budget and waits for a fresh event
exactly as it does today.

## 3. Failure modes

| Failure | Effect | Guard |
|---|---|---|
| A path forgets to stamp `depth` | Its requests are unknown: calls under them wait, as today. Lost saving only. | A test per RPC entry point asserts the field is set (lint over the RPC interfaces). |
| A path stamps a depth that is too **low** (a bug) | The real platform depth exceeds our count. The worst case is #529's 1011 close. | `DEPTH_BUDGET` well under the limit. On a `Subrequest depth limit exceeded` error, the call is retried once deferred (as today) and the object stops launching early for N minutes (circuit breaker). Dev spans count early launches and depth refusals. |
| Liveness misjudged (a finished request that is still the front) | Same as above. | The liveness rule is unchanged from today's `safe()`; only the "is this front shallow" check is added. |
| Mixed versions during a rollout | An old peer sends no depth (unknown, so we wait); an old receiver ignores the field. | No flag day needed. |
| Platform changes its depth semantics | Early launches could hit the limit. | Same circuit breaker; spans make it visible. |

No ordering changes: calls still launch in their queue order, one turn earlier. Nothing is forwarded out of order (the
problem that ended #549).

## 4. What it does not do

- It does not remove the hops themselves (gateway → tenant → gateway), the output-gate durability wait (17–106 ms on
  dev), or the post-commit delivery CPU. Those are covered by A, (d) and the variance analysis.
- It does not change the client protocol, Rust-observable frames, or ordering.

## 5. Validation plan if chosen

1. Unit tests for `CallOrigin` depth bookkeeping: fresh, notice, known peer, unknown peer, the liveness rules, and
   the budget edge.
2. A gateway↔tenant ping-pong repro (a dev Worker, as in X17's repro) driven past the budget: it must wait at the
   budget and never hit the platform limit.
3. An n ≥ 5 SJC create check on -04 and -12 against b29's Rust_RTT numbers. Expected: −20 to −40 ms on the create
   medians, and no 1011 closes over a 30-minute busy-session soak.
