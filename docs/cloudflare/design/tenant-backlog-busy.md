# Design note: bound a tenant's backlog and answer "busy" (gateway-absorbed)

Status: **v1 implemented behind `GONVEX_TENANT_BACKLOG_UNITS`, default off** (see "Implementation v1" at the end). This is option 2 of #552, after option 1 (#553: runtime `cpu_ms` 30000).
Option 3 (per-load CPU) belongs to X17 and F01.

## Problem

A tenant Durable Object can be **saturated**: more work arrives than it can run. On the platform, the calls that
queue at a busy object are delivered as **one invocation with one `cpu_ms` budget**, and that invocation keeps
absorbing calls while the object stays saturated (#552: measured on a throwaway Worker). At `cpu_ms` 5000 a burst of
5–7 concurrent cold -04 loads reset the object (dev, 2026-11-20 14:30:41Z).

#553 raises the budget to 30 s. That moves the cliff; it does not remove it. Saturation also hurts before any reset:
every caller waits behind the whole backlog. The goal is to **keep a tenant's accepted backlog bounded**, so that no
batched invocation can approach the budget and queued work cannot grow without limit. Clients must see no errors,
only a slower first load.

What the measurements say about the work:

| Work | Enters the tenant as | Is the call held open while the work runs? |
| --- | --- | --- |
| `replica.open` / `openMany` | `dispatch` call; the opens are queued in the engine (`connection.pending`) | **No.** `dispatch` returns at once; the engine loop runs the opens later. |
| `query.subscribe` | `dispatch`, then `querySubscribe` through the serial queue (`kernel.reducers.serial`) | **Yes**, until its snapshot ran. |
| `reducer.call` / `action.call` tool calls | `dispatch`, then the serial queue | **Yes**, until it committed. |
| Commit delivery, catch-up | engine loop after a wake | No incoming call. |

Calls held open while the serial queue drains are what batch into a large invocation. Work started after a call
returned is charged to later invocations (#552, E3), but under saturation every invocation is long. So the bound
must cover **both** the engine's pending opens and the serial queue.

## Where the bound lives: the tenant decides, the gateway absorbs

| | Decides "busy" | Holds and retries | Why |
| --- | --- | --- | --- |
| **Tenant** | yes | no | Only the tenant sees its whole backlog: every gateway's opens, the serial queue, and its outlet's holder queues. |
| **Gateway** (per-socket queue, `CallOrigin`-style deferral) | no | **yes** | It already holds frames and resumes them from a fresh event (X02 deferral, alarm wake, `ORIGIN_MAX_WAIT_MS`). The client keeps its request in flight and sees nothing. |
| `PublicLimiterObject` | no | no | It is per client address (public sockets, call rate), not per tenant. It cannot see tenant load. |
| Gateway controller / placement | no | no | It decides sockets per gateway, not work per tenant. |

A gateway-only cap ("at most N in-flight tenant calls per gateway per tenant") is cheap and complements the bound,
because it limits how many calls any one gateway can queue at the tenant. It cannot bound the total, though: a tenant
has sockets on many gateways, and that cap does not know whether the tenant is idle or saturated. Recommended as a
second line, not the mechanism.

## Mechanism

### Tenant side (`kernel/delivery/engine.ts`, `ReducerTenantObject.dispatch`)

1. **Backlog estimate in work units**, never in time. Clocks do not advance during synchronous work in production,
   so any duration measured inside the object reads 0. Each unit is an estimate computed at admission:
   - **Opens:** each pending open costs `cost(path)`: an EWMA of the SQL rows its last opens read (`sqlTotals`
     deltas are already taken per open for spans), with a floor of one statement.
   - **Serial queue:** each queued subscribe or call costs a fixed unit, or `cost(path)` for live queries.
   - **Outlet:** when the holder is `backlogged()`, or its RPCs wait for isolate slots (#550 `waiters`), new opens
     for that holder only grow memory, so it counts as full.
2. **Budget** `BACKLOG_UNITS`, calibrated so that a full backlog is well under the budget at the measured cost per
   unit. From #552, a cold -04 load is 90 opens and 0.7–1 s of CPU. A first budget of about 1.5 s of estimated work
   leaves ample margin even at `cpu_ms` 5000.
3. **Admission:** when accepting a new open or subscribe would exceed the budget, the tenant does not queue it. It
   answers the gateway with `busy { retryAfterMs, units }` for those frame entries. `dispatch` keeps returning at
   once.
   - **Reducer and Action calls are never refused**, because they are user writes with their own idempotency and
     outbox. They are queued as today: few, and short.
   - The existing hard caps stay as the last resort: `MAX_WAITING_OPENS` (4096), `OPEN_BATCH_LIMIT` (256), and the
     authority admission.
4. **No starvation:** `retryAfterMs` grows with the backlog and is jittered by the gateway. A frame refused 3 times
   is admitted on its next try, ahead of fresh opens (an aging lane), so a busy tenant still makes progress for
   everyone.
5. **Skew-safe:** the tenant answers `busy` only to a gateway that advertised it in the `dispatch` call
   (`acceptsBusy: 1`). An older gateway gets today's behavior (queued), so deployment skew never loses frames.

### Gateway side (`kernel/session/session.ts`, `placement/gateway.ts`)

1. **Holding refused entries:** a refused open or subscribe stays in the **socket's ordered queue**. Its later
   tenant frames of the same kind wait behind it, so per-socket order is kept.
   - Reducer and Action calls are not held behind a refused open: they do not depend on it, and the watermark rule
     (`subscriptionFloor`) only counts opens the tenant accepted. Whether that matches Rust's per-socket ordering
     for writes issued during a first load is to be checked against `lib.rs` before implementation.
2. **Resending:** the gateway resends after `retryAfterMs` from a fresh event (alarm), as `CallOrigin` does now. A
   resend is the same frame: same id, args and cursor, so it is idempotent at the tenant (re-opening an id replaces
   the pending one).
3. **Limits:** total hold time is capped by `ORIGIN_MAX_WAIT_MS`. After it, the frame goes to the tenant **without**
   the busy flag, which means accepted. A refused frame is never dropped, and nothing waits forever.

### Client

**No change.** The client keeps its open or subscribe in flight and simply gets the snapshot later. No
`replica.error` (which the SDK retries, but its hooks show as an error until ready) and no `query.error` (thrown to
the UI, not retried). A future capability could let the SDK show "busy" progress, but nothing depends on it.

## Interaction with the #550 outlet

- **Order of gates:** busy happens **before** work enters the engine. The outlet never sees refused work. Its
  per-holder ordering, recovery barriers and catch-up are unchanged.
- **Outlet pressure feeds admission** (step 1, the outlet unit). A holder whose channel is `backlogged`, deferred
  (`behind`), paused for recovery, or waiting for an isolate RPC slot (#550 `waiters`) counts as full. New opens for
  it are refused with busy instead of queueing snapshot pages behind a stalled channel. That also reduces what a
  reset can drop: #550 retirement recovers credits, but the queued output is lost.
- **Reset behavior** is unchanged and still needed: the bound makes resets rare, and #550 makes them survivable.
- **No cross-context work:** gateway resends start from the gateway's own alarm event, and the tenant answers within
  the gateway's call. Nothing is dispatched from another object's context, consistent with #550 round 3.

## Rollout and tests

1. **Metrics first** (no behavior change): the backlog units per tenant and the would-be-busy count in the tenant
   spans, then a dev burst (#552's harness at N = 6 and 10) to calibrate `BACKLOG_UNITS`.
2. **Gateway capability and hold path**, tested in workerd with a stub tenant that answers busy. Order of frames
   behind a held open; resend idempotency; max-wait fallthrough; skew (old gateway, new tenant).
3. **Tenant admission behind a flag.** Tests:
   - a burst of N clients never puts more than `BACKLOG_UNITS` in flight, and every open is eventually answered;
   - Reducer calls are never refused;
   - the aging lane prevents starvation.
4. **Dev measurement:** the -04 burst with the flag on. The largest tenant invocation CPU from Workers Logs must stay
   far under the limit, and the first-load p95 must stay within X17's budget.

## Open questions (for the lead)

- **Budget unit:** SQL rows per path is cheap and available, but snapshot encoding and visibility work are not row
  bound. Is a calibrated constant per open acceptable as a first version?
- **Live-query subscribes:** these are the heaviest single units (~500 ms, #552), so they may deserve their own
  sub-budget.
- **Accepted latency:** first loads under a burst get slower by design, and the bound trades a reset for a wait. A
  cap on the extra wait (e.g. `ORIGIN_MAX_WAIT_MS`) sets the worst case. The value needs a decision.

## Implementation v1 (behind `GONVEX_TENANT_BACKLOG_UNITS`, default off)

**Flag.** The runtime var `GONVEX_TENANT_BACKLOG_UNITS` is the budget in work units. Unset or `0` means off: a
tenant never answers busy, and every open is queued as before. It is not set in any wrangler config. Turn it on per
deployment after the calibration run (rollout step 1).

**What it does:**
- **Tenant (`kernel/delivery/engine.ts`).**
  - **Cost:** an open costs `1 + rows / 100` units. `rows` is the EWMA of the SQL rows that path's opens read
    (`progress.rowsRead`), and 100 before the path was ever measured.
  - **Admission:** for a `replica.open`/`openMany` whose view carries `acceptsBusy`, opens are queued while
    `backlogUnits() + cost <= budget`. The rest come back as one reply,
    `{"type":"gonvex.busy","ids":[...],"retryAfterMs"}`, with `retryAfterMs` between 50 and 1,000.
  - **Outlet pressure:** a holder that is paused, deferred (`behind`) or `backlogged` refuses all of a frame's opens.
  - **Visibility:** a `tenant.busy` span, and a `busyRefusals` counter.
- **Gateway (`kernel/session/session.ts` `forwardOpens`).**
  - Every replica open/openMany dispatch carries `acceptsBusy`, which is harmless while the tenant flag is off. The
    busy reply is stripped in `forward` and never reaches the client.
  - The refused subset is resent after `retryAfterMs` (jittered ±25%, through the session's timers) as an `openMany`
    with its original initial-request refs, aligned by index as the engine matches them.
  - After `BUSY_MAX_ATTEMPTS` (3), or at `BUSY_MAX_WAIT_MS` (**2,000 ms after the first refusal**, the lead's cap),
    whichever comes first, the gateway sends without `acceptsBusy`, and the tenant queues them. No retry wait runs
    past that deadline. This is the aging lane and the max-wait fallthrough in one rule: nothing waits for good.
  - It stops resending if the socket closes, its subscription epoch changes (a reset cleared the opens), or it loses
    its tenant.
- **Shared** (`kernel/delivery/protocol.ts`): `BUSY_FRAME_TYPE`, `BusyReply`, `busyReply()` and the two limits. This
  is a leaf module, so there is no new import edge between the session and the engine.

**v1 differs from the design in three ways, all deliberately:**
1. **Opens only.** Live-query subscribes (`query.subscribe`, the heaviest single units) are not gated yet.
2. **The hold blocks the socket's frame queue.** Refused opens wait inside the socket's ordered turn, so that
   socket's later frames wait behind them, Reducer calls included. That keeps order trivially. The worst case is
   bounded: at most 2,000 ms after the first refusal (plus the final dispatch), well under the 60 s stall close. The design's non-blocking hold, where
   Reducer calls overtake held opens, needs the order check against `lib.rs` first.
3. **Aging is attempt-based at the gateway,** not a tenant-side lane.

**Tests:**
- `kernel/delivery/test/backlog-busy.test.ts`:
  - off by default;
  - the budget split, and busy for a second socket while the backlog is full;
  - never busy without `acceptsBusy`.
- `kernel/session/test/backlog-busy.test.ts`:
  - the refused subset is resent with its initial refs, and the busy reply is never sent to the client;
  - a later Reducer call keeps its order behind the held opens;
  - after 3 attempts the gateway stops advertising `acceptsBusy`;
  - with long retries, it falls through no later than 2,000 ms after the first refusal.

**Not done yet:**
- the calibration run on dev (needs a dev window coordinated with X17);
- subscribes;
- the non-blocking hold.
