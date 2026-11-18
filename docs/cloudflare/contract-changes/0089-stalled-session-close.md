# 0089: a stalled session closes 1012 instead of going silent (X07 BUG-5)

Raised by X07 on 2026-11-18. **Adapt**, additive: a new server close; no frame, field or SDK change.

## Observation

During X03 run5's fixture publications on dev (`858f19e0`, 2026-11-18 00:59–01:05 UTC), the application
sockets of two browser pages stopped answering for about 10 minutes. They got no `auth.result`, no
`auth.error` and no close frame. The SDK's auth watchdog re-sent `auth` every 10 s on the same open socket,
and queued creates were never sent. Only a page reload recovered them
(`gonvex-c3-walkthrough-20261117/bugs/BUG-3…md`, "Reproduced on 858f19e0").

Workers Logs for the holder of one page (DO `c0173f…`) show its last completed client `message` invocation
at 01:01:39. None of the 42 later `auth` frames completed. Its alarms stopped after a `canceled` alarm at
01:04:10. The page's close at reload (01:17:26) still reached the holder. That pattern fits a socket whose
ordered `Session` queue is held by an item that never settles. Every later frame waits behind it, and so do
the host alarm's `liveness()` and the tenant's deliveries. Several awaits on that queue have no time bound:
`tenantDispatchWithDeadline` (it only maps the CPU-reset error), Control `sessionDispatch`, `moduleInfo` in
the per-frame update check, and `revalidate` in delivery recovery. A publication replaces the runtime Worker's
tenants and keeps Control busy for tens of seconds. The specific call that never settled on dev is not
identified: the platform logs only completed invocations.

## Decision

`Session` bounds the item holding its queue at `LIMITS.stallMs` = 60 s. That is above the 30 s tenant CPU
limit and the ~40 s Control RPCs seen during publications, and it equals Rust's presence liveness timeout.
Past the bound, the socket closes with **1012 `session stalled; reconnect`**, and everything queued behind
the item is released (deliveries see "delivery recovery scope changed"). The check runs from three places:

- an in-memory timer armed per item;
- the next inbound frame;
- the host alarm (`nextWake` includes the bound).

The SDK reconnects on any close and re-authenticates. Its queued work then flushes on the new socket. Rust has
no identical exit. It closes stuck or silent sockets by other means: 4000 after 60 s without inbound activity,
and a TCP RST after a 30 s send stall. So the kernel's 1012 is an adapt.

## Not covered

A transport that loses frames in both directions while the holder's queue is healthy needs a client liveness
frame (0017). An old SDK cannot be told apart from an idle one.
