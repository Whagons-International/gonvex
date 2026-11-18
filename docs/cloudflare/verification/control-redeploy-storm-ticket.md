# Ticket: Control serializes session calls during a redeploy reconnect storm

Status: open, not owned. Raised by X13b (2026-11-18). This note changes no code.

## What happened

On dev (devcopy-stg1117-12 sweep, kernel/runtime 944d8298), a runtime publication and redeploy ran from
about 08:18 to 08:22:50Z. In the minutes after it, `SessionControlObject` (`55b02c`, the only Control
object) answered session calls very slowly:

| Window (UTC) | Control calls over 2 s (Workers Logs) | Examples |
|---|---|---|
| 08:19–08:23 (publication) | several | `admit…,sessionDispatch` 2.4 s; `watch` 6.8 s; `telemetryRecordPerformance` 2.4–4.9 s, cpu 0 |
| 08:23:50–08:25:02 (reconnect storm) | many | `sessionDispatch` 11.2 s, 15.5 s, 16.3 s, 23.4 s, 24.2 s, 29.1 s, all cpu ≤ 3 ms |
| 08:46–09:00 (quiet) | none | – |
| 09:02–09:06 (quiet) | one, 2.4 s | `admit…,sessionDispatch,controlChanged` |

Effect seen in the browser: `/admin/plugins` logged "Query control.developer.status timed out after
20000ms" at 08:24:27Z. The query was sent at about 08:24:07.6Z, into the storm. It did not recur in the
quiet windows, and the rest of the 147-route sweep was clean.

## What is known

- Control is one Durable Object. During the storm every reconnecting socket's control frames
  (`sessionDispatch`), Member admissions (`admit`) and telemetry writes (`telemetryRecordPerformance`) queue
  on it.
- The slow calls burn almost no CPU (0–3 ms against 2–29 s wall). They are waiting, not computing.
- In the same minutes, tenant objects answered `admit` slowly after the redeploy. These were cold starts:
  for example 959 ms wall with 354 ms CPU at 08:04:16, and 2.4 s at 06:08:50. Control's `admit` path waits
  on them.
- Rust has no single Control object. Its control plane is a Postgres connection pool, so one slow tenant
  does not hold every other socket's control call.

## Hypotheses to check (in order)

1. **Control's handlers await tenant RPCs (cold after the redeploy).** Interleaved handlers then hold
   later calls behind input gates or the session-dispatch path. Find out whether `sessionDispatch` for
   `control.developer.status` awaits a tenant `admit` or a runtime probe (`runtimeInstance`, `watch`).
2. **Telemetry writes share the object with session dispatch.** `telemetryRecordPerformance` takes 2–5 s
   with cpu 0, so it is waiting on something, probably storage or a gate. Under a storm these writes
   compete with interactive calls.
3. **Every socket re-authenticates at the same moment.** There is no jitter on reconnect after the
   1012/1011 closes of a publication, so admissions arrive in one burst.

## Done means

- Workers Logs show what each slow Control call waited on. Add a span or log per awaited peer call in
  `sessionDispatch` and `admit`, carrying method names only and no identifiers.
- During a repeated dev publication, interactive control queries (`developer.status`, `publicSettings`,
  `tenants.mine`) answer within 2 s at p95, while the other sockets reconnect.
  - Candidate fixes, each for the lead to choose: move telemetry writes off the Control object, don't
    await a tenant admission inside unrelated control queries, add reconnect jitter on the server close.
- No change to the contract or to Rust-parity answers.

Evidence: Workers Logs queries over the windows above. The X13 live tool is
`/home/gabriel/work/gonvex-x13/live/logs.mjs`, which is read-only and run under
`whagons-dev credentials exec full-cloudflare-access`.
