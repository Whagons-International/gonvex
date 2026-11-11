# S4: long Actions on Cloudflare

Package S4, stage 0. Code: [`cloudflare/spikes/s4/`](../../../cloudflare/spikes/s4/). Per-run results
(74 runs: outcome, attempts, effects applied, renewal cadence, step measurements, event timeline):
[`cloudflare/spikes/s4/results/summary.json`](../../../cloudflare/spikes/s4/results/summary.json).
Measured 2026-11-11 between 02:00 and 04:00 UTC: locally in workerd (`wrangler dev` 4.153.0), then on
the development account (Workers Paid) as `gonvex-cf-dev-s4-*`. Those resources are deleted.

## Verdict

**Pass. Run long Actions as Workflow steps (runner B).** The `done_when` holds on the platform: the
lease renews every 30 s (60 renewals over a 30-minute run, gaps 29.987–30.021 s), a 5-minute SSE
stream is consumed without a gap, a 64 MB download streams and uploads intact, and no platform limit
was hit on the Workflow path. A 30-minute agent loop, an 800 s request and a 1 GB streamed download
all completed in one step.

The two alternatives fail for long Actions. The Queue consumer (runner A) is killed at 15 minutes on
every attempt, and one long message holds the consumer, so queued Actions behind it waited 5–6.5
minutes to start. The Durable Object alarm runner (runner C) works when it re-arms its alarm every
30 s, but costs about 17–43 times more because a Durable Object is billed for wall-clock time while it
waits; run inside a single alarm call it is killed at 15 minutes like the Queue.

Across all 74 runs, including crashes, lease expiry, duplicate hand-offs, replays and zombies, no
Action's result was applied more than once.

One design rule came out of a bug the platform found (below): the lease that bounds recovery must be
persisted, not kept only in memory.

## What the reference does

Rust runtime at `0bf3ad13`:

- An Action queued by a Reducer is a row in `_gonvex_action_outbox`, written in the Reducer's
  transaction and drained after commit (`rust/crates/runtime/src/execution.rs`,
  `deliver_outbox_action`).
- A claim sets `status='processing'`, `locked_at=now`, `attempts=attempts+1`. A row whose `locked_at`
  is more than 5 minutes old can be claimed again (`rust/crates/store/src/lib.rs`, `claim_outbox`).
- While the Action runs, the drainer renews `locked_at` every 30 s, fenced on `attempts`
  (`renew_action`). A refused renewal drops the delivery and leaves the row for recovery.
- Success deletes the row (`complete_action`), unfenced. Failure resets it to `pending` with
  exponential backoff capped at 15 minutes (`retry_action`). It retries forever.
- Actions have a 15-minute budget (`action_execution_timeout`). `actionProfile: "agent"` Actions
  have none (`rust/crates/module-runtime-v8/src/lib.rs`, `call_budget`).
- Scheduled jobs use the same pattern with a 2-minute lease renewed every 30 s
  (`rust/crates/runtime/src/scheduler.rs`).

What Whagons Actions do with that time (client repo `gonvex/`):

| Pattern | Where | Shape |
| --- | --- | --- |
| LLM stream | agents, OpenRouter | SSE for minutes |
| Recording download | `calling.ts` via `calling/provider.ts` `responseBytes` | up to 64 MB, **buffered** into one `Uint8Array` (chunks plus the copy: about 128 MB at peak), then uploaded |
| Media transcode | `taskVideoProcessing.ts` | one `POST /v1/transcode` with `AbortSignal.timeout(800_000)`; no response until the job is done |
| Sandbox poll | agent tools | polls for up to 80 s |

## What was built

```
driver ─► runner Worker ─► TenantObject (SQLite, one per tenant)
                             │ enqueue(): one transaction writes the caller's row + the outbox row
                             │ after commit, hand-off ─┬─► Queue.send() ──────► queue() consumer       (A)
                             │                         ├─► Workflow.create() ─► step "run action"     (B)
                             │                         └─► RunnerObject.start() ► alarm()             (C)
                             │
          runner ── claim(id, owner) ─────────────────► attempts+1, lease_until = now + 5 min
          runner ── renew(id, attempt) every 30 s ────► fenced on attempt
          runner ── complete(id, attempt, result) ────► one transaction: fence, write effect, mark done
                             │ alarm(): sweep. An expired lease, or a hand-off not claimed within
                             │ one lease, is handed off again.
```

- **A: Queue consumer.** `max_batch_size: 1`, `max_concurrency: 20`. A refused claim (`leased`)
  retries the message with a delay; a failed attempt retries with backoff.
- **B: Workflow.** Step `run action` (10 retries, exponential from 5 s, timeout 6 h) claims, runs
  and returns the result. Step `apply result` calls `complete`. The owner is the instance id, so the
  engine's retry of the same step takes the lease over immediately.
- **C: Durable Object alarm.** One `RunnerObject` per Action. In `heartbeat` mode it starts the
  Action as a detached promise and re-arms its alarm every 30 s while the Action runs; after a reset,
  the next alarm restarts it. In `await` mode the whole Action runs inside one `alarm()` call.
- **Upstream Worker** (stands in for OpenRouter, Telnyx, the media worker and the sandbox):
  - `/sse`: an event every 200 ms.
  - `/slow`: sleeps, then answers.
  - `/download`: deterministic bytes with `Content-Length` set.
  - `/sink`: counts and hashes an upload.
  - `/poll`: a status endpoint that turns done after a deadline.

  It is fetched over the public internet (`global_fetch_strictly_public`) and protected by a bearer
  token. No paid API was called.

The claim protocol keeps the reference's shape: a claim bumps `attempts`, and that number fences
renew and completion. Four parts differ on purpose (intended behaviour from the brief, or forced by
the platform):

1. **Completion is fenced and idempotent.** `complete` applies only for the current attempt, in one
   transaction with the effect. The row stays `done` as a receipt, so a replay is a no-op. Rust deletes
   the row unfenced, so a zombie attempt can complete it there.
2. **A maximum attempt count and a `dead` state**: 8 attempts, as `CONTRACT.md` IB2 fixes. `cancel`
   moves a row to `dead`; the running attempt finds out at its next renewal.
3. **The lease is persisted sparingly.** A renewal extends the lease in memory and writes
   `lease_until` only when less than half the persisted lease is left: about 24 row writes per running
   hour instead of 120. See "A design bug the platform found".
4. **The tenant object recovers lost hand-offs itself.** Its alarm re-dispatches a row that was handed
   off but not claimed within one lease, and a `processing` row whose lease has expired.

## Results

### Long runs (platform, each run on its own tenant unless noted)

| Measurement | B Workflow | C alarm, heartbeat | C alarm, await | A Queue |
| --- | --- | --- | --- | --- |
| 30-minute agent loop (60 SSE turns of 20 s + 10 s pause) | **done**, 1 attempt, 1,801 s, 6,060 events in order | **done**, 1 attempt, 1,801 s | killed at 894–910 s on every attempt; cancelled after 5 | killed at 900–903 s on every attempt; cancelled after 4 |
| Lease renewals in that run | 60, every 29.987–30.021 s (σ 7 ms) | 60, every 29.982–30.016 s | every 30 s until each kill | every 30 s until each kill |
| SSE, 5 min (1,501 events) | done, contiguous, max gap 429 ms | done, max gap 224 ms | — | done, max gap 234 ms |
| 800 s wait on one request (no response headers for 800 s) | **done**, 800.07 s, 26 renewals | done, 800.04 s | — | done, 800.01 s (after a 384 s wait to start, see below) |
| Poll every 2 s for 80 s | done, 41 polls | done, 41 polls | — | done, 41 polls |

No proxy, idle or time-to-first-byte timeout ended the 800 s request: a Worker-to-Worker fetch over
the public internet held a request with no response headers for 800 s.

### Downloads (128 MB isolate)

| Measurement | Result |
| --- | --- |
| 64 MB streamed: hashed in a `TransformStream` and piped into an upload (`FixedLengthStream`) | done on all three runners in 7.6–14.4 s. SHA-256 at source, runner and sink identical. The platform delivers 4 KiB chunks (16,384 of them). |
| 512 MB and 1 GB streamed and hashed, no upload | done: 512 MB in 24 s (Workflow) and 36 s (alarm DO), 1 GB in 126 s (Workflow). That is 8× the isolate limit, so nothing was buffered. |
| 512 MB streamed into an upload | **failed** on every attempt with `Network connection lost` and went `dead` after 8 attempts. Consistent with the documented 100 MB request-body limit (Free/Pro zones) cutting the upload; the 512 MB download alone succeeds. Whagons uploads at most 64 MB. |
| Whagons' buffered `responseBytes`, 64 MB (peak about 128 MB: chunks plus the copy) | **done** on all three runners when run alone, and with 4 Workflow plus 4 Queue copies running at the same time. |
| CPU per streamed download (Workflow analytics) | 64 MB stream + upload: 3.0 s. 512 MB hash: 20.1 s. 1 GB hash: 103.5 s. Buffered 64 MB (native digest): 0.95 s. |

Per-chunk JavaScript (4 KiB chunks through a `TransformStream`) costs 40–100 ms of CPU per MB here, so
streaming more than about 300 MB this way exceeds the default 30 s CPU limit. The spike set
`limits.cpu_ms: 300000`. A native `pipeTo` without a JavaScript transform should cost far less; that was
not measured.

### Crash, retry and lease expiry (platform)

| Event | What happened | Recovery |
| --- | --- | --- |
| Isolate OOM-killed 90 s into attempt 1 (allocate past 128 MB; analytics record `exceededMemory` for all three) | The same owner re-claimed as attempt 2 and ran from the start; result applied once | Queue: redelivered after 0.4 s. Alarm: next heartbeat after 30.1 s. Workflow: engine retry after 37 s. |
| Two bystander Actions on the same runner during that OOM | Unaffected on every runner: attempt 1, applied once. On the Queue they waited 100 and 280 s to start. | — |
| `RunnerObject` reset (`ctx.abort`) 61 s into a run | The heartbeat alarm restarted it 29 s later (attempt 2) | 29 s |
| Tenant object reset 60 s into a Workflow run | No effect: renewals stayed at 30.0 s and the result was applied once | none needed |
| Runner stops renewing at 45 s but keeps running (zombie) | The lease expired at 330 s (last renewal plus 5 min), the sweep re-dispatched, and a new owner claimed within 0.1–1.4 s. At 600 s the zombie's result was **rejected**; the new attempt's result was applied (Workflow, Queue). | 5 min (one lease) |
| Same zombie on runner C | The re-dispatch reached the same `RunnerObject`, which was still running the zombie. Nobody re-claimed, so the zombie's result was applied. Correct, but C recovers a stuck-but-alive runner only if the object is reset. | — |
| Hand-off lost (commit done, `create()`/`send()` never called) | The sweep found the unclaimed row at 300.0 s and handed it off; done 12 s later | 5 min |
| Duplicate hand-off (a second Workflow instance or message) | Second claim refused while leased, then refused as `done`; one effect | — |
| Replay of an applied result, and a stale-attempt completion | `duplicate`, no effect | — |
| `cancel` during a run | The next renewal was refused 5.1 s later and the attempt aborted. Queue and alarm-await churn stopped. | ≤ 30 s |
| Runner Worker redeployed 60 s into four 3-minute Actions (one per runner) | None interrupted: all finished on attempt 1 with 30 s renewals. A busy tenant object served the previous version's code for 10–40 s after an earlier deploy. | — |

### Hand-off and start latency (platform, uncontended)

| Runner | Hand-off call from the tenant object, p50 / max | Enqueue to first claim, p50 / max |
| --- | --- | --- |
| B Workflow `create()` | 740 / 1,214 ms (n = 22) | 1.8 / 3.9 s (n = 7) |
| C `RunnerObject.start()` | 287 / 358 ms (n = 15) | 0.32 / 0.39 s (n = 5) |
| A `Queue.send()` | 294 / 995 ms (n = 17) | 3.5 / 5.3 s (n = 4) |

**Queue head-of-line blocking.** In phase 1, 8 Actions were queued at once next to an SSE run. One
consumer invocation took the first message and held it for 5 minutes. Queues rescales consumer
concurrency only at batch boundaries ([consumer concurrency docs]), so every other message waited:
poll80 started after 303 s, the 64 MB download after 393 s, slow800 after 384 s. Average consumer
concurrency over that window was 0.67. Long Actions cannot share a queue with anything that must start
promptly.

### Platform limits hit

| Limit | Runner | Hit? |
| --- | --- | --- |
| Queue consumer wall time, 15 min | A | yes, every attempt of the 30-minute run (900–903 s) |
| Alarm handler wall time, 15 min | C await | yes (894–910 s) |
| Request body size (upload > 100 MB) | all | yes, 512 MB upload |
| 128 MB isolate | all | only by the injected OOM |
| CPU (default 30 s, raised to 300 s) | all | no. At the default it would be hit beyond about 300 MB streamed through a JavaScript transform. |
| Workflow step wall time | B | no limit hit at 1,801 s |

## A design bug the platform found

The first version kept the lease deadline only in memory, so renewals wrote nothing, and gave every
`processing` row a fresh lease when the tenant object woke. It passed locally. On the platform, the
phase 1 stall runs never expired. Once renewals stopped, the tenant object went idle and was evicted,
and each sweep alarm woke a *new* instance that granted a new grace period. A dead runner would have
held its row forever.

The fix persists `lease_until` at claim and again whenever less than half of it is left. The phase 2
stall runs use the fix and expire on time. A local repro (stall at 20 s, tenant reset at 40 s) shows
the same.

Rule for the kernel: **nothing that bounds recovery may live only in memory**, because the alarm that
checks it is also what wakes a fresh instance.

## Cost

List prices on 2026-11-11, after included allowances:
- **Workers and Workflows:** $0.02 per million CPU-ms, $0.30 per million requests, Workflow steps $8 per
  million.
- **Durable Objects:** $12.50 per million GB-s (billed on 128 MB while active), $0.15 per million
  requests, $1 per million rows written.
- **Queues:** $0.40 per million operations, 3 per message.

Inputs are measured from the GraphQL Analytics API (`scripts/cost.graphql`): Workflow `cpuTime` per
instance, Durable Object `duration` and `activeTime` per object.

| One Action | B Workflow | C alarm, heartbeat | A Queue |
| --- | --- | --- | --- |
| 30-minute agent loop | CPU 2,050 ms ($0.000041) + 2 steps ($0.000016) + about 63 tenant RPCs and 18 rows ($0.00003) ≈ **$0.0001** | RunnerObject 230.5 GB-s ($0.0029) + 60 alarms and their writes ($0.00007) + tenant side ≈ **$0.0030** | cannot finish |
| 800 s wait | CPU 14 ms + 2 steps + tenant ≈ **$0.00003** | 102.4 GB-s ≈ **$0.0013** | ≈ $0.00002, if it can start |
| 5-minute SSE | CPU 492 ms + 2 steps ≈ **$0.00003** | 38.4 GB-s ≈ **$0.0005** | ≈ $0.00001 |

Per 1,000 thirty-minute Actions: about $0.10 on Workflows and $3.00 on alarm runners. Waiting is
nearly free on a Workflow because it bills CPU, and costs 128 MB of duration per second on a Durable
Object.

The tenant object is cheap either way. Renewal RPCs every 30 s do not keep it billed in between: its
active time stayed at 1–13 s per tenant over runs of up to 30 minutes, and 60 tenant objects totalled
17.9 GB-s.

At Whagons' volume all of the Workflow cost falls inside the included 30 M CPU-ms and 500 k steps per
month. The whole spike, about 75 platform Actions, used 145 s of Workflow CPU, 1,176 GB-s of runner
objects (inside the included 400 k GB-s) and 3,242 tenant rows written.

## Recommendation for K12

1. **Run every Action that may exceed a few seconds as a Workflow instance**, one instance per outbox
   attempt chain, created by the tenant object after the commit that wrote the outbox row. Use the
   outbox row id plus a dispatch counter as the instance id. Run the Action in one `step.do` with a
   long timeout. Apply the result in a second `step.do` that calls the tenant object's
   Reducer-like `complete`.
   - The Workflow default step timeout is 10 minutes; set it explicitly. The spike used 6 h; agent
     Actions need a documented ceiling.
   - Raise `limits.cpu_ms` on the Action Worker (the spike used 300,000).
2. **Keep the outbox in the tenant object as the source of truth**, with the reference's claim
   protocol plus:
   - completion fenced on the attempt, applied in the same transaction as its effects, leaving a
     receipt;
   - a persisted `lease_until` (claim, then whenever less than half is left);
   - a sweep alarm that re-dispatches expired leases and unclaimed hand-offs;
   - a maximum attempt count with a `dead` state;
   - `cancel`.

   The Workflow engine's own retries handle crashes quickly (same instance, same owner). The tenant's
   sweep handles everything the engine cannot see: a lost `create()`, a zombie, an engine that gave
   up.
3. **Renew every 30 s with a 5-minute lease**, as the reference does. Measured cost is one RPC and
   about 0.2 rows per renewal. The tenant object is not kept billed between renewals.
4. **Do not use Queues for Action execution.** The 15-minute consumer limit and the head-of-line
   blocking both rule it out. A Queue is still fine for short fan-out work. **Do not use alarm-held
   Durable Objects for waiting**: they cost 17–43 times more than a Workflow for the same Action (measured above) and
   offer nothing B lacks.
5. **Host `ctx.fetch` must stream.** Return the response body as a stream and let uploads take a
   stream (`FixedLengthStream` when the length is known). Whagons' buffered 64 MB `responseBytes`
   worked in every run here, alone and 8 at once, so it does not block the contract. Uploads above
   100 MB fail on this zone plan.
6. **Deploys:** in-flight Workflow steps, queue consumers and alarm runs survived a redeploy, but a
   tenant object can keep the old code for tens of seconds. Kernel deploys must keep the tenant RPC
   interface backward compatible for at least one version. A new RPC method returned
   `RPC receiver does not implement the method` from a not-yet-restarted object.

Open for K12, not measured here: Workflow behaviour on runs of many hours (only 30 minutes was run),
instance creation rate under load (documented at 100 per second per Workflow), and step results near
the 1 MiB limit.

## Conformance scenarios for K12

There is no conformance harness on `cloudflare/integration` yet. These are the scenarios this spike ran,
as presets in `scripts/scenario.mjs`. K12 should port them to the harness, unweakened:

1. An agent Action runs 30 minutes; renewals every 30 s ± 1 s; result applied once.
2. An SSE stream of 5 minutes arrives complete and in order.
3. A 64 MB body streams into an upload with matching SHA-256; a 1 GB body streams without exceeding 128 MB.
4. One request waits 800 s for its response.
5. The runner is killed mid-run; the same owner re-claims as attempt 2; one effect.
6. The runner stops renewing but keeps running; the lease expires after 5 min; a new owner re-claims;
   the zombie's completion is rejected; one effect. Also with the tenant object reset in between.
7. The hand-off after commit is lost; the sweep hands it off after one lease.
8. The hand-off is duplicated; the second claim is refused; one effect.
9. An applied result is replayed, and a stale attempt completes; both are no-ops.
10. An Action is cancelled mid-run; its next renewal is refused; no effect.
11. An Action fails `MAX_ATTEMPTS` times; it ends `dead`.
12. The kernel is redeployed mid-run; the Action finishes or re-claims; one effect.

## Numbers this package moved

- **Bundle:** runner Worker 27.2 KiB (7.8 KiB gzip), upstream Worker about 4 KiB (1.6 KiB gzip),
  from `wrangler deploy`. Spike code only; nothing ships.
- **Tenant object rows per Action,** excluding the spike's `events` instrumentation table:
  - enqueue: 2 rows (the caller's row and the outbox row) plus 1 alarm write;
  - claim: 1;
  - renewal: 1 per ~150 s;
  - completion: 3 (effect, counter, outbox row).

  A 30-minute Action writes about 18 rows and reads 1 row per RPC.
- **Memory:** 64 MB streamed and uploaded and 1 GB streamed and hashed inside a 128 MB isolate.
  Whagons' buffered 64 MB `responseBytes` fits.

## Reproduce

See [`cloudflare/spikes/s4/README.md`](../../../cloudflare/spikes/s4/README.md). Batches:
`scripts/local-batch.sh`, `scripts/platform-phase1.sh`, `scripts/platform-phase2.sh` (`PART=a`, then
`PART=b`), `scripts/redeploy-midrun.sh`. `scripts/summarize.py` folds the raw results into
`summary.json`; `scripts/cost.graphql` is the analytics query.

Caveats:
- Phase 1 ran the first lease design; its stall rows are superseded by phase 2.
- In phase 1, a few runs shared a tenant because of a driver naming bug (since fixed). The
  tenant-crash test reset the tenant that also held the 30-minute Workflow run, and that run still
  passed. `effectsApplied` is per Action and unaffected.
- Local runs do not enforce memory, CPU or 15-minute limits. Editing a source file reloads
  `wrangler dev` and kills in-flight work, so the local stall and crash rows also contain reloads.

[consumer concurrency docs]: https://developers.cloudflare.com/queues/configuration/consumer-concurrency/
